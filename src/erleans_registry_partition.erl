%% -----------------------------------------------------------------------------
%% Copyright Tristan Sloughter 2019. All Rights Reserved.
%% Copyright Leapsight 2020 - 2023. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% -----------------------------------------------------------------------------

-module(erleans_registry_partition).

-feature(maybe_expr, enable).

-behaviour(bondy_mst_crdt).
-behaviour(partisan_gen_server).
-behaviour(partisan_plumtree_broadcast_handler).

-include_lib("kernel/include/logger.hrl").
-include_lib("partisan/include/partisan.hrl").
-include("erleans.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
This module implements a single partition of the Erleans grain process registry.
Each partition is a State-based CRDT using bondy_mst that manages a subset of grains.

The server state consists of the following elements:
* A set of local monitor references with form
`{pid(), erleans:grain_ref(), reference()}` for every local registration.
This is stored in a protected `ets` set table managed by the
`erleans_table_owner` process to ensure the table survives this
server's crashes.
* A distributed and globally-replicated set of mappings from
`grain_key()` to a single `partisan_remote_ref:p()`.
This is stored on `bondy_mst`.
""").

-define(PERSISTENT_KEY(PartitionId), {?MODULE, tree, PartitionId}).
-define(TREE(PartitionId), persistent_term:get(?PERSISTENT_KEY(PartitionId))).
-define(MONITOR_TAB(PartitionId), list_to_atom("erleans_registry_partition_monitor_" ++ integer_to_list(PartitionId))).
-define(TIMEOUT, 15000).

%% This server may receive a huge amount of messages.
%% We make sure that they are stored off heap to avoid excessive GCs.
-define(OPTS, [
    {channel, application:get_env(erleans, partisan_channel, undefined)},
    {spawn_opt, [{message_queue_data, off_heap}]}
]).

-record(state, {
    partition_id                ::  pos_integer(),
    crdt                        ::  bondy_mst_crdt:t(),
    partisan_channel            ::  partisan:channel(),
    initial_sync = false        ::  boolean()
}).

-type t()                   ::  #state{}.
-type grain_key()           ::  {GrainId :: any(), ImplMod :: module()}.
-type gossip_id()           ::  {
                                    Peer :: bondy_mst_crdt:node_id(),
                                    Root :: bondy_mst:hash()
                                }.

%% API - Partition-specific functions (called by erleans_pm router)
-export([start_link/2]).
-export([register_name/2]).
-export([unregister_name/2]).
-export([whereis_name/2]).
-export([whereis_name/3]).
-export([lookup/2]).
-export([grain_ref/2]).
-export([to_list/1]).
-export([to_list/2]).
-export([info/1]).

%% BONDY_MST_CRDT CALLBACKS
-export([broadcast/1]).
-export([on_merge/1]).
-export([send/2]).
-export([sync/1]).

%% PARTISAN_PLUMTREE_BROADCAST_HANDLER CALLBACKS
-export([broadcast_data/1]).
-export([broadcast_channel/0]).
-export([exchange/1]).
-export([exchange/2]).
-export([graft/1]).
-export([is_stale/1]).
-export([merge/2]).

%% PARTISAN_GEN_SERVER CALLBACKS
-export([init/1]).
-export([handle_continue/2]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).

%% TEST API
-ifdef(TEST).
    -export([add_/3]).
    -export([remove_/3]).
    -export([register_name_/3]).
    -export([unregister_name_/3]).
-endif.

-compile({no_auto_import, [monitor/2]}).
-compile({no_auto_import, [monitor/3]}).
-compile({no_auto_import, [demonitor/1]}).
-compile({no_auto_import, [demonitor/2]}).


%% =============================================================================
%% API
%% =============================================================================

?DOC("""
Starts a partition server with given ID and registers it in gproc_pool.
""").
-spec start_link(pos_integer(), atom()) -> {ok, pid()} | {error, term()}.
start_link(PartitionId, PoolName) ->
    Name = {?MODULE, PartitionId},
    partisan_gen_server:start_link({local, Name}, ?MODULE, [PartitionId, PoolName], ?OPTS).

?DOC("""
Registers a grain in this specific partition.
""").
-spec register_name(pid(), erleans:grain_ref()) ->
    ok | {error, badgrain} | {error, timeout} | {error, {already_in_use, partisan_remote_ref:p()}}.
register_name(PartitionPid, GrainRef) ->
    partisan_gen_server:call(PartitionPid, {register_name, GrainRef}, ?TIMEOUT).

?DOC("""
Unregisters a grain from this specific partition.
""").
-spec unregister_name(pid(), erleans:grain_ref()) -> ok | {error, badgrain}.
unregister_name(PartitionPid, GrainRef) ->
    partisan_gen_server:call(PartitionPid, {unregister_name, GrainRef}).

?DOC("""
Returns a process reference for GrainRef from this partition.
""").
-spec whereis_name(pid(), erleans:grain_ref()) ->
    partisan_remote_ref:p() | undefined.
whereis_name(PartitionPid, GrainRef) ->
    whereis_name(PartitionPid, GrainRef, [safe]).

?DOC("""
Returns a process reference for GrainRef from this partition with options.
""").
-spec whereis_name(pid(), erleans:grain_ref(), [safe | unsafe]) ->
    partisan_remote_ref:p() | undefined.
whereis_name(PartitionPid, GrainRef, Opts) ->
    partisan_gen_server:call(PartitionPid, {whereis_name, GrainRef, Opts}).

?DOC("""
Lookups all registered grains under name GrainRef in this partition.
""").
-spec lookup(pid(), erleans:grain_ref()) -> [partisan_remote_ref:p()].
lookup(PartitionPid, GrainRef) ->
    partisan_gen_server:call(PartitionPid, {lookup, GrainRef}).

?DOC("""
Returns the grain_ref for a process reference from this partition.
""").
-spec grain_ref(pid(), partisan:any_pid()) ->
    {ok, erleans:grain_ref()} | {error, timeout | any()}.
grain_ref(PartitionPid, ProcRef) ->
    partisan_gen_server:call(PartitionPid, {grain_ref, ProcRef}).

?DOC("""
Returns list of all registry entries from this partition.
""").
-spec to_list(pid()) -> [{grain_key(), partisan_remote_ref:p()}].
to_list(PartitionPid) ->
    to_list(PartitionPid, [safe]).

-spec to_list(pid(), [safe | unsafe]) -> [{grain_key(), partisan_remote_ref:p()}].
to_list(PartitionPid, Opts) ->
    partisan_gen_server:call(PartitionPid, {to_list, Opts}).

info(PartitionPid) ->
    partisan_gen_server:call(PartitionPid, info).

%% =============================================================================
%% BONDY_MST_CRDT CALLBACKS
%% =============================================================================

?DOC("""
Implementation of the `bondy_mst_crdt` callback.
Casts message `Message` to this server on node `Peer` using `partisan`.
""").
send(Peer, Message) ->
    partisan_gen_server:cast({?MODULE, Peer}, {crdt_message, Message}).

?DOC("""
Implementation of the `bondy_mst_crdt` callback.
Broadcasts message `Gossip` to peers using Plumtree.
""").
broadcast(Gossip) ->
    partisan:broadcast(Gossip, ?MODULE).

?DOC("""
Implementation of the `bondy_mst_crdt` callback.
Removes stale entries and duplicates after merge.
""").
on_merge(Peer) ->
    partisan_gen_server:cast(self(), {crdt_on_merge, Peer}).

%% =============================================================================
%% PARTISAN_PLUMTREE_BROADCAST_HANDLER CALLBACKS
%% =============================================================================

?DOC("""
Implementation of the `partisan_plumtree_backend` callback.
Returns the channel to be used when broadcasting.
""").
-spec broadcast_channel() -> partisan:channel().
broadcast_channel() ->
    application:get_env(erleans, partisan_broadcast_channel, undefined).

?DOC("""
Implementation of the `partisan_plumtree_backend` callback.
""").
-spec broadcast_data(Gossip :: bondy_mst_crdt:gossip()) ->
    {MessageId :: {bondy_mst_crdt:node_id(), bondy_mst:hash()}, Payload :: bondy_mst_crdt:gossip()}.
broadcast_data(Gossip) ->
    #{from := Peer, root := Root} = bondy_mst_crdt:gossip_data(Gossip),
    {{Peer, Root}, Gossip}.

-spec merge(GossipId :: gossip_id(), Payload :: bondy_mst_crdt:gossip()) -> boolean().
merge(_Id, Gossip) ->
    partisan_gen_server:call(self(), {crdt_merge, Gossip}).

-spec merge(Peer :: node(), Root :: bondy_mst:hash(), Payload :: bondy_mst_crdt:gossip()) -> boolean().
merge(Peer, _Root, Gossip) ->
    partisan_gen_server:call({?MODULE, Peer}, {crdt_merge, Gossip}).

-spec is_stale(gossip_id()) -> boolean().
is_stale({Peer, Root}) ->
    ok = partisan_gen_server:cast(self(), {crdt_maybe_merge, Peer, Root}),
    true.

-spec graft(gossip_id()) -> stale | {ok, bondy_mst_crdt:gossip()} | {error, term()}.
graft({_Peer, _Root}) ->
    {error, disabled}.

-spec exchange(node()) -> {ok, pid()} | {error, term()}.
exchange(Peer) ->
    exchange(Peer, #{}).

-spec exchange(node(), map()) -> ok | {error, term()}.
exchange(Peer, Opts) ->
    sync(Peer, Opts).

?DOC("""
Triggers a synchronisation exchange with a peer.
""").
sync(Peer) ->
    sync(Peer, #{}).

sync(Peer, Opts) ->
    partisan_gen_server:call(self(), {crdt_trigger, Peer, Opts}).

%% =============================================================================
%% PARTISAN_GEN_SERVER BEHAVIOR CALLBACKS
%% =============================================================================

-spec init([pos_integer() | atom()]) -> {ok, State :: t()}.
init([PartitionId, PoolName]) ->
    erlang:process_flag(trap_exit, true),

    MonitorTab = ?MONITOR_TAB(PartitionId),
    {ok, MonitorTab} = erleans_table_owner:add_or_claim(
        MonitorTab,
        [
            set,
            protected,
            named_table,
            {keypos, 1},
            {write_concurrency, true},
            {read_concurrency, true},
            {decentralized_counters, true}
        ]
    ),

    partisan:monitor_nodes(true),

    {channel, Channel} = lists:keyfind(channel, 1, partisan_gen:get_opts()),

    Node = partisan:node(),
    Opts = #{
        hash_algorithm => sha256,
        merger => fun(GrainKey, AWSet1, AWSet2) -> 
            mst_merge_value(PartitionId, GrainKey, AWSet1, AWSet2) 
        end,
        store => bondy_mst_ets_store,
        store_opts => #{
            name => atom_to_binary(list_to_atom("erleans_registry_partition_" ++ integer_to_list(PartitionId))),
            persistent => true
        },
        callback_mod => ?MODULE,
        max_merges => 1,
        max_merges_per_root => 1,
        max_versions => 10,
        version_ttl => timer:seconds(30),
        fwd_bcast => false,
        consistency_model => eventual
    },

    CRDT = bondy_mst_crdt:new(Node, Opts),
    Tree = bondy_mst_crdt:tree(CRDT),

    ok = persistent_term:put(?PERSISTENT_KEY(PartitionId), Tree),

    State = #state{
        partition_id = PartitionId,
        crdt = CRDT,
        partisan_channel = Channel
    },
    
    
    %% Connect to gproc_pool immediately
    case gproc_pool:connect_worker(PoolName, {partition, PartitionId}) of
        true ->
            ?LOG_INFO("Successfully connected partition ~p to pool ~p", [PartitionId, PoolName]);
        Error ->
            ?LOG_ERROR("Failed to connect partition ~p to pool ~p: ~p", [PartitionId, PoolName, Error]),
            error({pool_connection_failed, Error})
    end,
    
    {ok, State, {continue, monitor_existing}}.

handle_continue(monitor_existing, #state{partition_id = PartitionId} = State0) ->
    MonitorTab = ?MONITOR_TAB(PartitionId),
    Fun = fun
        ({Pid, GrainRef, _OldMRef}, Acc0) ->
            case erlang:is_process_alive(Pid) of
                true ->
                    {_, Acc} = do_register_name(Acc0, GrainRef, Pid, relaxed),
                    Acc;
                false ->
                    {_, Acc} = do_unregister_name(Acc0, GrainRef, Pid),
                    Acc
            end
    end,
    State = lists:foldl(Fun, State0, ets:tab2list(MonitorTab)),
    {noreply, State};

handle_continue(_, State) ->
    {noreply, State}.

handle_call({register_name, GrainRef}, {Caller, _}, State0) when is_pid(Caller) ->
    case lookup_local_pid(State0#state.partition_id, GrainRef) of
        undefined ->
            Processes = do_lookup(State0, GrainRef),
            case filter_alive(Processes) of
                [] ->
                    {Reply, State} = do_register_name(State0, GrainRef, Caller),
                    {reply, Reply, State};
                [ProcRef|_] ->
                    {reply, {error, {already_in_use, ProcRef}}, State0}
            end;
        Pid when Pid == Caller ->
            {reply, ok, State0};
        Pid when is_pid(Pid) ->
            ProcRef = partisan_remote_ref:from_term(Pid),
            {reply, {error, {already_in_use, ProcRef}}, State0}
    end;

handle_call({register_name, _}, _From, State) ->
    {reply, {error, not_local}, State};

handle_call({unregister_name, GrainRef}, {Caller, _}, State0) when is_pid(Caller) ->
    {Reply, State} = do_unregister_name(State0, GrainRef, Caller),
    {reply, Reply, State};

handle_call({unregister_name, _}, _From, State) ->
    {reply, {error, not_local}, State};

handle_call({whereis_name, #{placement := stateless} = GrainRef, _}, _From, State) ->
    Reply = whereis_stateless(GrainRef),
    {reply, Reply, State};

handle_call({whereis_name, #{placement := {stateless, _}} = GrainRef, _}, _From, State) ->
    Reply = whereis_stateless(GrainRef),
    {reply, Reply, State};

handle_call({whereis_name, GrainRef, Opts}, _From, State) ->
    case do_lookup(State, GrainRef) of
        [] ->
            {reply, undefined, State};
        ProcRefs ->
            Reply = pick(ProcRefs, Opts),
            {reply, Reply, State}
    end;

handle_call({lookup, GrainRef}, _From, State) ->
    Reply = do_lookup(State, GrainRef),
    {reply, Reply, State};

handle_call({grain_ref, Pid}, _From, State) when is_pid(Pid) ->
    Reply = case monitor_lookup(State#state.partition_id, Pid) of
        {Pid, GrainRef, _} ->
            {ok, GrainRef};
        undefined ->
            {error, not_found}
    end,
    {reply, Reply, State};

handle_call({grain_ref, ProcRef}, _From, State) ->
    Reply = case partisan_remote_ref:is_local(ProcRef) of
        true ->
            {_, Reply0, _} = handle_call({grain_ref, partisan_remote_ref:to_term(ProcRef)}, undefined, State),
            Reply0;
        false ->
            Peer = partisan:node(ProcRef),
            case partisan_rpc:call(Peer, ?MODULE, grain_ref, [self(), ProcRef], 5000) of
                {badrpc, Reason} ->
                    {error, Reason};
                Result ->
                    Result
            end
    end,
    {reply, Reply, State};

handle_call({to_list, Opts}, _From, #state{partition_id = _PartitionId, crdt = CRDT} = State) ->
    Tree = bondy_mst_crdt:tree(CRDT),
    L = bondy_mst:fold(
        Tree,
        fun({GrainKey, Value}, Acc) ->
            case sets:to_list(state_awset:query(Value)) of
                [] ->
                    Acc;
                L ->
                    case pick(L, Opts) of
                        undefined ->
                            Acc;
                        ProcRef ->
                            [{GrainKey, ProcRef} | Acc]
                    end
            end
        end,
        []
    ),
    {reply, lists:reverse(L), State};

handle_call({crdt_merge, Gossip}, _From, State) ->
    CRDT0 = State#state.crdt,
    Root0 = bondy_mst_crdt:root(CRDT0),
    CRDT = bondy_mst_crdt:handle(CRDT0, Gossip),
    Root = bondy_mst_crdt:root(CRDT),
    Reply = Root =/= Root0,
    {reply, Reply, State#state{crdt = CRDT}};

handle_call({crdt_trigger, Peer, _Opts}, _From, State) ->
    Reply = bondy_mst_crdt:trigger(State#state.crdt, Peer),
    {reply, Reply, State};

handle_call(info, _From, #state{partition_id = PartitionId, crdt = CRDT} = State) ->
    MonitorTab = ?MONITOR_TAB(PartitionId),
    Reply = #{
        partition_id => PartitionId,
        tree => #{
            root => bondy_mst_crdt:root(CRDT)
        },
        local_registry => #{
            memory => ets:info(MonitorTab, memory),
            size => ets:info(MonitorTab, size)
        }
    },
    {reply, Reply, State};

%% TEST API - These handle_call clauses are used by the test suite
handle_call({register_name_test, GrainRef, ProcRef}, _From, State0) ->
    %% Used for testing only
    {Reply, State} = do_register_name_test(State0, GrainRef, ProcRef),
    {reply, Reply, State};

handle_call({unregister_name_test, GrainRef, ProcRef}, _From, State0) ->
    %% Used for testing only
    Key = grain_key(GrainRef),
    {Reply, State} = do_unregister_name_test(State0, Key, ProcRef),
    {reply, Reply, State};

handle_call({add_test, GrainRef, ProcRef}, _From, State0) ->
    %% Used for testing only
    Key = grain_key(GrainRef),
    State = add(State0, Key, ProcRef, partisan_remote_ref:node(ProcRef)),
    {reply, ok, State};

handle_call({remove_test, GrainRef, ProcRef}, _From, State0) ->
    %% Used for testing only
    Key = grain_key(GrainRef),
    State = remove(State0, Key, ProcRef, partisan_remote_ref:node(ProcRef)),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

-spec handle_cast(Request :: term(), State :: t()) -> {noreply, NewState :: t()}.
handle_cast({crdt_maybe_merge, Peer, Root}, State) ->
    Root == bondy_mst_crdt:root(State#state.crdt)
        andalso bondy_mst_crdt:trigger(State#state.crdt, Peer),
    {noreply, State};

handle_cast({crdt_on_merge, _Peer}, #state{initial_sync = false} = State0) ->
    State = remove_stale(State0#state{initial_sync = true}),
    ok = maybe_deactivate_local_duplicates(State),
    {noreply, State};

handle_cast({crdt_on_merge, _Peer}, #state{initial_sync = true} = State) ->
    ok = maybe_deactivate_local_duplicates(State),
    {noreply, State};

handle_cast({crdt_message, Msg}, State) ->
    CRDT = bondy_mst_crdt:handle(State#state.crdt, Msg),
    {noreply, State#state{crdt = CRDT}};

handle_cast({force_unregister_name, GrainKey, ProcRef}, State0) ->
    case partisan_remote_ref:is_local(ProcRef) of
        true ->
            Pid = partisan_remote_ref:to_pid(ProcRef),
            {_, State} = do_unregister_name_by_key(State0, GrainKey, Pid),
            {noreply, State};
        false ->
            {noreply, State0}
    end;

handle_cast(_Request, State) ->
    {noreply, State}.

-spec handle_info(Message :: term(), State :: t()) -> {noreply, NewState :: t()}.
handle_info({'ETS-TRANSFER', _, _, []}, State) ->
    {noreply, State};

handle_info({nodedown, Node}, State) ->
    CRDT = bondy_mst_crdt:cancel_merge(State#state.crdt, Node),
    {noreply, State#state{crdt = CRDT}};

handle_info({nodeup, _Node}, State) ->
    {noreply, State};

handle_info({'DOWN', MRef, process, Pid, _Info}, State0) when is_pid(Pid) ->
    ?LOG_INFO("Grain down ~p", [{Pid, MRef}]),
    {_, State} = do_unregister_process(State0, Pid),
    {noreply, State};

handle_info(Event, State) ->
    ?LOG_INFO("Received unknown event ~p", [Event]),
    {noreply, State}.

-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), State :: t()) -> ok.
terminate(_Reason, #state{partition_id = PartitionId} = State) ->
    ok = unregister_all_local(State),
    _ = persistent_term:erase(?PERSISTENT_KEY(PartitionId)),
    ok.

%% =============================================================================
%% PRIVATE
%% =============================================================================


do_lookup(#state{partition_id = PartitionId, crdt = CRDT}, #{id := _} = GrainRef) ->
    do_lookup(#state{partition_id = PartitionId, crdt = CRDT}, grain_key(GrainRef));

do_lookup(#state{partition_id = PartitionId}, {_, _} = GrainKey) ->
    Tree = ?TREE(PartitionId),
    case bondy_mst:get(Tree, GrainKey) of
        undefined ->
            [];
        AWSet ->
            sort_conflicting_values(AWSet)
    end.

add(#state{} = State, GrainKey, Value) ->
    add(#state{} = State, GrainKey, Value, partisan:node()).

add(#state{crdt = CRDT0} = State, Key, Value, Node) ->
    Tree = bondy_mst_crdt:tree(CRDT0),
    AWSet1 = case bondy_mst:get(Tree, Key) of
        undefined ->
            state_awset:new();
        AWSet0 ->
            AWSet0
    end,
    {ok, AWSet} = state_type:mutate({add, Value}, Node, AWSet1),
    CRDT = bondy_mst_crdt:put(CRDT0, Key, AWSet),
    State#state{crdt = CRDT}.

remove(State, GrainKey, Value) ->
    remove(State, GrainKey, Value, partisan:node()).

remove(State, Key, Value, Node) ->
    remove(State, Key, Value, Node, #{}).

remove(#state{crdt = CRDT0} = State, Key, Value, Node, Opts) ->
    CRDT = crdt_remove(CRDT0, Key, Value, Node, Opts),
    State#state{crdt = CRDT}.

crdt_remove(CRDT, Key, Value, Node, Opts) ->
    Tree = bondy_mst_crdt:tree(CRDT),
    AWSet1 = case bondy_mst:get(Tree, Key) of
        undefined ->
            state_awset:new();
        AWSet0 ->
            AWSet0
    end,
    {ok, AWSet} = state_type:mutate({rmv, Value}, Node, AWSet1),
    bondy_mst_crdt:put(CRDT, Key, AWSet, Opts).

monitor_lookup(PartitionId, Pid) ->
    MonitorTab = ?MONITOR_TAB(PartitionId),
    case ets:lookup(MonitorTab, Pid) of
        [Monitor] ->
            Monitor;
        _ ->
            undefined
    end.

lookup_local_pid(PartitionId, GrainRef) ->
    MonitorTab = ?MONITOR_TAB(PartitionId),
    case ets:match_object(MonitorTab, {'_', GrainRef, '_'}) of
        [{Pid, GrainRef, _}] ->
            Pid;
        _ ->
            undefined
    end.

mst_merge_value(PartitionId, GrainKey, AWSet1, AWSet2) ->
    AWSet3 = state_awset:merge(AWSet1, AWSet2),
    AWSet = remove_deactivated(PartitionId, AWSet3),
    ?LOG_DEBUG(#{
        description => "Merged values",
        partition_id => PartitionId,
        key => GrainKey,
        rhs => AWSet1,
        lhs => AWSet2,
        result => AWSet
    }),
    ok = maybe_deactivate_local_duplicate(PartitionId, GrainKey, AWSet),
    AWSet.

remove_deactivated(PartitionId, AWSet) ->
    Fun = fun(ProcRef, Acc) ->
        maybe
            true ?= partisan_remote_ref:is_local(ProcRef),
            Pid ?= partisan_remote_ref:to_pid(ProcRef),
            undefined ?= monitor_lookup(PartitionId, Pid),
            ?LOG_DEBUG(#{
                message => "Removing grain from registry",
                partition_id => PartitionId,
                process_ref => ProcRef,
                reason => deactivated
            }),
            awset_remove(Acc, ProcRef)
        else
            false ->
                Acc;
            {_Pid, _GrainRef, _Mref} ->
                Acc
        end
    end,
    sets:fold(Fun, AWSet, state_awset:query(AWSet)).

awset_remove(AWSet0, Value) ->
    {ok, AWSet} = state_type:mutate({rmv, Value}, partisan:node(), AWSet0),
    AWSet.

maybe_deactivate_local_duplicate(_PartitionId, GrainKey, AWSet) ->
    All = sets:to_list(state_awset:query(AWSet)),
    maybe
        {[ProcRef], [_ | _] = Remotes} ?=
            lists:partition(fun partisan_remote_ref:is_local/1, All),
        false ?= safe_is_location_right(GrainKey, ProcRef),
        true ?= lists:any(fun is_reachable/1, Remotes),
        deactivate_grain(GrainKey, ProcRef)
    else
        _ ->
            ok
    end.

maybe_deactivate_local_duplicates(#state{crdt = CRDT, partition_id = PartitionId}) ->
    Tree = bondy_mst_crdt:tree(CRDT),
    Fun = fun({Key, AWSet}) -> maybe_deactivate_local_duplicate(PartitionId, Key, AWSet) end,
    bondy_mst:foreach(Tree, Fun).

safe_is_location_right({_, Mod}, LocalPRef) ->
    try
        Pid = partisan_remote_ref:to_pid(LocalPRef),
        erleans_grain:is_location_right(Mod, Pid)
    catch
        Class:Reason:Stacktrace ->
            ?LOG_WARNING(#{
                message =>
                    "erleans_grain:is_location_right/2 failed. "
                    "Returning true by default",
                implementing_module => Mod,
                process_ref => LocalPRef,
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            true
    end.

remove_stale(#state{crdt = CRDT} = State) ->
    Tree = bondy_mst_crdt:tree(CRDT),
    Fun = fun({Key, AWSet}, Acc) -> remove_stale(Acc, Key, AWSet) end,
    bondy_mst:fold(Tree, Fun, State).

remove_stale(State, Key, AWSet) ->
    Set = state_awset:query(AWSet),
    Fun = fun(ProcRef, Acc) ->
        maybe
            true ?= partisan_remote_ref:is_local(ProcRef),
            Pid ?= partisan_remote_ref:to_pid(ProcRef),
            undefined ?= monitor_lookup(Acc#state.partition_id, Pid),
            ?LOG_DEBUG(#{
                message => "Removing grain from registry",
                process_ref => ProcRef,
                reason => deactivated
            }),
            remove(Acc, Key, ProcRef, partisan:node(), #{broadcast => false})
        else
            _ ->
                Acc
        end
    end,
    sets:fold(Fun, State, Set).

sort_conflicting_values(AWSet) ->
    Set = state_awset:query(AWSet),
    lists:sort(
        fun(A, B) ->
            Result = {
                partisan_remote_ref:is_local(A),
                partisan_remote_ref:is_local(B)
            },
            case Result of
                {true, _} ->
                    true;
                {_, true} ->
                    false;
                {false, false} ->
                    A =< B
            end
        end,
        sets:to_list(Set)
    ).

do_register_name(State, GrainRef, Pid) ->
    do_register_name(State, GrainRef, Pid, strict).

do_register_name(#state{partition_id = PartitionId} = State0, GrainRef, Pid, Mode) when is_pid(Pid) ->
    case monitor(PartitionId, GrainRef, Pid, Mode) of
        ok ->
            Key = grain_key(GrainRef),
            Value = partisan_remote_ref:from_term(Pid),
            State = add(State0, Key, Value),
            {ok, State};
        {error, _} = Error ->
            {Error, State0}
    end.

do_unregister_process(#state{partition_id = PartitionId} = State0, Pid) when is_pid(Pid) ->
    case monitor_lookup(PartitionId, Pid) of
        {Pid, GrainRef, _} ->
            do_unregister_name(State0, GrainRef, Pid);
        undefined ->
            {ok, State0}
    end.

do_unregister_name(#state{partition_id = PartitionId} = State0, GrainRef, Pid) when is_pid(Pid) ->
    MonitorTab = ?MONITOR_TAB(PartitionId),
    ok = demonitor(MonitorTab, Pid),
    true = ets:delete(MonitorTab, Pid),
    Key = grain_key(GrainRef),
    Value = partisan_remote_ref:from_term(Pid),
    State = remove(State0, Key, Value),
    {ok, State}.

do_unregister_name_by_key(#state{partition_id = PartitionId} = State0, GrainKey, Pid) when is_pid(Pid) ->
    MonitorTab = ?MONITOR_TAB(PartitionId),
    ok = demonitor(MonitorTab, Pid),
    true = ets:delete(MonitorTab, Pid),
    Value = partisan_remote_ref:from_term(Pid),
    State = remove(State0, GrainKey, Value),
    {ok, State}.

grain_key(#{id := Id, implementing_module := Mod}) ->
    {Id, Mod}.

%% @private
do_register_name_test(State0, GrainRef, ProcRef) ->
    Key = grain_key(GrainRef),
    State = add(State0, Key, ProcRef, partisan:node(ProcRef)),
    {ok, State}.

%% @private
do_unregister_name_test(State0, GrainKey, ProcRef) ->
    State = remove(State0, GrainKey, ProcRef, partisan:node(ProcRef)),
    {ok, State}.

monitor(PartitionId, GrainRef, Pid, strict) when is_pid(Pid) ->
    MonitorTab = ?MONITOR_TAB(PartitionId),
    Mref = erlang:monitor(process, Pid),
    case ets:insert_new(MonitorTab, {Pid, GrainRef, Mref}) of
        true ->
            ok;
        false ->
            true = erlang:demonitor(Mref, [flush]),
            {OtherPid, GrainRef, _} = monitor_lookup(PartitionId, Pid),
            {error, {already_in_use, partisan_remote_ref:from_term(OtherPid)}}
    end;

monitor(PartitionId, GrainRef, Pid, relaxed) when is_pid(Pid) ->
    MonitorTab = ?MONITOR_TAB(PartitionId),
    Mref = erlang:monitor(process, Pid),
    true = ets:insert(MonitorTab, {Pid, GrainRef, Mref}),
    ok.

demonitor(MonitorTab, Pid) ->
    case ets:take(MonitorTab, Pid) of
        [{Pid, _, Mref}] ->
            true = erlang:demonitor(Mref, [flush]),
            ok;
        [] ->
            ok
    end.

deactivate_grain(GrainKey, ProcRef) ->
    case erleans_grain:deactivate(ProcRef) of
        ok ->
            ?LOG_NOTICE(#{
                description => "Succeeded to deactivate duplicate",
                grain => GrainKey,
                pid => ProcRef
            }),
            ok;
        {error, Reason} when Reason == not_found; Reason == not_active ->
            ?LOG_ERROR(#{
                description => "Failed to deactivate duplicate",
                grain => GrainKey,
                pid => ProcRef,
                reason => Reason
            }),
            partisan_gen_server:cast(
                {?MODULE, partisan_remote_ref:node(ProcRef)},
                {force_unregister_name, GrainKey, ProcRef}
            );
        {error, Reason} ->
            ?LOG_ERROR(#{
                description => "Failed to deactivate duplicate",
                grain => GrainKey,
                pid => ProcRef,
                reason => Reason
            }),
            ok
    end.

whereis_stateless(GrainRef) ->
    case gproc_pool:pick_worker(GrainRef) of
        false ->
            undefined;
        Pid ->
            partisan_remote_ref:from_term(Pid)
    end.

pick([], _) ->
    undefined;

pick(L, []) ->
    pick(L, [unsafe]);

pick([H], [unsafe]) ->
    H;

pick([H | _], [unsafe]) ->
    H;

pick(List, [safe]) ->
    pick_alive(List).

pick_alive([H | T]) ->
    try partisan:is_process_alive(H) of
        true ->
            H;
        false ->
            pick_alive(T)
    catch
        error:_ ->
            pick_alive(T)
    end;

pick_alive([]) ->
    undefined.

filter_alive(undefined) ->
    [];

filter_alive(ProcRefs) when is_list(ProcRefs) ->
    lists:filter(
        fun(ProcRef) ->
            try
                partisan:is_process_alive(ProcRef)
            catch
                _:_ ->
                    false
            end
        end,
        ProcRefs
    ).

is_reachable(ProcRef) ->
    try
        partisan:is_connected(partisan:node(ProcRef))
    catch
        _:_ ->
            false
    end.

unregister_all_local(#state{partition_id = PartitionId} = State) ->
    MonitorTab = ?MONITOR_TAB(PartitionId),
    true = ets:safe_fixtable(MonitorTab, true),
    try
        unregister_local(State, ets:first(MonitorTab))
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(#{
                message => "Unexpected error",
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ok
    after
        true = ets:safe_fixtable(MonitorTab, false)
    end.

unregister_local(#state{partition_id = PartitionId} = State0, Pid) when is_pid(Pid) ->
    MonitorTab = ?MONITOR_TAB(PartitionId),
    GrainRef = ets:lookup_element(MonitorTab, Pid, 2),
    {ok, State} = do_unregister_name(State0, GrainRef, Pid),
    unregister_local(State, ets:next(MonitorTab, Pid));

unregister_local(_, '$end_of_table') ->
    ok.

%% =============================================================================
%% TEST
%% =============================================================================

-ifdef(TEST).

register_name_(PartitionPid, GrainRef, ProcRef) ->
    partisan_gen_server:call(PartitionPid, {register_name_test, GrainRef, ProcRef}).

unregister_name_(PartitionPid, GrainRef, ProcRef) ->
    partisan_gen_server:call(PartitionPid, {unregister_name_test, GrainRef, ProcRef}).

add_(PartitionPid, GrainRef, ProcRef) ->
    partisan_gen_server:call(PartitionPid, {add_test, GrainRef, ProcRef}).

remove_(PartitionPid, GrainRef, ProcRef) ->
    partisan_gen_server:call(PartitionPid, {remove_test, GrainRef, ProcRef}).

-endif.