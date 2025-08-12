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

-module(erleans_pm).

-feature(maybe_expr, enable).

-behaviour(bondy_mst_crdt).
-behaviour(partisan_gen_server).
-behaviour(partisan_plumtree_broadcast_handler).

-include_lib("kernel/include/logger.hrl").
-include_lib("partisan/include/partisan.hrl").
-include("erleans.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
This module implements the `erleans_pm` server process, the Erleans
grain process registry as a State-based CRDT.

The server state consists of the following elements:
* A set of local monitor references with form
`{pid(), erleans:grain_ref(), reference()}` for every local registration.
This is stored in a protected `ets` set table managed by the
`erleans_table_owner` process to ensure the table survives this
server's crashes.
* A distributed and globally-replicated set of mappings from
`grain_key()` to a single `partisan_remote_ref:p()`.
This is stored on `bondy_mst`.

## Controls
* A grain registers itself and can only do it using its
`erleans:grain_ref()` as name. This is ensured by this server by
calling `erleans_grain:grain_ref()` on the process calling the
function `register_name/0`. There is no provision in the API for a process to
register another process.
* A grain unregisters itself. There is no provision in the API for a process to
unregister another process.

## Events
* A local registered grain `DOWN` signal is received.

## Garbage Collection

* Tombstones are not garbage collected but this is not a major problem as we
use the `grain_ref()` as key for the MST, so the size of the MST is always
bounded to the max number of grains that ever existed. In the near future we
will support tombstone removal.

""").

-define(PERSISTENT_KEY, {?MODULE, tree}).
-define(TREE, persistent_term:get(?PERSISTENT_KEY)).
-define(MONITOR_TAB, erleans_pm_monitor).
-define(TIMEOUT, 15000).

%% This server may receive a huge amount of messages.
%% We make sure that they are stored off heap to avoid excessive GCs.
-define(OPTS, [
    {channel, application:get_env(erleans, partisan_channel, undefined)},
    {spawn_opt, [{message_queue_data, off_heap}]}
]).

-record(state, {
    crdt                    ::  bondy_mst_crdt:t(),
    partisan_channel        ::  partisan:channel(),
    initial_sync = false    ::  boolean()
}).

-type t()                   ::  #state{}.
-type grain_key()           ::  {GrainId :: any(), ImplMod :: module()}.
-type gossip_id()           ::  {
                                    Peer :: bondy_mst_crdt:node_id(),
                                    Root :: bondy_mst:hash()
                                }.
%% API
-export([start_link/0]).
-export([register_name/0]).
-export([register_name/1]).
-export([unregister_name/0]).
-export([whereis_name/1]).
-export([whereis_name/2]).
-export([grain_ref/1]).
-export([to_list/0]).
-export([lookup/1]).
-export([info/0]).

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
    -export([add_/2]).
    -export([remove_/2]).
    -export([register_name_/2]).
    -export([unregister_name_/2]).
    -dialyzer({nowarn_function, register_name_/2}).
-endif.

-dialyzer({nowarn_function, register_name/0}).

-compile({no_auto_import, [monitor/2]}).
-compile({no_auto_import, [monitor/3]}).
-compile({no_auto_import, [demonitor/1]}).
-compile({no_auto_import, [demonitor/2]}).


%% =============================================================================
%% API
%% =============================================================================



?DOC("""
Starts the `erleans_pm` server.
""").
start_link() ->
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, [], ?OPTS).


?DOC("""
Registers the calling process with the `grain_key()` derived from its
`erleans:grain_ref()`.

The duplicate check logic is executed by the caller concurrently while the
actual registration is serialised via the `erleans_pm` server process.

Returns an error with the following reasons:
* `badgrain` if the calling process is not an Erleans grain.
* `timeout` if there was no response from the server within the requested time
* `{already_in_use, partisan_remote_ref:p()}` if there is already a process
registered for the same `grain_key()`.
""").
-spec register_name() ->
    ok
    | {error, badgrain}
    | {error, timeout}
    | {error, noproc}
    | {error, {already_in_use, partisan_remote_ref:p()}}.

register_name() ->
    register_name(?TIMEOUT).


?DOC("""
Registers the calling process with the `grain_key()` derived from its
`erleans:grain_ref()`.

The duplicate check logic is executed by the caller concurrently while the
actual registration is serialised via the `erleans_pm` server process.

Returns an error with the following reasons:
* `badgrain` if the calling process is not an Erleans grain.
* `timeout` if there was no response from the server within the requested time
* `{already_in_use, partisan_remote_ref:p()}` if there is already a process
registered for the same `grain_key()`.
""").
-spec register_name(timeout()) ->
    ok
    | {error, badgrain}
    | {error, timeout}
    | {error, {already_in_use, partisan_remote_ref:p()}}.

register_name(Timeout) ->
    case erleans:grain_ref() of
        undefined ->
            {error, badgrain};

        GrainRef ->
            case lookup_local_pid(GrainRef) of
                undefined ->
                    %% We get all known registrations order by location local <
                    %% node(), and then by node().
                    Processes = lookup(GrainRef),

                    case filter_alive(Processes) of
                        [] ->
                            safe_call(
                                ?MODULE, {register_name, GrainRef}, Timeout
                            );

                        [ProcRef|_] ->
                            %% We found at least one active grain that is
                            %% reachable, so we pick it.
                            {error, {already_in_use, ProcRef}}
                    end;

                Pid when Pid == self() ->
                    %% Idempotent, although it should not happen that the grain
                    %% calls us twice
                    ok;

                Pid when is_pid(Pid) ->
                    %% Anotehr local pid registered this name
                    ProcRef = partisan_remote_ref:from_term(Pid),
                    {error, {already_in_use, ProcRef}}
            end
    end.


?DOC("""
Unregisters a grain. This call fails with `badgrain` if the calling
process is not the original caller to `register_name/0`.

This call is serialised through the `erleans_pm` server process.
""").
-spec unregister_name() -> ok | {error, badgrain}.

unregister_name() ->
    %% Gets the calling process grain_ref
    case erleans:grain_ref() of
        undefined ->
            {error, badgrain};

        GrainRef ->
            partisan_gen_server:call(?MODULE, {unregister_name, GrainRef})
    end.


?DOC("""
Returns a process reference for `GrainRef` unless there is no reference
in which case returns `undefined`. This function calls
`erleans_pm:whereis_name/2` passing the options `[safe]`.

Notice that as we use an eventually consistent model and temporarily support
duplicated activations for a grain reference in different locations we could
have multiple instances in the global registry. This function chooses the
first reference in the list that represents a live process. Checking for
liveness incurs in a remote call for remote processes and thus can be
expensive in the presence of multiple instantiations. If you prefer to avoid
this check you can call `erleans_pm:whereis_name/2` passing [unsafe] as
the second argument.
""").
-spec whereis_name(GrainRef :: erleans:grain_ref()) ->
    partisan_remote_ref:p() | undefined.

whereis_name(GrainRef) ->
    whereis_name(GrainRef, [safe]).


?DOC("""
Returns a process reference for `GrainRef` unless there is no reference
in which case returns `undefined`.
If the option `[safe]` is used it will return the process reference only if
its process is alive. Checking for liveness on remote processes incurs a
remote call. If there is no connection to the node in which the
process lives, it is deemed dead.

If Opts is `[]` or `[unsafe]` the function will not check for liveness.
""").
-spec whereis_name(GrainRef :: erleans:grain_ref(), Opts :: [safe | unsafe]) ->
    partisan_remote_ref:p() | undefined.

whereis_name(#{placement := stateless} = GrainRef, _) ->
    whereis_stateless(GrainRef);

whereis_name(#{placement := {stateless, _}} = GrainRef, _) ->
    whereis_stateless(GrainRef);

whereis_name(GrainRef, []) ->
    whereis_name(GrainRef, [safe]);

whereis_name(GrainRef, [_|T] = L) when T =/= [] ->
    case lists:member(safe, L) of
        true ->
            whereis_name(GrainRef, [safe]);

        false ->
            whereis_name(GrainRef, [unsafe])
    end;

whereis_name(#{id := _} = GrainRef, [Flag]) ->
    case lookup(GrainRef) of
        [] ->
            undefined;

        ProcRefs ->
            pick(ProcRefs, [Flag])
    end.


?DOC("""
Lookups all the registered grains under name `GrainRef` and returns a list of
registered references for the ref.
The list sorts local references first.
""").
-spec lookup(GrainRef :: erleans:grain_ref() | grain_key()) ->
    [partisan_remote_ref:p()].

lookup(#{id := _} = GrainRef) ->
    lookup(grain_key(GrainRef));

lookup({_, _} = GrainKey) ->
    case bondy_mst:get(?TREE, GrainKey) of
        undefined ->
            [];

        AWSet ->
            sort_conflicting_values(AWSet)
    end.


?DOC("""
Returns the `erleans:grain_ref` for a pid or Partisan process reference.
This is more efficient than `erleans_grain:grain_ref` as it doesn't call the
grain process for local grains, which might be busy handling signals, but
instead uses this module's ets table used for monitoring.

In case of a remote reference, this incurs in an RPC to the peer node where the
grain is activated.
""").
-spec grain_ref(partisan:any_pid()) ->
    {ok, erleans:grain_ref()} | {error, timeout | any()}.

grain_ref(Pid) when is_pid(Pid) ->
    %% A local grain so we use the monitor table which is faster
    case monitor_lookup(Pid) of
        {Pid, GrainRef, _} ->
            {ok, GrainRef};

        undefined ->
            {error, not_found}
    end;

grain_ref(ProcRef) ->
    %% We know this is not a pid so it must be a partisan process reference.
    partisan:is_pid(ProcRef) orelse error({badarg, [ProcRef]}),

    case partisan_remote_ref:is_local(ProcRef) of
        true ->
            grain_ref(partisan_remote_ref:to_term(ProcRef));

        false ->
            %% We use RPC cause grain_ref uses ets directly so we avoid blocking
            %% our peer process
            Peer = partisan:node(ProcRef),
            case partisan_rpc:call(Peer, ?MODULE, grain_ref, [ProcRef], 5000) of
                {badrpc, Reason} ->
                    {error, Reason};

                Result ->
                    Result
            end
    end.


?DOC("""
The same as calling `to_list([safe])`.
""").
-spec to_list() -> [{grain_key(), partisan_remote_ref:p()}].

to_list() ->
    to_list([safe]).


?DOC("""
Returns the list of all registry entries.

## Options
* `safe` - returns only entries for grains that are known to be alive
* `unsafe` - returns all entries without checking for liveness.
""").
-spec to_list([safe | unsafe]) -> [{grain_key(), partisan_remote_ref:p()}].

to_list([Flag]) ->
    L = bondy_mst:fold(
        ?TREE,
        fun({GrainKey, Value}, Acc) ->
            case sets:to_list(state_awset:query(Value)) of
                [] ->
                    Acc;

                L ->
                    case pick(L, [Flag]) of
                        undefined ->
                            Acc;

                        ProcRef ->
                            [{GrainKey, ProcRef} | Acc]
                    end
            end
        end,
        []
    ),
    lists:reverse(L).


?DOC("""
Triggers a synchronisation exchange with a peer.
Calls `exchange/2` with an empty map as the second argument.
""").
-spec sync(node()) -> {ok, pid()} | {error, term()}.

sync(Peer) ->
    sync(Peer, #{}).


?DOC("""
Triggers a synchronisation exchange with a peer.
""").
-spec sync(node(), map()) -> ok | {error, term()}.

sync(Peer, Opts) ->
    partisan_gen_server:call(?MODULE, {crdt_trigger, Peer, Opts}).


info() ->
    partisan_gen_server:call(?MODULE, info).



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
Broadcasts message `Gossip` to peers using Plumtree (Epidemis broadcast trees).
""").
broadcast(Gossip) ->
    partisan:broadcast(Gossip, ?MODULE).


?DOC("""
Implementation of the `bondy_mst_crdt` callback.
Removes stale entries and duplicates after merge.
""").
on_merge(Peer) ->
    partisan_gen_server:cast(?MODULE, {crdt_on_merge, Peer}).



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
Deconstructs a broadcast that is sent using `broadcast/2` returning the message
id and payload.

> This function is part of the implementation of the
partisan_plumtree_broadcast_handler behaviour.
> You should never call it directly.
""").
-spec broadcast_data(Gossip :: bondy_mst_crdt:gossip()) ->
    {
        MessageId :: {bondy_mst_crdt:node_id(), bondy_mst:hash()},
        Payload :: bondy_mst_crdt:gossip()
    }.

broadcast_data(Gossip) ->
    #{from := Peer, root := Root} = bondy_mst_crdt:gossip_data(Gossip),
    {{Peer, Root}, Gossip}.


?DOC("""
Implementation of the `partisan_plumtree_backend` callback.
Merges a remote copy of an object record sent via broadcast w/ the
local view for the key contained in the message id. If the remote copy is
causally older than the current data stored then `false` is returned and no
updates are merged. Otherwise, the remote copy is merged (possibly
generating siblings) and `true` is returned.

> This function is part of the implementation of the
partisan_plumtree_broadcast_handler behaviour.
> You should never call it directly.
""").
-spec merge(GossipId :: gossip_id(), Payload :: bondy_mst_crdt:gossip()) ->
    boolean().

merge(_Id, Gossip) ->
    partisan_gen_server:call(?MODULE, {crdt_merge, Gossip}).


?DOC("""
Implementation of the `partisan_plumtree_backend` callback.
Same as merge/2 but merges the object on `Node'

> This function is part of the implementation of the
partisan_plumtree_broadcast_handler behaviour.
> You should never call it directly.
""").
-spec merge(
    Peer :: node(),
    Root :: bondy_mst:hash(),
    Payload :: bondy_mst_crdt:gossip()) -> boolean().

merge(Peer, _Root, Gossip) ->
    partisan_gen_server:call({?MODULE, Peer}, {crdt_merge, Gossip}).


?DOC("""
Implementation of the `partisan_plumtree_backend` callback.
When a peer broadcasts a message it does it to the nodes in its eager-push set
only, but also simultaneously sends I_HAVE notifications to nodes in its
lazy-push set instead of the entire message. This callback is the one that
Plumtree calls when receiving an I_HAVE message.

The main idea is:
“I have seen a broadcast message with this root. If you need it, let me know.”

This saves bandwidth, because instead of blindly sending every neighbor the full
payload, the node sends just the root hash. The lazy neighbors can decide
whether they need the full message or not.

If function returns `true` then Plumtree will do nothing. However,if it returns
`false` then Plumtree will `graft` the message from the peer and send it to us.

> This function is part of the implementation of the
partisan_plumtree_broadcast_handler behaviour.
> You should never call it directly.
""").
-spec is_stale(gossip_id()) -> boolean().

is_stale({Peer, Root}) ->
    %% In our case the I_HAVE message is the root of the peer's tree, so we
    %% always return `true` signaling Plumtree that we do not need the message,
    %% and we send ourself a message to potentially init a merge with the peer
    %% i.e. in this case we take the job of synchonising the CRDT in out hands
    %% instead of relying on Plumtree.
    ok = partisan_gen_server:cast(?MODULE, {crdt_maybe_merge, Peer, Root}),
    true.


?DOC("""
Implementation of the `partisan_plumtree_backend` callback.
In Plumtree this is used to return the object associated with the given prefixed
message id if the currently stored version has an equal context. Otherwise
returning the atom `stale`.

Because it assumes that a grafted context can only be causally older than
the local view, a `stale` response means there is another message that
subsumes the grafted one.

> This function is part of the implementation of the
partisan_plumtree_broadcast_handler behaviour.
> You should never call it directly.
""").
-spec graft(gossip_id()) ->
    stale | {ok, bondy_mst_crdt:gossip()} | {error, term()}.

graft({_Peer, _Root}) ->
    %% In our case, the message_id is just the peer's root hash, so in case
    %% we contain the root we return a Gossip message with our root. Otherwise
    %% we return 'stale'.
    %% partisan_gen_server:call(?MODULE, {crdt_graft, Peer, Root}).
    {error, disabled}.


?DOC("""
Calls `sync/1`.
""").
-spec exchange(node()) -> {ok, pid()} | {error, term()}.

exchange(Peer) ->
    exchange(Peer, #{}).


?DOC("""
Calls `sync/2`.
""").
-spec exchange(node(), map()) -> ok | {error, term()}.

exchange(Peer, Opts) ->
    sync(Peer, Opts).



%% =============================================================================
%% PARTISAN_GEN_SERVER BEHAVIOR CALLBACKS
%% ============================================================================



-spec init(Args :: term()) -> {ok, State :: t()}.

init(_) ->
    %% Trap exists otherwise terminate/1 won't be called when shutdown by
    %% supervisor.
    erlang:process_flag(trap_exit, true),

    %% Create or claim ets table.
    %% If this server crashes, data will be preserved.
    {ok, ?MONITOR_TAB} = erleans_table_owner:add_or_claim(
        ?MONITOR_TAB,
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

    %% We monitor all nodes so that we can cleanup our view of the registry
    partisan:monitor_nodes(true),

    {channel, Channel} = lists:keyfind(channel, 1, partisan_gen:get_opts()),

    %% We wrap the tree using the exchange module
    Node = partisan:node(),
    Opts = #{
        %% MST opts
        hash_algorithm => sha256,
        merger => fun mst_merge_value/3,
        store => bondy_mst_ets_store,
        store_opts => #{
            name => atom_to_binary(?MODULE),
            persistent => true
        },
        %% CRDT opts
        callback_mod => ?MODULE,
        max_merges => 1,
        max_merges_per_root => 1,
        max_versions => 10,
        version_ttl => timer:seconds(30),
        fwd_bcast => false,
        consistency_model => eventual
    },

    %% We create an ets-based MST bound to this process.
    %% The ets table will be garbage collected if this process terminates.
    CRDT = bondy_mst_crdt:new(Node, Opts),
    Tree = bondy_mst_crdt:tree(CRDT),

    %% ets-based trees support read_concurrency (option store_opts.persistent)
    %% so we can cache and share it using persistent_term to avoid a call to
    %% this process.
    ok = persistent_term:put(?PERSISTENT_KEY, Tree),

    State = #state{
        crdt = CRDT,
        partisan_channel = Channel
    },

    {ok, State, {continue, monitor_existing}}.


handle_continue(monitor_existing, State0) ->
    %% This prevents any grain to be registered as we are blocking the server
    %% until we finish.
    %% We fold the claimed ?MONITOR_TAB table to find any existing
    %% registrations. In case the table is new, it would be empty. Otherwise,
    %% we would iterate over registrations that were done by a previous
    %% instance of this server before it crashed.
    %% We re-register/monitor alive pids and remove dead ones.
    Fun = fun
        ({Pid, GrainRef, _OldMRef}, Acc0) ->
            case erlang:is_process_alive(Pid) of
                true ->
                    %% The process is still alive, but the monitor has died with
                    %% the previous instance of this gen_server, so we monitor
                    %% again. We use relaxed mode which allows us to update the
                    %% existing registration on ?MONITOR_TAB and the MST.
                    {_, Acc} = do_register_name(Acc0, GrainRef, Pid, relaxed),
                    Acc;

                false ->
                    %% The process has died, so we unregister. This will also
                    %% remove the registration from the MST.
                    {_, Acc} = do_unregister_name(Acc0, GrainRef, Pid),
                    Acc
            end
    end,
    State = lists:foldl(Fun, State0, ets:tab2list(?MONITOR_TAB)),

    %% We should now have all existing local grains re-registered on this
    %% server and broadcast messages sent to cluster peers.
    {noreply, State};

handle_continue(_, State) ->
    {noreply, State}.

handle_call({register_name, GrainRef}, {Caller, _}, State0)
when is_pid(Caller) ->
    {Reply, State} = do_register_name(State0, GrainRef, Caller),
    {reply, Reply, State};

handle_call({register_name, _}, _From, State) ->
    %% A call from a remote node, not allowed
    {reply, {error, not_local}, State};

handle_call({unregister_name, GrainRef}, {Caller, _}, State0)
when is_pid(Caller) ->
    {Reply, State} = do_unregister_name(State0, GrainRef, Caller),
    {reply, Reply, State};

handle_call({unregister_name, _}, _From, State) ->
    %% A call from a remote node, now allowed
    {reply, {error, not_local}, State};

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

handle_call({crdt_merge, Gossip}, _From, State) ->
    CRDT0 = State#state.crdt,
    Root0 = bondy_mst_crdt:root(CRDT0),
    CRDT = bondy_mst_crdt:handle(CRDT0, Gossip),
    Root = bondy_mst_crdt:root(CRDT),

    %% Required by Plumtree.
    %% Merges a remote copy of an object record sent via broadcast w/ the
    %% local view for the key contained in the message id. If the remote copy is
    %% causally older than the current data stored then `false` is returned and
    %% no updates are merged. Otherwise, the remote copy is merged (possibly
    %% generating siblings) and `true` is returned.
    %% Since we will performing a merge if required during
    %% bondy_mst_crdt:handle/2 we reply `false`.
    Reply = Root =/= Root0,
    {reply, Reply, State#state{crdt = CRDT}};

handle_call({crdt_trigger, Peer, _Opts}, _From, State) ->
    Reply = bondy_mst_crdt:trigger(State#state.crdt, Peer),
    {reply, Reply, State};

handle_call(info, _From, State) ->
    Reply = #{
        tree => #{
            root => bondy_mst_crdt:root(State#state.crdt)
        },
        local_registry => #{
            memory => ets:info(?MONITOR_TAB, memory),
            size => ets:info(?MONITOR_TAB, size)
        }
    },
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.


-spec handle_cast(Request :: term(), State :: t()) ->
    {noreply, NewState :: t()}.

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
    %% Fwd message to bondy_mst_crdt
    CRDT = bondy_mst_crdt:handle(State#state.crdt, Msg),
    {noreply, State#state{crdt = CRDT}};

handle_cast({force_unregister_name, GrainKey, ProcRef}, State0) ->
    %% Internal case to deal with inconsistencies
    case partisan_remote_ref:is_local(ProcRef) of
        true ->
            Pid = partisan_remote_ref:to_pid(ProcRef),
            {_, State} = do_unregister_name(State0, GrainKey, Pid),
            {noreply, State};

        false ->
         {noreply, State0}
    end;

handle_cast(_Request, State) ->
    {noreply, State}.


-spec handle_info(Message :: term(), State :: t()) ->
    {noreply, NewState :: t()}.

handle_info({'ETS-TRANSFER', erleans_pm_monitor, _, []}, State) ->
    {noreply, State};

handle_info({nodedown, Node}, State) ->
    CRDT = bondy_mst_crdt:cancel_merge(State#state.crdt, Node),
    {noreply, State#state{crdt = CRDT}};

handle_info({nodeup, _Node}, State) ->
    {noreply, State};

handle_info({'DOWN', MRef, process, Pid, _Info}, State0) when is_pid(Pid) ->
    %% Registered (monitored) grain exit
    ?LOG_INFO("Grain down ~p", [{Pid, MRef}]),
    {_, State} = do_unregister_process(State0, Pid),
    {noreply, State};

handle_info(Event, State) ->
    ?LOG_INFO("Received unknown event ~p", [Event]),
    {noreply, State}.


-spec terminate(
    Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: t()) -> ok.

terminate(_Reason, State) ->
    ok = unregister_all_local(State),
    _ = persistent_term:erase(?PERSISTENT_KEY),
    ok.



%% =============================================================================
%% PRIVATE
%% =============================================================================


%% @private
safe_call(ServerRef, Cmd) ->
    safe_call(ServerRef, Cmd, ?TIMEOUT).


%% @private
safe_call(ServerRef, Cmd, Timeout) ->
    try
        partisan_gen_server:call(ServerRef, Cmd, Timeout)
    catch
      _:Reason:_ ->
        {error, Reason}
    end.


%% @private
add(#state{} = State, GrainKey, Value) ->
    add(#state{} = State, GrainKey, Value, partisan:node()).


%% @private
add(#state{crdt = CRDT0} = State, Key, Value, Node) ->
    Tree = bondy_mst_crdt:tree(CRDT0),

    AWSet1 =
        case bondy_mst:get(Tree, Key) of
            undefined ->
                state_awset:new();

            AWSet0 ->
                AWSet0
        end,

    {ok, AWSet} = state_type:mutate({add, Value}, Node, AWSet1),
    CRDT = bondy_mst_crdt:put(CRDT0, Key, AWSet),
    State#state{crdt = CRDT}.


%% @private
remove(State, GrainKey, Value) ->
    remove(State, GrainKey, Value, partisan:node()).


%% @private
remove(State, Key, Value, Node) ->
    remove(State, Key, Value, Node, #{}).


%% @private
remove(#state{crdt = CRDT0} = State, Key, Value, Node, Opts) ->
    CRDT = crdt_remove(CRDT0, Key, Value, Node, Opts),
    State#state{crdt = CRDT}.


%% @private
crdt_remove(CRDT, Key, Value, Node, Opts) ->
    Tree = bondy_mst_crdt:tree(CRDT),
    AWSet1 =
        case bondy_mst:get(Tree, Key) of
            undefined ->
                state_awset:new();

            AWSet0 ->
                AWSet0
        end,
    {ok, AWSet} = state_type:mutate({rmv, Value}, Node, AWSet1),
    bondy_mst_crdt:put(CRDT, Key, AWSet, Opts).


%% @private
awset_remove(AWSet0, Value) ->
    {ok, AWSet} = state_type:mutate({rmv, Value}, partisan:node(), AWSet0),
    AWSet.


%% @private
is_monitored(Pid) ->
    monitor_lookup(Pid) =/= undefined.


%% @private
monitor_lookup(Pid) ->
     case ets:lookup(?MONITOR_TAB, Pid) of
        [Monitor] ->
            Monitor;

        _ ->
            undefined
    end.

%% @private
lookup_local_pid(GrainRef) ->
     case ets:match_object(?MONITOR_TAB, {'_', GrainRef, '_'}) of
        [{Pid, GrainRef, _}] ->
            Pid;

        _ ->
            undefined
    end.


%% @private
mst_merge_value(GrainKey, AWSet1, AWSet2) ->
    %% We merge de CRDTs
    AWSet3 = state_awset:merge(AWSet1, AWSet2),
    %% We remove local grains that have been deactivated
    AWSet = remove_deactivated(AWSet3),
    ?LOG_DEBUG(#{
        description => "Merged values",
        key => GrainKey,
        rhs => AWSet1,
        lhs => AWSet2,
        result => AWSet
    }),
    ok = maybe_deactivate_local_duplicate(GrainKey, AWSet),
    AWSet.


%% @private
-spec remove_deactivated(state_awset:state_awset()) ->
    state_awset:state_awset().

remove_deactivated(AWSet) ->
    Fun = fun(ProcRef, Acc) ->
        maybe
            true ?= partisan_remote_ref:is_local(ProcRef),
            Pid ?= partisan_remote_ref:to_pid(ProcRef),
            undefined ?= monitor_lookup(Pid),
            %% Not monitored so it has been deactivated i.e. the peer node has a
            %% stale entry. We remove it from the set.
            ?LOG_DEBUG(#{
                message => "Removing grain from registry",
                process_ref => ProcRef,
                reason => deactivated
            }),
            awset_remove(Acc, ProcRef)
        else
            false ->
                %% Not local, so we ignore it
                Acc;

            {_Pid, _GrainRef, _Mref} ->
                %% Monitored, so we ignore it
                Acc

        end
    end,
    sets:fold(Fun, AWSet, state_awset:query(AWSet)).


%% This function assumes remove_deactivated/2 was called on AWSet before.
maybe_deactivate_local_duplicate(GrainKey, AWSet) ->
    All = sets:to_list(state_awset:query(AWSet)),

    maybe
        %% Partition based on locality
        {[ProcRef], [_ | _] = Remotes} ?=
            lists:partition(fun partisan_remote_ref:is_local/1, All),
        %% We have duplicates, so we need to check if our local duplicate should
        %% belong here.
        false ?= safe_is_location_right(GrainKey, ProcRef),
        %% The grain should not be here, so we will deactivate but only if
        %% we can reach any of the remote duplicates
        true ?= lists:any(fun ?MODULE:is_reachable/1, Remotes),
        %% Since at least one remote grain is reachable, we deactivate the
        %% local one
        deactivate_grain(GrainKey, ProcRef)
    else
        _ ->
            ok
    end.


%% @private
maybe_deactivate_local_duplicates(#state{crdt = CRDT}) ->
    Tree = bondy_mst_crdt:tree(CRDT),
    Fun = fun({Key, AWSet}) -> maybe_deactivate_local_duplicate(Key, AWSet) end,
    bondy_mst:foreach(Tree, Fun).


%% @private
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


%% @private
remove_stale(State, Key, AWSet) ->
    Set = state_awset:query(AWSet),
    Fun = fun(ProcRef, Acc) ->
        maybe
            true ?= partisan_remote_ref:is_local(ProcRef),
            Pid ?= partisan_remote_ref:to_pid(ProcRef),
            undefined ?= monitor_lookup(Pid),
            %% Not monitored so it has been deactivated i.e. the peer node has a
            %% stale entry. We remove it from the set.
            ?LOG_DEBUG(#{
                message => "Removing grain from registry",
                process_ref => ProcRef,
                reason => deactivated
            }),
            %% We disable broadcasting
            remove(Acc, Key, ProcRef, partisan:node(), #{broadcast => false})
        else
            _ ->
                Acc
        end
    end,
    sets:fold(Fun, State, Set).


%% @private
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


%% @private
%% Register the calling process with GrainRef unless another local
%% registration exists.
-spec do_register_name(t(), GrainRef :: erleans:grain_ref(), Pid :: pid()) ->
    {ok, t()} | {{error, {already_in_use, partisan_remote_ref:p()}}, t()}.

do_register_name(State, GrainRef, Pid) ->
    do_register_name(State, GrainRef, Pid, strict).


%% @private
%% Register the calling process with GrainRef unless another local
%% registration exists.
-spec do_register_name(
    t(), GrainRef :: erleans:grain_ref(), Pid :: pid(), strict | relaxed) ->
    {ok, t()} | {{error, {already_in_use, partisan_remote_ref:p()}}, t()}.

do_register_name(State0, GrainRef, Pid, Mode) when is_pid(Pid) ->
    case monitor(GrainRef, Pid, Mode) of
        ok ->
            Key = grain_key(GrainRef),
            Value = partisan_remote_ref:from_term(Pid),
            State = add(State0, Key, Value),
            {ok, State};

        {error, _} = Error ->
            {Error, State0}
    end.


%% Used for testing only (see export of register_name/2)
do_register_name_test(State0, GrainRef, ProcRef) ->
    Key = grain_key(GrainRef),
    State = add(State0, Key, ProcRef, partisan:node(ProcRef)),
    {ok, State}.


%% @private
-spec do_unregister_process(t(), Pid :: pid()) -> {ok, t()}.

do_unregister_process(State0, Pid) when is_pid(Pid) ->
    case monitor_lookup(Pid) of
        {Pid, GrainRef, _} ->
            Key = grain_key(GrainRef),
            do_unregister_name(State0, Key, Pid);
        undefined ->
            {ok, State0}
    end.


%% @private
-spec do_unregister_name(t(), GrainKey :: grain_key(), Pid :: pid()) ->
    {ok, t()}.

do_unregister_name(State0, GrainKey, Pid) when is_pid(Pid) ->
    %% Demonitor
    ok = demonitor(Pid),
    true = ets:delete(?MONITOR_TAB, Pid),

    Value = partisan_remote_ref:from_term(Pid),
    State = remove(State0, GrainKey, Value),
    {ok, State}.


do_unregister_name_test(State0, GrainKey, ProcRef) ->
    State = remove(State0, GrainKey, ProcRef, partisan:node(ProcRef)),
    {ok, State}.


%% @private
grain_key(#{id := Id, implementing_module := Mod}) ->
    {Id, Mod}.


%% @private
monitor(GrainRef, Pid, strict) when is_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),

    case ets:insert_new(?MONITOR_TAB, {Pid, GrainRef, Mref}) of
        true ->
            ok;

        false ->
            true = erlang:demonitor(Mref, [flush]),
            {OtherPid, GrainRef, _} = monitor_lookup(Pid),
            {error, {already_in_use, partisan_remote_ref:from_term(OtherPid)}}
    end;

monitor(GrainRef, Pid, relaxed) when is_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    true = ets:insert(?MONITOR_TAB, {Pid, GrainRef, Mref}),
    ok.


%% @private
demonitor(Pid) ->
    case ets:take(?MONITOR_TAB, Pid) of
        [{Pid, _, Mref}] ->
            true = erlang:demonitor(Mref, [flush]),
            ok;

        [] ->
            ok
    end.


%% @private
-spec deactivate_grain(grain_key(), partisan_remote_ref:t()) -> ok.

deactivate_grain(GrainKey, ProcRef) ->
    %% This call is async (uses a cast) so we are safe to do it
    case erleans_grain:deactivate(ProcRef) of
        ok ->
            ?LOG_NOTICE(#{
                description => "Succeded to deactivate duplicate",
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
            %% This is an inconsistency, we need to cleanup.
            %% We ask the peer to do it, via a private cast (peer can be us)
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


%% @private
whereis_stateless(GrainRef) ->
    case gproc_pool:pick_worker(GrainRef) of
        false ->
            undefined;
        Pid ->
            partisan_remote_ref:from_term(Pid)
    end.


%% @private
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


%% @private
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


%% @private
%% Returns a new list where all the process references are know to be
%% reachable. If the remote check fails, it returns false.
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


%% @private
is_reachable(ProcRef) ->
    try
        partisan:is_connected(partisan:node(ProcRef))
    catch
        _:_ ->
            false
    end.



%% @private
%% Unregisters all local alive processes.
-spec unregister_all_local(t()) -> ok.

unregister_all_local(State) ->
    true = ets:safe_fixtable(?MONITOR_TAB, true),
    try
        unregister_local(State, ets:first(?MONITOR_TAB))
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
        true = ets:safe_fixtable(?MONITOR_TAB, false)
    end.


%% @private
unregister_local(State0, Pid) when is_pid(Pid) ->
    %% {Pid, GrainRef, MRef}
    GrainRef = ets:lookup_element(?MONITOR_TAB, Pid, 2),
    {ok, State} = do_unregister_name(State0, GrainRef, Pid),
    unregister_local(State, ets:next(?MONITOR_TAB, Pid));

%% unregister_local(State, #{id := _} = GrainRef) ->
%%     %% Ignore as we have two entries per registration
%%     %% {Pid, GrainRef} and {GrainRef, Pid}, we just use the first
%%     unregister_local(State, ets:next(?MONITOR_TAB, GrainRef));

unregister_local(_, '$end_of_table') ->
    ok.



%% =============================================================================
%% TEST
%% =============================================================================



-ifdef(TEST).



%% Registers the calling process with the `id` attribute of `GrainRef`.
%% This call is serialised the `erleans_pm` server process.
-spec register_name_(erleans:grain_ref(), partisan_remote_ref:p()) ->
    ok
    | {error, {already_in_use, partisan_remote_ref:p()}}.

register_name_(GrainRef, ProcRef) ->
    partisan_gen_server:call(?MODULE, {register_name_test, GrainRef, ProcRef}).



%% It can only be called by the caller
%% This call is serialised the `erleans_pm` server process.
-spec unregister_name_(erleans:grain_ref(), partisan_remote_ref:p()) ->
    ok | {error, badgrain | not_owner}.

unregister_name_(#{id := _} = GrainRef, ProcRef) ->
    partisan_gen_server:call(
        ?MODULE, {unregister_name_test, GrainRef, ProcRef}
    ).


%% Registers the calling process with the `id` attribute of `GrainRef`.
%% This call is serialised the `erleans_pm` server process.
-spec add_(erleans:grain_ref(), partisan_remote_ref:p()) ->
    ok
    | {error, {already_in_use, partisan_remote_ref:p()}}.

add_(GrainRef, ProcRef) ->
    partisan_gen_server:call(?MODULE, {add_test, GrainRef, ProcRef}).


%% It can only be called by the caller
%% This call is serialised the `erleans_pm` server process.
-spec remove_(erleans:grain_ref(), partisan_remote_ref:p()) ->
    ok | {error, badgrain | not_owner}.

remove_(#{id := _} = GrainRef, ProcRef) ->
    partisan_gen_server:call(
        ?MODULE, {remove_test, GrainRef, ProcRef}
    ).


-endif.



