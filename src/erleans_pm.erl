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

%% -----------------------------------------------------------------------------
%% @doc This module implements the `erleans_pm' server process, the Erleans
%% grain process registry.
%%
%% The server state consists of the following elements:
%% <ul>
%% <li>
%% A set of local monitor references with form
%% `{pid(), erleans:grain_ref(), reference()}' for every local registration.
%% This is stored in a protected @{link ets} `set' table managed by the
%% {@link erleans_table_owner} process to ensure the table survives this
%% server's crashes.
%% </li>
%% <li>
%% A distributed and globally-replicated set of mappings from
%% {@link grain_key()} to a single {@link partisan_remote_ref:p()}.
%% This is stored on {@link bondy_mst}.
%% </li>
%% <li>
%% A materialised view containing all the registrations (local and
%% remote) for a {@link erleans:grain_ref()}.
%% This is stored in a protected @{link ets} `bag' table.
%% This table is used to resolve lookups and is constructued based on the
%% insertions and deletions that happen on the previous two collections.
%% </li>
%% </ul>
%%
%% == Controls ==
%% <ul>
%% <li>
%% A grain registers itself and can only do it using its
%% {@link erleans:grain_ref()} as name. This is ensured by this server by
%% calling {@link erleans_grain:grain_ref()} on the process calling the
%% function {register_name/0}. There is no provision in the API for a process to
%% register another process.
%% </li>
%% <li>
%% A grain unregisters itself. There is no provision in the API for a process to
%% unregister another process.
%% </li>
%% </ul>
%%
%% == Events ==
%% <ul>
%% <li>
%% A local registered grain `DOWN` signal is received.
%% </li>
%% </ul>
%% @end
%% -----------------------------------------------------------------------------
-module(erleans_pm).
-behaviour(bondy_mst_exchange).
-behaviour(partisan_gen_server).
-behaviour(partisan_plumtree_broadcast_handler).

-include_lib("kernel/include/logger.hrl").
-include_lib("partisan/include/partisan.hrl").
-include("erleans.hrl").

-define(PERSISTENT_KEY, {?MODULE, tree}).
-define(TREE, persistent_term:get(?PERSISTENT_KEY)).

-define(MONITOR_TAB, erleans_pm_monitor).

-define(PDB_PREFIX, {?MODULE, registry}).
-define(TOMBSTONE, '$deleted').


%% This server may receive a huge amount of messages.
%% Make sure that they are stored off heap to avoid excessive GCs.
-define(OPTS, [
    {channel, application:get_env(erleans, partisan_channel, undefined)},
    {spawn_opt, [{message_queue_data, off_heap}]}
]).

-record(state, {
    partisan_channel :: partisan:channel(),
    exchange_state   :: bondy_mst_exchange:t()
}).


-type grain_key() :: {GrainId :: any(), ImplMod :: module()}.

%% API
-export([start_link/0]).
-export([register_name/0]).
-export([unregister_name/0]).
-export([whereis_name/1]).
-export([whereis_name/2]).
-export([grain_ref/1]).
-export([to_list/0]).

%% BONDY_MST_EXCHANGE CALLBACKS
-export([send/2]).
-export([broadcast/1]).
-export([on_merge/1]).

%% PARTISAN_PLUMTREE_BROADCAST_HANLDER CALLBACKS
-export([broadcast_data/1]).
-export([broadcast_channel/0]).
-export([exchange/1]).
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
-export([register_name/2]).
-export([unregister_name/2]).
-dialyzer({nowarn_function, register_name/2}).
-endif.

-dialyzer({nowarn_function, register_name/0}).

-compile({no_auto_import, [monitor/2]}).
-compile({no_auto_import, [monitor/3]}).
-compile({no_auto_import, [demonitor/1]}).
-compile({no_auto_import, [demonitor/2]}).




%% =============================================================================
%% API
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Starts the `erleans_pm' server.
%% @end
%% -----------------------------------------------------------------------------
start_link() ->
    partisan_gen_server:start_link({local, ?MODULE}, ?MODULE, [], ?OPTS).


%% -----------------------------------------------------------------------------
%% @doc Registers the calling process with the `grain_key()' derived from its
%% `erleans:grain_ref()'.
%% This call is serialised via the `erleans_pm' server process.
%%
%% Returns an error with the following reasons:
%% <ul>
%% <li>`{already_in_use, partisan_remote_ref:p()}' if there is already a process
%% registered for the same `grain_key()'.</li>
%% <li>`badgrain' if the calling process is not an {@link erleans_grain}</li>
%% </ul>
%% @end
%% -----------------------------------------------------------------------------
-spec register_name() ->
    ok
    | {error, badgrain}
    | {error, {already_in_use, partisan_remote_ref:p()}}.

register_name() ->
    case erleans:grain_ref() of
        undefined ->
            {error, badgrain};

        GrainRef ->
            partisan_gen_server:call(?MODULE, {register_name, GrainRef})
    end.


%% -----------------------------------------------------------------------------
%% @doc Unregisters a grain. This call fails with `badgrain' if the calling
%% process is not the original caller to {@link register_name/0}.
%% This call is serialised through the `erleans_pm' server process.
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_name() ->
    ok | {error, badgrain | not_owner}.

unregister_name() ->
    GrainRef = erleans:grain_ref(),

    case GrainRef == undefined of
        true ->
            {error, badgrain};

        false ->
            partisan_gen_server:call(?MODULE, {unregister_name, GrainRef})
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns a process reference for `GrainRef' unless there is no reference
%% in which case returns `undefined'. This function calls
%% {@link erleans_pm:whereis_name/2} passing the options `[safe]'.
%%
%% Notice that as we use an eventually consistent model and temporarily support
%% duplicated activations for a grain reference in different locations we could
%% have multiple instances in the global registry. This function chooses the
%% first reference in the list that represents a live process. Checking for
%% liveness incurs in a remote call for remote processes and thus can be
%% expensive in the presence of multiple instanciations. If you prefer to avoid
%% this check you can call {@link erleans_pm:whereis_name/2} passing [unsafe] as
%% the second argument.
%% @end
%% -----------------------------------------------------------------------------
-spec whereis_name(GrainRef :: erleans:grain_ref()) ->
    partisan_remote_ref:p() | undefined.

whereis_name(GrainRef) ->
    whereis_name(GrainRef, [safe]).


%% -----------------------------------------------------------------------------
%% @doc Returns a process reference for `GrainRef' unless there is no reference
%% in which case returns `undefined'.
%% If the option `[safe]` is used it will return the process reference only if
%% its process is alive. Checking for liveness on remote processes incurs a
%% remote call. If there is no connection to the node in which the
%% process lives, it is deemed dead.
%%
%%
%% If Opts is `[]` or `[unsafe]` it will not check for liveness.
%% @end
%% -----------------------------------------------------------------------------
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

        ProcRefs when Flag == safe ->
            safe_pick(ProcRefs, GrainRef);

        [ProcRef|_]  when Flag == unsafe ->
            ProcRef
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the `erleans:grain_ref' for a Pid. This is more efficient than
%% {@link erleans_grain:grain_ref} as it is not calling the grain (which might
%% be busy handling signals) but using this module's ets table.
%% @end
%% -----------------------------------------------------------------------------
-spec grain_ref(partisan:any_pid()) ->
    {ok, erleans:grain_ref()} | {error, timeout | any()}.

grain_ref(Pid) when is_pid(Pid) ->
    case ets:lookup(?MONITOR_TAB, Pid) of
        [] ->
            {error, not_found};

        [{Pid, GrainRef, _}] ->
            {ok, GrainRef}
    end;

grain_ref(ProcRef) ->
    %% Fail if this is not a partisan pid reference
    partisan:is_pid(ProcRef) orelse error({badarg, [ProcRef]}),

    Peer = partisan:node(ProcRef),

    case Peer == partisan:node() of
        true ->
            grain_ref(partisan_remote_ref:to_term(ProcRef));

        false ->
            case partisan_rpc:call(Peer, ?MODULE, grain_ref, [ProcRef], 5000) of
                {badrpc, Reason} ->
                    {error, Reason};

                Result ->
                    Result
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec to_list() -> [{grain_key(), partisan_remote_ref:p()}].

to_list() ->
    L = bondy_mst:fold(
        ?TREE,
        fun({GrainKey, Value}, Acc) ->
            case sets:to_list(state_mvregister:query(Value)) of
                [] ->
                    Acc;

                L ->
                    case safe_pick(L) of
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



%% =============================================================================
%% BONDY_MST_EXCHANGE CALLBACKS
%% =============================================================================



send(Peer, Message) ->
    partisan_gen_server:cast({?MODULE, Peer}, {exchange_message, Message}).


broadcast(Event) ->
    partisan:broadcast(Event, ?MODULE).


on_merge(_Page) ->
    %% We do nothing as we are using the MST as the store itself.
    ok.



%% =============================================================================
%% PARTISAN_PLUMTREE_BROADCAST_HANDLER CALLBACKS
%% =============================================================================



%% -----------------------------------------------------------------------------
%% @doc Returns the channel to be used when broadcasting.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_channel() -> partisan:channel().

broadcast_channel() ->
    application:get_env(erleans, partisan_channel, undefined).


%% -----------------------------------------------------------------------------
%% @doc Deconstructs a broadcast that is sent using
%% `broadcast/2' as the handling module returning the message id
%% and payload.
%%
%% > This function is part of the implementation of the
%% partisan_plumtree_broadcast_handler behaviour.
%% > You should never call it directly.
%% @end
%% -----------------------------------------------------------------------------
-spec broadcast_data(bondy_mst_exchange:event()) ->
    {MessageId :: any(), Payload :: any()}.

broadcast_data(Event) ->
    %% We use the whole event as messageID
    {Event, undefined}.


%% -----------------------------------------------------------------------------
%% @doc Merges a remote copy of an object record sent via broadcast w/ the
%% local view for the key contained in the message id. If the remote copy is
%% causally older than the current data stored then `false' is returned and no
%% updates are merged. Otherwise, the remote copy is merged (possibly
%% generating siblings) and `true' is returned.
%%
%% > This function is part of the implementation of the
%% partisan_plumtree_broadcast_handler behaviour.
%% > You should never call it directly.
%% @end
%% -----------------------------------------------------------------------------
-spec merge(MessageId :: any(), Payload :: any()) -> boolean().

merge(Event, undefined) ->
    partisan_gen_server:call(?MODULE, {exchange_merge, Event}).


%% -----------------------------------------------------------------------------
%% @doc Same as merge/2 but merges the object on `Node'
%%
%% > This function is part of the implementation of the
%% partisan_plumtree_broadcast_handler behaviour.
%% > You should never call it directly.
%% @end
%% -----------------------------------------------------------------------------
-spec merge(Peer :: node(), MessageId :: any(), Payload :: any()) -> boolean().

merge(Peer, Event, undefined) ->
    partisan_gen_server:call({?MODULE, Peer}, {exchange_merge, Event}).


%% -----------------------------------------------------------------------------
%% @doc Determines if the given context (version vector) is causually newer than
%% an existing object. If the object missing or if the context does not represent
%% an anscestor of the current key, false is returned. Otherwise, when the
%% context does represent an ancestor of the existing object or the existing
%% object itself, true is returned.
%%
%%
%% > This function is part of the implementation of the
%% partisan_plumtree_broadcast_handler behaviour.
%% > You should never call it directly.
%% @end
%% -----------------------------------------------------------------------------
-spec is_stale(MessageId :: any()) -> boolean().

is_stale(Event) ->
    {Key, Value1} = bondy_mst_exchange:event_data(Event),

    case bondy_mst:get(?TREE, Key) of
        undefined ->
            false;

        Value0 ->
            %% Checks if Value1 is an inflation of Value0, i.e. Value0 is an
            %% ancestor of Value1
            state_type:is_inflation(Value0, Value1)
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the object associated with the given prefixed key `Pkey' and
%% context `Context' (message id) if the currently stored version has an equal
%% context. Otherwise returns the atom `stale'.
%%
%% Because it assumes that a grafted context can only be causally older than
%% the local view, a `stale' response means there is another message that
%% subsumes the grafted one.
%%
%% > This function is part of the implementation of the
%% partisan_plumtree_broadcast_handler behaviour.
%% > You should never call it directly.
%% @end
%% -----------------------------------------------------------------------------
-spec graft(MessageId :: any()) ->
    stale | {ok, state_mvregister:state_mvregister()} | {error, term()}.

graft({Event, undefined}) ->
    Tree = ?TREE,
    {Key, Value1} = bondy_mst_exchange:event_data(Event),

    case bondy_mst:get(Tree, Key) of
        undefined ->
            %% There would have to be a serious error in implementation to hit
            %% this case.
            %% Catch it here b/c it would be much harder to detect
            {error, {not_found, Key}};

         Value0 ->
            %% when grafting the context will never be causally newer
            %% than what we have locally. Since its not equal,
            %% it must be an ancestor. Thus we've sent another, newer
            %% update that contains this context's information in
            %% addition to its own.  This graft is deemed stale
            case state_type:is_inflation(Value0, Value1) of
                true ->
                    stale;

                false ->
                    {ok, Value0}
            end
    end.


%% -----------------------------------------------------------------------------
%% @doc Triggers an exchange.
%% Calls {@link exchange/2} with an empty map as the second argument.
%% > The exchange is only triggered if the application option `aae_enabled' is
%% set to `true'.
%% @end
%% -----------------------------------------------------------------------------
-spec exchange(node()) -> {ok, pid()} | {error, term()}.

exchange(Peer) ->
    exchange(Peer, #{}).


%% -----------------------------------------------------------------------------
%% @doc Triggers an exchange.
%% @end
%% -----------------------------------------------------------------------------
-spec exchange(node(), map()) -> {ok, pid()} | {error, term()} | ignore.

exchange(Peer, Opts) ->
    case partisan_gen_server:call(?MODULE, {exchange_trigger, Peer, Opts}) of
        ok ->
            %% We handle exchanges outselves so we return ignore
            ignore;

        {error, _} = Error ->
            Error
    end.




%% =============================================================================
%% PARTISAN_GEN_SERVER BEHAVIOR CALLBACKS
%% ============================================================================



-spec init(Args :: term()) -> {ok, State :: term()}.

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

    %% We create an ets-based MST bound to this process.
    %% The ets table will be garbage collected if this process terminates.
    %% ets-based trees support read_concurrency so we can share the it using
    %% persistent_term
    TreeStore = bondy_mst_store:new(
        bondy_mst_ets_store, [{name, ~"erleans_pm"}]
    ),
    Tree = bondy_mst:new(#{store => TreeStore, merger => fun mst_merge/3}),

    ok = persistent_term:put(?PERSISTENT_KEY, Tree),

    %% We wrap the tree using the exchange module
    Node = partisan:node(),
    Opts = #{callback_mod => ?MODULE, max_merges => 3, max_same_merge => 1},
    {ok, ExchangeState} = bondy_mst_exchange:init(Node, Tree, Opts),

    State = #state{
        partisan_channel = Channel,
        exchange_state = ExchangeState
    },

    {ok, State, {continue, monitor_existing}}.


handle_continue(monitor_existing, State) ->
    %% This prevents any grain to be registered as we are blocking the server
    %% until we finish.
    %% We fold the claimed ?MONITOR_TAB table to find any existing registrations.
    %% In case the table is new, it would be empty. Otherwise, we would iterate
    %% over registrations that were registered by a previous instance of this
    %% gen_server before it crashed.
    %% We re-monitor alive pids and remove dead ones.
    Fun = fun
        ({Pid, GrainRef, _OldRef}) ->
            case erlang:is_process_alive(Pid) of
                true ->
                    %% The process is still alive, but the monitor has died with
                    %% the previous instance of this gen_server, so we monitor
                    %% again. We use relaxed mode which allows us to update the
                    %% existing registration on the 3 tables, ?MONITOR_TAB,
                    %% ?VIEW_TAB and ?PDB_PREFIX.
                    ok = do_register_name(GrainRef, Pid, relaxed);
                false ->
                    %% THe process has died, so we unregister. This will also
                    %% remove the registration from the global table (bondy_mst).
                    ok = do_unregister_name(GrainRef, Pid)
            end;

        ({_, _, _, _}) ->
            ok
    end,
    ok = lists:foreach(Fun, ets:tab2list(?MONITOR_TAB)),

    %% We should now have all existing local grains re-registered on this
    %% server and gossip messages sent to cluster peers.
    {noreply, State};

handle_continue(_, State) ->
    {noreply, State}.


handle_call({register_name, GrainRef}, {Caller, _}, State)
when is_pid(Caller) ->
    %% This call can only be made locally, so if Caller is not a pid it would be
    %% a partisan:pid() and thus we will match the fallback clause returning an
    %% error.

    %% We get all known registrations order by location local < node(), and then
    %% by node().
    Processes = lookup(GrainRef),

    %% We then exclude unreachable grains
    Reply = case exclude_unreachable(Processes) of
        [] ->
            %% Nothing registered or all unreachable, so we allow the local
            %% registration
            do_register_name(GrainRef, Caller);

        [ProcRef|_] ->
            %% We found at least one active grain that is reachable, so we pick
            %% it. If there was a local grain registered under GrainRef,
            %% ProcRef would be it (becuase of ordering guarantee).
            {error, {already_in_use, ProcRef}}
    end,

    {reply, Reply, State};

handle_call({register_name, _}, _From, State) ->
    %% A call from a remote node, now allowed
    {reply, {error, not_local}, State};

handle_call({unregister_name, GrainRef}, {Caller, _}, State)
when is_pid(Caller) ->
    Reply = do_unregister_name(GrainRef, Caller),
    {reply, Reply, State};

handle_call({unregister_name, _}, _From, State) ->
    %% A call from a remote node, now allowed
    {reply, {error, not_local}, State};

%% handle_call({register_name_test, GrainRef, ProcRef}, _From, State) ->
%%     %% Only for testing (se export of register_name/2)
%%     %% Add to local materialised view
%%     Node = partisan:node(ProcRef),

%%     Reply =
%%         case Node == partisan:node() of
%%             true ->
%%                 Pid = partisan_remote_ref:to_pid(ProcRef),
%%                 do_register_name(GrainRef, Pid);
%%             false ->
%%                 Key = {Node, grain_key(GrainRef)},
%%                 ok = plum_db:put(?PDB_PREFIX, Key, ProcRef)
%%         end,
%%     {reply, Reply, State};

%% handle_call({unregister_name_test, GrainRef, ProcRef}, _From, State) ->
%%     %% Only for testing (se export of unregister_name/2)
%%     Node = partisan:node(ProcRef),

%%     Reply =
%%         case Node == partisan:node() of
%%             true ->
%%                 do_unregister_name(
%%                     GrainRef, partisan_remote_ref:to_pid(ProcRef)
%%                 );

%%             false ->
%%                 %% We simulate a remote registration
%%                 %% Remove to globally replicated table
%%                 Key = {Node, grain_key(GrainRef)},
%%                 ok = plum_db:delete(?PDB_PREFIX, Key)
%%         end,
%%     {reply, Reply, State};

handle_call({exchange_merge, Event}, _From, State) ->
    ES = bondy_mst_exchange:handle(Event, State#state.exchange_state),
    %% Required by Plumtree, but not sure we need this as bondy_mst.
    %% Merges a remote copy of an object record sent via broadcast w/ the
    %% local view for the key contained in the message id. If the remote copy is
    %% causally older than the current data stored then `false' is returned and
    %% no updates are merged. Otherwise, the remote copy is merged (possibly
    %% generating siblings) and `true' is returned.
    Reply = true,
    {reply, Reply, State#state{exchange_state = ES}};

handle_call({exchange_trigger, Peer, _Opts}, _From, State) ->
    ES = bondy_mst_exchange:trigger(Peer, State#state.exchange_state),
    Reply = ok,
    {reply, Reply, State#state{exchange_state = ES}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


-spec handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()}.

handle_cast({exchange_message, Message}, State) ->
    ES = bondy_mst_exchange:handle(Message, State#state.exchange_state),
    {noreply, State#state{exchange_state = ES}};

handle_cast({force_unregister_name, GrainRef, ProcRef}, State) ->
    %% Internal case to deal with inconsistencies
    case partisan_remote_ref:is_local(ProcRef) of
        true ->
            Pid = partisan_remote_ref:to_pid(ProcRef),
            do_unregister_name(GrainRef, Pid);

        false ->
            ok
    end,
    {noreply, State};

handle_cast(_Request, State) ->
    {noreply, State}.


-spec handle_info(Message :: term(), State :: term()) ->
    {noreply, NewState :: term()}.

handle_info({nodedown, _Node}, State) ->
    {noreply, State};

handle_info({nodeup, _Node}, State) ->
    {noreply, State};

handle_info({'DOWN', MRef, process, Pid, _}, State) ->
    ?LOG_DEBUG("Process down ~p", [{Pid, MRef}]),
    ok = do_unregister_name(Pid),
    {noreply, State};


handle_info(_, State) ->
    {noreply, State}.


-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: term()) ->
    term().

terminate(_Reason, _State) ->
    _ = persistent_term:erase(?PERSISTENT_KEY),
    ok = unregister_all().



%% =============================================================================
%% PRIVATE
%% =============================================================================



mst_merge(_Key, A, B) ->
    state_mvregister:merge(A, B).



sort_conflicting_values(MVRegister) ->
    Set = state_mvregister:query(MVRegister),
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


%% -----------------------------------------------------------------------------
%% @private
%% @doc Register the calling process with GrainRef unless another local
%% registration exists.
%% The call
%% @end
%% -----------------------------------------------------------------------------
-spec do_register_name(GrainRef :: erleans:grain_ref(), Pid :: pid()) ->
    ok | {error, {already_in_use, partisan_remote_ref:p()}}.

do_register_name(GrainRef, Pid) ->
    do_register_name(GrainRef, Pid, strict).


%% -----------------------------------------------------------------------------
%% @private
%% @doc Register the calling process with GrainRef unless another local
%% registration exists.
%% The call
%% @end
%% -----------------------------------------------------------------------------
-spec do_register_name(
    GrainRef :: erleans:grain_ref(), Pid :: pid(), strict | relaxed) ->
    ok | {error, {already_in_use, partisan_remote_ref:p()}}.

do_register_name(GrainRef, Pid, Mode) when is_pid(Pid) ->
    case monitor(GrainRef, Pid, Mode) of
        ok ->
            Key = grain_key(GrainRef),
            ProcRef = partisan_remote_ref:from_term(Pid),
            {ok, Value} = state_type:mutate(
                {set, 0, ProcRef}, partisan:node(), state_mvregister:new()
            ),
            _Tree = bondy_mst:insert(?TREE, Key, Value),
            ok;

        {error, _} = Error ->
            Error
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec do_unregister_name(Pid :: pid()) -> true.

do_unregister_name(Pid) when is_pid(Pid) ->
    case ets:lookup(?MONITOR_TAB, Pid) of
        [{Pid, GrainRef, _}] ->
            ok = do_unregister_name(GrainRef, Pid);
        _ ->
            ok
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec do_unregister_name(GrainRef :: erleans:grain_ref(), Pid :: pid()) -> true.

do_unregister_name(GrainRef, Pid) when is_pid(Pid) ->
    %% Demonitor
    ok = demonitor(Pid),
    true = ets:delete(?MONITOR_TAB, Pid),

    %% Insert TOMBSTONE
    Key = grain_key(GrainRef),

    case bondy_mst:get(?TREE, Key) of
        undefined ->
            %% SHOULD NOT HAPPEN
            ok;
        MVRegister ->
            ProcRef = partisan_remote_ref:from_term(Pid),
            {ok, Value} = state_type:mutate(
                {set, 0, ProcRef}, ?TOMBSTONE, MVRegister
            ),
            _Tree = bondy_mst:insert(?TREE, Key, Value),
            ok
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
grain_key(#{id := Id, implementing_module := Mod}) ->
    {Id, Mod}.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
monitor(GrainRef, Pid, strict) when is_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),

    case ets:insert_new(?MONITOR_TAB, {Pid, GrainRef, Mref}) of
        true ->
            ok;
        false ->
            true = erlang:demonitor(Mref, [flush]),
            [{OtherPid, GrainRef, _}] = ets:lookup(?MONITOR_TAB, Pid),
            {error, {already_in_use, partisan_remote_ref:from_term(OtherPid)}}
    end;

monitor(GrainRef, Pid, relaxed) when is_pid(Pid) ->
    Mref = erlang:monitor(process, Pid),
    true = ets:insert(?MONITOR_TAB, {Pid, GrainRef, Mref}),
    ok.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
demonitor(Pid) ->
    case ets:take(?MONITOR_TAB, Pid) of
        [{Pid, _, Mref}] ->
            true = erlang:demonitor(Mref, [flush]),
            ok;
        [] ->
            ok
    end.



%% -----------------------------------------------------------------------------
%% @private
%% @doc Lookups all the registered grains under name `GrainRef' using the
%% local ets-based materialised view.
%% @end
%% -----------------------------------------------------------------------------
-spec lookup(GrainRef :: erleans:grain_ref() | grain_key()) ->
    [partisan_remote_ref:p()].

lookup(#{id := _} = GrainRef) ->
    lookup(grain_key(GrainRef));

lookup({_, _} = GrainKey) ->
    case bondy_mst:get(?TREE, GrainKey) of
        undefined ->
            [];
        MVRegister ->
            sort_conflicting_values(MVRegister)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
deactivate_grain(GrainRef, ProcRef) ->
    case erleans_grain:deactivate(ProcRef) of
        ok ->
            ?LOG_NOTICE(#{
                description => "Requested duplicate deactivation",
                grain => GrainRef,
                pid => ProcRef
            });

        {error, Reason} when Reason == not_found; Reason == not_active ->
            ?LOG_ERROR(#{
                description => "Failed to deactivate duplicate",
                grain => GrainRef,
                pid => ProcRef,
                reason => Reason
            }),
            %% This is an inconsistency, we need to cleanup.
            %% We ask the peer to do it, via a private cast (peer can be us)
            partisan_gen_server:cast(
                {?MODULE, partisan_remote_ref:node(ProcRef)},
                {force_unregister_name, GrainRef, ProcRef}
            );

        {error, Reason} ->
            ?LOG_ERROR(#{
                description => "Failed to deactivate duplicate",
                grain => GrainRef,
                pid => ProcRef,
                reason => Reason
            }),
            ok
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
whereis_stateless(GrainRef) ->
    case gproc_pool:pick_worker(GrainRef) of
        false ->
            undefined;
        Pid ->
            partisan_remote_ref:from_term(Pid)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
safe_pick(L) ->
    safe_pick(L, undefined).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
safe_pick([], _) ->
    undefined;

safe_pick([ProcRef | Rest], GrainRef) ->
    try is_proc_alive(ProcRef, GrainRef) of
        true ->
            ProcRef;
        false ->
            safe_pick(Rest, GrainRef)
    catch
        error:_ ->
            safe_pick(Rest, GrainRef)
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_proc_alive(partisan_remote_ref:p()) -> boolean() | no_return().

is_proc_alive(ProcRef) ->
    is_proc_alive(ProcRef, undefined).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_proc_alive(partisan_remote_ref:p(), erleans:grain_ref() | undefined) ->
    boolean() | no_return().

is_proc_alive(ProcRef, undefined) ->
    partisan:is_process_alive(ProcRef);

is_proc_alive(ProcRef, GrainRef) ->
    case grain_ref(ProcRef) of
        {ok, GrainRef} ->
            true;
        {ok, _} ->
            %% TODO send a cast to delete this entry!
            false;
        {error, _} ->
            false
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc Returns a new list where all the process references are know to be
%% reachable. A process is reachable if the process is local (and alive
%% according to the existance of a monitor) or is remote and
%% {@link partisan:is_process_alive/1} returns `true' for that process.
%% @end
%% -----------------------------------------------------------------------------
exclude_unreachable(undefined) ->
    [];

exclude_unreachable(ProcRefs) when is_list(ProcRefs) ->
    lists:filter(fun is_reachable/1, ProcRefs).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
is_reachable(ProcRef) ->
    try
        is_proc_alive(ProcRef)
    catch
        _:_ ->
            false
    end.



%% -----------------------------------------------------------------------------
%% @doc Unregisters all local alive processes.
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_all() -> ok.

unregister_all() ->
    true = ets:safe_fixtable(?MONITOR_TAB, true),
    unregister_all(ets:first(?MONITOR_TAB)).

unregister_all(Pid) when is_pid(Pid) ->
    %% {Pid, GrainRef, MRef}
    GrainRef = ets:lookup_element(?MONITOR_TAB, Pid, 2),
    ok = do_unregister_name(GrainRef, Pid),
    unregister_all(ets:next(?MONITOR_TAB, Pid));

unregister_all(#{id := _} = GrainRef) ->
    %% Ignore as we have two entries per registration
    %% {Pid, GrainRef} and {GrainRef, Pid}, we just use the first
    unregister_all(ets:next(?MONITOR_TAB, GrainRef));

unregister_all('$end_of_table') ->
    true = ets:safe_fixtable(?MONITOR_TAB, false),
    ok.



%% =============================================================================
%% TEST
%% =============================================================================



-ifdef(TEST).


%% -----------------------------------------------------------------------------
%% @doc Registers the calling process with the `id' attribute of `GrainRef'.
%% This call is serialised the `erleans_pm' server process.
%% @end
%% -----------------------------------------------------------------------------
-spec register_name(erleans:grain_ref(), partisan_remote_ref:p()) ->
    ok
    | {error, {already_in_use, partisan_remote_ref:p()}}.

register_name(GrainRef, ProcRef) ->
    partisan_gen_server:call(?MODULE, {register_name_test, GrainRef, ProcRef}).


%% -----------------------------------------------------------------------------
%% @doc It can only be called by the caller
%% This call is serialised the `erleans_pm' server process.
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_name(erleans:grain_ref(), partisan_remote_ref:p()) ->
    ok | {error, badgrain | not_owner}.

unregister_name(#{id := _} = GrainRef, ProcRef) ->
    partisan_gen_server:call(
        ?MODULE, {unregister_name_test, GrainRef, ProcRef}
    ).


-endif.



