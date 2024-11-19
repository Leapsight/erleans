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
%% This is stored on {@link plum_db}.
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
-behavior(partisan_gen_server).

-include_lib("kernel/include/logger.hrl").
-include_lib("partisan/include/partisan.hrl").
-include("erleans.hrl").

-define(PDB_PREFIX, {?MODULE, registry}).
-define(TOMBSTONE, '$deleted').

-define(MONITOR_TAB, erleans_pm_monitor).
-define(VIEW_TAB, erleans_pm_view).

%% Record stored on ?VIEW_TAB
-record(reg, {
    key     ::  grain_key(),
    pid     ::  partisan_remote_ref:p(),
    type    ::  local | node() | '_'
}).


%% This server may receive a huge amount of messages.
%% Make sure that they are stored off heap to avoid excessive GCs.
-define(OPTS, [
    {channel, application:get_env(erleans, partisan_channel, undefined)},
    {spawn_opt, [{message_queue_data, off_heap}]}
]).

-record(state, {
    partisan_channel :: partisan:channel()
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

        ProcessRefs when Flag == safe ->
            safe_pick(ProcessRefs, GrainRef);

        [ProcessRef|_]  when Flag == unsafe ->
            ProcessRef
    end.


%% -----------------------------------------------------------------------------
%% @doc Returns the `erleans:grain_ref' for a Pid. This is more efficient than
%% {@link erleans_grain:grain_ref} as it is not calling the grain (which might
%% be busy handling signals) but using this module's ets table.
%% @end
%% -----------------------------------------------------------------------------
-spec grain_ref(partisan:any_pid()) ->
    {ok, erleans:grain_ref()}
    | {error, timeout | any()}.

grain_ref(Process) when is_pid(Process) ->
    case ets:lookup(?MONITOR_TAB, Process) of
        [] ->
            {error, not_found};

        [{Process, GrainRef, _}] ->
            {ok, GrainRef}
    end;

grain_ref(Process) ->
    %% Fail if this si not a partisan pid reference
    partisan:is_pid(Process) orelse error({badarg, [Process]}),

    Node = partisan:node(Process),

    case Node == partisan:node() of
        true ->
            grain_ref(partisan_remote_ref:to_term(Process));

        false ->
            case partisan_rpc:call(Node, ?MODULE, grain_ref, [Process], 5000) of
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
    ets:match(?VIEW_TAB, #reg{key = '$1', pid = '$2', type = '_'}).



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

    %% Materialised view derived from the global store (plum_db table)
    %% In this table we store at most one #reg{type = local} and/or one or more
    %% #reg{type = node()} for a Key where Key grain_key()
    ?VIEW_TAB = ets:new(
        ?VIEW_TAB,
        [
            bag,
            protected,
            named_table,
            {keypos, 2},
            {write_concurrency, true},
            {read_concurrency, true},
            {decentralized_counters, true}
        ]
    ),

    %% Subscribe to plum_db object_update events
    MS = [{
        %% {
        %%    {{?MODULE, registry}, {node(), grain_key()} = Key},
        %%    NewObj,
        %%    ExistingObj
        %% }
        {{?PDB_PREFIX, '_'}, '_', '_'},
        [],
        [true]
    }],
    ok = plum_db_events:subscribe(object_update, MS),

    %% We monitor all nodes so that we can cleanup our view of the registry
    partisan:monitor_nodes(true),

    {channel, Channel} = lists:keyfind(channel, 1, partisan_gen:get_opts()),

    State = #state{
        partisan_channel = Channel
    },

    {ok, State, {continue, monitor_existing}}.


handle_continue(monitor_existing, State) ->
    %% This prevents any grain to be registered as we are blocking the server
    %% until we finish.
    %% We fold the claimed ?MONITOR_TAB table to find any existing registrations.
    %% In case the table is new, it would be empty. Otherwise, we would iterate
    %% over registrations that were registered by a previous instance of this
    %% gen_server before it crashed.
    %% TWe re-monitor alive pids and remove dead ones.
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
                    %% remove the registration from the global table (plum_db).
                    ok = do_unregister_name(GrainRef, Pid)
            end;

        ({_, _, _, _}) ->
            ok
    end,
    ok = lists:foreach(Fun, ets:tab2list(?MONITOR_TAB)),

    %% We should now have all existing local grains re-registered on this
    %% server and gossip messages sent to cluster peers.
    {noreply, State, {continue, sync_remote}};

handle_continue(sync_remote, State) ->
    %% This prevents any grain to be registered as we are blocking the server
    %% until we finish.
    Node = partisan:node(),

    Fun = fun
        ({{Peer, GrainKey}, ProcessRef}) when Peer == Node ->
            sync_global_registration(GrainKey, ProcessRef);

        ({{Peer, GrainKey}, ProcessRef}) when Peer =/= Node ->
            sync_local_view(Peer, GrainKey, ProcessRef)
    end,

    Opts = [
        {remove_tombstones, true},
        %% We want lww to resolve any conflict. In general they shouldn't exist
        %% as only the owner of a registration can update it
        {resolver, lww},
        %% We do not want to update registrations we do not own, so our fun
        %% considers this
        {allow_put, false}
    ],

    ok = plum_db:foreach(Fun, ?PDB_PREFIX, Opts),

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
    Registrations = lookup(GrainRef),

    %% We then exclude unreachable grains
    Reply = case exclude_unreachable(Registrations) of
        [] ->
            %% Nothing registered or unreachable, so we allow the local
            %% registration
            do_register_name(GrainRef, Caller);

        [ProcessRef|_] ->
            %% We found at least one active grain that is reachable, so we pick
            %% it. If there was a local grain registered under GrainRef,
            %% ProcessRef would be it (becuase of ordering guarantee).
            {error, {already_in_use, ProcessRef}}
    end,

    {reply, Reply, State};

handle_call({register_name_test, GrainRef, PRef}, _From, State) ->
    %% Only for testing (se export of register_name/2)
    %% Add to local materialised view
    Node = partisan:node(PRef),

    Reply =
        case Node == partisan:node() of
            true ->
                do_register_name(GrainRef, partisan_remote_ref:to_pid(PRef));
            false ->
                %% We simulate a remote registration
                Obj = new_reg(grain_key(GrainRef), PRef, Node),
                true = ets:insert(?VIEW_TAB, Obj),

                %% Add to globally replicated table
                Key = {Node, grain_key(GrainRef)},
                ok = plum_db:put(?PDB_PREFIX, Key, PRef)
        end,
    {reply, Reply, State};

handle_call({unregister_name_test, GrainRef, PRef}, _From, State) ->
    %% Only for testing (se export of unregister_name/2)
    Node = partisan:node(PRef),

    Reply =
        case Node == partisan:node() of
            true ->
                do_unregister_name(GrainRef, partisan_remote_ref:to_pid(PRef));
            false ->
                %% We simulate a remote registration
                Obj = new_reg(grain_key(GrainRef), PRef, Node),
                true = ets:delete_object(?VIEW_TAB, Obj),

                %% Remove to globally replicated table
                Key = {Node, grain_key(GrainRef)},
                ok = plum_db:delete(?PDB_PREFIX, Key)
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

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


-spec handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()}.

handle_cast({force_unregister_name, GrainRef, Pref}, State) ->
    %% Internal case to deal with inconsistencies
    case ets:lookup(?VIEW_TAB, grain_key(GrainRef)) of
        [#reg{type = local, pid = X}] when X =/= Pref ->
            Pid = partisan_remote_ref:to_pid(Pref),
            ok = do_unregister_name(GrainRef, Pid);

        _ ->
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

handle_info({plum_db_event, object_update, Payload}, State) ->
    Node = partisan:node(),
    {{_Prefix, Key}, Obj, PrevObj} = Payload,

    ?LOG_DEBUG(#{
        message => "plum_db_event object_update received",
        key => Key,
        object => Obj,
        prev_obj => PrevObj
    }),

    %% Value can be a partisan_remote_reg:p() or ?TOMBSTONE
    Value = plum_db_object:value(plum_db_object:resolve(Obj, lww)),

    case Key of
        {Node, GrainKey} ->
            %% A peer updated or deleted our registration on the global registry.
            %% This should not happen, ensure we are in sync.
            sync_global_registration(GrainKey, Value);

        {Peer, GrainKey} ->
            sync_local_view(Peer, GrainKey, Value)
    end,


    {noreply, State};

handle_info(_, State) ->
    {noreply, State}.


-spec terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: term()) ->
    term().

terminate(_Reason, _State) ->
    ok = unregister_all().



%% =============================================================================
%% PRIVATE
%% =============================================================================



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
            %% Add to local materialised view
            Obj = new_reg(grain_key(GrainRef), Pid, local),
            true = ets:insert(?VIEW_TAB, Obj),

            %% Add to global registry (globally replicated table)
            Node = partisan:node(),
            Key = {Node, grain_key(GrainRef)},
            Pref = partisan_remote_ref:from_term(Pid),
            ok = plum_db:put(?PDB_PREFIX, Key, Pref);

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
    GrainKey = grain_key(GrainRef),
    Key = {partisan:node(), GrainKey},

    ok = demonitor(Pid),
    true = ets:delete(?MONITOR_TAB, Pid),
    true = ets:delete_object(?VIEW_TAB, new_reg(GrainKey, Pid, local)),
    ok = plum_db:delete(?PDB_PREFIX, Key).


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
new_reg(Key, Pid, Type) when is_pid(Pid) ->
    new_reg(Key, partisan_remote_ref:from_term(Pid), Type);

new_reg({_, _} = Key, PidRef, Type) when Type == local; is_atom(Type) ->
    #reg{
        key = Key,
        pid = PidRef,
        type = Type
    }.


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
%% @doc
%% @end
%% -----------------------------------------------------------------------------
lookup(Term) ->
    [P || #reg{pid = P} <- lookup_registrations(Term)].


%% -----------------------------------------------------------------------------
%% @private
%% @doc Lookups all the registered grains under name `GrainRef' using the
%% local ets-based materialised view.
%% @end
%% -----------------------------------------------------------------------------
-spec lookup_registrations(GrainRef :: erleans:grain_ref() | grain_key()) ->
    [partisan_remote_ref:p()].

lookup_registrations(#{id := _} = GrainRef) ->
    lookup_registrations(grain_key(GrainRef));

lookup_registrations({_, _} = GrainKey) ->
    lists:sort(
        fun
            (#reg{type = local}, #reg{type = _}) ->
                true;

            (#reg{type = _}, #reg{type = local}) ->
                false;

            (#reg{type = A}, #reg{type = B}) ->
                A =< B
        end,
        ets:lookup(?VIEW_TAB, GrainKey)
    ).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
sync_global_registration(GrainKey, Term) ->
    PidPattern =
        case Term of
            ?TOMBSTONE ->
                '_';
            _ ->
                partisan_remote_ref:to_pid(Term)
        end,

    Pattern = #reg{
        key = GrainKey,
        pid = PidPattern,
        type = local
    },

    case ets:match_object(?VIEW_TAB, Pattern) of
        [] when Term == ?TOMBSTONE ->
            %% This should not happen as peers are only allowed to delete
            %% owned registrations. But the global registry is in sync.
            ok;

        [] ->
            %% The case for and invalid references in the global registry.
            %% This could be a registration that we were not able to remove
            %% on terminate/2 the last time we shutdown/crashed e.g.
            %% gossip message loss and/or network split when shutdown/
            %% crash occured.
            Key = {partisan:node(), GrainKey},
            ok = plum_db:delete(?PDB_PREFIX, Key);

        [#reg{pid = ProcessRef}] when Term == ?TOMBSTONE ->
            %% This should not happen as no other node should be deleting
            %% our registrations.
            Key = {partisan:node(), GrainKey},
            ok = plum_db:put(?PDB_PREFIX, Key, ProcessRef);

        [#reg{pid = ProcessRef}] when Term == ProcessRef ->
            %% This should not happen as no other node should be deleting
            %% our registrations.
            %% But the global registry is in sync.
            ok;

        [#reg{pid = ProcessRef}] when Term =/= ProcessRef ->
            %% An old entry remained in the global registry, we rectify
            Key = {partisan:node(), GrainKey},
            ok = plum_db:put(?PDB_PREFIX, Key, ProcessRef)

    end.

%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
sync_local_view(Peer, GrainKey, ?TOMBSTONE) ->
    %% A Peer removed a registration, we need to update our view
    Pattern = #reg{
        key = GrainKey,
        pid = '_',
        type = Peer
    },
    true = ets:match_delete(?VIEW_TAB, Pattern),
    ok;

sync_local_view(Peer, GrainKey, RemotePRef) ->
    %% A peer created or updated a registration, we update our view
    true = ets:insert(?VIEW_TAB, new_reg(GrainKey, RemotePRef, Peer)),

    %% But we need to remove any locl duplicates, as local registration are
    %% preferred by lookup/1 and thus whereis_name/1
    case lookup_registrations(GrainKey) of
        [] ->
            ok;

        [#reg{key = {Id, ImplMod} = type = local, pid = LocalPRef} | _] ->
            GrainRef = erleans:get_grain(ImplMod, Id),
            case erleans_grain:is_location_right(GrainRef, LocalPRef) of
                true ->
                    %% Keep local and request deactivation of remote one.
                    ok = deactivate_grain(GrainRef, RemotePRef);

                false ->
                    %% Keep remote and request deactivation of local
                    ok = deactivate_grain(GrainRef, LocalPRef);

                noproc ->
                    %% Just died while we are blocking this server
                    %% no need to request deactivation as it will be done
                    %% after we return
                    ok
            end;
        _List ->
            %% More remote duplicates, this should converge by every peer
            %% applying this algorithm on every new registration.
            ok
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
deactivate_grain(GrainRef, ProcessRef) ->
    case erleans_grain:deactivate(ProcessRef) of
        ok ->
            ?LOG_NOTICE(#{
                description => "Requested duplicate deactivation",
                grain => GrainRef,
                pid => ProcessRef
            });

        {error, Reason} when Reason == not_found; Reason == not_active ->
            ?LOG_ERROR(#{
                description => "Failed to deactivate duplicate",
                grain => GrainRef,
                pid => ProcessRef,
                reason => Reason
            }),
            %% This is an inconsistency, we need to cleanup.
            %% We ask the peer to do it, via a private cast (peer can be us)
            remote_unregister_name(GrainRef, ProcessRef);

        {error, Reason} ->
            ?LOG_ERROR(#{
                description => "Failed to deactivate duplicate",
                grain => GrainRef,
                pid => ProcessRef,
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

safe_pick([ProcessRef | Rest], GrainRef) ->
    try is_proc_alive(ProcessRef, GrainRef) of
        true ->
            ProcessRef;
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

is_proc_alive(ProcessRef) ->
    is_proc_alive(ProcessRef, undefined).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
-spec is_proc_alive(partisan_remote_ref:p(), erleans:grain_ref() | undefined) ->
    boolean() | no_return().

is_proc_alive(ProcessRef, undefined) ->
    partisan:is_process_alive(ProcessRef);

is_proc_alive(ProcessRef, GrainRef) ->
    case grain_ref(ProcessRef) of
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

exclude_unreachable(ProcessRefs) when is_list(ProcessRefs) ->
    lists:filter(fun is_reachable/1, ProcessRefs).


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
is_reachable(ProcessRef) ->
    try
        is_proc_alive(ProcessRef)
    catch
        _:_ ->
            false
    end.


%% -----------------------------------------------------------------------------
%% @private
%% @doc
%% @end
%% -----------------------------------------------------------------------------
remote_unregister_name(GrainRef, ProcessRef) ->
    ServerRef = {?MODULE, partisan:node(ProcessRef)},
    partisan_gen_server:cast(
        ServerRef, {force_unregister_name, GrainRef, ProcessRef}
    ).


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

register_name(GrainRef, PRef) ->
    partisan_gen_server:call(?MODULE, {register_name_test, GrainRef, PRef}).


%% -----------------------------------------------------------------------------
%% @doc It can only be called by the caller
%% This call is serialised the `erleans_pm' server process.
%% @end
%% -----------------------------------------------------------------------------
-spec unregister_name(erleans:grain_ref(), partisan_remote_ref:p()) ->
    ok | {error, badgrain | not_owner}.

unregister_name(#{id := _} = GrainRef, PRef) ->
    partisan_gen_server:call(?MODULE, {unregister_name_test, GrainRef, PRef}).


-endif.



