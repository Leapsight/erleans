-module(erleans_pm_SUITE).

-define(DEACTIVATE_AFTER, 5).
-define(SLEEP, timer:sleep(?DEACTIVATE_AFTER + 1)).
-define(PDB_PREFIX, {erleans_pm, grain_ref}).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("test_utils.hrl").


all() ->
     [
        {group, main}
    ].

groups() ->
    [
        {main, [], [
            register_name,
            whereis_name,
            already_in_use,
            stale_local_entry,
            unreachable_remote_entry,
            reachable_stale_remote_entry
        ]}
    ].


init_per_group(_, Config) ->
    application:load(partisan),
    application:load(bondy_mst),
    application:load(erleans),
    logger:set_application_level(partisan, error),
    logger:set_application_level(bondy_mst, error),
    application:set_env(erleans, deactivate_after, ?DEACTIVATE_AFTER),
    {ok, _} = application:ensure_all_started(partisan),
    {ok, _} = application:ensure_all_started(bondy_mst),
    {ok, _} = application:ensure_all_started(erleans),
    GrainRef = erleans:get_grain(test_grain, <<"grain1">>),
    [{grainref, GrainRef}|Config].


end_per_group(_, _Config) ->
    application:stop(erleans),
    application:stop(partisan),
    application:stop(bondy_mst),
    application:unload(erleans),
    application:unload(bondy_mst),
    ok.


init_per_suite(Config) ->
    dbg:stop(),
    Config.


end_per_suite(_Config) ->
    ok.


init_per_testcase(_, Config) ->
    Config.


end_per_testcase(_, _Config) ->
    ok.




%% =============================================================================
%% CASES
%% =============================================================================



register_name(_) ->
    ?assertMatch(
        {error, badgrain},
        erleans_pm:register_name(),
        "I (caller) am not a grain, so register should be disallowed"
    ).


whereis_name(Config) ->
    GrainRef = ?config(grainref, Config),
    {ok, 0} = test_grain:call_counter(GrainRef),
    %% Grain should have beend activated and registered

    PidRef = erleans_pm:whereis_name(GrainRef),
    Node = partisan:node(),
    ?assertMatch([Node|_PidStr], PidRef),

    ?SLEEP,

    ?assertMatch(undefined, erleans_pm:whereis_name(GrainRef)),
    ?assertMatch(undefined, erleans_pm:whereis_name(GrainRef, [safe])),
    ?assertMatch(undefined, erleans_pm:whereis_name(GrainRef, [unsafe])).


already_in_use(Config) ->
    GrainRef = ?config(grainref, Config),

    %% We simulate duplicate registrations
    ok = erleans_pm:register_name(GrainRef, partisan:self()),

    ?assertMatch(
        {error, {already_in_use, _}},
        erleans_pm:register_name(GrainRef, partisan:self())
    ).



stale_local_entry(Config) ->
    GrainRef = ?config(grainref, Config),

    %% We simulate a previous local registration. This case can happen
    %% when there was a registration on a previous instantiation of this node
    %% that remained in the global store (another node's replica)
    %% and re-emerges here via active anti-entropy (plum_db).
    ok = plum_db:put(?PDB_PREFIX, GrainRef, [partisan:self()]),

    ?assertMatch(
        undefined,
        erleans_pm:whereis_name(GrainRef),
        "Should not return the pid it is a stale entry and thus "
        "it is not present in the local erleans_pm ets table."
    ),
    ok = plum_db:delete(?PDB_PREFIX, GrainRef).


unreachable_remote_entry(Config) ->
    GrainRef = ?config(grainref, Config),

    %% We simulate a previous remote registration. It could be alive or dead
    %% but as we are not connected to that node we should consider it dead.
    [_|PidStr] = partisan:self(),
    UnreachableNode = 'foo@127.0.0.1',
    PidRef1 = [UnreachableNode|PidStr],
    ok = erleans_pm:register_name(GrainRef, PidRef1),

    ?assertMatch(
        PidRef1,
        erleans_pm:whereis_name(GrainRef, [unsafe]),
        "Should return because we use option 'unsafe'"
    ),

    ?assertMatch(
        undefined,
        erleans_pm:whereis_name(GrainRef, [safe]),
        "Should not return it because node is not connected"
    ),

    {ok, 1} = test_grain:call_counter(GrainRef),
    PidRef2 = erleans_pm:whereis_name(GrainRef, [safe]),

    ?assertMatch(
        PidRef2,
        erleans_pm:whereis_name(GrainRef),
        "Safe should be the default"
    ),

    ?assertNotMatch(
        undefined,
        PidRef2,
        "Should be active"
    ),

    ?assertNotMatch(
        [PidRef1, PidRef2],
        plum_db:get(?PDB_PREFIX,GrainRef),
        "We should have 2 pids"
    ),

    ok = erleans_grain:deactivate(GrainRef),
    ok = erleans_pm:unregister_name(GrainRef, PidRef1),
    {error, not_active} = erleans_grain:deactivate(GrainRef).

reachable_stale_remote_entry(Config) ->
    GrainRef = ?config(grainref, Config),

    %% We simulate a previous registration. This case can happen
    %% when there was a registration on a previous instantiation of a node
    %% that remained in the global store (another node's replica)
    %% and re-emerges here via active anti-entropy (plum_db).
    PidRef1 = ['foo@127.0.0.1'|<<"#Pid<0.5000.0>">>],
    ok = erleans_pm:register_name(GrainRef, PidRef1),


    meck:new(erleans_pm, [passthrough]),
    meck:expect(erleans_pm, grain_ref,
        fun
            (Pid) when is_pid(Pid) ->
                meck:passthrough(Pid);

            (PidRef) ->
                case partisan:node(PidRef) == partisan:node() of
                    true ->
                        Pid = partisan_remote_ref:to_term(PidRef),
                        meck:passthrough(Pid);
                    false ->
                        %% We simulate the remote grain is reachable and returns
                        %% a diff GrainRef
                        GrainRef#{id => foo}
                end
        end
    ),

    ?assertEqual(
        PidRef1,
        erleans_pm:whereis_name(GrainRef, [unsafe]),
        "Should return Pid even though is the wrong one as it is associated "
        "with a different Grain, but based on our copy of the global store it "
        "is still associated with it"
    ),

    ?assertEqual(
        undefined,
        erleans_pm:whereis_name(GrainRef, [safe]),
        "Should be unreachable and considered dead because node is not connected"
    ),

    ok.




