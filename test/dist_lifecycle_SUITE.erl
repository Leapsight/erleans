%%% ---------------------------------------------------------------------------
%%% @author Tristan Sloughter <tristan.sloughter@spacetimeinsight.com>
%%% @copyright 2016 Space-Time Insight <tristan.sloughter@spacetimeinsight.com>
%%%
%%% @doc
%%% @end
%%% ---------------------------------------------------------------------------
-module(dist_lifecycle_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("test_utils.hrl").

-define(NODE_CT, 'ct@127.0.0.1').
-define(NODE_A, 'a@127.0.0.1').

all() ->
    [manual_start_stop, activate_callback].

init_per_suite(Config) ->
    application:load(partisan),
    application:load(bondy_mst),
    application:load(erleans),
    application:set_env(partisan, peer_port, 10200),
    application:set_env(partisan, pid_encoding, false),
    %% lower gossip interval of partisan membership so it triggers more often
    %% in tests
    application:set_env(partisan, periodic_enabled, true),
    application:set_env(partisan, periodic_interval, 100),
    logger:set_application_level(partisan, error),
    logger:set_application_level(erleans, debug),
    {ok, _} = application:ensure_all_started(partisan),
    {ok, _} = application:ensure_all_started(bondy_mst),
    {ok, _} = application:ensure_all_started(erleans),
    start_nodes(),
    Config.

end_per_suite(_Config) ->
    application:stop(erleans),
    application:stop(partisan),
    application:stop(bondy_mst),
    application:unload(erleans),
    application:unload(bondy_mst),

    {ok, _} = ct_slave:stop(?NODE_A),
    ok.

start_nodes() ->
    Nodes = [{?NODE_A, 10201}], %, b, c, d],
    ct:log("\e[32m Starting nodes ~p \e[0m", [Nodes]),
    start_nodes(Nodes, []).

start_nodes([], Acc) ->
    Acc;
start_nodes([{Node, PeerPort} | T], Acc) ->
    ct:log("\e[32m Starting node ~p \e[0m", [Node]),
    CodePath = code:get_path(),
    Paths = lists:flatten([["-pa ", Path, " "] || Path <- CodePath]),
    ErlFlags = "-config ../../../../test/sys.config " ++ Paths,
    PDBPrefixes = [{erleans_pm, #{shard_by => prefix, type => ram}}],
    DataDir = atom_to_list(Node) ++ "_data",

    {ok, HostNode} = ct_slave:start(Node,[
        {kill_if_fail, true},
        {monitor_master, true},
        {init_timeout, 3000},
        {startup_timeout, 3000},
        {startup_functions, [
            {logger, set_handler_config, [default, config,
                #{file => "log/ct_console.log"}
            ]},
            {logger, set_handler_config, [default, formatter,
             {logger_formatter, #{}}]},
            {application, load, [plum_db]},
            {application, load, [erleans]},
            {application, set_env, [partisan, pid_encoding, false]},
            {application, set_env, [partisan, remote_ref_as_uri, true]},
            {application, set_env, [partisan, periodic_enabled, true]},
            {application, set_env, [partisan, periodic_interval, 100]},
            {application, set_env, [partisan, peer_port, PeerPort]},
            {application, set_env, [plum_db, data_dir, DataDir]},
            {application, set_env, [plum_db, prefixes, PDBPrefixes]},
            {application, ensure_all_started, [plum_db]},
            {application, ensure_all_started, [erleans]}
        ]},
        {erl_flags, ErlFlags}
    ]),
    timer:sleep(1000),

    ct:pal("\e[32m Node ~p [OK] \e[0m", [HostNode]),
    true = net_kernel:connect_node(?NODE_A),
    rpc:call(?NODE_A, partisan_peer_service, join, [#{name => ?NODE_CT,
                                                      listen_addrs => [#{ip => {127,0,0,1}, port => 10200}],
                                                      parallelism => 1}]),
    ok = partisan_peer_service:join(#{name => ?NODE_A,
                                  listen_addrs => [#{ip => {127,0,0,1}, port => PeerPort}],
                                  parallelism => 1}),
    start_nodes(T, [HostNode | Acc]).

manual_start_stop(_Config) ->
    Grain1 = erleans:get_grain(test_grain, <<"grain1">>),
    Grain2 = erleans:get_grain(test_grain, <<"grain2">>),

    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain1)),
    ?assertEqual({ok, 1}, rpc:call(?NODE_A, test_grain, activated_counter, [Grain2])),

    %% ensure we've waited a broadcast interval
    timer:sleep(500),

    %% verify grain1 is on node ct and grain2 is on node a
    ?assertEqual({ok, ?NODE_CT}, test_grain:node(Grain1)),
    ?assertEqual({ok, ?NODE_A}, test_grain:node(Grain2)),

    ?assertEqual({ok, ?NODE_CT}, rpc:call(?NODE_A, test_grain, node, [Grain1])),
    ?assertEqual({ok, 1}, rpc:call(?NODE_A, test_grain, activated_counter, [Grain2])),

    timer:sleep(200),

    ?assertEqual({ok, ?NODE_A}, rpc:call(?NODE_A, test_grain, node, [Grain2])),
    ?assertEqual({ok, ?NODE_A}, test_grain:node(Grain2)),

    ok.


activate_callback(_Config) ->
    meck:new(test_grain, [passthrough]),
    meck:expect(test_grain, placement,
        fun() -> {callback, ?MODULE, activate_callback_placement} end
    ),
    Grain3 = erleans:get_grain(test_grain, <<"grain3">>),
    Expected = activate_callback_placement(Grain3),
    ?assertEqual({ok, Expected}, test_grain:node(Grain3)).


%% used by activate_callback
activate_callback_placement(_GrainRef) ->
    ?NODE_A.



