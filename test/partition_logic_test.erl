%%%--------------------------------------------------------------------
%%% @doc
%%% Standalone test to analyze partition selection logic
%%% @end
%%%--------------------------------------------------------------------
-module(partition_logic_test).

-export([test_partition_logic/0]).

-include_lib("eunit/include/eunit.hrl").

test_partition_logic() ->
    %% Start applications
    application:ensure_all_started(gproc),
    application:load(erleans),
    application:set_env(erleans, pm_partitions, 4),
    application:ensure_all_started(erleans),
    
    io:format("=== Testing Partition Selection Logic ===~n"),
    
    %% Test 1: Analyze gproc_pool setup
    analyze_gproc_pool(),
    
    %% Test 2: Test grain_key generation
    test_grain_key_generation(),
    
    %% Test 3: Test consistent routing
    test_consistent_routing(),
    
    %% Test 4: Test distribution
    test_distribution(),
    
    %% Clean up
    application:stop(erleans),
    ok.

analyze_gproc_pool() ->
    io:format("~n--- Analyzing gproc_pool setup ---~n"),
    
    %% Check if pool exists
    PoolName = erleans_registry_pool,
    
    try
        Workers = gproc_pool:active_workers(PoolName),
        io:format("Pool ~p has ~p active workers:~n", [PoolName, length(Workers)]),
        [io:format("  Worker: ~p -> PID: ~p~n", [WorkerKey, WorkerPid]) 
         || {WorkerKey, WorkerPid} <- Workers],
        
        %% Test pool strategy
        io:format("Pool strategy: hash~n"),
        io:format("Pool size: ~p~n", [length(Workers)])
    catch
        Error:Reason ->
            io:format("ERROR accessing pool: ~p:~p~n", [Error, Reason])
    end.

test_grain_key_generation() ->
    io:format("~n--- Testing grain_key generation ---~n"),
    
    %% Create some test grain references
    TestGrains = [
        #{id => <<"grain-1">>, implementing_module => test_grain, placement => prefer_local},
        #{id => <<"grain-2">>, implementing_module => test_grain, placement => prefer_local},  
        #{id => <<"grain-3">>, implementing_module => test_grain, placement => prefer_local},
        #{id => 123, implementing_module => my_grain, placement => prefer_local},
        #{id => {complex, key}, implementing_module => other_grain, placement => prefer_local}
    ],
    
    [begin
        GrainKey = grain_key(Grain),
        io:format("Grain ~p -> Key: ~p~n", [maps:get(id, Grain), GrainKey])
    end || Grain <- TestGrains].

test_consistent_routing() ->
    io:format("~n--- Testing consistent routing ---~n"),
    
    %% Test the same grain multiple times
    TestGrain = #{id => <<"consistency-test">>, implementing_module => test_grain, placement => prefer_local},
    
    Results = [begin
        try
            Pid = erleans_pm:select_partition(TestGrain),
            {ok, Pid}
        catch
            Error:Reason -> {error, {Error, Reason}}
        end
    end || _ <- lists:seq(1, 5)],
    
    io:format("Routing results for same grain:~n"),
    [io:format("  Attempt ~p: ~p~n", [N, Result]) || {N, Result} <- lists:zip(lists:seq(1, 5), Results)],
    
    %% Check consistency
    case Results of
        [{ok, FirstPid} | _] ->
            AllSame = lists:all(fun({ok, Pid}) -> Pid == FirstPid; (_) -> false end, Results),
            io:format("All results consistent: ~p~n", [AllSame]);
        _ ->
            io:format("ERROR: No successful routing~n")
    end.

test_distribution() ->
    io:format("~n--- Testing distribution across partitions ---~n"),
    
    %% Create many grains to test distribution
    NumGrains = 20,
    TestGrains = [#{id => list_to_binary("grain-" ++ integer_to_list(N)), 
                    implementing_module => test_grain, 
                    placement => prefer_local} 
                  || N <- lists:seq(1, NumGrains)],
    
    %% Get partition assignments
    Assignments = [begin
        try
            Pid = erleans_pm:select_partition(Grain),
            GrainKey = grain_key(Grain),
            
            %% Get partition info to find partition ID
            Info = partisan_gen_server:call(Pid, info),
            PartitionId = maps:get(partition_id, Info),
            
            {GrainKey, PartitionId, Pid}
        catch
            Error:Reason -> 
                {grain_key(Grain), error, {Error, Reason}}
        end
    end || Grain <- TestGrains],
    
    io:format("Grain distribution:~n"),
    [io:format("  ~p -> Partition ~p (PID: ~p)~n", [GrainKey, PartitionId, Pid]) 
     || {GrainKey, PartitionId, Pid} <- Assignments, PartitionId =/= error],
    
    %% Count distribution
    PartitionCounts = lists:foldl(
        fun({_, PartitionId, _}, Acc) when is_integer(PartitionId) ->
            maps:update_with(PartitionId, fun(X) -> X + 1 end, 1, Acc);
           (_, Acc) -> Acc
        end,
        #{},
        Assignments
    ),
    
    io:format("Partition counts: ~p~n", [PartitionCounts]),
    
    %% Check if distribution is reasonable
    Counts = maps:values(PartitionCounts),
    case Counts of
        [] ->
            io:format("ERROR: No successful assignments~n");
        _ ->
            MinCount = lists:min(Counts),
            MaxCount = lists:max(Counts),
            UsedPartitions = length(Counts),
            io:format("Used partitions: ~p/4, Min: ~p, Max: ~p~n", [UsedPartitions, MinCount, MaxCount])
    end.

%% Helper function (copied from erleans_pm)
grain_key(#{id := Id, implementing_module := Mod}) ->
    {Id, Mod}.