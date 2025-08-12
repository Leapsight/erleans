%%%--------------------------------------------------------------------
%%% @author Alejandro Miguez
%%% @doc
%%% Test suite for partitioned registry implementation.
%%% Verifies that grains are consistently routed to the same partition
%%% and that the partitioned system works correctly.
%%% @end
%%%--------------------------------------------------------------------
-module(registry_partition_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include("test_utils.hrl").

%% Test cases
all() ->
    [
        partition_consistency,
        partition_distribution, 
        cross_partition_lookup,
        partition_isolation,
        router_aggregation,
        grain_ref_lookup,
        partition_info
    ].

init_per_suite(Config) ->
    %% Ensure we're using partitioned configuration
    application:load(erleans),
    application:set_env(erleans, pm_partitions, 4),
    Config.

end_per_suite(_Config) ->
    application:unload(erleans),
    ok.

init_per_testcase(_TestCase, Config) ->
    %% Start erleans fresh for each test
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_testcase(_TestCase, _Config) ->
    application:stop(erleans),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

partition_consistency(_Config) ->
    ct:log("Testing that the same grain always goes to the same partition"),
    
    %% First, let's verify our pool setup
    verify_pool_setup(),
    
    %% Create multiple grain references with same ID
    GrainRef1 = erleans:get_grain(test_grain, <<"consistent-grain-1">>),
    GrainRef2 = erleans:get_grain(test_grain, <<"consistent-grain-2">>), 
    GrainRef3 = erleans:get_grain(test_grain, <<"consistent-grain-3">>),
    
    %% Test the underlying gproc_pool logic directly
    test_gproc_pool_consistency(GrainRef1),
    test_gproc_pool_consistency(GrainRef2),
    test_gproc_pool_consistency(GrainRef3),
    
    %% Get partition assignments multiple times
    Partition1_A = get_grain_partition(GrainRef1),
    Partition1_B = get_grain_partition(GrainRef1),  % Should be same as A
    Partition1_C = get_grain_partition(GrainRef1),  % Should be same as A
    
    Partition2_A = get_grain_partition(GrainRef2),
    Partition2_B = get_grain_partition(GrainRef2),  % Should be same as A
    
    Partition3_A = get_grain_partition(GrainRef3),
    Partition3_B = get_grain_partition(GrainRef3),  % Should be same as A
    
    %% Verify consistency - same grain always goes to same partition
    ?assertEqual(Partition1_A, Partition1_B),
    ?assertEqual(Partition1_B, Partition1_C),
    ?assertEqual(Partition2_A, Partition2_B),
    ?assertEqual(Partition3_A, Partition3_B),
    
    ct:log("Grain1 consistently assigned to partition ~p", [Partition1_A]),
    ct:log("Grain2 consistently assigned to partition ~p", [Partition2_A]),
    ct:log("Grain3 consistently assigned to partition ~p", [Partition3_A]),
    
    ok.

partition_distribution(_Config) ->
    ct:log("Testing that grains are distributed across multiple partitions"),
    
    %% Create many grains to test distribution
    NumGrains = 100,
    Grains = [erleans:get_grain(test_grain, list_to_binary("grain-" ++ integer_to_list(N))) 
              || N <- lists:seq(1, NumGrains)],
    
    %% Get partition for each grain
    Partitions = [get_grain_partition(Grain) || Grain <- Grains],
    
    %% Count grains per partition
    PartitionCounts = count_partitions(Partitions),
    
    ct:log("Partition distribution: ~p", [PartitionCounts]),
    
    %% Verify we're using multiple partitions (at least 2 out of 4)
    NumUsedPartitions = length(PartitionCounts),
    ?assert(NumUsedPartitions >= 2),
    
    %% Verify no partition is completely empty (reasonable distribution)
    MaxCount = lists:max([Count || {_Partition, Count} <- PartitionCounts]),
    MinCount = lists:min([Count || {_Partition, Count} <- PartitionCounts]),
    
    %% Distribution shouldn't be too skewed (max shouldn't be more than 3x min)
    ?assert(MaxCount =< (MinCount * 3)),
    
    ok.

cross_partition_lookup(_Config) ->
    ct:log("Testing lookup operations across partitions"),
    
    %% Create grains that will likely go to different partitions
    Grain1 = erleans:get_grain(test_grain, <<"cross-lookup-1">>),
    Grain2 = erleans:get_grain(test_grain, <<"cross-lookup-2">>),
    Grain3 = erleans:get_grain(test_grain, <<"cross-lookup-3">>),
    Grain4 = erleans:get_grain(test_grain, <<"cross-lookup-4">>),
    
    %% Register the grains by activating them
    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain1)),
    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain2)),
    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain3)),
    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain4)),
    
    timer:sleep(100), % Allow registration to complete
    
    %% Test whereis_name works for all grains regardless of partition
    ProcRef1 = erleans_pm:whereis_name(Grain1),
    ProcRef2 = erleans_pm:whereis_name(Grain2),
    ProcRef3 = erleans_pm:whereis_name(Grain3),
    ProcRef4 = erleans_pm:whereis_name(Grain4),
    
    ?assert(ProcRef1 =/= undefined),
    ?assert(ProcRef2 =/= undefined),
    ?assert(ProcRef3 =/= undefined),
    ?assert(ProcRef4 =/= undefined),
    
    %% Test lookup works for all grains
    ?assert(erleans_pm:lookup(Grain1) =/= []),
    ?assert(erleans_pm:lookup(Grain2) =/= []),
    ?assert(erleans_pm:lookup(Grain3) =/= []),
    ?assert(erleans_pm:lookup(Grain4) =/= []),
    
    ct:log("Successfully looked up grains across partitions"),
    ok.

partition_isolation(_Config) ->
    ct:log("Testing that each partition manages its own grains"),
    
    %% Create grains and ensure they go to specific partitions
    TestGrains = [
        erleans:get_grain(test_grain, <<"isolation-test-1">>),
        erleans:get_grain(test_grain, <<"isolation-test-2">>),
        erleans:get_grain(test_grain, <<"isolation-test-3">>)
    ],
    
    %% Activate grains
    [?assertEqual({ok, 1}, test_grain:activated_counter(Grain)) || Grain <- TestGrains],
    timer:sleep(100),
    
    %% Get partition assignments
    PartitionAssignments = [{Grain, get_grain_partition(Grain)} || Grain <- TestGrains],
    
    ct:log("Partition assignments: ~p", [PartitionAssignments]),
    
    %% Group grains by partition
    PartitionGroups = group_by_partition(PartitionAssignments),
    
    ct:log("Grains grouped by partition: ~p", [PartitionGroups]),
    
    %% For each partition, verify it only knows about its own grains
    [verify_partition_isolation(Partition, Grains, TestGrains) 
     || {Partition, Grains} <- PartitionGroups],
    
    ok.

router_aggregation(_Config) ->
    ct:log("Testing that router correctly aggregates data from all partitions"),
    
    %% Create several grains
    TestGrains = [
        erleans:get_grain(test_grain, <<"aggregation-1">>),
        erleans:get_grain(test_grain, <<"aggregation-2">>),
        erleans:get_grain(test_grain, <<"aggregation-3">>),
        erleans:get_grain(test_grain, <<"aggregation-4">>)
    ],
    
    %% Activate all grains
    [?assertEqual({ok, 1}, test_grain:activated_counter(Grain)) || Grain <- TestGrains],
    timer:sleep(100),
    
    %% Test to_list aggregates from all partitions
    AllEntries = erleans_pm:to_list(),
    GrainKeys = [grain_key(Grain) || Grain <- TestGrains],
    
    %% Verify all our test grains are in the aggregated list
    [?assert(lists:keymember(GrainKey, 1, AllEntries)) || GrainKey <- GrainKeys],
    
    %% Test info aggregates from all partitions  
    Info = erleans_pm:info(),
    ?assert(maps:is_key(num_partitions, Info)),
    ?assert(maps:is_key(partitions, Info)),
    
    NumPartitions = maps:get(num_partitions, Info),
    PartitionInfos = maps:get(partitions, Info),
    
    ?assertEqual(4, NumPartitions), % We configured 4 partitions
    ?assertEqual(4, length(PartitionInfos)), % Should have info for all 4
    
    ct:log("Successfully aggregated data from ~p partitions", [NumPartitions]),
    ok.

grain_ref_lookup(_Config) ->
    ct:log("Testing grain_ref lookup across partitions"),
    
    %% Create and activate a grain
    Grain = erleans:get_grain(test_grain, <<"grain-ref-test">>),
    ?assertEqual({ok, 1}, test_grain:activated_counter(Grain)),
    timer:sleep(100),
    
    %% Get the process reference
    ProcRef = erleans_pm:whereis_name(Grain),
    ?assert(ProcRef =/= undefined),
    
    %% Test grain_ref lookup (this searches all partitions)
    {ok, FoundGrain} = erleans_pm:grain_ref(ProcRef),
    
    %% Should find the same grain
    ?assertEqual(Grain, FoundGrain),
    
    ct:log("Successfully found grain ~p from process ~p", [FoundGrain, ProcRef]),
    ok.

partition_info(_Config) ->
    ct:log("Testing partition-specific info functions"),
    
    %% Get global info
    GlobalInfo = erleans_pm:info(),
    
    %% Verify structure
    ?assert(maps:is_key(num_partitions, GlobalInfo)),
    ?assert(maps:is_key(partitions, GlobalInfo)),
    
    NumPartitions = maps:get(num_partitions, GlobalInfo),
    PartitionInfos = maps:get(partitions, GlobalInfo),
    
    ?assertEqual(4, NumPartitions),
    ?assertEqual(4, length(PartitionInfos)),
    
    %% Each partition info should have required fields
    [begin
        ?assert(maps:is_key(partition_id, PartInfo)),
        ?assert(maps:is_key(tree, PartInfo)),
        ?assert(maps:is_key(local_registry, PartInfo))
    end || PartInfo <- PartitionInfos],
    
    ct:log("Partition info structure verified"),
    ok.

%%====================================================================
%% Helper Functions
%%====================================================================

%% Get which partition a grain is assigned to
get_grain_partition(GrainRef) ->
    PartitionPid = erleans_pm:select_partition(GrainRef),
    Info = partisan_gen_server:call(PartitionPid, info),
    maps:get(partition_id, Info).

%% Convert grain ref to grain key for comparison
grain_key(#{id := Id, implementing_module := Mod}) ->
    {Id, Mod}.

%% Get partition PID by partition ID
get_partition_pid_by_id(PartitionId) ->
    %% Get all active workers from gproc_pool
    case gproc_pool:active_workers(erleans_registry_pool) of
        [] -> 
            undefined;
        Workers ->
            %% Find the worker for this partition ID
            case lists:keyfind({partition, PartitionId}, 1, Workers) of
                {{partition, PartitionId}, Pid} -> Pid;
                false -> undefined
            end
    end.

%% Count how many grains are assigned to each partition
count_partitions(Partitions) ->
    Counts = lists:foldl(
        fun(Partition, Acc) ->
            maps:update_with(Partition, fun(X) -> X + 1 end, 1, Acc)
        end,
        #{},
        Partitions
    ),
    maps:to_list(Counts).

%% Group grains by their partition assignment
group_by_partition(PartitionAssignments) ->
    Groups = lists:foldl(
        fun({Grain, Partition}, Acc) ->
            maps:update_with(Partition, fun(Grains) -> [Grain | Grains] end, [Grain], Acc)
        end,
        #{},
        PartitionAssignments
    ),
    maps:to_list(Groups).

%% Verify that a partition only knows about its assigned grains
verify_partition_isolation(PartitionId, PartitionGrains, AllTestGrains) ->
    ct:log("Verifying partition ~p isolation", [PartitionId]),
    
    %% Get partition PID via gproc_pool
    PartitionPid = get_partition_pid_by_id(PartitionId),
    ?assert(PartitionPid =/= undefined),
    
    %% Check each test grain
    [begin
        case lists:member(Grain, PartitionGrains) of
            true ->
                %% This grain should be found in this partition
                Result = erleans_registry_partition:lookup(PartitionPid, Grain),
                ?assert(Result =/= [], {should_find_grain, Grain, in_partition, PartitionId});
            false ->
                %% This grain should NOT be found in this partition  
                Result = erleans_registry_partition:lookup(PartitionPid, Grain),
                ?assertEqual([], Result, {should_not_find_grain, Grain, in_partition, PartitionId})
        end
    end || Grain <- AllTestGrains],
    
    ct:log("Partition ~p isolation verified", [PartitionId]),
    ok.

%% Verify gproc_pool setup is correct
verify_pool_setup() ->
    PoolName = erleans_registry_pool,
    
    %% Check pool exists and has workers
    case gproc_pool:active_workers(PoolName) of
        [] ->
            ct:fail("No active workers in pool ~p", [PoolName]);
        Workers ->
            ct:log("Pool ~p has ~p workers: ~p", [PoolName, length(Workers), Workers]),
            
            %% Verify we have 4 workers as configured
            ?assertEqual(4, length(Workers)),
            
            %% Verify worker keys are correct
            ExpectedKeys = [{partition, I} || I <- [1,2,3,4]],
            ActualKeys = [Key || {Key, _Pid} <- Workers],
            SortedExpected = lists:sort(ExpectedKeys),
            SortedActual = lists:sort(ActualKeys),
            ?assertEqual(SortedExpected, SortedActual),
            
            ct:log("Pool setup verified correctly")
    end.

%% Test gproc_pool consistency directly
test_gproc_pool_consistency(GrainRef) ->
    GrainKey = grain_key(GrainRef),
    PoolName = erleans_registry_pool,
    
    %% Call gproc_pool:pick_worker multiple times
    Results = [gproc_pool:pick_worker(PoolName, GrainKey) || _ <- lists:seq(1, 5)],
    
    ct:log("gproc_pool consistency for ~p: ~p", [GrainKey, Results]),
    
    %% Verify all results are the same
    [FirstResult | RestResults] = Results,
    ?assert(FirstResult =/= false),
    [?assertEqual(FirstResult, Result) || Result <- RestResults],
    
    ok.