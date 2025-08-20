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

-module(erleans_registry_sup).

-behaviour(supervisor).

-include_lib("kernel/include/logger.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(POOL_NAME, erleans_registry_pool).

%% =============================================================================
%% API
%% =============================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% =============================================================================
%% SUPERVISOR CALLBACKS
%% =============================================================================

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    %% Get number of partitions from configuration
    N = erleans_config:get(pm_partitions, 1),
    
    ?LOG_INFO("Starting erleans registry with ~p partitions", [N]),
    
    %% Create the gproc_pool first
    try
        gproc_pool:new(?POOL_NAME, hash, [{size, N}]),
        ?LOG_INFO("Created gproc_pool ~p with size ~p", [?POOL_NAME, N]),
        
        %% Add workers to the pool for each partition
        [gproc_pool:add_worker(?POOL_NAME, {partition, PartitionId}, PartitionId) 
         || PartitionId <- lists:seq(1, N)],
        ?LOG_INFO("Added ~p workers to pool ~p", [N, ?POOL_NAME])
    catch
        error:exists ->
            ?LOG_INFO("gproc_pool ~p already exists", [?POOL_NAME]);
        Error:Reason ->
            ?LOG_ERROR("Failed to create gproc_pool ~p: ~p:~p", [?POOL_NAME, Error, Reason]),
            error({gproc_pool_creation_failed, Error, Reason})
    end,
    
    %% Create child specs for N partition workers
    Children = [
        #{
            id => erleans_registry_partition:partition_name(PartitionId),
            start => {erleans_registry_partition, start_link, [PartitionId, ?POOL_NAME]},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [erleans_registry_partition]
        } || PartitionId <- lists:seq(1, N)
    ],
    
    %% Supervisor flags
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    
    {ok, {SupFlags, Children}}.