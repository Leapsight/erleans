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

-include_lib("kernel/include/logger.hrl").
-include("docs.hrl").

-moduledoc #{format => "text/markdown"}.
?MODULEDOC("""
This module implements the Erleans grain process registry router.
It maintains the same API as the original erleans_pm but distributes
grains across N partitioned bondy_mst_crdt instances for better performance.

The router uses consistent hashing via gproc_pool to select the appropriate
partition for each grain based on its grain_key().
""").

-define(TIMEOUT, 15000).

-type grain_key() :: {GrainId :: any(), ImplMod :: module()}.

%% API - Maintains exact same interface as original erleans_pm
-export([start_link/0]).
-export([register_name/0]).
-export([register_name/1]).
-export([unregister_name/0]).
-export([whereis_name/1]).
-export([whereis_name/2]).
-export([grain_ref/1]).
-export([to_list/0]).
-export([to_list/1]).
-export([lookup/1]).
-export([info/0]).

%% Partition selection
-export([select_partition/1]).
-export([get_all_partition_pids/0]).

%% TEST API
-ifdef(TEST).
    -export([add_/2]).
    -export([remove_/2]).
    -export([register_name_/2]).
    -export([unregister_name_/2]).
-endif.



%% =============================================================================
%% API
%% =============================================================================



?DOC("""
Starts the registry supervisor which manages N partition processes.
""").
-spec start_link() -> {ok, pid()} | {error, term()}.

start_link() ->
    erleans_registry_sup:start_link().


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

Routes the call to the appropriate partition based on consistent hashing.
""").
-spec register_name() ->
    ok
    | {error, badgrain}
    | {error, timeout}
    | {error, noproc}
    | {error, {already_in_use, partisan_remote_ref:p()}}.

register_name() ->
    register_name(?TIMEOUT).


-spec register_name(timeout()) ->
    ok
    | {error, badgrain}
    | {error, timeout}
    | {error, {already_in_use, partisan_remote_ref:p()}}.

register_name(_Timeout) ->
    case erleans:grain_ref() of
        undefined ->
            {error, badgrain};
        GrainRef ->
            PartitionPid = select_partition(GrainRef),
            erleans_registry_partition:register_name(PartitionPid, GrainRef)
    end.


?DOC("""
Unregisters a grain from the appropriate partition.
""").
-spec unregister_name() -> ok | {error, badgrain}.

unregister_name() ->
    case erleans:grain_ref() of
        undefined ->
            {error, badgrain};
        GrainRef ->
            PartitionPid = select_partition(GrainRef),
            erleans_registry_partition:unregister_name(PartitionPid, GrainRef)
    end.


?DOC("""
Returns a process reference for `GrainRef` from the appropriate partition.
""").
-spec whereis_name(GrainRef :: erleans:grain_ref()) ->
    partisan_remote_ref:p() | undefined.

whereis_name(GrainRef) ->
    whereis_name(GrainRef, [safe]).


-spec whereis_name(GrainRef :: erleans:grain_ref(), Opts :: [safe | unsafe]) ->
    partisan_remote_ref:p() | undefined.

whereis_name(GrainRef, Opts) ->
    PartitionPid = select_partition(GrainRef),
    erleans_registry_partition:whereis_name(PartitionPid, GrainRef, Opts).


?DOC("""
Lookups all registered grains under name `GrainRef` from the appropriate partition.
""").
-spec lookup(GrainRef :: erleans:grain_ref()) -> [partisan_remote_ref:p()].

lookup(GrainRef) ->
    PartitionPid = select_partition(GrainRef),
    erleans_registry_partition:lookup(PartitionPid, GrainRef).


?DOC("""
Returns the `erleans:grain_ref` for a pid or process reference.
""").
-spec grain_ref(partisan:any_pid()) ->
    {ok, erleans:grain_ref()} | {error, timeout | any()}.

grain_ref(ProcRef) ->
    %% For grain_ref lookup, we need to try all partitions since we don't know
    %% which partition the process belongs to. We can optimize this later.
    try_all_partitions(fun(PartitionPid) ->
        case erleans_registry_partition:grain_ref(PartitionPid, ProcRef) of
            {ok, GrainRef} -> {found, GrainRef};
            {error, not_found} -> continue;
            {error, Reason} -> {error, Reason}
        end
    end).


?DOC("""
Returns the list of all registry entries from all partitions.
""").
-spec to_list() -> [{grain_key(), partisan_remote_ref:p()}].

to_list() ->
    to_list([safe]).


-spec to_list([safe | unsafe]) -> [{grain_key(), partisan_remote_ref:p()}].

to_list(Opts) ->
    %% Collect results from all partitions
    Workers = get_all_partition_pids(),
    lists:flatten([
        erleans_registry_partition:to_list(PartitionPid, Opts)
        || PartitionPid <- Workers
    ]).


?DOC("""
Returns information from all partitions.
""").
info() ->
    Workers = get_all_partition_pids(),
    PartitionInfos = [
        erleans_registry_partition:info(PartitionPid)
        || PartitionPid <- Workers
    ],
    #{
        num_partitions => length(Workers),
        partitions => PartitionInfos
    }.


%% =============================================================================
%% PARTITION SELECTION
%% =============================================================================



?DOC("""
Selects the appropriate partition for a given GrainRef using gproc_pool.
""").
-spec select_partition(erleans:grain_ref()) -> pid().

select_partition(GrainRef) ->
    GrainKey = grain_key(GrainRef),
    case gproc_pool:pick_worker(erleans_registry_pool, GrainKey) of
        false ->
            error({no_partition_available, GrainKey});
        Pid when is_pid(Pid) ->
            Pid
    end.



%% =============================================================================
%% PRIVATE
%% =============================================================================



%% @private
-spec grain_key(erleans:grain_ref()) -> {term(), module()}.

grain_key(#{id := Id, implementing_module := Mod}) ->
    {Id, Mod}.


%% @private  
-spec get_all_partition_pids() -> [pid()].

get_all_partition_pids() ->
    case gproc_pool:active_workers(erleans_registry_pool) of
        [] ->
            [];
        Workers ->
            [Pid || {_, Pid} <- Workers]
    end.


%% @private
-spec try_all_partitions(fun((pid()) -> continue | {found, term()} | {error, term()})) ->
    {ok, term()} | {error, not_found}.

try_all_partitions(Fun) ->
    Workers = get_all_partition_pids(),
    try_partitions(Fun, Workers).


%% @private
try_partitions(_Fun, []) ->
    {error, not_found};

try_partitions(Fun, [PartitionPid | Rest]) ->
    try
        case Fun(PartitionPid) of
            continue ->
                try_partitions(Fun, Rest);
            {found, Result} ->
                {ok, Result};
            {error, _} = Error ->
                Error
        end
    catch
        _:_ ->
            try_partitions(Fun, Rest)
    end.



%% =============================================================================
%% TEST
%% =============================================================================



-ifdef(TEST).

%% For testing - route to appropriate partition
add_(GrainRef, ProcRef) ->
    PartitionPid = select_partition(GrainRef),
    erleans_registry_partition:add_(PartitionPid, GrainRef, ProcRef).

remove_(GrainRef, ProcRef) ->
    PartitionPid = select_partition(GrainRef),
    erleans_registry_partition:remove_(PartitionPid, GrainRef, ProcRef).

register_name_(GrainRef, ProcRef) ->
    PartitionPid = select_partition(GrainRef),
    erleans_registry_partition:register_name_(PartitionPid, GrainRef, ProcRef).

unregister_name_(GrainRef, ProcRef) ->
    PartitionPid = select_partition(GrainRef),
    erleans_registry_partition:unregister_name_(PartitionPid, GrainRef, ProcRef).

-endif.