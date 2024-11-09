%%%--------------------------------------------------------------------
%%% Copyright Tristan Sloughter 2019. All Rights Reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%
%% @doc
%% @end
%%%-------------------------------------------------------------------

-module(erleans_app).

-behaviour(application).

-export([start/2,
         stop/1]).

-include_lib("kernel/include/logger.hrl").




%% =============================================================================
%% APPLICATION CALLBACKS
%% =============================================================================



start(_StartType, _StartArgs) ->
    Config = application:get_all_env(erleans),

    %% We temporarily disable plum_db's AAE to avoid rebuilding hashtrees
    %% until we are ready to do it
    ok = suspend_aae(),

    %% plum_db will start partisan
    _ = application:ensure_all_started(plum_db, permanent),
    ok = maybe_wait_for_plum_db_partitions(),
    %% We need to re-enable AAE (if it was enabled) so that hashtrees
    %% are build
    ok = restore_aae(),
    ok = maybe_wait_for_plum_db_hashtrees(),

    erleans_sup:start_link(Config).

stop(_State) ->
    erleans_cluster:leave(),
    ok.




%% =============================================================================
%% PRIVATE
%% =============================================================================




%% @private
suspend_aae() ->
    case application:get_env(plum_db, aae_enabled, true) of
        true ->
            ok = application:set_env(plum_db, priv_aae_enabled, true),
            ok = application:set_env(plum_db, aae_enabled, false),
            ?LOG_NOTICE(#{
                description =>
                "Temporarily disabled active anti-entropy (AAE) during "
                "initialisation"
            }),
            ok;
        false ->
            ok
    end.


%% @private
restore_aae() ->
    case application:get_env(plum_db, priv_aae_enabled, false) of
        true ->
            %% plum_db should have started so we call plum_db_config
            ok = plum_db_config:set(aae_enabled, true),
            ?LOG_NOTICE(#{
                description => "Active anti-entropy (AAE) re-enabled"
            }),
            ok;
        false ->
            ok
    end.


%% @private
maybe_wait_for_plum_db_partitions() ->
    case wait_for_partitions() of
        true ->
            %% We block until all partitions are initialised
            ?LOG_NOTICE(#{
                description =>
                    "Application master is waiting for plum_db partitions "
                    "to be initialised"
            }),
            plum_db_startup_coordinator:wait_for_partitions();
        false ->
            ok
    end.


%% @private
maybe_wait_for_plum_db_hashtrees() ->
    case wait_for_hashtrees() of
        true ->
            %% We block until all hashtrees are built
            ?LOG_NOTICE(#{
                description =>
                "Application master is waiting for plum_db hashtrees "
                "to be built"
            }),
            plum_db_startup_coordinator:wait_for_hashtrees();
        false ->
            ok
    end,

    %% We stop the coordinator as it is a transient worker
    plum_db_startup_coordinator:stop().


%% @private
wait_for_partitions() ->
    %% Waiting for hashtrees implies waiting for partitions
    plum_db_config:get(wait_for_partitions, true) orelse wait_for_hashtrees().


%% @private
wait_for_hashtrees() ->
    %% If aae is disabled the hastrees will never get build
    %% and we would block forever
    plum_db_config:get(aae_enabled, true) andalso
    plum_db_config:get(wait_for_hashtrees, true).


