-module(test_utils).

-export([start/0]).
-export([start/1]).
-export([stop/0]).

start() ->
    start(fun() -> ok end).


start(Fun) when is_function(Fun, 0) ->
    %% ok = filelib:ensure_dir("data"),
    %% application:load(key_value),
    %% application:load(app_config),
    %% application:load(maps_utils),
    %% application:load(utils),

    application:load(plum_db), % will load partisan
    application:load(erleans),
    logger:set_application_level(partisan, error),
    logger:set_application_level(plum_db, error),

    _ = Fun(),

    {ok, _} = application:ensure_all_started(plum_db),
    ct:pal(info, "Waiting for PlumDB partitions to be ready"),
    ct:pal(info, "Waiting for PlumDB hashtrees to be ready"),
    ok = plum_db_startup_coordinator:wait_for_partitions(),
    ok = plum_db_startup_coordinator:wait_for_hashtrees(),

    case net_kernel:start([partisan:node()]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok
    end,

    application:ensure_all_started(erleans).


stop() ->
    application:stop(erleans),
    application:stop(plum_db),
    application:unload(erleans),
    application:unload(plum_db),
    ok.