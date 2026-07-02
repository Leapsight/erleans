-module(erleans_deactivate_error_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("erleans.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([deactivate_error_test/1]).

all() ->
    [deactivate_error_test].

init_per_suite(Config) ->
    _ = application:load(partisan),
    _ = application:load(bondy_mst),
    _ = application:load(erleans),
    {ok, _} = application:ensure_all_started(partisan),
    {ok, _} = application:ensure_all_started(bondy_mst),
    {ok, _} = application:ensure_all_started(erleans),
    Config.

end_per_suite(_Config) ->
    application:stop(erleans),
    application:stop(bondy_mst),
    application:stop(partisan),
    ok.

deactivate_error_test(_Config) ->
    GrainId = <<"test_grain_error_id">>,
    GrainRef = erleans:get_grain(error_deactivate_grain, GrainId),

    Pid = erleans_grain:call(GrainRef, get_pid),
    true = is_pid(Pid),

    MonitorRef = monitor(process, Pid),

    ok = erleans_grain:deactivate(Pid),

    %% 4. Wait for the process to exit and verify the reason
    receive
        {'DOWN', MonitorRef, process, Pid, Reason} ->
            check_exit_reason(Reason)
    after 5000 ->
        ct:fail("Grain did not exit within timeout")
    end.

check_exit_reason({shutdown, deactivated}) ->
    ct:pal("SUCCESS: Grain handled the error and shut down gracefully.");
check_exit_reason(shutdown) ->
    ct:pal("SUCCESS: Grain shut down gracefully.");
check_exit_reason({case_clause, {error, wrong_grain}}) ->
    ct:fail("FAILURE: Grain crashed with case_clause! The fix in erleans_grain.erl is MISSING.");
check_exit_reason({Reason, Stack}) ->
    case Reason of
        {case_clause, {error, wrong_grain}} ->
            ct:fail("FAILURE: Grain crashed with case_clause! The fix is missing.");
        _ ->
            ct:fail("FAILURE: Unexpected crash reason: ~p Stack: ~p", [Reason, Stack])
    end;
check_exit_reason(Reason) ->
    ct:fail("FAILURE: Unexpected exit reason: ~p", [Reason]).
