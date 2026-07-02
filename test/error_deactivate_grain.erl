-module(error_deactivate_grain).
-behaviour(erleans_grain).

-export([
    placement/0,
    provider/0,
    options/0,
    activate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    deactivate/1
]).

-include("erleans.hrl").

placement() ->
    prefer_local.

provider() ->
    undefined.

options() ->
    #{}.

activate(_Ref, _Args) ->
    {ok, #{}, #{}}.

%% --- HANDLERS ---

handle_call(get_pid, From, State) ->
    {ok, State, [{reply, From, self()}]};
handle_call(ping, From, State) ->
    {ok, State, [{reply, From, pong}]};
handle_call(_Msg, _From, State) ->
    {ok, State}.

handle_cast(_Msg, State) ->
    {ok, State}.

handle_info(_Msg, State) ->
    {ok, State}.

deactivate(_State) ->
    {error, wrong_grain}.
