-module(erleans_telemetry_logger).

%% API
-export([attach/1, attach/2]).

%% Telemetry handler callback
-export([handle_event/4]).





%% =============================================================================
%% API
%% =============================================================================




-spec attach(Events :: [telemetry:event_name()]) -> ok | {error, already_exists}.

attach(Events) ->
    attach(Events, []).

-spec attach(Events :: [telemetry:event_name()], Opts :: proplists:proplist()) -> ok | {error, already_exists}.

attach(Events, Opts) when is_list(Events), is_list(Opts) ->
    telemetry:attach_many(?MODULE, Events, fun ?MODULE:handle_event/4, Opts).




%% =============================================================================
%% TELEMETRY HANDLER CALLBACK
%% =============================================================================


handle_event(Event, Measurements, Metadata, Opts) ->
    Level = case lists:keyfind(level, 1, Opts) of
        {level, Val} -> Val;
        false -> info
    end,

    Fun = fun(_) ->
        #{event => Event,
          measurements => Measurements,
          metadata => Metadata}
    end,

    logger:log(Level, Fun, Metadata).


