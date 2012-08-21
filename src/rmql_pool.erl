-module(rmql_pool).

-include("otp_records.hrl").

-behaviour(gen_server).

%% API exports
-export([start_link/0,
         open_channel/0,
         close_channel/1]).

%% gen_server exports
-export([init/1,
         terminate/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3]).

-record(st, {amqp_conn :: pid(), amqp_chans :: ets:tid()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link() -> {'ok', pid()} | 'ignore' | {'error', any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec open_channel() -> {ok, pid()}.
open_channel() ->
    gen_server:call(?MODULE, open_channel, infinity).

-spec close_channel(pid()) -> ok.
close_channel(Chan) ->
    gen_server:cast(?MODULE, {close_channel, Chan}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([]) ->
    lager:info("amqp pool: initializing", []),
    case rmql:connection_start() of
        {ok, Conn} ->
            link(Conn),
            {ok, #st{amqp_conn = Conn, amqp_chans = ets:new(amqp_chans, [])}};
        {error, Reason} ->
            lager:error("amqp pool: failed to start (~p)", [Reason]),
            {stop, Reason}
    end.

terminate(_Reason, _State) ->
    ok.

handle_call(open_channel, {Pid, _Tag}, St) ->
    case rmql:channel_open(St#st.amqp_conn) of
        {ok, Chan} ->
            Ref = monitor(process, Pid),
            ets:insert(St#st.amqp_chans, {Ref, Chan}),
            {reply, {ok, Chan}, St};
        {error, Reason} ->
            {stop, {error, Reason}, St}
    end.

handle_cast({close_channel, Chan}, St) ->
    case ets:match(St#st.amqp_chans, {'$1', Chan}) of
        [[Ref]] ->
            demonitor(Ref),
		    catch(amqp_channel:close(Chan)),
            ets:delete(St#st.amqp_chans, Ref);
        [] ->
            ignore
    end,
    {noreply, St}.

handle_info(#'DOWN'{ref = Ref}, St) ->
    case ets:lookup(St#st.amqp_chans, Ref) of
        [{_, Chan}] ->
            rmql:channel_close(Chan),
            ets:delete(St#st.amqp_chans, Ref);
        [] ->
            ignore
    end,
    {noreply, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.
