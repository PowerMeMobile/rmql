-module(rmql_pool).

-include("otp_records.hrl").

-behaviour(gen_server).

-ignore_xref([{start_link, 0}]).

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

-record(st, {
	conn 			:: pid(),
	pid_list = [] 	:: [pid()]
}).

-define(RECONNECT_INTERVAL, 5000).

%% ===================================================================
%% API
%% ===================================================================

-spec start_link() -> {'ok', pid()} | 'ignore' | {'error', any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec open_channel() -> {ok, pid()} | {error, term()} | unavailable.
open_channel() ->
    gen_server:call(?MODULE, open_channel).

-spec close_channel(pid()) -> ok.
close_channel(Chan) ->
    gen_server:cast(?MODULE, {close_channel, Chan}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    schedule_connect(0),
    error_logger:info_msg("rmql_pool: started~n"),
	?MODULE = ets:new(?MODULE, [named_table]),
    {ok, #st{}}.

terminate(Reason, St) ->
	case St#st.conn of
		undefined -> ok;
		Conn ->
			unlink(Conn),
		    catch(amqp_connection:close(Conn)),
		    error_logger:info_msg("rmql_pool: terminated (~p)~n", [Reason])
	end.

handle_call(open_channel, {Pid, _Tag}, St = #st{conn = undefined}) ->
	NewList = [Pid | St#st.pid_list],
	{reply, unavailable, St#st{pid_list = NewList}};

handle_call(open_channel, {Pid, _Tag}, St) ->
    case rmql:channel_open(St#st.conn) of
        {ok, Chan} ->
            Ref = monitor(process, Pid),
            ets:insert(?MODULE, {Ref, Chan}),
            {reply, {ok, Chan}, St};
        {error, Reason} ->
            {stop, {error, Reason}, St}
    end.

handle_cast({close_channel, Chan}, St) ->
    case ets:match(?MODULE, {'$1', Chan}) of
        [[Ref]] ->
            demonitor(Ref),
            ets:delete(?MODULE, Ref);
        [] ->
            ignore
    end,
    catch(amqp_channel:close(Chan)),
    {noreply, St}.

handle_info({timeout, _Ref, connect}, St) ->
    case rmql:connection_start() of
        {ok, Conn} ->
            link(Conn),
            error_logger:info_msg("rmql_pool: amqp connection up~n"),
			[Pid ! amqp_available || Pid <- St#st.pid_list],
			{noreply, St#st{conn = Conn, pid_list = []}};
        {error, Reason} ->
            error_logger:warning_msg("rmql_pool: couldn't connect to the broker (~p)~n", [Reason]),
            schedule_connect(?RECONNECT_INTERVAL),
            {noreply, St}
    end;

handle_info(#'DOWN'{ref = Ref}, St) ->
    case ets:lookup(?MODULE, Ref) of
        [{_, Chan}] ->
		    catch(amqp_channel:close(Chan)),
            ets:delete(?MODULE, Ref);
        [] ->
            ignore
    end,
    {noreply, St};

handle_info({'EXIT', Pid, Reason}, #st{conn = Pid} = St) ->
    error_logger:warning_msg("rmql_pool: amqp connection down (~p)~n", [Reason]),
    schedule_connect(?RECONNECT_INTERVAL),
    {noreply, St#st{conn = undefined}}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% ===================================================================
%% Internals
%% ===================================================================

schedule_connect(Time) ->
    erlang:start_timer(Time, self(), connect).
