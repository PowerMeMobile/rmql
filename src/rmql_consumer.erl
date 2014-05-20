-module(rmql_consumer).

%% @TODO
%% #'queue.declare_ok'{} = amqp_channel:call(Channel, Method),
%% multiproc processing
%% avoid handle_info(basic.

-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-export([start_link/3]).

-record('DOWN',{
	ref 			:: reference(),
	type = process 	:: process,
	object 			:: pid(),
	info 			:: term() | noproc | noconnection
}).

-record(st, {
	channel :: pid(),
	handler :: function(),
	fun_arity :: 1 | 2,
	chan_mon_ref :: reference(),
	queue_spec :: #'queue.declare'{},
	queue :: binary(),
	survive :: boolean()
}).

%% ===================================================================
%% API
%% ===================================================================

-spec start_link(atom(), #'queue.declare'{}, function()) -> {ok, pid()}.
start_link(Name, QueueSpec, Fun) ->
    gen_server:start_link({local, Name}, ?MODULE, [QueueSpec, Fun], []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([QueueSpec, Fun]) ->
	{ok, IsSurvive} = application:get_env(rmql, survive),
    FunArity = case erlang:fun_info(Fun, arity) of
		{arity, 1} -> 1;
		{arity, 2} -> 2
	end,
	St = #st{
			survive = IsSurvive,
			queue_spec = QueueSpec,
			queue = QueueSpec#'queue.declare'.queue,
			handler = Fun,
			fun_arity = FunArity
	},
	case setup_channel(St) of
		#st{channel = undefined, survive = false} -> {stop, amqp_unavailable};
		St2 = #st{} -> {ok, St2}
	end.

handle_call(Call, _From, State) ->
	{stop, {unexpected_call, Call}, State}.

handle_cast(Message, State) ->
    {stop, {unexpected_cast, Message}, State}.

handle_info(amqp_available, St = #st{}) ->
	{noreply, setup_channel(St)};

handle_info(Down = #'DOWN'{ref = Ref}, St = #st{chan_mon_ref = Ref, survive = false}) ->
	lager:error("rmql_consumer (~p): amqp channel down (~p)", [St#st.queue, Down#'DOWN'.info]),
	{stop, amqp_channel_down, St};

handle_info(Down = #'DOWN'{ref = Ref}, St = #st{chan_mon_ref = Ref, survive = true}) ->
	lager:warning("rmql_consumer (~p): amqp channel down (~p)", [St#st.queue, Down#'DOWN'.info]),
	{noreply, setup_channel(St)};

handle_info({#'basic.consume'{}, _}, State) ->
    {noreply, State};

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.cancel'{}, State) ->
    {noreply, State};

handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};

handle_info({#'basic.deliver'{delivery_tag = DeliveryTag},
             #amqp_msg{props = Props, payload = Payload}},
            St = #st{handler = Fun, channel = Channel}) ->
    #'P_basic'{content_type = ContentType} = Props,
	case St#st.fun_arity of
		1 -> Fun(Payload);
		2 -> Fun(ContentType, Payload)
	end,
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = DeliveryTag}),
    {noreply, St};

handle_info(Message, State) ->
	{stop, {unexpected_info, Message}, State}.

terminate(_Reason, #st{channel = Channel}) ->
    catch(amqp_channel:close(Channel)),
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

%% ===================================================================
%% Internals
%% ===================================================================

setup_channel(St) ->
	case rmql:channel_open() of
		{ok, Channel} ->
			lager:info("rmql_consumer (~p): connected", [St#st.queue]),
			MonRef = erlang:monitor(process, Channel),
		    amqp_channel:call(Channel, St#st.queue_spec),
		    amqp_channel:call(Channel, #'basic.consume'{queue = St#st.queue}),
		    St#st{
				channel = Channel,
				chan_mon_ref = MonRef};
		unavailable -> St
	end.
