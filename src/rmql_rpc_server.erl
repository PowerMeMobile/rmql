-module(rmql_rpc_server).

%% @TODO
%% add multiprocessing support
%% add try catch

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
	queue :: binary(),
	survive :: boolean(),

	%% for step only (temp fields)
	dtag :: pos_integer(),
	payload :: binary(),
	bprops :: #'P_basic'{},
	response :: binary() | {binary(), binary()} | skip
}).

%% ===================================================================
%% API
%% ===================================================================

-spec start_link(atom(), binary(), function()) -> {ok, pid()}.
start_link(Name, Queue, Fun) ->
    gen_server:start_link({local, Name}, ?MODULE, [Queue, Fun], []).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([Queue, Fun]) ->
	{ok, IsSurvive} = application:get_env(rmql, survive),
    FunArity = case erlang:fun_info(Fun, arity) of
		{arity, 1} -> 1;
		{arity, 2} -> 2
	end,
	St = #st{
		survive = IsSurvive,
		queue = Queue,
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
	lager:error("rmql_rpc_srv (~p): amqp channel down (~p)", [St#st.queue, Down#'DOWN'.info]),
	{stop, amqp_channel_down, St};

handle_info(Down = #'DOWN'{ref = Ref}, St = #st{chan_mon_ref = Ref, survive = true}) ->
	lager:warning("rmql_rpc_srv (~p): amqp channel down (~p)", [St#st.queue, Down#'DOWN'.info]),
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
             #amqp_msg{props = Props, payload = Payload}}, St = #st{}) ->
	St1 = St#st{
		dtag = DeliveryTag,
		payload = Payload,
		bprops = Props
	},
	step(process, St1),
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
			lager:info("rmql_rpc_server (~p): connected", [St#st.queue]),
			MonRef = erlang:monitor(process, Channel),
		    amqp_channel:call(Channel, #'queue.declare'{queue = St#st.queue}),
		    amqp_channel:call(Channel, #'basic.consume'{queue = St#st.queue}),
		    St#st{
				channel = Channel,
				chan_mon_ref = MonRef};
		unavailable -> St
	end.

step(process, St = #st{fun_arity = 1}) ->
	Fun = St#st.handler,
	step(respond, St#st{response = Fun(St#st.payload)});
step(process, St = #st{fun_arity = 2}) ->
    #'P_basic'{
		content_type = ContentType
	} = St#st.bprops,
	Fun = St#st.handler,
	step(respond, St#st{response = Fun(ContentType, St#st.payload)});

step(respond, St = #st{response = noreply}) ->
	step(ack, St);
step(respond, St = #st{response = RespPayload}) when is_binary(RespPayload) ->
    #'P_basic'{
		correlation_id = CorrelationId,
		reply_to = Q
	} = St#st.bprops,
    Publish = #'basic.publish'{
		exchange = <<>>,
		routing_key = Q
	},
    RespProps = #'P_basic'{
		correlation_id = CorrelationId,
		content_type = <<>>
	},
	Channel = St#st.channel,
    amqp_channel:call(Channel, Publish, #amqp_msg{props = RespProps,
                                                  payload = RespPayload}),
	step(ack, St);
step(respond, St = #st{response = {RespContentType, RespPayload}}) when
			is_binary(RespPayload) andalso
			is_binary(RespContentType) ->
    #'P_basic'{
		correlation_id = CorrelationId,
		reply_to = Q
	} = St#st.bprops,
    Publish = #'basic.publish'{
		exchange = <<>>,
		routing_key = Q
	},
    RespProps = #'P_basic'{
		correlation_id = CorrelationId,
		content_type = RespContentType
	},
	Channel = St#st.channel,
    amqp_channel:call(Channel, Publish, #amqp_msg{props = RespProps,
                                                  payload = RespPayload}),
	step(ack, St);

step(ack, #st{channel = Channel, dtag = DeliveryTag}) ->
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = DeliveryTag}).
