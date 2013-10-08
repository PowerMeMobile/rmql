-module(rmql_rpc_server).

%% @TODO
%% add multiprocessing support
%% add try catch

-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").

-ignore_xref([{start_link, 3}]).

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
	props :: [term()],

	%% for step only (temp fields)
	dtag :: pos_integer(),
	payload :: binary(),
	bprops :: #'P_basic'{},
	response :: binary() | {binary(), binary()} | skip
}).

%% ===================================================================
%% API
%% ===================================================================

-spec start_link(atom(), list() | binary(), function()) ->
	{ok, pid()} |
	{error, more_than_one_queue_defined} |
	{error, subscribe_queue_undef}.
start_link(Name, Queue, Fun) when
		is_atom(Name) andalso
		is_binary(Queue) andalso
		is_function(Fun) ->
	Props = [
		#'queue.declare'{
		queue = Queue
	},
	#'basic.consume'{
		queue = Queue
	}],
    gen_server:start_link({local, Name}, ?MODULE, [Queue, Props, Fun], []);
start_link(Name, Props, Fun) when
		is_atom(Name) andalso
		is_list(Props) andalso
		is_function(Fun) ->
	Filter =
	fun(#'basic.consume'{}) -> true;
		(_) -> false
	end,
	case lists:filter(Filter, Props) of
		[] -> {error, subscribe_queue_undef};
	   	[_BC1, _BC2 | _] ->
			{error, more_than_one_queue_defined};
		[#'basic.consume'{queue = Q}] when is_binary(Q) ->
			gen_server:start_link({local, Name}, ?MODULE, [Q, Props, Fun], [])
	end.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([Queue, Props, Fun]) ->
	{ok, IsSurvive} = application:get_env(rmql, survive),
    FunArity = case erlang:fun_info(Fun, arity) of
		{arity, 1} -> 1;
		{arity, 2} -> 2
	end,
	St = #st{
		survive = IsSurvive,
		queue = Queue,
		handler = Fun,
		fun_arity = FunArity,
		props = Props
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
	error_logger:error_msg("rmql_rpc_srv (~s): amqp channel down (~p)~n",
			[St#st.queue, Down#'DOWN'.info]),
	{stop, amqp_channel_down, St};

handle_info(Down = #'DOWN'{ref = Ref}, St = #st{chan_mon_ref = Ref, survive = true}) ->
	error_logger:warning_msg("rmql_rpc_srv (~s): amqp channel down (~p)~n",
			[St#st.queue, Down#'DOWN'.info]),
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

setup_channel(St = #st{props = Props}) ->
	case rmql:channel_open() of
		{ok, Channel} ->
			error_logger:info_msg("rmql_rpc_server (~s): connected~n", [St#st.queue]),
			ok = set_qos(Channel, get_qos(Props)),
			MonRef = erlang:monitor(process, Channel),
			[ok = queue_declare(Channel, QDeclare) || QDeclare <- get_queue_declare(Props)],
			{ok, _ConsumerTag} = basic_consume(Channel, get_basic_consume(Props)),
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

step(respond, St = #st{response = reject}) ->
	step(reject, St);
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
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = DeliveryTag});

step(reject, #st{channel = Channel, dtag = DeliveryTag}) ->
	Method =
	#'basic.reject'{
		delivery_tag = DeliveryTag,
		requeue = false
	},
    amqp_channel:call(Channel, Method).

queue_declare(Chan, QueueDelare = #'queue.declare'{}) ->
    try amqp_channel:call(Chan, QueueDelare) of
        #'queue.declare_ok'{} -> ok;
        Other                 -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

get_queue_declare(Props) ->
	Filter =
	fun(#'queue.declare'{}) -> true;
		(_) -> false
	end,
	lists:filter(Filter, Props).

basic_consume(Chan, BasicConsume = #'basic.consume'{}) ->
    try
        amqp_channel:subscribe(Chan, BasicConsume, self()),
        receive
            #'basic.consume_ok'{consumer_tag = ConsumerTag} ->
                {ok, ConsumerTag}
        after
            10000 -> {error, timeout}
        end
    catch
        _:Reason -> {error, Reason}
    end.

get_basic_consume(Props) ->
	Filter =
	fun(#'basic.consume'{}) -> true;
		(_) -> false
	end,
	[BasicConsume] = lists:filter(Filter, Props),
	BasicConsume.

set_qos(_Chan, undefined) -> ok;
set_qos(Chan, QOS = #'basic.qos'{}) ->
	try amqp_channel:call(Chan, QOS) of
		#'basic.qos_ok'{} -> ok;
		Any -> {error, Any}
	catch
		_Class:Error -> {error, Error}
	end.

get_qos(Props) ->
	Filter =
	fun(#'basic.qos'{}) -> true;
		(_) -> false
	end,
	case lists:filter(Filter, Props) of
		[QOS] -> QOS;
		[] -> undefined;
		[_,_ | _] -> error(more_than_one_qos_declaration)
	end.
