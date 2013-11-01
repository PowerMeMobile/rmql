-module(rmql_concurrent_rpc_srv).

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
	tid :: ets:tid()
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
	Tid = ets:new(?MODULE, []),
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
		props = Props,
		tid = Tid
	},
	case setup_channel(St) of
		#st{channel = undefined, survive = false} -> {stop, amqp_unavailable};
		St2 = #st{} -> {ok, St2}
	end.

handle_call({worker_reply, Req, Reply}, _From, St = #st{}) ->
	{#'basic.deliver'{delivery_tag = DTag},
		#amqp_msg{}} = Req,
    [{DTag, MonRef}] = ets:lookup(St#st.tid, DTag),
	demonitor(MonRef),
	process_reply(Req, Reply, St),
	ets:delete(St#st.tid, DTag),
	{reply, ok, St};

handle_call(Call, _From, State) ->
	{stop, {unexpected_call, Call}, State}.

handle_cast(Message, State) ->
    {stop, {unexpected_cast, Message}, State}.

handle_info(amqp_available, St = #st{}) ->
	{noreply, setup_channel(St)};

%% amqp channel down
handle_info(Down = #'DOWN'{ref = Ref}, St = #st{chan_mon_ref = Ref, survive = false}) ->
	error_logger:error_msg("rmql_concurrent_rpc_srv (~s): amqp channel down (~p)~n",
			[St#st.queue, Down#'DOWN'.info]),
	{stop, amqp_channel_down, St};

handle_info(Down = #'DOWN'{ref = Ref}, St = #st{chan_mon_ref = Ref, survive = true}) ->
	error_logger:warning_msg("rmql_concurrent_rpc_srv (~s): amqp channel down (~p)~n",
			[St#st.queue, Down#'DOWN'.info]),
	{noreply, setup_channel(St)};

%% worker failed
handle_info(#'DOWN'{ref = Ref, info = Reason}, St = #st{}) ->
	error_logger:warning_msg("rmql_concurrent_rpc_srv: worker down (~p)~n", [Reason]),
    case ets:match(St#st.tid, {'$1', Ref}) of
        [[DTag]] ->
            ets:delete(St#st.tid, DTag),
			%% call nack with no requeu to avoid recursive error
			ok = rmql:basic_reject(St#st.channel, DTag, false);
        [] ->
            ignore
    end,
    {noreply, St};

handle_info(Req = {#'basic.deliver'{}, #amqp_msg{}}, St = #st{}) ->
	spawn_worker(Req, St),
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
			error_logger:info_msg("rmql_concurrent_rpc_server (~s): connected~n", [St#st.queue]),
			ok = set_qos(Channel, get_qos(Props)),
			MonRef = erlang:monitor(process, Channel),
			[ok = queue_declare(Channel, QDeclare) || QDeclare <- get_queue_declare(Props)],
			[ok = exchange_declare(Channel, EDeclare) || EDeclare <- get_exch_declare(Props)],
			[ok = queue_bind(Channel, QBind) || QBind <- get_queue_bind(Props)],
			{ok, _ConsumerTag} = rmql:basic_consume(Channel, get_basic_consume(Props)),
		    St#st{
				channel = Channel,
				chan_mon_ref = MonRef};
		unavailable -> St
	end.

spawn_worker(Req, St = #st{}) ->
	{#'basic.deliver'{delivery_tag = DTag},
		#amqp_msg{props = Props, payload = Payload}} = Req,
    #'P_basic'{
		content_type = ContentType
	} = Props,
	Fun = St#st.handler,
	Srv = self(),
	Pid = spawn(fun() ->
		Reply =
		case St#st.fun_arity of
			1 -> Fun(Payload);
			2 -> Fun(ContentType, Payload)
		end,
		%% sync call to SRV to avoid 'DOWN' monitor msg
		%% and not effective ets:match
		gen_server:call(Srv, {worker_reply, Req, Reply})
	end),
	MonRef = monitor(process, Pid),
	true = ets:insert(St#st.tid, {DTag, MonRef}).

process_reply(Req, reject, St) ->
	{#'basic.deliver'{delivery_tag = DTag},
		#amqp_msg{}} = Req,
	ok = rmql:basic_reject(St#st.channel, DTag, false); %% requeue = false
process_reply(Req, RespPayload, St) when is_binary(RespPayload) ->
	{#'basic.deliver'{delivery_tag = DTag},
		#amqp_msg{props = Props}} = Req,
    #'P_basic'{
		correlation_id = CorrelationId,
		reply_to = ReplyTo
	} = Props,
    Publish = #'basic.publish'{
		routing_key = ReplyTo
	},
    RespProps = #'P_basic'{
		correlation_id = CorrelationId
	},
	Channel = St#st.channel,
	RespAmqpMsg = #amqp_msg{props = RespProps, payload = RespPayload},
    ok = amqp_channel:call(Channel, Publish, RespAmqpMsg),
	ok = rmql:basic_ack(Channel, DTag).

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

exchange_declare(Channel, ExchangeDeclare = #'exchange.declare'{}) ->
    try amqp_channel:call(Channel, ExchangeDeclare) of
		#'exchange.declare_ok'{} -> ok;
        Other                 -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

get_exch_declare(Props) ->
	Filter =
	fun(#'exchange.declare'{}) -> true;
		(_) -> false
	end,
	lists:filter(Filter, Props).

queue_bind(Channel, QueueBind = #'queue.bind'{}) ->
	try amqp_channel:call(Channel, QueueBind) of
		#'queue.bind_ok'{} -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

get_queue_bind(Props) ->
	Filter =
	fun(#'queue.bind'{}) -> true;
		(_) -> false
	end,
	lists:filter(Filter, Props).

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
