-module(rmql_rpc_client).

%% @TODO
%% purge dict timeout
%% dedicated connection
%% fun specs

-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(gen_server).

-export([
	start_link/1, start_link/2,
	call/2, call/3, call/4
]).
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3
]).

-record('DOWN',{
	ref 			:: reference(),
	type = process 	:: process,
	object 			:: pid(),
	info 			:: term() | noproc | noconnection
}).

-record(st, {
	channel :: pid(),
	chan_mon_ref :: reference(),
	reply_queue :: binary(),
	routing_key :: binary(),
	continuations = dict:new(),
	correlation_id = 0 :: non_neg_integer(),
	survive = false :: boolean()
}).

%% ===================================================================
%% API
%% ===================================================================

-spec start_link(atom()) -> {ok, pid()}.
start_link(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, [], []).

-spec start_link(atom(), binary()) -> {ok, pid()}.
start_link(Name, Queue) when is_atom(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [Queue], []).

-spec call(pid(), binary()) ->
	{ok, binary()} |
	{error, timeout} |
	{error, disconnected} |
	{error, non_routable}.
call(RpcClient, Payload) ->
    {ok, Timeout} = application:get_env(rmql, rpc_timeout),
    try	gen_server:call(RpcClient, {call, Payload}, Timeout)
	catch
		_:{timeout, _} -> {error, timeout}
	end.

-spec call(pid(), binary(), binary()) ->
	{ok, binary()} |
	{error, timeout} |
	{error, disconnected} |
	{error, non_routable}.
call(RpcClient, Payload, ContentType) ->
    {ok, Timeout} = application:get_env(rmql, rpc_timeout),
    try	gen_server:call(RpcClient, {call, Payload, ContentType}, Timeout)
	catch
		_:{timeout, _} -> {error, timeout}
	end.

-spec call(pid(), binary(), binary(), binary()) ->
	{ok, binary()} |
	{error, timeout} |
	{error, disconnected} |
	{error, non_routable}.
call(RpcClient, ContentType, Payload, Queue) ->
    {ok, Timeout} = application:get_env(rmql, rpc_timeout),
	try gen_server:call(RpcClient, {call, ContentType, Payload, Queue}, Timeout)
	catch
		_:{timeout, _} -> {error, timeout}
	end.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
	init([undefined]);
init([RoutingKey]) ->
	{ok, IsSurvive} = application:get_env(rmql, survive),
	St = #st{
		routing_key = RoutingKey,
		survive = IsSurvive
	},
	case setup_channel(St) of
		#st{channel = undefined, survive = false} -> {stop, amqp_unavailable};
		St2 = #st{} -> {ok, St2}
	end.

handle_call({call, _}, _From, St = #st{channel = undefined}) ->
	{reply, {error, disconnected}, St};
handle_call({call, _, _}, _From, St = #st{channel = undefined}) ->
	{reply, {error, disconnected}, St};
handle_call({call, _, _, _}, _From, St = #st{channel = undefined}) ->
	{reply, {error, disconnected}, St};

handle_call({call, ContentType, Payload, Queue}, From, State) ->
	NewState = publish(ContentType, Payload, Queue, From, State),
	{noreply, NewState};

handle_call({call, Payload}, From, State) ->
    NewState = publish(Payload, From, State),
    {noreply, NewState};

handle_call({call, Payload, ContentType}, From, State) ->
    NewState = publish(ContentType, Payload, From, State),
    {noreply, NewState};

handle_call(Msg, _From, State) ->
	{stop, {unexpected_call, Msg}, State}.

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(amqp_available, St = #st{}) ->
	{noreply, setup_channel(St)};

handle_info(Down = #'DOWN'{ref = Ref}, St = #st{chan_mon_ref = Ref, survive = false}) ->
	lager:error("rmql_rpc_srv (~p): amqp channel down (~p)", [St#st.routing_key, Down#'DOWN'.info]),
	{stop, amqp_channel_down, St#st{channel = undefined}};

handle_info(Down = #'DOWN'{ref = Ref}, St = #st{chan_mon_ref = Ref, survive = true}) ->
	lager:warning("rmql_rpc_srv (~p): amqp channel down (~p)", [St#st.routing_key, Down#'DOWN'.info]),
	{noreply, setup_channel(St#st{channel = undefined})};

handle_info({#'basic.consume'{}, _Pid}, State) ->
    {noreply, State};

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.cancel'{}, State) ->
    {noreply, State};

handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};

handle_info({#'basic.deliver'{},
             #amqp_msg{props = #'P_basic'{correlation_id = <<CorrID:64>>},
                       payload = Payload}}, St = #st{}) ->
    From = dict:fetch(CorrID, St#st.continuations),
    gen_server:reply(From, {ok, Payload}),
    {noreply, St#st{continuations = dict:erase(CorrID, St#st.continuations)}};

%% reply_text = <<"NO_ROUTE">>
handle_info({#'basic.return'{reply_code = 312}, AMQPMsg = #amqp_msg{}}, St = #st{}) ->
	BasicProps = AMQPMsg#amqp_msg.props,
	<<CorrelationID:64>> = BasicProps#'P_basic'.correlation_id,
    From = dict:fetch(CorrelationID, St#st.continuations),
	gen_server:reply(From, {error, non_routable}),
    {noreply, St#st{continuations = dict:erase(CorrelationID, St#st.continuations)}};

handle_info(Msg, St) ->
	{{unexpected_info, Msg}, St}.

terminate(_Reason, #st{channel = Channel}) ->
    amqp_channel:close(Channel),
    ok.

code_change(_OldVsn, State, _Extra) ->
    State.

%% ===================================================================
%% Internals
%% ===================================================================

setup_channel(St) ->
	case rmql:channel_open() of
		{ok, Channel} ->
			lager:info("rmql_rpc_client (~p): connected", [St#st.routing_key]),
			MonRef = erlang:monitor(process, Channel),
		    #'queue.declare_ok'{queue = Q} =
		        amqp_channel:call(Channel, #'queue.declare'{auto_delete = true}),
			case St#st.routing_key of
				undefined -> ignore;
				RoutingKey ->
					#'queue.declare_ok'{} =
						amqp_channel:call(Channel, #'queue.declare'{queue = RoutingKey})
			end,
			amqp_channel:register_return_handler(Channel, self()),
		    amqp_channel:call(Channel, #'basic.consume'{no_ack = true, queue = Q}),
			St#st{reply_queue = Q, chan_mon_ref = MonRef, channel = Channel};
		unavailable -> St
	end.

publish(Payload, From, St = #st{}) ->
	publish(<<"application/octet-stream">>, Payload, St#st.routing_key, From, St).
publish(ContentType, Payload, From, St = #st{}) ->
	publish(ContentType, Payload, St#st.routing_key, From, St).
publish(ContentType, Payload, RoutingKey, From, St) ->
	#st{
		channel = Channel,
		reply_queue = Q,
		correlation_id = CorrelationId,
		continuations = Continuations
	} = St,
    Props = #'P_basic'{
		correlation_id = <<CorrelationId:64>>,
		content_type = ContentType,
		reply_to = Q
	},
    Publish = #'basic.publish'{
		routing_key = RoutingKey,
		mandatory = true
	},
	AMQPMsg = #amqp_msg{props = Props, payload = Payload},
    amqp_channel:call(Channel, Publish, AMQPMsg),
    St#st{correlation_id = CorrelationId + 1,
                continuations = dict:store(CorrelationId, From, Continuations)}.
