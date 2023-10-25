-module(rmql).

-compile([{parse_transform, lager_transform}]).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([connection_start/0, connection_close/1]).
-export([channel_open/0, channel_open/1, channel_close/1]).
-export([exchange_declare/4]).
-export([queue_declare/3, queue_declare/5, queue_bind/3]).
-export([basic_qos/2, basic_consume/3, basic_cancel/2]).
-export([basic_publish/4, basic_publish/5, basic_ack/2, basic_reject/3]).
-export([tx_select/1, tx_commit/1]).

%% -------------------------------------------------------------------------
%% Connection methods
%% -------------------------------------------------------------------------

-spec connection_start() -> {'ok', pid()} | {'error', any()}.
connection_start() ->
	{ok, AmqpSpec, _Qos} = parse_opts([]),
	%% To avoid deadlock on app shutdown add timeout to start amqp connection
	Pid = self(),
	spawn(fun() ->
		Result = amqp_connection:start(AmqpSpec),
		Pid ! {amqp_connection, Result}
	end),
	receive
		{amqp_connection, Result} -> Result
	after
		10000 -> {error, timeout}
	end.


-spec connection_close(pid()) -> 'ok'.
connection_close(Conn) ->
    catch(amqp_connection:close(Conn)),
    ok.

%% -------------------------------------------------------------------------
%% Channel methods
%% -------------------------------------------------------------------------

-spec channel_open() -> {'ok', pid()} | {'error', any()}.
channel_open() ->
	rmql_pool:open_channel().

-spec channel_open(pid()) -> {'ok', pid()} | {'error', any()}.
channel_open(Conn) ->
    amqp_connection:open_channel(Conn).

-spec channel_close(pid()) -> 'ok'.
channel_close(Chan) ->
	rmql_pool:close_channel(Chan).

%% -------------------------------------------------------------------------
%% Exchange methods
%% -------------------------------------------------------------------------

-type exch_type() :: fanout.

-spec exchange_declare(pid(), binary(), exch_type(), boolean()) -> 'ok' | {'error', any()}.
exchange_declare(Channel, Exchange, Type, Durable) ->
	XDeclare = #'exchange.declare'{ exchange = Exchange,
									durable = Durable,
									type = atom_to_binary(Type, utf8)},
    try amqp_channel:call(Channel, XDeclare) of
		#'exchange.declare_ok'{} -> ok;
        Other                 -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

%% -------------------------------------------------------------------------
%% Queue methods
%% -------------------------------------------------------------------------

-spec queue_declare(pid(), binary(), [{atom(), term()}]) ->
                    'ok' | {'error', any()}.
queue_declare(Chan, Queue, Props) when is_list(Props) ->
    Durable = proplists:get_value(durable, Props, true), % default is false
    Exclusive = proplists:get_value(exclusive, Props, false),
    AutoDelete = proplists:get_value(auto_delete, Props, false),
    queue_declare(Chan, Queue, Durable, Exclusive, AutoDelete).

-spec queue_declare(pid(), binary(), boolean(), boolean(), boolean()) ->
                    'ok' | {'error', any()}.
queue_declare(Chan, Queue, Durable, Exclusive, AutoDelete) ->
    Method = #'queue.declare'{queue = Queue,
                              durable = Durable,
                              exclusive = Exclusive,
                              auto_delete = AutoDelete},
    try amqp_channel:call(Chan, Method) of
        #'queue.declare_ok'{} -> ok;
        Other                 -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

-spec queue_bind(pid(), binary(), binary()) ->
                    'ok' | {'error', any()}.
queue_bind(Chan, Queue, Exchange) ->
	QBind = #'queue.bind'{queue = Queue, exchange = Exchange},
	try amqp_channel:call(Chan, QBind) of
		#'queue.bind_ok'{} -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

%% -------------------------------------------------------------------------
%% Basic methods
%% -------------------------------------------------------------------------

-spec basic_qos(pid(), non_neg_integer()) -> 'ok' | {'error', any()}.
basic_qos(Chan, PrefetchCount) ->
    Method = #'basic.qos'{prefetch_count = PrefetchCount},
    try amqp_channel:call(Chan, Method) of
        #'basic.qos_ok'{} -> ok;
        Other             -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

-spec basic_consume(pid(), binary(), boolean()) ->
                    {'ok', binary()} | {'error', any()}.
basic_consume(Chan, Queue, NoAck) ->
    Method = #'basic.consume'{queue = Queue, no_ack = NoAck},
    try
        amqp_channel:subscribe(Chan, Method, self()),
        receive
            #'basic.consume_ok'{consumer_tag = ConsumerTag} ->
                {ok, ConsumerTag}
        after
            10000 -> {error, timeout}
        end
    catch
        _:Reason -> {error, Reason}
    end.

-spec basic_cancel(pid(), binary()) -> 'ok' | {'error', any()}.
basic_cancel(Chan, ConsumerTag) ->
    Method = #'basic.cancel'{consumer_tag = ConsumerTag},
    try
        amqp_channel:call(Chan, Method),
        receive
            #'basic.cancel_ok'{consumer_tag = ConsumerTag} -> ok
        after
            10000 -> {error, timeout}
        end
    catch
        _:Reason -> {error, Reason}
    end.

-spec basic_publish(pid(), binary(), binary(), binary(), #'P_basic'{} | list()) ->
    'ok' | {'error', any()}.
basic_publish(Chan, Exchange, RoutingKey, Payload, Props = #'P_basic'{}) ->
    Method = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    Content = #amqp_msg{payload = Payload, props = Props},
    try amqp_channel:call(Chan, Method, Content) of
        ok    -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end;
basic_publish(Chan, Exchange, RoutingKey, Payload, PropList) ->
	Props = prepare_basic_props(PropList),
	basic_publish(Chan, Exchange, RoutingKey, Payload, Props).

-spec basic_publish(pid(), binary(), binary(), #'P_basic'{} | list()) ->
    'ok' | {'error', any()}.
basic_publish(Chan, RoutingKey, Payload, Props) ->
    basic_publish(Chan, <<"">>, RoutingKey, Payload, Props).

-spec basic_ack(pid(), non_neg_integer()) -> 'ok' | {'error', any()}.
basic_ack(Chan, DeliveryTag) ->
    Method = #'basic.ack'{delivery_tag = DeliveryTag},
    try amqp_channel:call(Chan, Method) of
        ok    -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

-spec basic_reject(pid(), non_neg_integer(), boolean()) ->
    'ok' | {'error', any()}.
basic_reject(Chan, DeliveryTag, Requeue) ->
    Method = #'basic.reject'{delivery_tag = DeliveryTag, requeue = Requeue},
    try amqp_channel:call(Chan, Method) of
        ok    -> ok;
        Other -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

%% -------------------------------------------------------------------------
%% Tx methods
%% -------------------------------------------------------------------------

-spec tx_select(pid()) -> 'ok' | {'error', any()}.
tx_select(Chan) ->
    Method = #'tx.select'{},
    try amqp_channel:call(Chan, Method) of
        #'tx.select_ok'{} -> ok;
        Other             -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

-spec tx_commit(pid()) -> 'ok' | {'error', any()}.
tx_commit(Chan) ->
    Method = #'tx.commit'{},
    try amqp_channel:call(Chan, Method) of
        #'tx.commit_ok'{} -> ok;
        Other             -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

%% -------------------------------------------------------------------------
%% Parse options
%% -------------------------------------------------------------------------

parse_opts(undefined) ->
	parse_opts([]);

parse_opts(Opts) ->
	DefaultPropList =
		case application:get_env(rmql, amqp_props) of
			{ok, Value} -> Value;
			undefined -> []
		end,

	%% default amqp props definition
	DHost 		= proplists:get_value(host, DefaultPropList, "127.0.0.1"),
	DPort 		= proplists:get_value(port, DefaultPropList, 5672),
	DVHost 		= proplists:get_value(vhost, DefaultPropList, <<"/">>),
	DUsername 	= proplists:get_value(username, DefaultPropList, <<"guest">>),
	DPass 		= proplists:get_value(password, DefaultPropList, <<"guest">>),
	DQos 		= proplists:get_value(qos, DefaultPropList, 0),

	%% custom amqp props definition
	Host    = proplists:get_value(host, Opts, DHost),
	Port 	= proplists:get_value(port, Opts, DPort),
	VHost 	= proplists:get_value(vhost, Opts, DVHost),
	User 	= proplists:get_value(username, Opts, DUsername),
	Pass 	= proplists:get_value(password, Opts, DPass),
	Qos 	= proplists:get_value(qos, Opts, DQos),

	AmqpSpec = #amqp_params_network{
					username = User,
					password = Pass,
					virtual_host = VHost,
					host = Host,
					port = Port,
					heartbeat = 0
				},
	{ok, AmqpSpec, Qos}.


prepare_basic_props(Props) ->
	#'P_basic'{
		message_id = proplists:get_value(message_id, Props),
		correlation_id = proplists:get_value(correlation_id, Props),
		content_type = proplists:get_value(content_type, Props),
		content_encoding = proplists:get_value(content_encoding, Props),
		delivery_mode = proplists:get_value(delivery_mode, Props),
		reply_to = proplists:get_value(reply_to, Props),
		expiration = proplists:get_value(expiration, Props),
		timestamp = proplists:get_value(timestamp, Props),
		app_id = proplists:get_value(app_id, Props),
		headers = proplists:get_value(headers, Props),
		priority = proplists:get_value(priority, Props),
		type = proplists:get_value(type, Props),
		user_id = proplists:get_value(user_id, Props),
		cluster_id = proplists:get_value(cluster_id, Props)
	}.
