# RMQL (RabbitMQ library)

## Features

- reconnect, notify workers about connection available
- close channel on worker down
- rpc client-server behaviour
- consumer behaviour
- shortcuts for basic operations

## RPC Client-Server

### Client

Start rpc client:

``` erlang
%% Started rpc client process localy registered in Erlang node as Name.
%% Also temprary queue declared for responses.
%% Name is atom. RPCClient is pid().
{ok, RPCClient} = rmql_rpc_client:start_link(Name).

%% OR

%% The same as start_link/1 but also declare non durable QueueName.
%% QueueName is binary.
{ok, RPCClient} = rmql_rpc_client:start_link(Name, QueueName).
```

Make request:

``` erlang
%% RPCClient is pid or registered name.
%% ResponsePayload is binary.
%% Also call may return:
%% {error, timeout} - if request was not received in 5 sec
%% {error, disconnected} - if client currently disconnected
%% {error, non_routable} - if rpc client got basic.return
%% when queue not exist for example

{ok, ResponsePayload} = rmql_rpc_client:call(RPCClient, Payload).

%% Also you can define ContentType or/and QueueName (Routing Key)
{ok, ResponsePayload} = rmql_rpc_client:call(RPCClient, Payload, ContentType).
{ok, ResponsePayload} = rmql_rpc_client:call(RPCClient, ContentType, Payload, QueueName).
```

### Server

Start server:

``` erlang
%% Start rpc server process localy regitstered as RPCSrvName,
%% declare non durable queue QueueName.
%% RPCSrvName is atom.
%% QueueName is binary.
%% Function is function with arity equal 1 or 2. See example below.

{ok, RPCSrv} = rmql_rpc_server:start_link(RPCSrvName, QueueName, Function).
```

Process request:

``` erlang
process(_ReqPayload) -> <<"OK">>.

%% OR, if function is 2 arity

process(_ReqContentType, _ReqPayload) -> <<"Hello">>.

%% Also function can return response content type
my_function(_) -> {<<"GreetingResponse">>, <<"Hello">>}.

%% OR noreply atom to skip response

my_function(_) -> noreply.
```

## Consumer

Start consumer:

``` erlang
%% Start consumer with localy registered name ConsumerName.
%% Consumer is pid.
%% QueueSpec is #'queue.declare'{} record
%% Fun is function with 1 or 2 arity
{ok, Consumer} = rmql_consumer:start_link(ConsumerName, QueueSpec, Fun).
```

Process message:
``` erlang
%% rmql_consumer ignores return value
%% if no exception generated, rmql_consumer ack message
my_function(_ReqPayload) ->
	%% some actions here
	ignore.

%% OR to get request content type
my_function(_ContentType, _Payload) ->
	%% some actions here
	ignore.
```

## AMQP connection failure

RPC server, client and, also, consumer support survive ampq connection failure.
By default survive is false.
To enable survive, set up rmql environment

``` erlang
{survive, true}
```

## Custom handler

Open channel:

``` erlang
{ok, Channel} = rmql:channel_open(),
MonRef = monitor(Channel).
```

On channel down:

``` erlang
handle_info({'DOWN',Ref, _, _, _}, State = #state{channel_mon_ref = Ref}) ->
	unavailable = rmql:channel_open().
	...
```

Now your gen_server pid saved in rmqp_pool and got message
when connection become available

``` erlang
handle_info(amqp_available, St = #st{}) ->
	{ok, Channel} = rmql:channel_open(),
	....
```

## Basic API

### Start connection

``` erlang
{ok, Connection} = rmql:connection_start(),

{ok, Channel} = rmql:channel_open(Connection),

%% or {ok, Channel} = rmql:channel_open(),
%% in order to use rmql connection and channels
%% management abilities

%% queue_declare(Channel, Queue, Durable, Exclusive, AutoDelete)
ok = rmql:queue_declare(Channel, <<"my_queue">>, false, false, false),
%% or
ok = rmql:queue_declare(Channel, <<"my_queue">>, [{durable, true}]),


ok = rmql:basic_publish(Channel, <<"my_queue">>, <<"payload">>, #'P_basic'{}),
%% or
ok = rmql:basic_publish(Channel, <<"my_queue">>, <<"payload">>, [{message_id, <<"some_id">>}]),

ok = rmql:channel_close(Channel),

ok = rmql:connection_close(Connection).
```

Also exchange_declare, queue_bind, basic_ack, basik_reject,
basic_cancel, tx_select, tx_commit methods are available.
See rmql.erl module for more info.

## Examples

You can find more examples in [kelly handlers]
[kelly handlers]: https://github.com/PowerMeMobile/kelly/tree/as_new_amqp_handlers/subapps/k_handlers/src
