[![Build Status](https://travis-ci.com/AccelByte/eventstream-go-sdk.svg?branch=master)](https://travis-ci.com/AccelByte/eventstream-go-sdk)

# eventstream-go-sdk
Go SDK for integrating with AccelByte's event stream

## Usage

### Install
The eventstream-kafka-go is available in two major versions:
#### v4
The eventstream-go-sdk v4 changed its underlying Kafka library from [segmentio/kafka-go](https://github.com/segmentio/kafka-go) 
to [confluentinc/confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) which is a lightweight wrapper around 
[librdkafka](https://github.com/confluentinc/librdkafka), a finely tuned C client.

Enabling CGO `CGO_ENABLED=1` is a must since the Go client is based on the C library librdkafka.

```
go get -u github.com/AccelByte/eventstream-go-sdk/v4
```

When building your application in Alpine Linux (musl libc), you must pass `-tags musl` to go build.

#### v3
```
go get -u github.com/AccelByte/eventstream-go-sdk/v3
```

### Importing
#### v4
```go
eventstream "github.com/AccelByte/eventstream-go-sdk/v4"
```

#### v3
```go
eventstream "github.com/AccelByte/eventstream-go-sdk/v3"
```

To create a new event stream client, use this function:

```go
client, err := eventstream.NewClient(prefix, stream, brokers, config)
``` 
``NewClient`` requires 4 parameters :
 * prefix : Topic prefix from client (string)
 * stream : Stream name. e.g. kafka, stdout, none (string)
 * brokers : List of kafka broker (array of string)
 * config : Custom broker configuration from client. 
 This is optional and only uses the first arguments. (variadic *BrokerConfig)   

## Note for Maintainers

Please follow [semantic versioning](https://semver.org/) rule when releasing new version.
New version releases must be tagged based on their own corresponding major version:
* If you make changes only in v3, you need to tag the new version as `3.x.x`
* If you make changes only in v4, you need to tag the new version as `4.x.x`
* If you make changes on both, you need to tag the new version as `3.x.x` and `4.x.x`

## Supported Stream
Currently event stream are supported by these stream:

### Kafka Stream
Publish and subscribe an event to / from Kafka stream. 

currently compatible with golang version from 1.12+ and Kafka versions from 0.10.1.0 to 2.1.0.

To create a kafka stream client, just pass the stream parameter with `kafka`.

#### Custom Configuration
SDK support with custom configuration for kafka stream, that is :

* DialTimeout : Timeout duration during connecting to kafka broker. Default: 10 Seconds (time.Duration)
* ReadTimeout : Timeout duration during consume topic from kafka broker. Default: 10 Seconds (time.Duration)
* WriteTimeout : Timeout duration during publish event to kafka broker. Default: 10 Seconds (time.Duration) 
* LogMode : eventstream will print log based on following levels: info, warn, debug, error and off. Default: off (string) 
* StrictValidation : If it set true, eventstream will enable strict validation for event fields, Default: False (boolean) 

```go
    config := &eventstream.BrokerConfig{
		LogMode:          eventstream.InfoLevel,
		StrictValidation: true,
		DialTimeout:      1 * time.Second,
		ReadTimeout:      1 * time.Second,
		WriteTimeout:     1 * time.Second,
	}
```

#### Authentication
Supported authentication mode for kafka stream:

**1. SASL SCRAM**

Example configuration:

```go
    config := &eventstream.BrokerConfig{
        ...
        SecurityConfig: &eventstream.SecurityConfig{
            AuthenticationType: "SASL-SCRAM",
            SASLUsername:       "your-username",
            SASLPassword:       "your-password",
        },
    }
```

### Stdout Stream
This stream is for testing purpose. This will print the event in stdout. It should not be used in production since this 
will print unnecessary log.

To create a stdout stream client, just pass the stream parameter with `stdout`.

### Blackhole
This is used when client don't want the service to send event data to anywhere.

To create a blackhole client, just pass the stream parameter with `none`.

## Publish Subscribe Event

### Publish
Publish or sent an event into stream. Client able to publish an event into a single topic.
Publish to `kafka` stream support with exponential backoff retry. (max 3x)

To publish an event, use this function:
```go
err := client.Publish(
		NewPublish().
			Topic(TopicName).
			EventName(EventName).
			Namespace(Namespace).
			Key(Key).
			ClientID(ClientID).
			UserID(UserID).
			SessionID(SessionID).
			TraceID(TraceID).
			SpanContext(SpanContext).
			Context(Context).
			EventID(eventID int).
			EventType(eventType int).
			EventLevel(eventLevel int).
			ServiceName(serviceName string).
			ClientIDs(clientIDs []string).
			TargetUserIDs(targetUserIDs []string).
			TargetNamespace(targetNamespace string).
			Privacy(privacy bool).
			AdditionalFields(additionalFields map[string]interface{}).
			Version(Version).
			Payload(Payload).
			ErrorCallback(func(event *Event, err error) {}))
```

#### Publish Parameters
* Topic : Event topic. (string - alphaNumeric(256) - Required)
* EventName : Event name. (string - alphaNumeric(256) - Required) 
* Namespace : Event namespace. (string - alphaNumeric(256) - Required)
* key : Kafka message key. (string - default. random UUID v4 without Hyphens - optional)
* ClientID : Publisher client ID. (string - UUID v4 without Hyphens)7
* UserID : Publisher user ID. (string - UUID v4 without Hyphens)
* SessionID : Publisher session ID. (string - UUID v4 without Hyphens)
* TraceID : Trace ID. (string - UUID v4 without Hyphens)
* SpanContext : Opentracing Jaeger Span Context(string - optional)
* Context : Golang context. (context - default: context.background)
* Version : Version of schema. (integer - default: `1`)
* EventID : Event ID. Backward compatibility. (integer)
* EventType : Event Type. Backward compatibility. (integer)
* EventLevel : Event Level. Backward compatibility. (integer)
* ServiceName : Service Name. Backward compatibility. (string)
* ClientIDs : List of client IDs. Backward compatibility. ([]string - UUID v4 without Hyphens)
* TargetUserIDs : List of target client IDs. Backward compatibility. ([]string - UUID v4 without Hyphens)
* TargetNamespace : Target Namespace. Backward compatibility. (string)
* Privacy : Privacy. Backward compatibility. (bool)
* AdditionalFields : Additional fields. Backward compatibility. (map[string]interface{})
* Payload : Additional attribute. (map[string]interface{})

### Subscribe
To subscribe an event from specific topic in stream, client should be register a callback function that executed once event received.
A callback aimed towards specific topic and event name.

To subscribe an event, use this function:
```go
err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			GroupID(groupID).
			Offset(offset).
			Context(ctx).
			Callback(func(ctx context.Context, event *Event, err error) error { return nil }))
```
### Unsubscribe
To unsubscribe a topic from the stream, client should close passed context

To unsubscribe from the topic, use this function:
```go
ctx, cancel := context.WithCancel(context.Background())

err := client.Register(
    NewSubscribe().
        Topic(topicName).
        EventName(mockEvent.EventName).
        GroupID(groupID).
        Context(ctx).
        Callback(func(ctx context.Context, event *Event, err error) error {
            if ctx.Error() != nil {
                // unsubscribed
                return nil
            }           

            return nil
        }))

cancel() // cancel context to unsubscribe
```

#### Subscribe Parameters
* Topic : Subscribed topic. (string - alphaNumeric(256) - Required)
* Offset : Offset(position) inside the topic from which processing begins(int64 - Optional - default: `-1` (the tail))
* GroupID : Message broker group ID. A random ID will be generated by the client if it's empty. (string - alphaNumeric(256) - default: generated ID)
* GroupInstanceID : Message broker group instance ID for static group membership. Optional. (string - alphaNumeric(256) - default: empty)
* EventName : Event name to subscribe. If it's empty, any event will trigger the callback. (string - alphaNumeric(256) - optional)
* Callback : Callback function to be called when an event is received. (func(ctx context.Context,event *Event, err error) error {} - required)
* CallbackRaw : Similar with Callback but a raw message will be passed instead. (func(ctx context.Context,msgValue []byte, err error) error {} - optional)
* Context : Golang context. (context - default: context.background)
* SendErrorDLQ : A flag to send error message of message processing to a DLQ topic which have following format: 'yourTopic-dlq' (string - alphaNumeric(256) - default: false)
* AsyncCommitMessage : A flag to asynchronously commit message offset. This setting will be overridden by AutoCommitInterval on BrokerConfig.
* Slug : Returns a string describing a unique subscriber (topic, eventName, groupID)


Callback function passing 3 parameters:
* ``ctx`` context to check that consumer unsubscribed 
* ``event`` is object that store event message. 
* ``err`` is an error that happen when consume the message.

Callback function return 1 result(error):
* return `nil` to commit the event(mark as processed)
* return any error to retry processing(worker will be selected randomly)

## Event Message
Event message is a set of event information that would be published or consumed by client.

Event message format :
* id : Event ID (string - UUID v4 without Hyphens)
* name : Event name (string)
* namespace : Event namespace (string)
* traceId : Trace ID (string - UUID v4 without Hyphens)
* spanContext : Opentracing Jaeger Span Context (string - optional)
* clientId : Publisher client ID (string - UUID v4 without Hyphens)
* userId : Publisher user ID (string - UUID v4 without Hyphens)
* sessionId : Publisher session ID (string - UUID v4 without Hyphens)
* timestamp : Event time (time.Time)
* event_id : Event id. backward compatibility. (integer)
* event_type : Event type. backward compatibility. (integer)
* event_level : Event level. backward compatibility. (integer)
* service : Service name. backward compatibility. (string)
* client_ids : Client IDs. backward compatibility. ([]string - UUID v4 without Hyphens)
* target_user_ids : Target user IDs. backward compatibility. ([]string - UUID v4 without Hyphens)
* target_namespace : Target namespace. backward compatibility. (string)
* privacy : Privacy. backward compatibility. (bool)
* additional_fields : Set of data / object that given by producer. Each data have own key for specific purpose. Backward compatibility. (map[string]interface{}) 
* version : Event schema version (integer)
* payload : Set of data / object that given by producer. Each data have own key for specific purpose. (map[string]interface{})

## Publish Audit Log

Publish or sent an audit log into stream. Client able to publish an audit log into single topic.
Publish to `kafka` stream support with exponential backoff retry. (max 3x)

Environment Variables:
* APP_EVENT_STREAM_AUDIT_LOG_TOPIC(optional, default: auditLog)
* APP_EVENT_STREAM_AUDIT_LOG_ENABLED(optional, default true)

To publish an audit log, use this function:
```go
err := client.PublishAuditLog(eventstream.
            NewAuditLogBuilder().
            Category(Category).
            ActionName(ActionName).
            IP(IP).
            Actor(Actor).
            IsActorTypeUser(true).
            ClientID(ClientID).
            ActorNamespace(ActorNamespace).
            ObjectID(ObjectID).
	        ObjectType(ObjectType)).
            ObjectNamespace(ObjectNamespace).
            TargetUserID(TargetUserID).
            DeviceID(DeviceID).
            Content(Content).
            Diff(Diff).
            ErrorCallback(func(message []byte, err error) {
            })
```

#### Parameter
* Category : Category. (string - Format: xxx_xxx - Required)
* ActionName : Log action name. (string - Format: xxx_xxx - Required)
* IP: IP address. (string - Optional)
* Actor: Publisher/studio user id or client id. (string - UUID v4 without Hyphens - Required)
* IsActorTypeUser: Is actor a user or not. (bool - true or false - Required)
* ClientID : OAuth client ID. (string - UUID v4 without Hyphens - Required)
* ActorNamespace : Actor namespace, should be publisher or studio namespace. (string - alphaNumeric(256) - Required)
* ObjectID : Target resource id. (string - Optional)
* ObjectType: Type of Object. (string - Optional)
* ObjectNamespace : Namespace of target resource. (string - alphaNumeric(256) - Required)
* TargetUserID : User id related to the resource. (string - UUID v4 without Hyphens - Optional)
* DeviceID : Device id. (string - Optional)
* Content : Resource Content. (map - Required)
* Diff : Changes of the resource. (map - Recommended format: `{"before":{}, "after":{}}` - Optional)
* ErrorCallback : Callback function when event failed to publish. (func(message []byte, err error){} - Optional)

## SpanContext usage
* Create Jaeger Span Context from an event
```go
	import "github.com/AccelByte/go-restful-plugins/v3/pkg/jaeger"

    spanContextString := event.SpanContext
	span, ctx := jaeger.ChildSpanFromRemoteSpan(rootCtx, "service-name.operation-name", spanContextString)
```

* Put existing Jaeger Span Context into an event
```go
	import "github.com/AccelByte/go-restful-plugins/v3/pkg/jaeger"

    spanContextString := jaeger.GetSpanContextString(span)
    
    err := client.Register(
        NewSubscribe().
            Topic(topicName).
            SpanContext(spanContextString).
            // ...
```

# Tips
## v3
### Faster Write

Here are some of the things you can do if you want to have faster writes.

#### Set `RequiredAcks`

From the `WriterConfig` documentation:

> Number of acknowledges from partition replicas required before receiving a response to a produce request. The default is -1, which means to wait for all replicas, and a value above 0 is required to indicate how many replicas should acknowledge a message to be considered successful.

Set `RequiredAcks` to `1` so that it does not need to wait for all replicas to acknowledge.

You can set it up by injecting the `WriterConfig` inside `BrokerConfig` when creating new `KafkaClient`.

Note that there's a gotcha in the library, you cannot set it to `0`, since it will be converted to `-1`, which is `RequireAll`.

```go
    if config.RequiredAcks == 0 {
		w.RequiredAcks = RequireAll
	}
```

#### Set `Async`

From the `WriterConfig` documentation:

> Setting this flag to true causes the WriteMessages method to never block.
It also means that errors are ignored since the caller will not receive
the returned value. Use this only if you don't care about guarantees of
whether the messages were written to kafka.

Set `Async` to `true` so that after the message is pushed into the writer's queue, the publishing function will return immediately.

You can set it up by injecting the `WriterConfig` inside `BrokerConfig` when creating new `KafkaClient`.

#### Use more partitions
Segmentio/kafka-go creates one `partitionWriter` per partition. So theoretically with more partitions, the more writer it has to parallelize the writing process.

#### User more broker
Kafka will divide partitions between brokers, so with more brokers, each will handle less partition.

### Handling Partition Change

While by default segmentio/kafka-go can automatically rebalance Consumer Group assignments whenever there's a member change (e.g. new service pod is spawned), it has specific config to detect and handle partition change.

From the `ReaderConfig` documentation:

> WatchForPartitionChanges is used to inform kafka-go that a consumer group should be
polling the brokers and rebalancing if any partition changes happen to the topic.

Set `WatchPartitionChanges` in `ReaderConfig` to true to tell the library to handle partition changes automatically. It can be injected inside `BrokerConfig` when creating new `KafkaClient`.

# License
    Copyright © 2020, AccelByte Inc. Released under the Apache License, Version 2.0
