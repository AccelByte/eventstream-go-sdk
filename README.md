[![Build Status](https://travis-ci.com/AccelByte/eventstream-go-sdk.svg?branch=master)](https://travis-ci.com/AccelByte/eventstream-go-sdk)

# eventstream-go-sdk
Go SDK for integrating with AccelByte's event stream

## Usage

### Install

```
go get -u github.com/AccelByte/eventstream-go-sdk
```

### Importing

```go
eventstream "github.com/AccelByte/eventstream-go-sdk"
```


## Supported Stream
Currently event stream are supported by these stream:

### Kafka Stream
Publish and subscribe an event to / from Kafka stream. 

currently compatible with golang version from 1.12+ and Kafka versions from 0.10.1.0 to 2.1.0.

To create a new Kafka stream client, use this function:
```go
client, err := NewKafkaClient(brokers []string, prefix string, config ...*KafkaConfig)
``` 
``NewKafkaClient`` requires 3 parameters :
 * brokers : List of kafka broker (array of string)
 * prefix : Topic prefix from client (string)
 * config : Custom kafka configuration from client. 
 This is optional and only uses the first arguments. (variadic KafkaConfig)   

#### Publish
Publish or sent an event into kafka stream. Client able to publish an event into single or multiple topic.
Publish support with exponential backoff retry. (max 3x)

To publish an event, use this function:
```go
client.Publish(
		NewPublish().
			Topic(TopicName).
			EventName(EventName).
			Namespace(Namespace).
			ClientID(ClientID).
			UserID(UserID).
			TraceID(TraceID).
			Context(Context).
			Payload(Payload))
```

#### Subscribe
To subscribe an event from specific topic, client should be register a callback function that executed once event received.
A callback function has specific topic and event name.

To subscribe an event, use this function:
```go
client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Context(Context).
			Callback(func(event *Event, err error) {}))
```

Note: Callback function should be ``func(event *Event, err error){}``. ``event`` is object that store event message 
and ``err`` is an error that happen when consume the message.

#### Custom Configuration
SDK support with custom configuration for kafka stream, that is :

* DialTimeout : Timeout duration during connecting to kafka broker (time.Duration)
* ReadTimeout : Timeout duration during consume topic from kafka broker (time.Duration)
* WriteTimeout : Timeout duration during publish event to kafka broker (time.Duration) 

### Stdout Stream
This stream is for testing purpose. This will print the event in stdout. It should not be used in production since this 
will print unnecessary log.

To create a client for stdout, use this function:
```go
client, err := NewStdoutClient(prefix string)
```

### Blackhole
This is used when client don't want the service to send event data to anywhere.

To create a client for stdout, use this function:
```go
client, err := NewBlackholeClient()
```

## Event Message
Event message is a set of event information that would be publish or consume by client.

Event message format :
* id : Event ID with UUID format (string)
* name : Event name (string)
* namespace : Event namespace (string)
* traceId : Trace ID (string)
* clientId : Publisher client ID (string)
* userId : Publisher user ID (string)
* timestamp : Event time (time.Time)
* version : Event schema version (string)
* payload : Set of data / object that given by producer. Each data have own key for specific purpose. (map[string]interface{})
