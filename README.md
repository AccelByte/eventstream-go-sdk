[![Build Status](https://travis-ci.com/AccelByte/eventstream-go-sdk.svg?branch=master)](https://travis-ci.com/AccelByte/eventstream-go-sdk)

# eventstream-go-sdk
Go SDK for integrating with AccelByte's event stream

Currently these stream are supported by this library:

## Kafka stream
This will publish an event in to Kafka stream. Each event type will be published into different topic by using this 
format: `topic_<event_type>`. So for example if event has `event_type=1`, it will be pushed to `topic_1` Kafka topic.

## stdout stream
This stream is for testing purpose. This will print the event in stdout. It should not be used in production since this 
will print unnecessary log.

## blackhole
This is used when we don't want the service to send event data to anywhere. 