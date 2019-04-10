[![Build Status](https://travis-ci.com/AccelByte/eventstream-go-sdk.svg?branch=master)](https://travis-ci.com/AccelByte/eventstream-go-sdk)

# eventstream-go-sdk
Go SDK for integrating with AccelByte's event stream

## Supported stream
Currently these stream are supported by this library:

### Kafka stream
This will publish an event in to Kafka stream. Each event type will be published into different topic by using the topic
field inside the event. For example, if event has `event.topic=update_user`, it will be pushed to `update_user` Kafka 
topic.

To create a client for publishing to Kafka stream, use this function:
```go
client, err := NewKafkaClient("realm-name", []string{"localhost:9092"})
``` 

### stdout stream
This stream is for testing purpose. This will print the event in stdout. It should not be used in production since this 
will print unnecessary log.

To create a client for stdout, use this function:
```go
client := NewStdoutClient("realm-name")
```

### blackhole
This is used when we don't want the service to send event data to anywhere.

To create a client for stdout, use this function:
```go
client := NewBlackholeClient()
```

## Usage

### Importing

```go
eventpublisher "github.com/AccelByte/eventstream-go-sdk"
```

### Creating an event

Although the Event is exposed, it's strongly recommended to not creating them by yourself. For that, we can use the 
`NewEvent()` function to make sure all required fields are not empty. 

To add additional fields, we can use `WithFields()` function. 

It's strongly recommended to create your own event constructor to wrap  the `NewEvent()` and `WithFields()` to force 
additional fields to be uniform. The example can be read in `/example` folder.

### Publishing an event
There are two type of event publishing in this library.

#### Synchronous
This type will publish an event in synchronous manner. The publish function will not return anything until the process
is done. The function will also return the status of the publishing.

```go
err := client.PublishEvent(event)
```

#### Asynchronous
This type will publish an event in asynchronous manner. The publish function will instantaneously return without having 
to wait the process. We can't catch the status of this publishing. However, if error occur, it will be logged. 

```go
client.PublishEventAsync(event)
```
