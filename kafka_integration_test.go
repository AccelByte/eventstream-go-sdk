// Copyright (c) 2019 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package eventstream

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/assert"
)

const (
	timeoutTest    = 120
	prefix         = "prefix"
	testPayload    = "testPayload"
	errorTimeout   = "timeout while executing test"
	errorPublish   = "error when publish event"
	errorSubscribe = "error when subscribe event"
)

type Payload struct {
	FriendID string `json:"friendId"`
}

func createKafkaClient(t *testing.T) Client {
	t.Helper()

	config := &BrokerConfig{
		LogMode:          OffLevel,
		StrictValidation: true,
	}

	brokerList := []string{"localhost:9092"}
	client, _ := NewClient(prefix, eventStreamKafka, brokerList, config)
	return client
}

func createInvalidKafkaClient(t *testing.T) Client {
	t.Helper()

	config := &BrokerConfig{
		LogMode:          DebugLevel,
		StrictValidation: true,
	}

	brokerList := []string{"invalidbroker:9092"}
	client, _ := NewClient(prefix, eventStreamKafka, brokerList, config)
	return client
}

func constructTopicTest() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("%s.%d", "testTopic", rand.Intn(1000))
}

func timeout(t *testing.T, timeoutChan chan bool) {
	t.Helper()
	timer := time.NewTimer(time.Duration(timeoutTest) * time.Second)
	go func() {
	loop:
		for {
			select {
			case <-timer.C:
				timeoutChan <- true
				break loop
			default:
				break
			}
		}
	}()
}

// nolint dupl
func TestKafkaPubSubSuccess(t *testing.T) {
	timeoutChan := make(chan bool, 1)
	doneChan := make(chan bool, 1)
	timeout(t, timeoutChan)

	client := createKafkaClient(t)

	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := &Event{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "7d480ce0e8624b02901bd80d9ba9817c",
		TraceID:   "01c34ec3b07f4bfaa59ba0184a3de14d",
		UserID:    "e95b150043ff4a2c88427a6eb25e5bc8",
		Version:   defaultVersion,
		Payload:   mockPayload,
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Callback(func(event *Event, err error) {
				var eventPayload Payload
				if err != nil {
					assert.Fail(t, "error when run callback")
				}
				if err = mapstructure.Decode(event.Payload[testPayload], &eventPayload); err != nil {
					assert.Fail(t, "unable to decode payload")
				}
				assert.Equal(t, mockEvent.EventName, event.EventName, "event name should be equal")
				assert.Equal(t, mockEvent.Namespace, event.Namespace, "namespace should be equal")
				assert.Equal(t, mockEvent.ClientID, event.ClientID, "client ID should be equal")
				assert.Equal(t, mockEvent.TraceID, event.TraceID, "trace ID should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true
			}))
	if err != nil {
		assert.FailNow(t, errorSubscribe, err)
		return
	}

	err = client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			Context(context.Background()).
			Payload(mockPayload))
	if err != nil {
		assert.FailNow(t, errorPublish, err)
		return
	}

	for {
		select {
		case <-doneChan:
			return
		case <-timeoutChan:
			assert.FailNow(t, errorTimeout)
		}
	}
}

// nolint dupl
func TestKafkaPubFailed(t *testing.T) {
	timeoutChan := make(chan bool, 1)
	doneChan := make(chan bool, 1)
	timeout(t, timeoutChan)

	client := createInvalidKafkaClient(t)

	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := &Event{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "7d480ce0e8624b02901bd80d9ba9817c",
		TraceID:   "01c34ec3b07f4bfaa59ba0184a3de14d",
		UserID:    "e95b150043ff4a2c88427a6eb25e5bc8",
		Version:   defaultVersion,
		Payload:   mockPayload,
	}

	errorCallback := func(event *Event, err error) {
		assert.NotNil(t, err, "error should not be nil")
		assert.Equal(t, mockEvent.EventName, event.EventName, "event name should be equal")
		assert.Equal(t, mockEvent.Namespace, event.Namespace, "namespace should be equal")
		assert.Equal(t, mockEvent.ClientID, event.ClientID, "client ID should be equal")
		assert.Equal(t, mockEvent.TraceID, event.TraceID, "trace ID should be equal")
		assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
		assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
		assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
		doneChan <- true
	}

	err := client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			Context(context.Background()).
			Payload(mockPayload).
			ErrorCallback(errorCallback))
	if err != nil {
		assert.FailNow(t, errorPublish, err)
		return
	}

	for {
		select {
		case <-doneChan:
			return
		case <-timeoutChan:
			assert.FailNow(t, errorTimeout)
		}
	}
}

// nolint dupl
func TestKafkaPubSubMultipleTopicSuccess(t *testing.T) {
	timeoutChan := make(chan bool, 1)
	doneChan := make(chan bool, 2)
	timeout(t, timeoutChan)

	client := createKafkaClient(t)

	topicName1, topicName2 := constructTopicTest(), constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := &Event{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "fe5bd0e3dc184d2d8ae0e09fcedf0f51",
		TraceID:   "882da8cddd174d12af25da6310b47bd5",
		UserID:    "48bf8a020b584f31bc605bf65d3300ed",
		SessionID: "c1ab4f754acc4cb48a8f68dd25cfca21",
		Version:   2,
		Payload:   mockPayload,
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName1).
			EventName(mockEvent.EventName).
			Callback(func(event *Event, err error) {
				var eventPayload Payload
				if err != nil {
					assert.Fail(t, "error when run callback 1")
				}
				if err = mapstructure.Decode(event.Payload[testPayload], &eventPayload); err != nil {
					assert.Fail(t, "unable to decode payload")
				}
				assert.Equal(t, mockEvent.EventName, event.EventName, "event name should be equal")
				assert.Equal(t, mockEvent.Namespace, event.Namespace, "namespace should be equal")
				assert.Equal(t, mockEvent.ClientID, event.ClientID, "client ID should be equal")
				assert.Equal(t, mockEvent.TraceID, event.TraceID, "trace ID should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true
			}))
	if err != nil {
		assert.Fail(t, errorSubscribe, err)
		return
	}

	err = client.Register(
		NewSubscribe().
			Topic(topicName2).
			EventName(mockEvent.EventName).
			Callback(func(event *Event, err error) {
				var eventPayload Payload
				if err != nil {
					assert.Fail(t, "error when run callback 2")
				}
				if err = mapstructure.Decode(event.Payload[testPayload], &eventPayload); err != nil {
					assert.Fail(t, "unable to decode payload")
				}
				assert.Equal(t, mockEvent.EventName, event.EventName, "event name should be equal")
				assert.Equal(t, mockEvent.Namespace, event.Namespace, "namespace should be equal")
				assert.Equal(t, mockEvent.ClientID, event.ClientID, "client ID should be equal")
				assert.Equal(t, mockEvent.TraceID, event.TraceID, "trace ID should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true
			}))
	if err != nil {
		assert.Fail(t, errorSubscribe, err)
		return
	}

	err = client.Publish(
		NewPublish().
			Topic(topicName1, topicName2).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			Version(2).
			Context(context.Background()).
			Payload(mockPayload))
	if err != nil {
		assert.Fail(t, errorPublish, err)
		return
	}

	doneItr := 0
	for {
		select {
		case <-doneChan:
			doneItr++
			if doneItr == 2 {
				return
			}
		case <-timeoutChan:
			assert.FailNow(t, errorTimeout)
		}
	}
}

// nolint dupl
func TestKafkaPubSubDifferentGroupID(t *testing.T) {
	timeoutChan := make(chan bool, 1)
	doneChan := make(chan bool, 2)
	timeout(t, timeoutChan)

	client := createKafkaClient(t)

	topicName := constructTopicTest()
	groupID, groupID2 := generateID(), generateID()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := &Event{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "6ab512c877c64d06911b4772fede4dd1",
		TraceID:   "2a44c482cd444f7cae29e90adb701315",
		UserID:    "f13db76e044f43d988a2df1c7ea0000f",
		SessionID: "77a0313e89a74c0684aacc1dc80329e6",
		Version:   2,
		Payload:   mockPayload,
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			GroupID(groupID).
			Callback(func(event *Event, err error) {
				var eventPayload Payload
				if err != nil {
					assert.Fail(t, "error when run callback 1")
				}
				if err = mapstructure.Decode(event.Payload[testPayload], &eventPayload); err != nil {
					assert.Fail(t, "unable to decode payload")
				}
				assert.Equal(t, mockEvent.EventName, event.EventName, "event name should be equal")
				assert.Equal(t, mockEvent.Namespace, event.Namespace, "namespace should be equal")
				assert.Equal(t, mockEvent.ClientID, event.ClientID, "client ID should be equal")
				assert.Equal(t, mockEvent.TraceID, event.TraceID, "trace ID should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true
			}))
	if err != nil {
		assert.Fail(t, errorSubscribe, err)
		return
	}

	err = client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			GroupID(groupID2).
			Callback(func(event *Event, err error) {
				var eventPayload Payload
				if err != nil {
					assert.Fail(t, "error when run callback 2")
				}
				if err = mapstructure.Decode(event.Payload[testPayload], &eventPayload); err != nil {
					assert.Fail(t, "unable to decode payload")
				}
				assert.Equal(t, mockEvent.EventName, event.EventName, "event name should be equal")
				assert.Equal(t, mockEvent.Namespace, event.Namespace, "namespace should be equal")
				assert.Equal(t, mockEvent.ClientID, event.ClientID, "client ID should be equal")
				assert.Equal(t, mockEvent.TraceID, event.TraceID, "trace ID should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true
			}))
	if err != nil {
		assert.Fail(t, errorSubscribe, err)
		return
	}

	err = client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			Version(2).
			Context(context.Background()).
			Payload(mockPayload))
	if err != nil {
		assert.Fail(t, errorPublish, err)
		return
	}

	doneItr := 0
	for {
		select {
		case <-doneChan:
			doneItr++
			if doneItr == 2 {
				return
			}
		case <-timeoutChan:
			assert.FailNow(t, errorTimeout)
		}
	}
}

// nolint dupl
func TestKafkaPubSubSameGroupID(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	timeoutChan := make(chan bool, 1)
	doneChan := make(chan bool, 1)
	timeout(t, timeoutChan)

	client := createKafkaClient(t)

	topicName := constructTopicTest()
	groupID := generateID()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := &Event{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "269b3ade83dd45ebbb896609bf10fe03",
		TraceID:   "b4a410fb53d2448b8648ba0c58f09ce4",
		UserID:    "71895627426741148ad2d85399c53d71",
		Version:   2,
		Payload:   mockPayload,
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			GroupID(groupID).
			Callback(func(event *Event, err error) {
				var eventPayload Payload
				if err != nil {
					assert.Fail(t, "error when run callback 1")
				}
				if err = mapstructure.Decode(event.Payload[testPayload], &eventPayload); err != nil {
					assert.Fail(t, "unable to decode payload")
				}
				assert.Equal(t, mockEvent.EventName, event.EventName, "event name should be equal")
				assert.Equal(t, mockEvent.Namespace, event.Namespace, "namespace should be equal")
				assert.Equal(t, mockEvent.ClientID, event.ClientID, "client ID should be equal")
				assert.Equal(t, mockEvent.TraceID, event.TraceID, "trace ID should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true
			}))
	if err != nil {
		assert.Fail(t, errorSubscribe, err)
		return
	}

	err = client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			GroupID(groupID).
			Callback(func(event *Event, err error) {
				var eventPayload Payload
				if err != nil {
					assert.Fail(t, "error when run callback 2")
				}
				if err = mapstructure.Decode(event.Payload[testPayload], &eventPayload); err != nil {
					assert.Fail(t, "unable to decode payload")
				}
				assert.Equal(t, mockEvent.EventName, event.EventName, "event name should be equal")
				assert.Equal(t, mockEvent.Namespace, event.Namespace, "namespace should be equal")
				assert.Equal(t, mockEvent.ClientID, event.ClientID, "client ID should be equal")
				assert.Equal(t, mockEvent.TraceID, event.TraceID, "trace ID should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true
			}))
	if err != nil {
		assert.Fail(t, errorSubscribe, err)
		return
	}

	err = client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			Version(2).
			Context(context.Background()).
			Payload(mockPayload))
	if err != nil {
		assert.Fail(t, errorPublish, err)
		return
	}

	for {
		select {
		case <-doneChan:
			return
		case <-timeoutChan:
			assert.FailNow(t, errorTimeout)
		}
	}
}

func TestKafkaRegisterMultipleSubscriberCallbackSuccess(t *testing.T) {
	timeoutChan := make(chan bool, 1)
	doneChan := make(chan bool, 1)
	timeout(t, timeoutChan)

	client := createKafkaClient(t)

	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = "testPayload"
	mockEvent := &Event{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "7d480ce0e8624b02901bd80d9ba9817c",
		TraceID:   "01c34ec3b07f4bfaa59ba0184a3de14d",
		UserID:    "e95b150043ff4a2c88427a6eb25e5bc8",
		Version:   defaultVersion,
		Payload:   mockPayload,
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Callback(func(_ *Event, err error) {
				assert.NoError(t, err, "there's error right before event consumed: %v", err)
				doneChan <- true
			}))
	if err != nil {
		assert.FailNow(t, errorSubscribe, err)
		return
	}

	err = client.Register(
		NewSubscribe().
			Topic("anotherevent").
			EventName(mockEvent.EventName).
			Callback(func(_ *Event, err error) {
				assert.NoError(t, err, "there's error right before event consumed: %v", err)
				// just to test subscriber, no need any  action here
				doneChan <- true
			}))
	if err != nil {
		assert.FailNow(t, errorSubscribe, err)
		return
	}

	err = client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			Context(context.Background()).
			Payload(mockPayload))
	if err != nil {
		assert.FailNow(t, errorPublish, err)
		return
	}

	for {
		select {
		case <-doneChan:
			return
		case <-timeoutChan:
			assert.FailNow(t, errorTimeout)
		}
	}
}

func TestKafkaUnregisterTopicSuccess(t *testing.T) {
	timeoutChan := make(chan bool, 1)
	doneChan := make(chan bool, 1)
	timeout(t, timeoutChan)

	client := createKafkaClient(t)

	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = "testPayload"
	mockEvent := &Event{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "7d480ce0e8624b02901bd80d9ba9817c",
		TraceID:   "01c34ec3b07f4bfaa59ba0184a3de14d",
		UserID:    "e95b150043ff4a2c88427a6eb25e5bc8",
		Version:   defaultVersion,
		Payload:   mockPayload,
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Callback(func(_ *Event, err error) {
				assert.NoError(t, err, "there's error right before event consumed: %v", err)
				doneChan <- true
			}))
	if err != nil {
		assert.FailNow(t, errorSubscribe, err)
		return
	}

	err = client.Register(
		NewSubscribe().
			Topic("anotherevent").
			EventName(mockEvent.EventName).
			Callback(func(_ *Event, err error) {
				assert.NoError(t, err, "there's error right before event consumed: %v", err)
				// just to test subscriber, no need any  action here
				doneChan <- true
			}))
	if err != nil {
		assert.FailNow(t, errorSubscribe, err)
		return
	}

	client.Unregister("anotherevent")

	err = client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			Context(context.Background()).
			Payload(mockPayload))
	if err != nil {
		assert.FailNow(t, errorPublish, err)
		return
	}

	for {
		select {
		case <-doneChan:
			return
		case <-timeoutChan:
			assert.FailNow(t, errorTimeout)
		}
	}
}
