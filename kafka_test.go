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

	"github.com/mitchellh/mapstructure"
	"github.com/stretchr/testify/assert"
)

const (
	timeoutTest  = 30
	prefix       = "prefix"
	testPayload  = "testPayload"
	errorTimeout = "timeout while executing test"
)

type Payload struct {
	FriendID string `json:"friendId"`
}

func createKafkaClient(t *testing.T) *KafkaClient {
	t.Helper()

	brokerList := []string{"localhost:9092"}
	client, _ := NewKafkaClient(brokerList, prefix)
	return client
}

func timeout(t *testing.T, timeout int, timeoutChan chan bool) {
	t.Helper()
	timer := time.NewTimer(time.Duration(timeout) * time.Second)
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

func constructTopicTest() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("%s.%d", "testTopic", rand.Intn(100))
}

// nolint dupl
func TestKafkaPubSubSuccess(t *testing.T) {
	timeoutChan := make(chan bool, 1)
	doneChan := make(chan bool, 1)
	timeout(t, timeoutTest, timeoutChan)

	client := createKafkaClient(t)

	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := struct {
		ID        string                 `json:"id"`
		EventName string                 `json:"name"`
		Namespace string                 `json:"namespace"`
		ClientID  string                 `json:"clientId"`
		TraceID   string                 `json:"traceId"`
		UserID    string                 `json:"userId"`
		Timestamp time.Time              `json:"timestamp"`
		Version   string                 `json:"version"`
		Payload   map[string]interface{} `json:"payload"`
	}{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "client123",
		TraceID:   "trace123",
		UserID:    "user123",
		Version:   defaultVersion,
		Payload:   mockPayload,
	}

	client.Register(
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
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true
			}))

	client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			TraceID(mockEvent.TraceID).
			Context(context.Background()).
			Payload(mockPayload))

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
	timeout(t, timeoutTest, timeoutChan)

	client := createKafkaClient(t)

	topicName1, topicName2 := constructTopicTest(), constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := struct {
		ID        string                 `json:"id"`
		EventName string                 `json:"name"`
		Namespace string                 `json:"namespace"`
		ClientID  string                 `json:"clientId"`
		UserID    string                 `json:"userId"`
		TraceID   string                 `json:"traceId"`
		Timestamp time.Time              `json:"timestamp"`
		Version   string                 `json:"version"`
		Payload   map[string]interface{} `json:"payload"`
	}{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "client123",
		TraceID:   "trace123",
		UserID:    "user123",
		Version:   "0.2.0",
		Payload:   mockPayload,
	}

	client.Register(
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
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true
			}))

	client.Register(
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
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true
			}))

	client.Publish(
		NewPublish().
			Topic(topicName1, topicName2).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			TraceID(mockEvent.TraceID).
			Version("0.2.0").
			Context(context.Background()).
			Payload(mockPayload))

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
