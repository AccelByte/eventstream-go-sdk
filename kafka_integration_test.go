/*
 * Copyright (c) 2020 AccelByte Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package eventstream

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	timeoutTest    = 60
	prefix         = "prefix"
	testPayload    = "testPayload"
	testKey        = "testKey"
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
		CACertFile:       "",
		StrictValidation: true,
		DialTimeout:      2 * time.Second,
	}

	brokerList := []string{"localhost:9094"}
	client, _ := NewClient(prefix, eventStreamKafka, brokerList, config)

	return client
}

func createInvalidKafkaClient(t *testing.T) Client {
	t.Helper()

	config := &BrokerConfig{
		CACertFile:       "",
		StrictValidation: true,
		DialTimeout:      time.Second,
	}

	brokerList := []string{"invalidbroker:9092"}
	client, _ := NewClient(prefix, eventStreamKafka, brokerList, config)

	return client
}

func constructTopicTest() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("%s.%s.%s", "testTopic", callerFuncName(), generateID()) // nolint:gomnd
}

// nolint dupl
func TestKafkaPubSubSuccess(t *testing.T) {
	t.Parallel()
	ctx, done := context.WithTimeout(context.Background(), time.Duration(timeoutTest)*time.Second)
	defer done()

	logrus.SetLevel(logrus.DebugLevel)

	doneChan := make(chan bool, 1)

	client := createKafkaClient(t)

	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: fmt.Sprintf("user-%d", rand.Int63())}

	mockAdditionalFields := map[string]interface{}{
		"summary": "user:_failed",
	}

	mockEvent := &Event{
		EventName:        fmt.Sprintf("testEvent-%d", rand.Int63()),
		Namespace:        "event",
		ClientID:         "7d480ce0e8624b02901bd80d9ba9817c",
		TraceID:          "01c34ec3b07f4bfaa59ba0184a3de14d",
		SpanContext:      "test-span-id",
		UserID:           "e95b150043ff4a2c88427a6eb25e5bc8",
		EventID:          3,
		EventType:        301,
		EventLevel:       3,
		ServiceName:      "test",
		ClientIDs:        []string{"7d480ce0e8624b02901bd80d9ba9817c"},
		TargetUserIDs:    []string{"1fe7f425a0e049d29d87ca3d32e45b5a"},
		TargetNamespace:  "publisher",
		Privacy:          true,
		AdditionalFields: mockAdditionalFields,
		Version:          defaultVersion,
		Key:              testKey,
		Payload:          mockPayload,
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			GroupID(generateID()).
			Offset(0).
			Context(ctx).
			Callback(func(ctx context.Context, event *Event, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

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
				assert.Equal(t, mockEvent.SpanContext, event.SpanContext, "span context should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.EventID, event.EventID, "EventID should be equal")
				assert.Equal(t, mockEvent.EventType, event.EventType, "EventType should be equal")
				assert.Equal(t, mockEvent.EventLevel, event.EventLevel, "EventLevel should be equal")
				assert.Equal(t, mockEvent.ServiceName, event.ServiceName, "ServiceName should be equal")
				assert.Equal(t, mockEvent.ClientIDs, event.ClientIDs, "ClientIDs should be equal")
				assert.Equal(t, mockEvent.TargetUserIDs, event.TargetUserIDs, "TargetUserIDs should be equal")
				assert.Equal(t, mockEvent.TargetNamespace, event.TargetNamespace, "TargetNamespace should be equal")
				assert.Equal(t, mockEvent.Privacy, event.Privacy, "Privacy should be equal")
				assert.Equal(t, mockEvent.AdditionalFields, event.AdditionalFields, "AdditionalFields should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				assert.Equal(t, mockEvent.Key, event.Key, "key should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true

				return nil
			}))
	require.NoError(t, err)

	go func() {
		for i := 0; i < 20; i++ {
			err = client.Publish(
				NewPublish().
					Topic(topicName).
					EventName(mockEvent.EventName).
					Namespace(mockEvent.Namespace).
					ClientID(mockEvent.ClientID).
					UserID(mockEvent.UserID).
					SessionID(mockEvent.SessionID).
					TraceID(mockEvent.TraceID).
					SpanContext(mockEvent.SpanContext).
					Context(context.Background()).
					EventID(mockEvent.EventID).
					EventType(mockEvent.EventType).
					EventLevel(mockEvent.EventLevel).
					ServiceName(mockEvent.ServiceName).
					ClientIDs(mockEvent.ClientIDs).
					TargetUserIDs(mockEvent.TargetUserIDs).
					TargetNamespace(mockEvent.TargetNamespace).
					Privacy(mockEvent.Privacy).
					AdditionalFields(mockEvent.AdditionalFields).
					Key(mockEvent.Key).
					Timeout(10 * time.Second).
					Payload(mockPayload))
			time.Sleep(time.Millisecond * 5)
			if err != nil {
				assert.FailNow(t, errorPublish, err)
				return
			}
		}
	}()

	doneCount := 0
	for {
		select {
		case <-doneChan:
			doneCount++
			if doneCount == 20 {
				return
			}
		case <-ctx.Done():
			assert.FailNow(t, errorTimeout)
		}
	}
}

// nolint dupl
func TestKafkaGroupIDNotSpecifiedSuccess(t *testing.T) {
	t.Parallel()
	ctx, done := context.WithTimeout(context.Background(), time.Duration(timeoutTest)*time.Second)
	defer done()

	doneChan := make(chan bool, 1)

	client := createKafkaClient(t)

	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}

	mockAdditionalFields := map[string]interface{}{
		"summary": "user:_failed",
	}

	mockEvent := &Event{
		EventName:        "testEvent",
		Namespace:        "event",
		ClientID:         "7d480ce0e8624b02901bd80d9ba9817c",
		TraceID:          "01c34ec3b07f4bfaa59ba0184a3de14d",
		SpanContext:      "test-span-id",
		UserID:           "e95b150043ff4a2c88427a6eb25e5bc8",
		EventID:          3,
		EventType:        301,
		EventLevel:       3,
		ServiceName:      "test",
		ClientIDs:        []string{"7d480ce0e8624b02901bd80d9ba9817c"},
		TargetUserIDs:    []string{"1fe7f425a0e049d29d87ca3d32e45b5a"},
		TargetNamespace:  "publisher",
		Privacy:          true,
		AdditionalFields: mockAdditionalFields,
		Version:          defaultVersion,
		Key:              testKey,
		Payload:          mockPayload,
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Offset(0).
			Context(ctx).
			Callback(func(ctx context.Context, event *Event, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

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
				assert.Equal(t, mockEvent.SpanContext, event.SpanContext, "span context should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.EventID, event.EventID, "EventID should be equal")
				assert.Equal(t, mockEvent.EventType, event.EventType, "EventType should be equal")
				assert.Equal(t, mockEvent.EventLevel, event.EventLevel, "EventLevel should be equal")
				assert.Equal(t, mockEvent.ServiceName, event.ServiceName, "ServiceName should be equal")
				assert.Equal(t, mockEvent.ClientIDs, event.ClientIDs, "ClientIDs should be equal")
				assert.Equal(t, mockEvent.TargetUserIDs, event.TargetUserIDs, "TargetUserIDs should be equal")
				assert.Equal(t, mockEvent.TargetNamespace, event.TargetNamespace, "TargetNamespace should be equal")
				assert.Equal(t, mockEvent.Privacy, event.Privacy, "Privacy should be equal")
				assert.Equal(t, mockEvent.AdditionalFields, event.AdditionalFields, "AdditionalFields should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				assert.Equal(t, mockEvent.Key, event.Key, "key should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true

				return nil
			}))
	require.NoError(t, err)

	err = client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName + "Other").
			Offset(0).
			Context(ctx).
			Callback(func(ctx context.Context, event *Event, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

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
				assert.Equal(t, mockEvent.SpanContext, event.SpanContext, "span context should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.EventID, event.EventID, "EventID should be equal")
				assert.Equal(t, mockEvent.EventType, event.EventType, "EventType should be equal")
				assert.Equal(t, mockEvent.EventLevel, event.EventLevel, "EventLevel should be equal")
				assert.Equal(t, mockEvent.ServiceName, event.ServiceName, "ServiceName should be equal")
				assert.Equal(t, mockEvent.ClientIDs, event.ClientIDs, "ClientIDs should be equal")
				assert.Equal(t, mockEvent.TargetUserIDs, event.TargetUserIDs, "TargetUserIDs should be equal")
				assert.Equal(t, mockEvent.TargetNamespace, event.TargetNamespace, "TargetNamespace should be equal")
				assert.Equal(t, mockEvent.Privacy, event.Privacy, "Privacy should be equal")
				assert.Equal(t, mockEvent.AdditionalFields, event.AdditionalFields, "AdditionalFields should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				assert.Equal(t, mockEvent.Key, event.Key, "key should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true

				return nil
			}))
	require.NoError(t, err)

	err = client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			SpanContext(mockEvent.SpanContext).
			Context(context.Background()).
			EventID(mockEvent.EventID).
			EventType(mockEvent.EventType).
			EventLevel(mockEvent.EventLevel).
			ServiceName(mockEvent.ServiceName).
			ClientIDs(mockEvent.ClientIDs).
			TargetUserIDs(mockEvent.TargetUserIDs).
			TargetNamespace(mockEvent.TargetNamespace).
			Privacy(mockEvent.Privacy).
			AdditionalFields(mockEvent.AdditionalFields).
			Key(testKey).
			Payload(mockPayload))
	if err != nil {
		assert.FailNow(t, errorPublish, err)
		return
	}

	select {
	case <-doneChan:
		return
	case <-ctx.Done():
		assert.FailNow(t, errorTimeout)
	}
}

// nolint dupl
func TestKafkaPubFailed(t *testing.T) {
	t.Parallel()
	ctx, done := context.WithTimeout(context.Background(), time.Duration(timeoutTest)*time.Second)
	defer done()

	client := createInvalidKafkaClient(t)

	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}

	mockAdditionalFields := map[string]interface{}{
		"summary": "user:_failed",
	}

	mockEvent := &Event{
		EventName:        "testEvent",
		Namespace:        "event",
		ClientID:         "7d480ce0e8624b02901bd80d9ba9817c",
		TraceID:          "01c34ec3b07f4bfaa59ba0184a3de14d",
		SpanContext:      "test-span-context",
		UserID:           "e95b150043ff4a2c88427a6eb25e5bc8",
		EventID:          3,
		EventType:        301,
		EventLevel:       3,
		ServiceName:      "test",
		ClientIDs:        []string{"7d480ce0e8624b02901bd80d9ba9817c"},
		TargetUserIDs:    []string{"1fe7f425a0e049d29d87ca3d32e45b5a"},
		TargetNamespace:  "publisher",
		Privacy:          true,
		AdditionalFields: mockAdditionalFields,
		Version:          defaultVersion,
		Payload:          mockPayload,
	}

	eventCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	err := client.PublishSync(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			SpanContext(mockEvent.SpanContext).
			Context(eventCtx).
			EventID(mockEvent.EventID).
			EventType(mockEvent.EventType).
			EventLevel(mockEvent.EventLevel).
			ServiceName(mockEvent.ServiceName).
			ClientIDs(mockEvent.ClientIDs).
			TargetUserIDs(mockEvent.TargetUserIDs).
			TargetNamespace(mockEvent.TargetNamespace).
			Privacy(mockEvent.Privacy).
			AdditionalFields(mockAdditionalFields).
			Payload(mockPayload).
			Timeout(time.Second))
	assert.Error(t, err)
}

// nolint dupl
func TestKafkaPubSubDifferentGroupID(t *testing.T) {
	t.Parallel()
	ctx, done := context.WithTimeout(context.Background(), time.Duration(timeoutTest)*time.Second)
	defer done()

	doneChan := make(chan bool, 2)

	client := createKafkaClient(t)

	topicName := constructTopicTest()
	groupID, groupID2 := generateID(), generateID()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}

	mockAdditionalFields := map[string]interface{}{
		"summary": "user:_failed",
	}

	mockEvent := &Event{
		EventName:        "testEvent",
		Namespace:        "event",
		ClientID:         "6ab512c877c64d06911b4772fede4dd1",
		TraceID:          "2a44c482cd444f7cae29e90adb701315",
		SpanContext:      "test-span-context",
		UserID:           "f13db76e044f43d988a2df1c7ea0000f",
		SessionID:        "77a0313e89a74c0684aacc1dc80329e6",
		EventID:          3,
		EventType:        301,
		EventLevel:       3,
		ServiceName:      "test",
		ClientIDs:        []string{"7d480ce0e8624b02901bd80d9ba9817c"},
		TargetUserIDs:    []string{"1fe7f425a0e049d29d87ca3d32e45b5a"},
		TargetNamespace:  "publisher",
		Privacy:          true,
		AdditionalFields: mockAdditionalFields,
		Version:          2,
		Payload:          mockPayload,
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			GroupID(groupID).
			Offset(0).
			Context(ctx).
			Callback(func(ctx context.Context, event *Event, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

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
				assert.Equal(t, mockEvent.SpanContext, event.SpanContext, "span context should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.EventID, event.EventID, "EventID should be equal")
				assert.Equal(t, mockEvent.EventType, event.EventType, "EventType should be equal")
				assert.Equal(t, mockEvent.EventLevel, event.EventLevel, "EventLevel should be equal")
				assert.Equal(t, mockEvent.ServiceName, event.ServiceName, "ServiceName should be equal")
				assert.Equal(t, mockEvent.ClientIDs, event.ClientIDs, "ClientIDs should be equal")
				assert.Equal(t, mockEvent.TargetUserIDs, event.TargetUserIDs, "TargetUserIDs should be equal")
				assert.Equal(t, mockEvent.TargetNamespace, event.TargetNamespace, "TargetNamespace should be equal")
				assert.Equal(t, mockEvent.Privacy, event.Privacy, "Privacy should be equal")
				assert.Equal(t, mockEvent.AdditionalFields, event.AdditionalFields, "AdditionalFields should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true

				return nil
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
			Offset(0).
			Context(ctx).
			Callback(func(ctx context.Context, event *Event, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

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
				assert.Equal(t, mockEvent.SpanContext, event.SpanContext, "span context should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.EventID, event.EventID, "EventID should be equal")
				assert.Equal(t, mockEvent.EventType, event.EventType, "EventType should be equal")
				assert.Equal(t, mockEvent.EventLevel, event.EventLevel, "EventLevel should be equal")
				assert.Equal(t, mockEvent.ServiceName, event.ServiceName, "ServiceName should be equal")
				assert.Equal(t, mockEvent.ClientIDs, event.ClientIDs, "ClientIDs should be equal")
				assert.Equal(t, mockEvent.TargetUserIDs, event.TargetUserIDs, "TargetUserIDs should be equal")
				assert.Equal(t, mockEvent.TargetNamespace, event.TargetNamespace, "TargetNamespace should be equal")
				assert.Equal(t, mockEvent.Privacy, event.Privacy, "Privacy should be equal")
				assert.Equal(t, mockEvent.AdditionalFields, event.AdditionalFields, "AdditionalFields should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true

				return nil
			}))
	require.NoError(t, err)

	err = client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			SpanContext(mockEvent.SpanContext).
			EventID(mockEvent.EventID).
			EventType(mockEvent.EventType).
			EventLevel(mockEvent.EventLevel).
			ServiceName(mockEvent.ServiceName).
			ClientIDs(mockEvent.ClientIDs).
			TargetUserIDs(mockEvent.TargetUserIDs).
			TargetNamespace(mockEvent.TargetNamespace).
			Privacy(mockEvent.Privacy).
			AdditionalFields(mockEvent.AdditionalFields).
			Version(2).
			Context(context.Background()).
			Timeout(10 * time.Second).
			Payload(mockPayload))
	if err != nil {
		assert.Fail(t, errorPublish, err)
		return
	}

	doneItr := 0
	select {
	case <-doneChan:
		doneItr++
		if doneItr == 2 {
			return
		}
	case <-ctx.Done():
		assert.FailNow(t, errorTimeout)
	}
}

// nolint dupl
func TestKafkaPubSubSameGroupID(t *testing.T) {
	t.Parallel()
	ctx, done := context.WithTimeout(context.Background(), time.Duration(timeoutTest)*time.Second)
	defer done()

	doneChan := make(chan bool, 2)

	client := createKafkaClient(t)

	topicName := constructTopicTest()
	groupID := generateID()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}

	mockAdditionalFields := map[string]interface{}{
		"summary": "user:_failed",
	}

	mockEvent := &Event{
		EventName:        "testEvent",
		Namespace:        "event",
		ClientID:         "269b3ade83dd45ebbb896609bf10fe03",
		TraceID:          "b4a410fb53d2448b8648ba0c58f09ce4",
		SpanContext:      "test-span-context",
		UserID:           "71895627426741148ad2d85399c53d71",
		EventID:          3,
		EventType:        301,
		EventLevel:       3,
		ServiceName:      "test",
		ClientIDs:        []string{"7d480ce0e8624b02901bd80d9ba9817c"},
		TargetUserIDs:    []string{"1fe7f425a0e049d29d87ca3d32e45b5a"},
		TargetNamespace:  "publisher",
		Privacy:          true,
		AdditionalFields: mockAdditionalFields,
		Version:          2,
		Payload:          mockPayload,
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			GroupID(groupID).
			Offset(0).
			Context(ctx).
			Callback(func(ctx context.Context, event *Event, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

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
				assert.Equal(t, mockEvent.SpanContext, event.SpanContext, "span context should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.EventID, event.EventID, "EventID should be equal")
				assert.Equal(t, mockEvent.EventType, event.EventType, "EventType should be equal")
				assert.Equal(t, mockEvent.EventLevel, event.EventLevel, "EventLevel should be equal")
				assert.Equal(t, mockEvent.ServiceName, event.ServiceName, "ServiceName should be equal")
				assert.Equal(t, mockEvent.ClientIDs, event.ClientIDs, "ClientIDs should be equal")
				assert.Equal(t, mockEvent.TargetUserIDs, event.TargetUserIDs, "TargetUserIDs should be equal")
				assert.Equal(t, mockEvent.TargetNamespace, event.TargetNamespace, "TargetNamespace should be equal")
				assert.Equal(t, mockEvent.Privacy, event.Privacy, "Privacy should be equal")
				assert.Equal(t, mockEvent.AdditionalFields, event.AdditionalFields, "AdditionalFields should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true

				return nil
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
			Offset(0).
			Context(ctx).
			Callback(func(ctx context.Context, event *Event, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

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
				assert.Equal(t, mockEvent.SpanContext, event.SpanContext, "span context should be equal")
				assert.Equal(t, mockEvent.UserID, event.UserID, "user ID should be equal")
				assert.Equal(t, mockEvent.SessionID, event.SessionID, "session ID should be equal")
				assert.Equal(t, mockEvent.EventID, event.EventID, "EventID should be equal")
				assert.Equal(t, mockEvent.EventType, event.EventType, "EventType should be equal")
				assert.Equal(t, mockEvent.EventLevel, event.EventLevel, "EventLevel should be equal")
				assert.Equal(t, mockEvent.ServiceName, event.ServiceName, "ServiceName should be equal")
				assert.Equal(t, mockEvent.ClientIDs, event.ClientIDs, "ClientIDs should be equal")
				assert.Equal(t, mockEvent.TargetUserIDs, event.TargetUserIDs, "TargetUserIDs should be equal")
				assert.Equal(t, mockEvent.TargetNamespace, event.TargetNamespace, "TargetNamespace should be equal")
				assert.Equal(t, mockEvent.Privacy, event.Privacy, "Privacy should be equal")
				assert.Equal(t, mockEvent.AdditionalFields, event.AdditionalFields, "AdditionalFields should be equal")
				assert.Equal(t, mockEvent.Version, event.Version, "version should be equal")
				if validPayload := reflect.DeepEqual(mockEvent.Payload[testPayload].(Payload), eventPayload); !validPayload {
					assert.Fail(t, "payload should be equal")
				}
				doneChan <- true

				return nil
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
			SpanContext(mockEvent.SpanContext).
			EventID(mockEvent.EventID).
			EventType(mockEvent.EventType).
			EventLevel(mockEvent.EventLevel).
			ServiceName(mockEvent.ServiceName).
			ClientIDs(mockEvent.ClientIDs).
			TargetUserIDs(mockEvent.TargetUserIDs).
			TargetNamespace(mockEvent.TargetNamespace).
			Privacy(mockEvent.Privacy).
			AdditionalFields(mockEvent.AdditionalFields).
			Version(2).
			Context(context.Background()).
			Payload(mockPayload))
	if err != nil {
		assert.Fail(t, errorPublish, err)
		return
	}

	awaitDurationTimer := time.NewTimer(time.Duration(timeoutTest) * time.Second / 2)
	defer awaitDurationTimer.Stop()

	doneItr := 0
L:
	for {
		select {
		case <-doneChan:
			doneItr++
			if doneItr > 1 {
				// expected to receive only one message
				break L
			}
		case <-awaitDurationTimer.C:
			// enough, let's go to check the number of triggered callbacks
			break L
		}
	}

	// only one subscriber should receive the event because there is the same subscription-group
	require.Equal(t, 1, doneItr)
}

// nolint:funlen
func TestKafkaRegisterMultipleSubscriberCallbackSuccess(t *testing.T) {
	t.Parallel()

	ctx, done := context.WithTimeout(context.Background(), time.Duration(timeoutTest)*time.Second)
	defer done()

	doneChan := make(chan bool, 1)

	client := createKafkaClient(t)

	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = "testPayload"

	mockAdditionalFields := map[string]interface{}{
		"summary": "user:_failed",
	}

	mockEvent := &Event{
		EventName:        "testEvent",
		Namespace:        "event",
		ClientID:         "7d480ce0e8624b02901bd80d9ba9817c",
		TraceID:          "01c34ec3b07f4bfaa59ba0184a3de14d",
		SpanContext:      "test-span-context",
		UserID:           "e95b150043ff4a2c88427a6eb25e5bc8",
		EventID:          3,   // nolint:gomnd
		EventType:        301, // nolint:gomnd
		EventLevel:       3,   //nolint:gomnd
		ServiceName:      "test",
		ClientIDs:        []string{"7d480ce0e8624b02901bd80d9ba9817c"},
		TargetUserIDs:    []string{"1fe7f425a0e049d29d87ca3d32e45b5a"},
		TargetNamespace:  "publisher",
		Privacy:          true,
		AdditionalFields: mockAdditionalFields,
		Version:          defaultVersion,
		Payload:          mockPayload,
	}

	groupID := generateID()
	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			GroupID(groupID).
			Offset(0).
			Context(ctx).
			Callback(func(ctx context.Context, _ *Event, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				if err != nil {
					return err
				}

				doneChan <- true

				return nil
			}))
	require.NoError(t, err)

	err = client.Register(
		NewSubscribe().
			Topic(constructTopicTest()). // new random topic
			EventName(mockEvent.EventName).
			GroupID(groupID).
			Offset(0).
			Context(ctx).
			Callback(func(ctx context.Context, _ *Event, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				if err != nil {
					return err
				}

				// just to test subscriber, no need any actions here
				doneChan <- true

				return nil
			}))
	require.NoError(t, err)

	err = client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			SpanContext(mockEvent.SpanContext).
			EventID(mockEvent.EventID).
			EventType(mockEvent.EventType).
			EventLevel(mockEvent.EventLevel).
			ServiceName(mockEvent.ServiceName).
			ClientIDs(mockEvent.ClientIDs).
			TargetUserIDs(mockEvent.TargetUserIDs).
			TargetNamespace(mockEvent.TargetNamespace).
			Privacy(mockEvent.Privacy).
			AdditionalFields(mockEvent.AdditionalFields).
			Context(context.Background()).
			Timeout(10 * time.Second).
			Payload(mockPayload))
	require.NoError(t, err)

	for {
		select {
		case <-doneChan:
			return
		case <-ctx.Done():
			assert.FailNow(t, errorTimeout)
		}
	}
}

//nolint:funlen
func TestKafkaUnregisterTopicSuccess(t *testing.T) {
	t.Parallel()

	ctx, done := context.WithTimeout(context.Background(), time.Duration(timeoutTest)*time.Second)
	defer done()

	doneChan := make(chan bool, 2) //nolint:gomnd

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
			GroupID(generateID()).
			Offset(0).
			Context(ctx).
			Callback(func(ctx context.Context, _ *Event, err error) error {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				require.NoError(t, err)
				require.NoError(t, ctx.Err())

				doneChan <- true

				return nil
			}))
	if err != nil {
		assert.FailNow(t, errorSubscribe, err)
		return
	}

	subscribeCtx, subscribeCancel := context.WithCancel(ctx)
	defer subscribeCancel()

	err = client.Register(
		NewSubscribe().
			Topic("anotherevent").
			EventName(mockEvent.EventName).
			GroupID(generateID()).
			Offset(0).
			Context(subscribeCtx).
			Callback(func(ctx context.Context, event *Event, err error) error {
				if ctx.Err() != nil {
					doneChan <- true

					return nil
				}

				require.Nil(t, event) // unregistered subscriber should not receive events

				return nil
			}))
	if err != nil {
		assert.FailNow(t, errorSubscribe, err)
		return
	}

	// unregister subscription
	subscribeCancel()

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
			Timeout(10 * time.Second).
			Payload(mockPayload))
	if err != nil {
		assert.FailNow(t, errorPublish, err)
		return
	}

	completions := 0

	for {
		select {
		case <-doneChan:
			completions++
			if completions == 2 { //nolint:gomnd
				return
			}
		case <-ctx.Done():
			assert.FailNow(t, errorTimeout)
		}
	}
}

func TestKafkaGetMetadata(t *testing.T) {
	t.Parallel()
	ctx, done := context.WithTimeout(context.Background(), time.Duration(timeoutTest)*time.Second)
	defer done()

	logrus.SetLevel(logrus.DebugLevel)

	client := createKafkaClient(t)

	deadline, _ := ctx.Deadline()
	metadata, err := client.GetMetadata("", time.Until(deadline))
	require.NoError(t, err)
	assert.NotEmpty(t, metadata.Brokers[0].ID)
}

func callerFuncName() string {
	pc, _, _, _ := runtime.Caller(2)
	callerFunc := runtime.FuncForPC(pc)
	if callerFunc != nil {
		fullName := callerFunc.Name()
		return path.Base(fullName)
	}
	return "UnknownFunction"
}
