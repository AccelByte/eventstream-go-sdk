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
	"testing"

	"github.com/stretchr/testify/assert"
)

// nolint dupl
func TestKafkaPubWithNilEvent(t *testing.T) {
	t.Parallel()
	client := createKafkaClient(t)

	err := client.Publish(nil)
	assert.Equal(t, err, errPubNilEvent, "error should be equal")
}

// nolint dupl
func TestKafkaSubWithNilEvent(t *testing.T) {
	t.Parallel()
	client := createKafkaClient(t)

	err := client.Register(nil)
	assert.Equal(t, err, errSubNilEvent, "error should be equal")
}

// nolint dupl
func TestKafkaPubWithEmptyTopic(t *testing.T) {
	t.Parallel()
	client := createKafkaClient(t)

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}

	mockAdditionalFields := map[string]interface{}{
		"summary": "user:_failed",
	}

	mockEvent := &Event{
		EventName:        "testEvent",
		Namespace:        "event",
		ClientID:         "661a4ac82b854f3ca3ac2e0377d356e4",
		TraceID:          "1e801bd0eb6946b88f4556d3c4c91e0c",
		SpanContext:      "test-span-context",
		UserID:           "1fe7f425a0e049d29d87ca3d32e45b5a",
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

	err := client.Publish(
		NewPublish().
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

	assert.NotNil(t, err, "error should not be nil")
}

// nolint dupl
func TestKafkaPubInvalidEventStruct(t *testing.T) {
	t.Parallel()
	client := createKafkaClient(t)
	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := &Event{
		EventName:        "testEvent,.,.",
		Namespace:        "event#$%",
		ClientID:         "661a4ac82b854f3ca3ac2e0377d356e4",
		TraceID:          "1e801bd0eb6946b88f4556d3c4c91e0c",
		SpanContext:      "test-span-context",
		UserID:           "1fe7f425a0e049d29d87ca3d32e45b5a",
		EventID:          3,
		EventType:        301,
		EventLevel:       3,
		ServiceName:      "test",
		ClientIDs:        []string{"7d480ce0e8624b02901bd80d9ba9817c"},
		TargetUserIDs:    []string{"1fe7f425a0e049d29d87ca3d32e45b5a"},
		TargetNamespace:  "publisher",
		Privacy:          true,
		AdditionalFields: mockPayload,
		Version:          2,
		Payload:          mockPayload,
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

	assert.Error(t, err, "error should be equal")
}

// nolint dupl
func TestKafkaPubInvalidUserID(t *testing.T) {
	t.Parallel()
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
		ClientID:         "691768fad8a443cd89aa73132ef47834",
		TraceID:          "c9bb37252d7246fab229ebd7d5d688ec",
		SpanContext:      "test-span-context",
		UserID:           "user123",
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

	err := client.Publish(
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

	assert.Equal(t, errInvalidUserID, err, "error should be equal")
}

// nolint dupl
func TestKafkaPubInvalidClientID(t *testing.T) {
	t.Parallel()
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
		ClientID:         "client123",
		TraceID:          "5005e27d01064f23b962e8fd2e560a8a",
		SpanContext:      "test-span-context",
		UserID:           "661a4ac82b854f3ca3ac2e0377d356e4",
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

	err := client.Publish(
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

	assert.Equal(t, errInvalidClientID, err, "error should be equal")
}

// nolint dupl
func TestKafkaSubWithEmptyTopic(t *testing.T) {
	t.Parallel()
	client := createKafkaClient(t)

	mockEvent := &Event{
		EventName: "testEvent,.,.",
	}

	err := client.Register(
		NewSubscribe().
			EventName(mockEvent.EventName).
			Callback(func(ctx context.Context, event *Event, err error) error {
				return nil
			}))

	assert.Error(t, err, "error should be not nil")
}

// nolint dupl
func TestKafkaSubInvalidEventStruct(t *testing.T) {
	t.Parallel()
	client := createKafkaClient(t)
	topicName := constructTopicTest()

	mockEvent := &Event{
		EventName: "testEvent,.,.",
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Callback(func(ctx context.Context, event *Event, err error) error {
				return nil
			}))

	assert.Error(t, err, "error should be not nil")
}

// nolint dupl
func TestKafkaSubNilCallback(t *testing.T) {
	t.Parallel()
	client := createKafkaClient(t)
	topicName := constructTopicTest()

	mockEvent := &Event{
		EventName: "testEvent",
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			GroupID(generateID()).
			Callback(nil))

	assert.Equal(t, errInvalidCallback, err, "error should be equal")
}
