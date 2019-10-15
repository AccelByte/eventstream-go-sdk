// Copyright (c) 2019 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package eventstream

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// nolint dupl
func TestKafkaPubWithNilEvent(t *testing.T) {
	client := createKafkaClient(t)

	err := client.Publish(nil)
	assert.Equal(t, err, errPubNilEvent, "error should be equal")
}

// nolint dupl
func TestKafkaSubWithNilEvent(t *testing.T) {
	client := createKafkaClient(t)

	err := client.Register(nil)
	assert.Equal(t, err, errSubNilEvent, "error should be equal")
}

// nolint dupl
func TestKafkaPubWithEmptyTopic(t *testing.T) {
	client := createKafkaClient(t)

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := &Event{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "661a4ac82b854f3ca3ac2e0377d356e4",
		TraceID:   "1e801bd0eb6946b88f4556d3c4c91e0c",
		UserID:    "1fe7f425a0e049d29d87ca3d32e45b5a",
		Version:   2,
		Payload:   mockPayload,
	}

	err := client.Publish(
		NewPublish().
			EventName(mockEvent.EventName).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			SessionID(mockEvent.SessionID).
			TraceID(mockEvent.TraceID).
			Version(2).
			Context(context.Background()).
			Payload(mockPayload))

	assert.NotNil(t, err, "error should not be nil")
}

// nolint dupl
func TestKafkaPubInvalidEventStruct(t *testing.T) {
	client := createKafkaClient(t)
	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := &Event{
		EventName: "testEvent,.,.",
		Namespace: "event#$%",
		ClientID:  "661a4ac82b854f3ca3ac2e0377d356e4",
		TraceID:   "1e801bd0eb6946b88f4556d3c4c91e0c",
		UserID:    "1fe7f425a0e049d29d87ca3d32e45b5a",
		Version:   2,
		Payload:   mockPayload,
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
			Version(2).
			Context(context.Background()).
			Payload(mockPayload))

	assert.Equal(t, errInvalidPubStruct, err, "error should be equal")
}

// nolint dupl
func TestKafkaPubInvalidUserID(t *testing.T) {
	client := createKafkaClient(t)
	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := &Event{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "691768fad8a443cd89aa73132ef47834",
		TraceID:   "c9bb37252d7246fab229ebd7d5d688ec",
		UserID:    "user123",
		Version:   2,
		Payload:   mockPayload,
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
			Version(2).
			Context(context.Background()).
			Payload(mockPayload))

	assert.Equal(t, errInvalidUserID, err, "error should be equal")
}

// nolint dupl
func TestKafkaPubInvalidClientID(t *testing.T) {
	client := createKafkaClient(t)
	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := &Event{
		EventName: "testEvent",
		Namespace: "event",
		ClientID:  "client123",
		TraceID:   "5005e27d01064f23b962e8fd2e560a8a",
		UserID:    "661a4ac82b854f3ca3ac2e0377d356e4",
		Version:   2,
		Payload:   mockPayload,
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
			Version(2).
			Context(context.Background()).
			Payload(mockPayload))

	assert.Equal(t, errInvalidClientID, err, "error should be equal")
}

// nolint dupl
func TestKafkaSubWithEmptyTopic(t *testing.T) {
	client := createKafkaClient(t)

	mockEvent := &Event{
		EventName: "testEvent,.,.",
	}

	err := client.Register(
		NewSubscribe().
			EventName(mockEvent.EventName).
			Callback(func(event *Event, err error) {

			}))

	assert.Equal(t, errInvalidSubStruct, err, "error should be equal")
}

// nolint dupl
func TestKafkaSubInvalidEventStruct(t *testing.T) {
	client := createKafkaClient(t)
	topicName := constructTopicTest()

	mockEvent := &Event{
		EventName: "testEvent,.,.",
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Callback(func(event *Event, err error) {

			}))

	assert.Equal(t, errInvalidSubStruct, err, "error should be equal")
}

// nolint dupl
func TestKafkaSubNilCallback(t *testing.T) {
	client := createKafkaClient(t)
	topicName := constructTopicTest()

	mockEvent := &Event{
		EventName: "testEvent",
	}

	err := client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Callback(nil))

	assert.Equal(t, errInvalidCallback, err, "error should be equal")
}
