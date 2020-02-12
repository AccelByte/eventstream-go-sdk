/*
 * Copyright 2019 AccelByte Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eventstream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStdoutClient(t *testing.T) {
	client := newStdoutClient("test")
	expectedClient := &StdoutClient{
		prefix: "test",
	}
	assert.Equal(t, expectedClient, client, "client should be equal")
}

func TestStdoutPublish(t *testing.T) {
	client := StdoutClient{
		prefix: "test",
	}

	topicName := "topicTest"

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}
	mockEvent := struct {
		ID          string      `json:"id"`
		EventName   string      `json:"name"`
		Namespace   string      `json:"namespace"`
		ClientID    string      `json:"clientId"`
		TraceID     string      `json:"traceId"`
		SpanContext string      `json:"spanContext"`
		UserID      string      `json:"userId"`
		Timestamp   time.Time   `json:"timestamp"`
		Version     string      `json:"version"`
		Payload     interface{} `json:"payload"`
	}{
		EventName:   "testEvent",
		Namespace:   "event",
		ClientID:    "client123",
		TraceID:     "trace123",
		SpanContext: "span123",
		UserID:      "user123",
		Version:     "0.1.0",
		Payload:     mockPayload,
	}

	err := client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			SpanContext(mockEvent.SpanContext).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			Payload(mockPayload))
	require.NoError(t, err)
}

func TestStdoutSubscribe(t *testing.T) {
	client := StdoutClient{
		prefix: "test",
	}

	topicName := "topicTest"

	mockEvent := struct {
		EventName string `json:"name"`
	}{
		EventName: "testEvent",
	}

	_ = client.Register(
		NewSubscribe().
			Topic(topicName).
			EventName(mockEvent.EventName).
			Callback(func(event *Event, err error) {}))
}
