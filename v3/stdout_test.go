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
	"context"
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

// nolint:funlen
func TestStdoutPublish(t *testing.T) {
	client := StdoutClient{
		prefix: "test",
	}

	topicName := "topicTest"

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = Payload{FriendID: "user456"}

	mockAdditionalFields := map[string]interface{}{
		"summary": "user:_failed",
	}

	mockEvent := struct {
		ID               string                 `json:"id"`
		EventName        string                 `json:"name"`
		Namespace        string                 `json:"namespace"`
		ClientID         string                 `json:"clientId"`
		TraceID          string                 `json:"traceId"`
		SpanContext      string                 `json:"spanContext"`
		UserID           string                 `json:"userId"`
		Timestamp        time.Time              `json:"timestamp"`
		EventID          int                    `json:"event_id"`
		EventType        int                    `json:"event_type"`
		EventLevel       int                    `json:"event_level"`
		ServiceName      string                 `json:"service"`
		ClientIDs        []string               `json:"client_ids"`
		TargetUserIDs    []string               `json:"target_user_ids"`
		TargetNamespace  string                 `json:"target_namespace"`
		Privacy          bool                   `json:"privacy"`
		AdditionalFields map[string]interface{} `json:"additional_fields,omitempty"`
		Version          string                 `json:"version"`
		Payload          interface{}            `json:"payload"`
	}{
		EventName:        "testEvent",
		Namespace:        "event",
		ClientID:         "client123",
		TraceID:          "trace123",
		SpanContext:      "span123",
		UserID:           "user123",
		EventID:          3,   // nolint:gomnd
		EventType:        301, // nolint:gomnd
		EventLevel:       3,   // nolint:gomnd
		ServiceName:      "test",
		ClientIDs:        []string{"7d480ce0e8624b02901bd80d9ba9817c"},
		TargetUserIDs:    []string{"1fe7f425a0e049d29d87ca3d32e45b5a"},
		TargetNamespace:  "publisher",
		Privacy:          true,
		AdditionalFields: mockAdditionalFields,
		Version:          "0.1.0",
		Payload:          mockPayload,
	}

	err := client.Publish(
		NewPublish().
			Topic(topicName).
			EventName(mockEvent.EventName).
			SpanContext(mockEvent.SpanContext).
			Namespace(mockEvent.Namespace).
			ClientID(mockEvent.ClientID).
			UserID(mockEvent.UserID).
			EventID(mockEvent.EventID).
			EventType(mockEvent.EventType).
			EventLevel(mockEvent.EventLevel).
			ServiceName(mockEvent.ServiceName).
			ClientIDs(mockEvent.ClientIDs).
			TargetUserIDs(mockEvent.TargetUserIDs).
			TargetNamespace(mockEvent.TargetNamespace).
			Privacy(mockEvent.Privacy).
			AdditionalFields(mockEvent.AdditionalFields).
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
			Callback(func(ctx context.Context, event *Event, err error) error {
				return nil
			}))
}
