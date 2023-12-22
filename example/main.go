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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/AccelByte/eventstream-go-sdk/v4"
	"github.com/sirupsen/logrus"
)

type Payload struct {
	FriendID string `json:"friendId"`
}

// nolint: funlen
func main() {

	logrus.SetLevel(logrus.DebugLevel)

	config := &eventstream.BrokerConfig{
		StrictValidation: true,
		DialTimeout:      0,
	}

	prefix := "example"

	client, err := eventstream.NewClient(prefix, "kafka", []string{"localhost:9092"}, config)
	if err != nil {
		logrus.Error(err)
	}

	var mockPayload = make(map[string]interface{})
	mockPayload["testPayload"] = Payload{FriendID: "user456"}

	mockAdditionalFields := map[string]interface{}{
		"summary": "user:_failed",
	}

	mockEvent := &eventstream.Event{
		EventName:        "testEvent",
		Namespace:        "event",
		ClientID:         "fe5bd0e3dc184d2d8ae0e09fcedf0f51",
		TraceID:          "882da8cddd174d12af25da6310b47bd5",
		SpanContext:      "test-span-context",
		UserID:           "48bf8a020b584f31bc605bf65d3300ed",
		SessionID:        "c1ab4f754acc4cb48a8f68dd25cfca21",
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

	err = client.Publish(eventstream.NewPublish().
		Topic("topic").
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
		Payload(mockPayload).
		Timeout(time.Millisecond * 1))
	if err != nil {
		logrus.Error(err)
	}

	err = client.Register(
		eventstream.NewSubscribe().
			EventName("eventName").
			Topic("topic").
			Context(context.Background()).
			GroupID("groupid").
			Callback(func(ctx context.Context, event *eventstream.Event, err error) error {
				if err != nil {
					logrus.Error(err)
				}
				fmt.Printf("%+v", event)

				return nil
			}).
			SendErrorDLQ(true).
			AsyncCommitMessage(true))

	if err != nil {
		logrus.Error(err)
	}

	time.Sleep(time.Hour * 4)

}
