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
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// StdoutClient satisfies the publisher for mocking
type StdoutClient struct {
	prefix string
}

// NewStdoutClient creates new telemetry client
func NewStdoutClient(prefix string) (*StdoutClient, error) {
	return &StdoutClient{
		prefix: prefix,
	}, nil
}

// Publish print event to console
func (client *StdoutClient) Publish(publishBuilder *PublishBuilder) {
	if publishBuilder == nil {
		logrus.Error("unable to publish nil event")
		return
	}
	event := &Event{
		ID:        generateID(),
		EventName: publishBuilder.eventName,
		Namespace: publishBuilder.namespace,
		ClientID:  publishBuilder.clientID,
		UserID:    publishBuilder.userID,
		TraceID:   publishBuilder.traceID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Version:   publishBuilder.version,
		Payload:   publishBuilder.payload,
	}

	eventByte, err := marshal(event)
	if err != nil {
		return
	}

	fmt.Println(string(eventByte))
}

// Register print event to console
func (client *StdoutClient) Register(subscribeBuilder *SubscribeBuilder) {
	subscribe := struct {
		Topic     string    `json:"topic"`
		EventName string    `json:"name"`
		Timestamp time.Time `json:"timestamp"`
		Version   string    `json:"version"`
	}{
		Topic:     subscribeBuilder.topic,
		EventName: subscribeBuilder.eventName,
		Version:   defaultVersion,
		Timestamp: time.Now().UTC(),
	}

	eventByte, err := json.Marshal(&subscribe)
	if err != nil {
		logrus.Errorf("unable to marshal event : %s , error : %v", subscribe.EventName, err)
		return
	}

	fmt.Println(string(eventByte))
}
