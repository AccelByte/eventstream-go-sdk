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
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// StdoutClient satisfies the publisher for mocking
type StdoutClient struct {
	prefix string
}

// newStdoutClient creates new telemetry client
func newStdoutClient(prefix string) *StdoutClient {
	return &StdoutClient{
		prefix: prefix,
	}
}

func (client *StdoutClient) PublishSync(publishBuilder *PublishBuilder) error {
	return client.Publish(publishBuilder)
}

// Publish print event to console
func (client *StdoutClient) Publish(publishBuilder *PublishBuilder) error {
	if publishBuilder == nil {
		logrus.Error("unable to publish nil event")
		return errors.New("unable to publish nil event")
	}

	event := &Event{
		ID:               generateID(),
		EventName:        publishBuilder.eventName,
		Namespace:        publishBuilder.namespace,
		ClientID:         publishBuilder.clientID,
		UserID:           publishBuilder.userID,
		TraceID:          publishBuilder.traceID,
		SpanContext:      publishBuilder.spanContext,
		Timestamp:        time.Now().UTC().Format(time.RFC3339),
		EventID:          publishBuilder.eventID,
		EventType:        publishBuilder.eventType,
		EventLevel:       publishBuilder.eventLevel,
		ServiceName:      publishBuilder.serviceName,
		ClientIDs:        publishBuilder.clientIDs,
		TargetUserIDs:    publishBuilder.targetUserIDs,
		TargetNamespace:  publishBuilder.targetNamespace,
		Privacy:          publishBuilder.privacy,
		AdditionalFields: publishBuilder.additionalFields,
		Version:          publishBuilder.version,
		Payload:          publishBuilder.payload,
	}

	eventByte, err := marshal(event)
	if err != nil {
		logrus.Errorf("unable to marshal event : %s , error : %v", publishBuilder.eventName, err)
	}

	fmt.Println(string(eventByte))

	return nil
}

// Register print event to console
func (client *StdoutClient) Register(subscribeBuilder *SubscribeBuilder) error {
	subscribe := struct {
		Topic     string    `json:"topic"`
		EventName string    `json:"name"`
		Timestamp time.Time `json:"timestamp"`
		Version   int       `json:"version"`
	}{
		Topic:     subscribeBuilder.topic,
		EventName: subscribeBuilder.eventName,
		Version:   defaultVersion,
		Timestamp: time.Now().UTC(),
	}

	eventByte, err := json.Marshal(&subscribe)
	if err != nil {
		logrus.Errorf("unable to marshal event : %s , error : %v", subscribe.EventName, err)
	}

	fmt.Println(string(eventByte))

	return nil
}

func (client *StdoutClient) PublishAuditLog(auditLogBuilder *AuditLogBuilder) error {
	message, err := auditLogBuilder.Build()
	if err != nil {
		logrus.Errorf("unable to marshal audit log : %s , error : %v", string(message.Value), err)
		return err
	}
	fmt.Println(string(message.Value))
	return nil
}
