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

package eventpublisher

import (
	"time"
)

// Event holds the outer telemetry Event structure
type Event struct {
	Time             time.Time              `json:"time"`
	EventID          int                    `json:"event_id"`
	EventType        int                    `json:"event_type"`
	EventLevel       int                    `json:"event_level"`
	Service          string                 `json:"service"`
	ClientIDs        []string               `json:"client_ids"`
	UserID           string                 `json:"user_id"`
	TargetUserIDs    []string               `json:"target_user_ids"`
	Namespace        string                 `json:"namespace"`
	TargetNamespace  string                 `json:"target_namespace"`
	TraceID          string                 `json:"trace_id"`
	SessionID        string                 `json:"session_id"`
	Privacy          bool                   `json:"privacy"`
	Realm            string                 `json:"realm"`
	Topic            string                 `json:"topic"`
	AdditionalFields map[string]interface{} `json:"additional_fields,omitempty"`
}

// NewEvent creates new Event without additional fields
func NewEvent(eventID int,
	eventType int,
	eventLevel int,
	service string,
	clientIDs []string,
	userID string,
	targetUserIDs []string,
	namespace string,
	targetNamespace string,
	traceID string,
	sessionID string,
	privacy bool,
	topic string) *Event {
	return &Event{
		EventID:         eventID,
		EventType:       eventType,
		EventLevel:      eventLevel,
		Service:         service,
		ClientIDs:       clientIDs,
		UserID:          userID,
		TargetUserIDs:   targetUserIDs,
		Namespace:       namespace,
		TargetNamespace: targetNamespace,
		TraceID:         traceID,
		SessionID:       sessionID,
		Privacy:         privacy,
		Topic:           topic,
	}
}

// WithFields add additional fields
func (event *Event) WithFields(fields map[string]interface{}) *Event {
	event.AdditionalFields = fields
	return event
}

// PublisherClient provides interface for publish the Event to stream
type PublisherClient interface {
	PublishEvent(event *Event) error
	PublishEventAsync(event *Event)
}
