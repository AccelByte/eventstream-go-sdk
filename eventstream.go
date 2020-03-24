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
	"errors"
	"time"
)

const (
	eventStreamNull   = "none"
	eventStreamStdout = "stdout"
	eventStreamKafka  = "kafka"
)

const (
	separator      = "."
	defaultVersion = 1
	defaultGroupID = "*"
)

// log level
const (
	OffLevel   = "off"
	InfoLevel  = "info"
	DebugLevel = "debug"
	WarnLevel  = "warn"
	ErrorLevel = "error"
)

// Event defines the structure of event
type Event struct {
	ID        string                 `json:"id"`
	EventName string                 `json:"name"`
	Namespace string                 `json:"namespace"`
	ClientID  string                 `json:"clientId"`
	TraceID   string                 `json:"traceId"`
	UserID    string                 `json:"userId"`
	SessionID string                 `json:"sessionId"`
	Timestamp string                 `json:"timestamp"`
	Version   int                    `json:"version"`
	Payload   map[string]interface{} `json:"payload"`
}

// BrokerConfig is custom configuration for message broker
type BrokerConfig struct {
	LogMode          string
	StrictValidation bool
	DialTimeout      time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
}

// PublishBuilder defines the structure of message which is sent through message broker
type PublishBuilder struct {
	topic         []string
	eventName     string
	namespace     string
	clientID      string
	traceID       string
	userID        string
	sessionID     string
	version       int
	payload       map[string]interface{}
	errorCallback func(event *Event, err error)
	ctx           context.Context
}

// NewPublish create new PublishBuilder instance
func NewPublish() *PublishBuilder {
	return &PublishBuilder{
		version:       defaultVersion,
		ctx:           context.Background(),
		errorCallback: nil,
	}
}

// Topic set channel / topic name
func (p *PublishBuilder) Topic(topics ...string) *PublishBuilder {
	p.topic = append(p.topic, topics...)
	return p
}

// EventName set name of published event
func (p *PublishBuilder) EventName(eventName string) *PublishBuilder {
	p.eventName = eventName
	return p
}

// Namespace set namespace of published event
func (p *PublishBuilder) Namespace(namespace string) *PublishBuilder {
	p.namespace = namespace
	return p
}

// ClientID set clientID of publisher event
func (p *PublishBuilder) ClientID(clientID string) *PublishBuilder {
	p.clientID = clientID
	return p
}

// TraceID set traceID of publisher event
func (p *PublishBuilder) TraceID(traceID string) *PublishBuilder {
	p.traceID = traceID
	return p
}

// SessionID set sessionID of publisher event
func (p *PublishBuilder) SessionID(sessionID string) *PublishBuilder {
	p.sessionID = sessionID
	return p
}

// UserID set userID of publisher event
func (p *PublishBuilder) UserID(userID string) *PublishBuilder {
	p.userID = userID
	return p
}

// Version set event schema version
func (p *PublishBuilder) Version(version int) *PublishBuilder {
	p.version = version
	return p
}

// Payload is a event payload that will be published
func (p *PublishBuilder) Payload(payload map[string]interface{}) *PublishBuilder {
	p.payload = payload
	return p
}

// ErrorCallback function to handle the event when failed to publish
func (p *PublishBuilder) ErrorCallback(errorCallback func(event *Event, err error)) *PublishBuilder {
	p.errorCallback = errorCallback
	return p
}

// Context define client context when publish event.
// default: context.Background()
func (p *PublishBuilder) Context(ctx context.Context) *PublishBuilder {
	p.ctx = ctx
	return p
}

// SubscribeBuilder defines the structure of message which is sent through message broker
type SubscribeBuilder struct {
	topic       string
	groupID     string
	callback    func(ctx context.Context, event *Event, err error)
	eventName   string
	ctx         context.Context
	callbackCtx context.Context
}

// NewSubscribe create new SubscribeBuilder instance
func NewSubscribe() *SubscribeBuilder {
	return &SubscribeBuilder{
		ctx: context.Background(),
	}
}

// Topic set topic that will be subscribe
func (s *SubscribeBuilder) Topic(topic string) *SubscribeBuilder {
	s.topic = topic
	return s
}

// GroupID set subscriber groupID or queue group name
func (s *SubscribeBuilder) GroupID(groupID string) *SubscribeBuilder {
	s.groupID = groupID
	return s
}

// EventName set event name that will be subscribe
func (s *SubscribeBuilder) EventName(eventName string) *SubscribeBuilder {
	s.eventName = eventName
	return s
}

// Callback to do when the event received
func (s *SubscribeBuilder) Callback(callback func(ctx context.Context, event *Event, err error)) *SubscribeBuilder {
	s.callback = callback
	return s
}

// Context define client context when subscribe event.
// default: context.Background()
func (s *SubscribeBuilder) Context(ctx context.Context) *SubscribeBuilder {
	s.ctx = ctx
	return s
}

// CallbackContext define callback context to stop callback function.
func (s *SubscribeBuilder) CallbackContext(ctx context.Context) *SubscribeBuilder {
	s.callbackCtx = ctx
	return s
}

func NewClient(prefix, stream string, brokers []string, config ...*BrokerConfig) (Client, error) {
	switch stream {
	case eventStreamNull:
		return newBlackholeClient(), nil
	case eventStreamStdout:
		return newStdoutClient(prefix), nil
	case eventStreamKafka:
		return newKafkaClient(brokers, prefix, config...), nil
	default:
		return nil, errors.New("unsupported stream")
	}
}

// Client is an interface for event stream functionality
type Client interface {
	Publish(publishBuilder *PublishBuilder) error
	Register(subscribeBuilder *SubscribeBuilder) error
}
