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
)

const (
	eventStreamNull   = "none"
	eventStreamStdout = "stdout"
	eventStreamKafka  = "kafka"
)

const (
	separator      = "."
	defaultVersion = "0.1.0"
)

// Event defines the structure of event
type Event struct {
	ID        string                 `json:"id"`
	EventName string                 `json:"name"`
	Namespace string                 `json:"namespace"`
	ClientID  string                 `json:"clientId"`
	TraceID   string                 `json:"traceId"`
	UserID    string                 `json:"userId"`
	Timestamp string                 `json:"timestamp"`
	Version   string                 `json:"version"`
	Payload   map[string]interface{} `json:"payload"`
}

// PublishBuilder defines the structure of message which is sent through message broker
type PublishBuilder struct {
	topic     []string
	eventName string
	namespace string
	clientID  string
	traceID   string
	userID    string
	version   string
	payload   map[string]interface{}
	ctx       context.Context
}

// NewPublish create new PublishBuilder instance
func NewPublish() *PublishBuilder {
	return &PublishBuilder{
		version: defaultVersion,
		ctx:     context.Background(),
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

// UserID set userID of publisher event
func (p *PublishBuilder) UserID(userID string) *PublishBuilder {
	p.userID = userID
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

// Version set event schema version
func (p *PublishBuilder) Version(version string) *PublishBuilder {
	p.version = version
	return p
}

// Payload is a event payload that will be published
func (p *PublishBuilder) Payload(payload map[string]interface{}) *PublishBuilder {
	p.payload = payload
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
	topic     string
	callback  func(event *Event, err error)
	eventName string
	ctx       context.Context
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

// EventName set event name that will be subscribe
func (s *SubscribeBuilder) EventName(eventName string) *SubscribeBuilder {
	s.eventName = eventName
	return s
}

// Callback to do when the event received
func (s *SubscribeBuilder) Callback(callback func(event *Event, err error)) *SubscribeBuilder {
	s.callback = callback
	return s
}

// Context define client context when subscribe event.
// default: context.Background()
func (s *SubscribeBuilder) Context(ctx context.Context) *SubscribeBuilder {
	s.ctx = ctx
	return s
}

func NewClient(prefix, stream string, brokers []string) (Client, error) {
	switch stream {
	case eventStreamNull:
		return NewBlackholeClient()
	case eventStreamStdout:
		return NewStdoutClient(prefix)
	case eventStreamKafka:
		return NewKafkaClient(brokers, prefix)
	default:
		return nil, errors.New("unsupported stream")
	}
}

// Client is an interface for event stream functionality
type Client interface {
	Publish(publishBuilder *PublishBuilder)
	Register(subscribeBuilder *SubscribeBuilder)
}
