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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
)

const (
	defaultReaderSize = 10e6 // 10MB
	maxBackOffCount   = 3
	kafkaMaxWait      = time.Second
	saslScramAuth     = "SASL-SCRAM"
)

var (
	errPubNilEvent = errors.New("unable to publish nil event")
	errSubNilEvent = errors.New("unable to subscribe nil event")
)

// KafkaClient wraps client's functionality for Kafka
type KafkaClient struct {

	// topic prefix
	prefix string

	// enable strict validation for event fields
	strictValidation bool

	// publish configuration
	publishConfig kafka.WriterConfig

	// subscribe configuration
	subscribeConfig kafka.ReaderConfig

	// current subscribers
	subscribers map[*SubscribeBuilder]struct{}

	// current writers
	writers map[string]*kafka.Writer

	// mutex to avoid runtime races to access subscribers map
	subLock sync.RWMutex

	// mutex to avoid runtime races to access writers map
	pubLock sync.RWMutex
}

func setConfig(writerConfig *kafka.WriterConfig, readerConfig *kafka.ReaderConfig, config *BrokerConfig) error {
	if config.ReadTimeout != 0 {
		writerConfig.ReadTimeout = config.ReadTimeout
	}

	if config.WriteTimeout != 0 {
		writerConfig.WriteTimeout = config.WriteTimeout
	}

	dialer := &kafka.Dialer{}
	if config.DialTimeout != 0 {
		dialer.Timeout = config.DialTimeout
	}

	if config.SecurityConfig != nil && config.SecurityConfig.AuthenticationType == saslScramAuth {
		mechanism, err := scram.Mechanism(scram.SHA512, config.SecurityConfig.SASLUsername, config.SecurityConfig.SASLPassword)
		if err != nil {
			logrus.Error("unable to initialize kafka scram authentication", err)
			return err
		}
		dialer.SASLMechanism = mechanism
		dialer.DualStack = true
		dialer.TLS = &tls.Config{}
	}

	if config.CACertFile != "" {
		logrus.Debug("set TLS certificate")

		cert, err := GetTLSCertFromFile(config.CACertFile)
		if err != nil {
			logrus.Error("unable to get TLS certificate", err)
			return err
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{*cert},
		}

		dialer.TLS = tlsConfig
	}

	writerConfig.Dialer = dialer
	readerConfig.Dialer = dialer

	return nil
}

// newKafkaClient create a new instance of KafkaClient
func newKafkaClient(brokers []string, prefix string, config ...*BrokerConfig) (*KafkaClient, error) {
	logrus.Info("create new kafka client")

	writerConfig := &kafka.WriterConfig{
		Brokers: brokers,
	}

	readerConfig := &kafka.ReaderConfig{
		Brokers:        brokers,
		MaxBytes:       defaultReaderSize,
		MaxWait:        kafkaMaxWait,
		CommitInterval: 100 * time.Millisecond, // 0 means synchronous (slow), > 0 is async.
	}

	// set client configuration
	// only uses first KafkaConfig arguments
	var strictValidation bool

	var err error

	if len(config) > 0 {
		err = setConfig(writerConfig, readerConfig, config[0])
		strictValidation = config[0].StrictValidation
		writerConfig.Balancer = config[0].Balancer
	}

	return &KafkaClient{
		prefix:           prefix,
		strictValidation: strictValidation,
		publishConfig:    *writerConfig,
		subscribeConfig:  *readerConfig,
		subscribers:      make(map[*SubscribeBuilder]struct{}),
		writers:          make(map[string]*kafka.Writer),
	}, err
}

// Publish send event to single or multiple topic with exponential backoff retry
func (client *KafkaClient) Publish(publishBuilder *PublishBuilder) error {
	if publishBuilder == nil {
		logrus.Error(errPubNilEvent)
		return errPubNilEvent
	}

	err := validatePublishEvent(publishBuilder, client.strictValidation)
	if err != nil {
		logrus.
			WithField("Topic Name", publishBuilder.topic).
			WithField("Event Name", publishBuilder.eventName).
			Error("incorrect publisher event: ", err)
		return err
	}

	message, event, err := ConstructEvent(publishBuilder)
	if err != nil {
		logrus.
			WithField("Topic Name", publishBuilder.topic).
			WithField("Event Name", publishBuilder.eventName).
			Error("unable to construct event: ", err)
		return fmt.Errorf("unable to construct event : %s , error : %v", publishBuilder.eventName, err)
	}

	config := client.publishConfig

	for _, pubTopic := range publishBuilder.topic {
		topic := constructTopic(client.prefix, pubTopic)

		go func() {
			err = backoff.RetryNotify(func() error {
				return client.publishEvent(publishBuilder.ctx, topic, publishBuilder.eventName, config, message)
			}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxBackOffCount),
				func(err error, _ time.Duration) {
					logrus.
						WithField("Topic Name", topic).
						WithField("Event Name", publishBuilder.eventName).
						Warn("retrying publish event: ", err)
				})
			if err != nil {
				logrus.
					WithField("Topic Name", topic).
					WithField("Event Name", publishBuilder.eventName).
					Error("giving up publishing event: ", err)

				if publishBuilder.errorCallback != nil {
					publishBuilder.errorCallback(event, err)
				}

				return
			}

			logrus.
				WithField("Topic Name", topic).
				WithField("Event Name", publishBuilder.eventName).
				Debug("successfully publish event")
		}()
	}

	return nil
}

// PublishSync send an event synchronously (blocking, without retry)
func (client *KafkaClient) PublishSync(publishBuilder *PublishBuilder) error {
	if publishBuilder == nil {
		logrus.Error(errPubNilEvent)
		return errPubNilEvent
	}

	err := validatePublishEvent(publishBuilder, client.strictValidation)
	if err != nil {
		logrus.
			WithField("Topic Name", publishBuilder.topic).
			WithField("Event Name", publishBuilder.eventName).
			Error("incorrect publisher event: ", err)
		return err
	}

	message, _, err := ConstructEvent(publishBuilder)
	if err != nil {
		logrus.
			WithField("Topic Name", publishBuilder.topic).
			WithField("Event Name", publishBuilder.eventName).
			Error("unable to construct event: ", err)
		return fmt.Errorf("unable to construct event : %s , error : %v", publishBuilder.eventName, err)
	}

	config := client.publishConfig
	if len(publishBuilder.topic) != 1 {
		return fmt.Errorf("incorrect number of topics for sync publish")
	}

	topic := constructTopic(client.prefix, publishBuilder.topic[0])

	return client.publishEvent(publishBuilder.ctx, topic, publishBuilder.eventName, config, message)
}

// Publish send event to a topic
func (client *KafkaClient) publishEvent(ctx context.Context, topic, eventName string, config kafka.WriterConfig,
	message kafka.Message) (err error) {
	writer := &kafka.Writer{}

	logrus.
		WithField("Topic Name", topic).
		WithField("Event Name", eventName).
		Debug("publish event")

	defer func() {
		if r := recover(); r != nil {
			logrus.
				WithField("Topic Name", topic).
				WithField("Event Name", eventName).
				Warn("unable to publish event: recover: ", r)

			if writer == nil {
				logrus.
					WithField("Topic Name", topic).
					WithField("Event Name", eventName).
					Warn("unable to publish event: writer is nil")

				err = errors.New("writer is nil")

				return
			}

			client.deleteWriter(config.Topic)

			err = fmt.Errorf("recover: %v", r)
		}
	}()

	config.Topic = topic
	writer = client.getWriter(config)
	err = writer.WriteMessages(ctx, message)

	if err != nil {
		if errors.Is(err, io.ErrClosedPipe) {
			// new a writer and retry
			writer = client.newWriter(config)
			err = writer.WriteMessages(ctx, message)
		}

		if err != nil {
			// delete writer if it fails to publish the event
			client.deleteWriter(config.Topic)

			return err
		}
	}

	return nil
}

// ConstructEvent construct event message
func ConstructEvent(publishBuilder *PublishBuilder) (kafka.Message, *Event, error) {
	id := generateID()
	key := publishBuilder.key
	if publishBuilder.key == "" {
		key = id
	}
	event := &Event{
		ID:               id,
		EventName:        publishBuilder.eventName,
		Namespace:        publishBuilder.namespace,
		ClientID:         publishBuilder.clientID,
		UserID:           publishBuilder.userID,
		TraceID:          publishBuilder.traceID,
		SpanContext:      publishBuilder.spanContext,
		SessionID:        publishBuilder.sessionID,
		Timestamp:        time.Now().UTC().Format(time.RFC3339),
		Version:          publishBuilder.version,
		EventID:          publishBuilder.eventID,
		EventType:        publishBuilder.eventType,
		EventLevel:       publishBuilder.eventLevel,
		ServiceName:      publishBuilder.serviceName,
		ClientIDs:        publishBuilder.clientIDs,
		TargetUserIDs:    publishBuilder.targetUserIDs,
		TargetNamespace:  publishBuilder.targetNamespace,
		Privacy:          publishBuilder.privacy,
		AdditionalFields: publishBuilder.additionalFields,
		Payload:          publishBuilder.payload,
	}

	eventBytes, err := marshal(event)
	if err != nil {
		return kafka.Message{}, event, err
	}

	return kafka.Message{
		Key:   []byte(key),
		Value: eventBytes,
	}, event, nil
}

// unregister unregister subscriber
func (client *KafkaClient) unregister(subscribeBuilder *SubscribeBuilder) {
	client.subLock.Lock()
	defer client.subLock.Unlock()

	delete(client.subscribers, subscribeBuilder)
}

// Register register callback function and then subscribe topic
//nolint: gocognit,funlen
func (client *KafkaClient) Register(subscribeBuilder *SubscribeBuilder) error {
	if subscribeBuilder == nil {
		logrus.Error(errSubNilEvent)
		return errSubNilEvent
	}

	logrus.
		WithField("Topic Name", subscribeBuilder.topic).
		WithField("Event Name", subscribeBuilder.eventName).
		Debug("register callback")

	err := validateSubscribeEvent(subscribeBuilder)
	if err != nil {
		logrus.
			WithField("Topic Name", subscribeBuilder.topic).
			WithField("Event Name", subscribeBuilder.eventName).
			Error("incorrect subscriber event: ", err)

		return err
	}

	topic := constructTopic(client.prefix, subscribeBuilder.topic)
	groupID := constructGroupID(client.prefix, subscribeBuilder.groupID)

	isRegistered, err := client.registerSubscriber(subscribeBuilder)
	if err != nil {
		logrus.
			WithField("Topic Name", topic).
			WithField("Event Name", subscribeBuilder.eventName).
			Error("unable to register callback: ", err)

		return err
	}

	if isRegistered {
		return fmt.Errorf(
			"topic and event already registered. topic: %s , event: %s",
			topic,
			subscribeBuilder.eventName,
		)
	}

	go func() {
		config := client.subscribeConfig
		config.Topic = topic
		config.GroupID = groupID
		config.StartOffset = subscribeBuilder.offset
		reader := kafka.NewReader(config)

		var eventProcessingFailed bool

		defer func() {
			if eventProcessingFailed {
				if subscribeBuilder.ctx.Err() != nil {
					// the subscription is shutting down. triggered by an external context cancellation
					logrus.
						WithField("Topic Name", topic).
						WithField("Event Name", subscribeBuilder.eventName).
						Debug("triggered an external context cancellation. Cancelling the subscription")
					return
				}

				// current worker can't process the event and we need to unblock the event for other workers
				// as we use kafka in the explicit commit mode - we can't send the "acknowledge" and have to interrupt connection
				time.Sleep(time.Second)

				err := client.Register(subscribeBuilder)
				if err != nil {
					logrus.Info(err)
				}
			}
		}()
		defer client.unregister(subscribeBuilder)
		defer reader.Close() // nolint: errcheck

		for {
			select {
			case <-subscribeBuilder.ctx.Done():
				// ignore error because client isn't processing events
				err = subscribeBuilder.callback(subscribeBuilder.ctx, nil, subscribeBuilder.ctx.Err())

				logrus.
					WithField("Topic Name", topic).
					WithField("Event Name", subscribeBuilder.eventName).
					Debug("triggered an external context cancellation. Cancelling the subscription")

				return
			default:
				consumerMessage, errRead := reader.FetchMessage(subscribeBuilder.ctx)
				if errRead != nil {
					logrus.
						WithField("Topic Name", topic).
						WithField("Event Name", subscribeBuilder.eventName).
						Error("subscriber unable to fetch message", errRead)

					if errClose := reader.Close(); errClose != nil {
						logrus.Error("unable to close subscriber", err)
					}

					if subscribeBuilder.ctx.Err() != nil {
						// the subscription is shutting down. triggered by an external context cancellation
						logrus.
							WithField("Topic Name", topic).
							WithField("Event Name", subscribeBuilder.eventName).
							Debug("triggered an external context cancellation. Cancelling the subscription")

						reader = kafka.NewReader(config)
						continue
					}

					reader = kafka.NewReader(config)
					continue
				}

				err := client.processMessage(subscribeBuilder, consumerMessage, topic)
				if err != nil {
					logrus.
						WithField("Topic Name", topic).
						WithField("Event Name", subscribeBuilder.eventName).
						Error("unable to process the event: ", err)

					// shutdown current subscriber and mark it for restarting
					eventProcessingFailed = true

					return
				}

				err = reader.CommitMessages(subscribeBuilder.ctx, consumerMessage)
				if err != nil {
					if subscribeBuilder.ctx.Err() == nil {
						// the subscription is shutting down. triggered by an external context cancellation
						logrus.
							WithField("Topic Name", topic).
							WithField("Event Name", subscribeBuilder.eventName).
							Debug("triggered an external context cancellation. Cancelling the subscription")
						continue
					}

					logrus.
						WithField("Topic Name", topic).
						WithField("Event Name", subscribeBuilder.eventName).
						Error("unable to commit the event: ", err)
				}
			}
		}
	}()

	return nil
}

// registerSubscriber add callback to map with topic and eventName as a key
func (client *KafkaClient) registerSubscriber(subscribeBuilder *SubscribeBuilder) (
	isRegistered bool,
	err error,
) {
	client.subLock.Lock()
	defer client.subLock.Unlock()

	for subs := range client.subscribers {
		if subs.topic == subscribeBuilder.topic && subs.eventName == subscribeBuilder.eventName {
			if subscribeBuilder.groupID == "" {
				return true, nil
			}
		}
	}

	client.subscribers[subscribeBuilder] = struct{}{}

	return false, nil
}

// getWriter get a writer based on config
func (client *KafkaClient) getWriter(config kafka.WriterConfig) *kafka.Writer {
	client.pubLock.Lock()
	defer client.pubLock.Unlock()

	if writer, ok := client.writers[config.Topic]; ok {
		return writer
	}

	if _, ok := client.writers[config.Topic]; !ok {
		writer := kafka.NewWriter(config)
		client.writers[config.Topic] = writer
	}

	return client.writers[config.Topic]
}

// newWriter new a writer
func (client *KafkaClient) newWriter(config kafka.WriterConfig) *kafka.Writer {
	client.pubLock.Lock()
	defer client.pubLock.Unlock()

	// let's always new a writer
	writer := kafka.NewWriter(config)
	client.writers[config.Topic] = writer

	return writer
}

// deleteWriter delete writer
func (client *KafkaClient) deleteWriter(topic string) {
	client.pubLock.Lock()
	defer client.pubLock.Unlock()

	writer, ok := client.writers[topic]
	if ok {
		_ = writer.Close()
	}

	// we only delete the writer from the slice but no close, should close in some interval?
	delete(client.writers, topic)
}

// processMessage process a message from kafka
func (client *KafkaClient) processMessage(subscribeBuilder *SubscribeBuilder, message kafka.Message, topic string) error {
	event, err := unmarshal(message)
	if err != nil {
		logrus.
			WithField("Topic Name", topic).
			WithField("Event Name", subscribeBuilder.eventName).
			Error("unable to unmarshal message from subscribe in kafka: ", err)

		// as retry will fail infinitely - return nil to ACK the event
		return nil
	}

	if subscribeBuilder.eventName != "" && subscribeBuilder.eventName != event.EventName {
		// don't send events if consumer subscribed on a non-empty event name
		// return nil to ACK the event
		return nil
	}

	return client.runCallback(subscribeBuilder, event)
}

// unmarshal unmarshal received message into event struct
func unmarshal(message kafka.Message) (*Event, error) {
	var event *Event

	err := json.Unmarshal(message.Value, &event)
	if err != nil {
		return &Event{}, err
	}

	event.Partition = message.Partition
	event.Offset = message.Offset
	event.Key = string(message.Key)

	return event, nil
}

// runCallback run callback function when receive an event
func (client *KafkaClient) runCallback(
	subscribeBuilder *SubscribeBuilder,
	event *Event,
) error {
	return subscribeBuilder.callback(subscribeBuilder.ctx, &Event{
		ID:               event.ID,
		EventName:        event.EventName,
		Namespace:        event.Namespace,
		ClientID:         event.ClientID,
		TraceID:          event.TraceID,
		SpanContext:      event.SpanContext,
		UserID:           event.UserID,
		SessionID:        event.SessionID,
		Timestamp:        event.Timestamp,
		Version:          event.Version,
		EventID:          event.EventID,
		EventType:        event.EventType,
		EventLevel:       event.EventLevel,
		ServiceName:      event.ServiceName,
		ClientIDs:        event.ClientIDs,
		TargetUserIDs:    event.TargetUserIDs,
		TargetNamespace:  event.TargetNamespace,
		Privacy:          event.Privacy,
		Topic:            subscribeBuilder.topic,
		AdditionalFields: event.AdditionalFields,
		Payload:          event.Payload,
		Partition:        event.Partition,
		Offset:           event.Offset,
		Key:              event.Key,
	}, nil)
}
