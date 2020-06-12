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
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const (
	defaultReaderSize = 10e6 // 10MB
	maxBackOffCount   = 3
	kafkaMaxWait      = time.Second
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

	// mutex to avoid runtime races to access subscribers map
	lock sync.RWMutex
}

func setConfig(writerConfig *kafka.WriterConfig, readerConfig *kafka.ReaderConfig, config *BrokerConfig) {
	if config.ReadTimeout != 0 {
		writerConfig.ReadTimeout = config.WriteTimeout
	}

	if config.WriteTimeout != 0 {
		writerConfig.WriteTimeout = config.WriteTimeout
	}

	if config.DialTimeout != 0 {
		dialer := &kafka.Dialer{
			Timeout: config.DialTimeout,
		}
		writerConfig.Dialer = dialer
		readerConfig.Dialer = dialer
	}

	setLogLevel(config.LogMode)
}

func setLogLevel(logMode string) {
	switch logMode {
	case DebugLevel:
		logrus.SetLevel(logrus.DebugLevel)
	case InfoLevel:
		logrus.SetLevel(logrus.InfoLevel)
	case WarnLevel:
		logrus.SetLevel(logrus.WarnLevel)
	case ErrorLevel:
		logrus.SetLevel(logrus.ErrorLevel)
	default:
		logrus.SetOutput(ioutil.Discard)
	}
}

// newKafkaClient create a new instance of KafkaClient
func newKafkaClient(brokers []string, prefix string, config ...*BrokerConfig) *KafkaClient {
	logrus.Info("create new kafka client")

	writerConfig := &kafka.WriterConfig{
		Brokers:  brokers,
		Balancer: &kafka.LeastBytes{},
	}

	readerConfig := &kafka.ReaderConfig{
		Brokers:  brokers,
		MaxBytes: defaultReaderSize,
	}

	// set client configuration
	// only uses first KafkaConfig arguments
	var strictValidation bool

	if len(config) > 0 {
		setConfig(writerConfig, readerConfig, config[0])
		strictValidation = config[0].StrictValidation
	}

	return &KafkaClient{
		prefix:           prefix,
		strictValidation: strictValidation,
		publishConfig:    *writerConfig,
		subscribeConfig:  *readerConfig,
		subscribers:      make(map[*SubscribeBuilder]struct{}),
	}
}

// Publish send event to single or multiple topic with exponential backoff retry
func (client *KafkaClient) Publish(publishBuilder *PublishBuilder) error {
	if publishBuilder == nil {
		logrus.Error(errPubNilEvent)
		return errPubNilEvent
	}

	err := validatePublishEvent(publishBuilder, client.strictValidation)
	if err != nil {
		logrus.Error(err)
		return err
	}

	message, event, err := ConstructEvent(publishBuilder)
	if err != nil {
		logrus.Errorf("unable to construct event: %s , error: %v", publishBuilder.eventName, err)
		return fmt.Errorf("unable to construct event : %s , error : %v", publishBuilder.eventName, err)
	}

	config := client.publishConfig

	for _, pubTopic := range publishBuilder.topic {
		topic := pubTopic

		go func() {
			err = backoff.RetryNotify(func() error {
				return client.publishEvent(publishBuilder.ctx, topic, publishBuilder.eventName, config, message)
			}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxBackOffCount),
				func(err error, _ time.Duration) {
					logrus.Debugf("retrying publish event: error %v: ", err)
				})
			if err != nil {
				logrus.Errorf("unable to publish event. topic: %s , event: %s , error: %v", topic,
					publishBuilder.eventName, err)

				if publishBuilder.errorCallback != nil {
					publishBuilder.errorCallback(event, err)
				}

				return
			}

			logrus.Debugf("successfully publish event %s into topic %s", publishBuilder.eventName,
				topic)
		}()
	}

	return nil
}

// Publish send event to a topic
func (client *KafkaClient) publishEvent(ctx context.Context, topic, eventName string, config kafka.WriterConfig,
	message kafka.Message) error {
	topicName := constructTopic(client.prefix, topic)
	logrus.Debugf("publish event %s into topic %s", eventName,
		topicName)

	config.Topic = topicName
	writer := kafka.NewWriter(config)

	defer func() {
		_ = writer.Close()
	}()

	err := writer.WriteMessages(ctx, message)
	if err != nil {
		logrus.Errorf("unable to publish event to kafka. topic: %s , error: %v", topicName, err)
		return fmt.Errorf("unable to publish event to kafka. topic: %s , error: %v", topicName, err)
	}

	return nil
}

// ConstructEvent construct event message
func ConstructEvent(publishBuilder *PublishBuilder) (kafka.Message, *Event, error) {
	id := generateID()
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
		logrus.Errorf("unable to marshal event: %s , error: %v", publishBuilder.eventName, err)
		return kafka.Message{}, event, err
	}

	return kafka.Message{
		Key:   []byte(id),
		Value: eventBytes,
	}, event, nil
}

// unregister unregister subscriber
func (client *KafkaClient) unregister(subscribeBuilder *SubscribeBuilder) {
	client.lock.Lock()
	defer client.lock.Unlock()

	delete(client.subscribers, subscribeBuilder)
}

// Register register callback function and then subscribe topic
//nolint: gocognit,funlen
func (client *KafkaClient) Register(subscribeBuilder *SubscribeBuilder) error {
	if subscribeBuilder == nil {
		logrus.Error(errSubNilEvent)
		return errSubNilEvent
	}

	logrus.Debugf("register callback to consume topic %s , event: %s", subscribeBuilder.topic,
		subscribeBuilder.eventName)

	err := validateSubscribeEvent(subscribeBuilder)
	if err != nil {
		logrus.Error(err)
		return err
	}

	topic := constructTopic(client.prefix, subscribeBuilder.topic)
	groupID := constructGroupID(client.prefix, subscribeBuilder.groupID)

	isRegistered, err := client.registerSubscriber(subscribeBuilder)
	if err != nil {
		logrus.Errorf("unable to register callback. error: %v", err)
		return err
	}

	if isRegistered {
		return fmt.Errorf(
			"topic and event already registered. topic: %s , event: %s",
			subscribeBuilder.topic,
			subscribeBuilder.eventName,
		)
	}

	config := client.subscribeConfig
	config.Topic = topic
	config.GroupID = groupID
	config.StartOffset = subscribeBuilder.offset
	config.MaxWait = kafkaMaxWait
	reader := kafka.NewReader(config)

	go func() {
		var eventProcessingFailed bool

		defer func() {
			if eventProcessingFailed {
				if subscribeBuilder.ctx.Err() != nil {
					// the subscription is shutting down. triggered by an external context cancellation
					logrus.Debug("triggered an external context cancellation. Cancelling the subscription")
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

				logrus.Debug("triggered an external context cancellation. Cancelling the subscription")

				return
			default:
				consumerMessage, errRead := reader.FetchMessage(subscribeBuilder.ctx)
				if errRead != nil {
					if subscribeBuilder.ctx.Err() != nil {
						// the subscription is shutting down. triggered by an external context cancellation
						logrus.Debug("triggered an external context cancellation. Cancelling the subscription")

						continue
					}

					logrus.Errorf("unable to subscribe. Topic: %s, Err: %s", subscribeBuilder.topic, errRead)

					return
				}

				err := client.processMessage(subscribeBuilder, consumerMessage)
				if err != nil {
					logrus.Debugf("cancelling the subscription as consumer can't process the event. Err %s", err.Error())

					// shutdown current subscriber and mark it for restarting
					eventProcessingFailed = true

					return
				}

				err = reader.CommitMessages(subscribeBuilder.ctx, consumerMessage)
				if err != nil {
					if subscribeBuilder.ctx.Err() == nil {
						// the subscription is shutting down. triggered by an external context cancellation
						logrus.Debug("triggered an external context cancellation. Cancelling the subscription")
						continue
					}

					logrus.Error("unable to commit the event. error: ", err)
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
	client.lock.Lock()
	defer client.lock.Unlock()

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

// processMessage process a message from kafka
func (client *KafkaClient) processMessage(subscribeBuilder *SubscribeBuilder, message kafka.Message) error {
	logrus.Debugf("process message from topic: %s, groupID : %s", message.Topic, subscribeBuilder.groupID)

	event, err := unmarshal(message)
	if err != nil {
		logrus.Error("unable to unmarshal message from subscribe in kafka. error: ", err)

		// as retry will fail infinitely - return nil to ACK the event
		return nil
	}

	if subscribeBuilder.eventName != "" && subscribeBuilder.eventName != event.EventName {
		// don't send events if consumer subscribed on a non-empty event name
		// return nil to ACK the event
		return nil
	}

	return client.runCallback(subscribeBuilder, event, message)
}

// unmarshal unmarshal received message into event struct
func unmarshal(message kafka.Message) (*Event, error) {
	var event Event

	err := json.Unmarshal(message.Value, &event)
	if err != nil {
		return &Event{}, err
	}

	return &event, nil
}

// runCallback run callback function when receive an event
func (client *KafkaClient) runCallback(
	subscribeBuilder *SubscribeBuilder,
	event *Event,
	consumerMessage kafka.Message,
) error {
	logrus.Debugf("run callback for topic: %s , event name: %s, groupID: %s", consumerMessage.Topic,
		event.EventName, subscribeBuilder.groupID)

	return subscribeBuilder.callback(subscribeBuilder.ctx, &Event{
		ID:               event.ID,
		ClientID:         event.ClientID,
		EventName:        event.EventName,
		Namespace:        event.Namespace,
		UserID:           event.UserID,
		SessionID:        event.SessionID,
		TraceID:          event.TraceID,
		SpanContext:      event.SpanContext,
		Timestamp:        event.Timestamp,
		EventID:          event.EventID,
		EventType:        event.EventType,
		EventLevel:       event.EventLevel,
		ServiceName:      event.ServiceName,
		ClientIDs:        event.ClientIDs,
		TargetUserIDs:    event.TargetUserIDs,
		TargetNamespace:  event.TargetNamespace,
		Privacy:          event.Privacy,
		Version:          event.Version,
		AdditionalFields: event.AdditionalFields,
		Payload:          event.Payload,
	}, nil)
}
