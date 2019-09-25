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
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const (
	defaultReaderSize = 10e6 // 10MB
	maxBackOffCount   = 3
)

var (
	errPubNilEvent = errors.New("unable to publish nil event")
	errSubNilEvent = errors.New("unable to subscribe nil event")
)

// KafkaClient wraps client's functionality for Kafka
type KafkaClient struct {

	// topic prefix
	prefix string

	// publish configuration
	publishConfig kafka.WriterConfig

	// subscribe configuration
	subscribeConfig kafka.ReaderConfig

	// map to store callback function
	subscribeMap *sync.Map
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
}

// newKafkaClient create a new instance of KafkaClient
func newKafkaClient(brokers []string, prefix string, config ...*BrokerConfig) *KafkaClient {

	logrus.Debug("create new kafka client")

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
	if len(config) > 0 {
		setConfig(writerConfig, readerConfig, config[0])
	}

	return &KafkaClient{
		prefix:          prefix,
		publishConfig:   *writerConfig,
		subscribeConfig: *readerConfig,
		subscribeMap:    &sync.Map{},
	}
}

// Publish send event to single or multiple topic with exponential backoff retry
func (client *KafkaClient) Publish(publishBuilder *PublishBuilder) error {
	if publishBuilder == nil {
		return errPubNilEvent
	}
	logrus.Debugf("publish event %s into topic %s", publishBuilder.eventName, publishBuilder.topic)

	err := validatePublishEvent(publishBuilder)
	if err != nil {
		return err
	}

	message, err := constructEvent(publishBuilder)
	if err != nil {
		return fmt.Errorf("unable to construct event : %s , error : %v", publishBuilder.eventName, err)
	}

	config := client.publishConfig
	for _, pubTopic := range publishBuilder.topic {
		topic := pubTopic
		go func() {
			err = backoff.RetryNotify(func() error {
				return client.publishEvent(publishBuilder.ctx, topic, config, message)
			}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxBackOffCount),
				func(err error, _ time.Duration) {
					logrus.Debugf("retrying publish event: error %v : ", err)
				})
			if err != nil {
				logrus.Errorf("unable to publish event. topic : %s , event : %s , error : %v", topic,
					publishBuilder.eventName, err)
			}
		}()
	}
	return nil
}

// Publish send event to a topic
func (client *KafkaClient) publishEvent(ctx context.Context, topic string, config kafka.WriterConfig,
	message kafka.Message) error {

	topicName := constructTopic(client.prefix, topic)
	config.Topic = topicName
	writer := kafka.NewWriter(config)
	defer func() {
		_ = writer.Close()
	}()

	err := writer.WriteMessages(ctx, message)
	if err != nil {
		logrus.Errorf("unable to publish event to kafka. topic : %s , error : %v", topicName, err)
		return fmt.Errorf("unable to publish event to kafka. topic : %s , error : %v", topicName, err)
	}
	return nil
}

// constructEvent construct event message
func constructEvent(publishBuilder *PublishBuilder) (kafka.Message, error) {
	id := generateID()
	event := &Event{
		ID:        id,
		EventName: publishBuilder.eventName,
		Namespace: publishBuilder.namespace,
		ClientID:  publishBuilder.clientID,
		UserID:    publishBuilder.userID,
		TraceID:   publishBuilder.traceID,
		SessionID: publishBuilder.sessionID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Version:   publishBuilder.version,
		Payload:   publishBuilder.payload,
	}

	eventBytes, err := marshal(event)
	if err != nil {
		logrus.Errorf("unable to marshal event : %s , error : %v", publishBuilder.eventName, err)
		return kafka.Message{}, err
	}

	return kafka.Message{
		Key:   []byte(id),
		Value: eventBytes,
	}, nil

}

// Register register callback function and then subscribe topic
func (client *KafkaClient) Register(subscribeBuilder *SubscribeBuilder) error {
	if subscribeBuilder == nil {
		return errSubNilEvent
	}
	logrus.Debugf("register callback to consume topic %s , event : %s", subscribeBuilder.topic,
		subscribeBuilder.eventName)

	err := validateSubscribeEvent(subscribeBuilder)
	if err != nil {
		return err
	}

	go func() {
		topic := constructTopic(client.prefix, subscribeBuilder.topic)
		groupID := constructGroupID(subscribeBuilder.groupID)
		isRegistered, err := client.registerCallback(topic, subscribeBuilder.eventName, subscribeBuilder.callback)
		if err != nil {
			logrus.Errorf("unable to register callback. error : %v", err)
			return
		}

		if isRegistered {
			logrus.Warnf("topic and event already registered. topic : %s , event : %s", subscribeBuilder.topic,
				subscribeBuilder.eventName)
		}

		config := client.subscribeConfig
		config.Topic = topic
		config.GroupID = groupID
		config.StartOffset = kafka.LastOffset
		reader := kafka.NewReader(config)
		defer func() {
			_ = reader.Close()
		}()

		for {
			consumerMessage, err := reader.ReadMessage(subscribeBuilder.ctx)
			if err != nil {
				logrus.Error("unable to subscribe topic from kafka. error : ", err)
				return
			}
			go client.processMessage(consumerMessage)
		}
	}()
	return nil
}

// registerCallback add callback to map with topic and eventName as a key
func (client *KafkaClient) registerCallback(topic, eventName string, callback func(event *Event, err error)) (
	isRegistered bool, err error) {

	if innerMap, ok := client.subscribeMap.Load(topic); ok {
		innerValue, ok := innerMap.(*sync.Map)
		if !ok {
			return false, errors.New("unable to convert interface to sync map")
		}

		if _, ok = innerValue.Load(eventName); ok {
			return true, nil
		}
	}

	var callbackMap = &sync.Map{}
	callbackMap.Store(eventName, callback)
	client.subscribeMap.Store(topic, callbackMap)
	return false, nil
}

// processMessage process a message from kafka
func (client *KafkaClient) processMessage(message kafka.Message) {
	event, err := unmarshal(message)
	if err != nil {
		logrus.Error("unable to unmarshal message from subscribe in kafka. error : ", err)
		return
	}
	client.runCallback(event, message)
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
func (client *KafkaClient) runCallback(event *Event, consumerMessage kafka.Message) {
	value, ok := client.subscribeMap.Load(consumerMessage.Topic)
	if !ok {
		logrus.Errorf("callback not found for topic : %s", consumerMessage.Topic)
		return
	}

	innerMap, ok := value.(*sync.Map)
	if !ok {
		logrus.Error("unable to convert interface to sync map")
		return
	}

	innerValue, ok := innerMap.Load(event.EventName)
	if !ok {
		logrus.Errorf("callback not found for topic : %s, event name : %s", consumerMessage.Topic,
			event.EventName)
		return
	}

	callback, ok := innerValue.(func(*Event, error))
	if !ok {
		logrus.Error("unable to convert interface to callback function")
		return
	}

	go callback(&Event{
		ID:        event.ID,
		ClientID:  event.ClientID,
		EventName: event.EventName,
		Namespace: event.Namespace,
		UserID:    event.UserID,
		SessionID: event.SessionID,
		TraceID:   event.TraceID,
		Timestamp: event.Timestamp,
		Version:   event.Version,
		Payload:   event.Payload,
	}, nil)
}
