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
	log "github.com/sirupsen/logrus"
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

	// enable strict validation for event fields
	strictValidation bool

	// publish configuration
	publishConfig kafka.WriterConfig

	// subscribe configuration
	subscribeConfig kafka.ReaderConfig

	// map to store callback function
	subscribeMap *sync.Map

	// map to store stop channel for unsubscribe
	stopChannelMap *sync.Map
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
		log.SetLevel(log.DebugLevel)
	case InfoLevel:
		log.SetLevel(log.InfoLevel)
	case WarnLevel:
		log.SetLevel(log.WarnLevel)
	case ErrorLevel:
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetOutput(ioutil.Discard)
	}
}

// newKafkaClient create a new instance of KafkaClient
func newKafkaClient(brokers []string, prefix string, config ...*BrokerConfig) *KafkaClient {

	log.Info("create new kafka client")

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
		subscribeMap:     &sync.Map{},
		stopChannelMap:   &sync.Map{},
	}
}

// Publish send event to single or multiple topic with exponential backoff retry
func (client *KafkaClient) Publish(publishBuilder *PublishBuilder) error {
	if publishBuilder == nil {
		log.Error(errPubNilEvent)
		return errPubNilEvent
	}

	err := validatePublishEvent(publishBuilder, client.strictValidation)
	if err != nil {
		log.Error(err)
		return err
	}

	message, event, err := constructEvent(publishBuilder)
	if err != nil {
		log.Errorf("unable to construct event: %s , error: %v", publishBuilder.eventName, err)
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
					log.Debugf("retrying publish event: error %v: ", err)
				})
			if err != nil {
				log.Errorf("unable to publish event. topic: %s , event: %s , error: %v", topic,
					publishBuilder.eventName, err)
				if publishBuilder.errorCallback != nil {
					publishBuilder.errorCallback(event, err)
				}
				return
			}
			log.Debugf("successfully publish event %s into topic %s", publishBuilder.eventName,
				topic)
		}()
	}
	return nil
}

// Publish send event to a topic
func (client *KafkaClient) publishEvent(ctx context.Context, topic, eventName string, config kafka.WriterConfig,
	message kafka.Message) error {

	topicName := constructTopic(client.prefix, topic)
	log.Debugf("publish event %s into topic %s", eventName,
		topicName)

	config.Topic = topicName
	writer := kafka.NewWriter(config)
	defer func() {
		_ = writer.Close()
	}()

	err := writer.WriteMessages(ctx, message)
	if err != nil {
		log.Errorf("unable to publish event to kafka. topic: %s , error: %v", topicName, err)
		return fmt.Errorf("unable to publish event to kafka. topic: %s , error: %v", topicName, err)
	}
	return nil
}

// constructEvent construct event message
func constructEvent(publishBuilder *PublishBuilder) (kafka.Message, *Event, error) {
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
		log.Errorf("unable to marshal event: %s , error: %v", publishBuilder.eventName, err)
		return kafka.Message{}, event, err
	}

	return kafka.Message{
		Key:   []byte(id),
		Value: eventBytes,
	}, event, nil

}

// Unregister unregister callback function and topic
func (client *KafkaClient) Unregister(topic string) {
	if _, ok := client.subscribeMap.Load(topic); ok {
		client.subscribeMap.Delete(topic)
	}
	if val, ok := client.stopChannelMap.Load(topic); ok {
		val.(chan interface{}) <- true
	}
}

// Register register callback function and then subscribe topic
func (client *KafkaClient) Register(subscribeBuilder *SubscribeBuilder) error {
	if subscribeBuilder == nil {
		log.Error(errSubNilEvent)
		return errSubNilEvent
	}
	log.Debugf("register callback to consume topic %s , event: %s", subscribeBuilder.topic,
		subscribeBuilder.eventName)

	err := validateSubscribeEvent(subscribeBuilder)
	if err != nil {
		log.Error(err)
		return err
	}

	go func() {
		topic := constructTopic(client.prefix, subscribeBuilder.topic)
		groupID := constructGroupID(subscribeBuilder.groupID)
		isRegistered, err := client.registerCallback(topic, subscribeBuilder.eventName, subscribeBuilder.callback)
		if err != nil {
			log.Errorf("unable to register callback. error: %v", err)
			return
		}

		if isRegistered {
			log.Warnf("topic and event already registered. topic: %s , event: %s", subscribeBuilder.topic,
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
		stopChan := make(chan interface{})
		client.stopChannelMap.Store(topic, stopChan)

		for {
			select {
			case <-stopChan:
				return
			default:
				consumerMessage, errRead := reader.ReadMessage(subscribeBuilder.ctx)
				if errRead != nil {
					log.Error("unable to subscribe topic from kafka. error: ", errRead)
					return
				}
				go client.processMessage(consumerMessage, groupID)
			}
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

		// event callback with this topic name is there with another event callback registered, so append it
		if innerValue != nil {
			innerValue.Store(eventName, callback)
			client.subscribeMap.Store(topic, innerValue)
			return false, nil
		}
	}

	var callbackMap = &sync.Map{}
	callbackMap.Store(eventName, callback)
	client.subscribeMap.Store(topic, callbackMap)
	return false, nil
}

// processMessage process a message from kafka
func (client *KafkaClient) processMessage(message kafka.Message, groupID string) {
	log.Debugf("process message from topic: %s, groupID : %s", message.Topic, groupID)

	event, err := unmarshal(message)
	if err != nil {
		log.Error("unable to unmarshal message from subscribe in kafka. error: ", err)
		return
	}
	client.runCallback(event, message, groupID)
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
func (client *KafkaClient) runCallback(event *Event, consumerMessage kafka.Message, groupID string) {
	value, ok := client.subscribeMap.Load(consumerMessage.Topic)
	if !ok {
		log.Errorf("callback not found for topic: %s", consumerMessage.Topic)
		return
	}

	innerMap, ok := value.(*sync.Map)
	if !ok {
		log.Error("unable to convert interface to sync map")
		return
	}

	innerValue, ok := innerMap.Load(event.EventName)
	if !ok {
		log.Errorf("callback not found for topic: %s, event name: %s", consumerMessage.Topic,
			event.EventName)
		return
	}

	callback, ok := innerValue.(func(*Event, error))
	if !ok {
		log.Error("unable to convert interface to callback function")
		return
	}

	log.Debugf("run callback for topic: %s , event name: %s, groupID: %s", consumerMessage.Topic,
		event.EventName, groupID)
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
