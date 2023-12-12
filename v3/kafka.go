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
	"strconv"
	"sync"
	"time"

	"github.com/AccelByte/eventstream-go-sdk/v3/pkg/kafkaprometheus"
	"github.com/cenkalti/backoff"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/sirupsen/logrus"
)

const (
	defaultReaderSize     = 10e6 // 10MB
	maxBackOffCount       = 4
	kafkaMaxWait          = time.Second // (for consumer message batching)
	saslScramAuth         = "SASL-SCRAM"
	defaultPublishTimeout = 60 * time.Second

	auditLogTopicEnvKey  = "APP_EVENT_STREAM_AUDIT_LOG_TOPIC"
	auditLogEnableEnvKey = "APP_EVENT_STREAM_AUDIT_LOG_ENABLED"
	auditLogTopicDefault = "auditLog"
)

var (
	auditLogTopic  = ""
	auditEnabled   = true
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
	readers map[string]*kafka.Reader

	// current writers
	writers map[string]*kafka.Writer

	// mutex to avoid runtime races to access subscribers map
	ReadersLock sync.RWMutex

	// mutex to avoid runtime races to access writers map
	WritersLock sync.RWMutex

	// current topic subscribed on the kafka client
	topicSubscribedCount map[string]int
}

// setConfig sets some defaults for producers and consumers. Needed for backwards compatibility.
func setConfig(configList []*BrokerConfig, brokers []string) (BrokerConfig, error) {
	// only uses first KafkaConfig arguments
	hasConfig := len(configList) > 0 && configList[0] != nil
	var config BrokerConfig
	if hasConfig {
		config = *configList[0]
	}
	if config.BaseWriterConfig == nil {
		config.BaseWriterConfig = &kafka.WriterConfig{}
	}
	if config.BaseReaderConfig == nil {
		config.BaseReaderConfig = &kafka.ReaderConfig{}
	}

	// set config defaults
	config.BaseWriterConfig.Brokers = brokers
	config.BaseReaderConfig.Brokers = brokers

	if config.BaseReaderConfig.MaxBytes == 0 {
		config.BaseReaderConfig.MaxBytes = defaultReaderSize
	}

	if config.BaseReaderConfig.MaxWait == 0 {
		config.BaseReaderConfig.MaxWait = kafkaMaxWait
	}

	if config.BaseReaderConfig.CommitInterval == 0 {
		config.BaseReaderConfig.CommitInterval = 100 * time.Millisecond // 0 means synchronous commits, we don't support it
	}

	if config.BaseWriterConfig.ReadTimeout == 0 && config.ReadTimeout != 0 {
		config.BaseWriterConfig.ReadTimeout = config.ReadTimeout
	}

	if config.BaseWriterConfig.WriteTimeout == 0 && config.WriteTimeout != 0 {
		config.BaseWriterConfig.WriteTimeout = config.WriteTimeout
	}

	if hasConfig {
		dialer := &kafka.Dialer{}
		if config.DialTimeout != 0 {
			dialer.Timeout = config.DialTimeout
		}

		if config.SecurityConfig != nil && config.SecurityConfig.AuthenticationType == saslScramAuth {
			mechanism, err := scram.Mechanism(scram.SHA512, config.SecurityConfig.SASLUsername, config.SecurityConfig.SASLPassword)
			if err != nil {
				logrus.Error("unable to initialize kafka scram authentication", err)
				return config, err
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
				return config, err
			}

			tlsConfig := &tls.Config{
				Certificates: []tls.Certificate{*cert},
			}

			dialer.TLS = tlsConfig
		}

		config.BaseWriterConfig.Dialer = dialer
		config.BaseReaderConfig.Dialer = dialer
		config.BaseWriterConfig.Balancer = config.Balancer
	}

	return config, nil
}

// newKafkaClient create a new instance of KafkaClient
func newKafkaClient(brokers []string, prefix string, configList ...*BrokerConfig) (*KafkaClient, error) {
	logrus.Info("create new kafka client")

	loadAuditEnv()

	config, err := setConfig(configList, brokers)

	client := &KafkaClient{
		prefix:               prefix,
		strictValidation:     config.StrictValidation,
		publishConfig:        *config.BaseWriterConfig,
		subscribeConfig:      *config.BaseReaderConfig,
		readers:              make(map[string]*kafka.Reader),
		writers:              make(map[string]*kafka.Writer),
		topicSubscribedCount: make(map[string]int),
	}
	if config.MetricsRegistry != nil {
		err = config.MetricsRegistry.Register(&kafkaprometheus.WriterCollector{Client: client})
		if err != nil {
			logrus.Errorf("failed to register kafka writers metrics: %v", err)
		}
		err = config.MetricsRegistry.Register(&kafkaprometheus.ReaderCollector{Client: client})
		if err != nil {
			logrus.Errorf("failed to register kafka Readers metrics: %v", err)
		}
	}
	return client, err
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

	if len(publishBuilder.topic) > 1 {
		// TODO, change Topic() api to only allow 1 topic so we can simplify this logic. It will be a breaking change.
		logrus.Warnf("eventstream got more than 1 topic per publish: %+v", publishBuilder.topic)
	}

	for _, pubTopic := range publishBuilder.topic {
		topic := constructTopic(client.prefix, pubTopic)

		if publishBuilder.timeout == 0 {
			publishBuilder.timeout = defaultPublishTimeout
		}

		go func(topic string) {
			publishCtx, cancelPublish := context.WithTimeout(context.Background(), publishBuilder.timeout)
			defer cancelPublish()

			err = backoff.RetryNotify(func() error {
				return client.publishEvent(publishCtx, topic, publishBuilder.eventName, config, message)
			}, backoff.WithContext(newPublishBackoff(), publishCtx),
				func(err error, d time.Duration) {
					logrus.
						WithField("Topic Name", topic).
						WithField("Event Name", publishBuilder.eventName).
						WithField("backoff-duration", d).
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
		}(topic)
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

	logFields := logrus.
		WithField("Topic Name", topic).
		WithField("Event Name", eventName)

	logFields.Debug("publish event")

	defer func() {
		if r := recover(); r != nil {
			logFields.Warn("unable to publish event: recover: ", r)

			if writer == nil {
				logFields.Warn("unable to publish event: writer is nil")

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

// PublishAuditLog send an audit log message
func (client *KafkaClient) PublishAuditLog(auditLogBuilder *AuditLogBuilder) error {
	if auditEnabled {
		var topic = auditLogTopicDefault
		if auditLogTopic != "" {
			topic = auditLogTopic
		}

		message, err := auditLogBuilder.Build()
		if err != nil {
			return err
		}
		return client.publishAndRetryFailure(context.Background(), topic, "", message, auditLogBuilder.errorCallback)
	}
	return nil
}

// publishAndRetryFailure will publish message to kafka, if it fails, will retry at most 3 times.
// If the message finally failed to publish, will call the error callback function to process this failure.
func (client *KafkaClient) publishAndRetryFailure(context context.Context, topic, eventName string, message kafka.Message, failureCallback PublishErrorCallbackFunc) error {

	config := client.publishConfig
	topic = constructTopic(client.prefix, topic)

	go func() {
		err := backoff.RetryNotify(func() error {
			return client.publishEvent(context, topic, eventName, config, message)
		}, backoff.WithMaxRetries(newPublishBackoff(), maxBackOffCount),
			func(err error, _ time.Duration) {
				logrus.WithField("topic", topic).
					Warn("retrying publish message: ", err)
			})
		if err != nil {
			logrus.WithField("topic", topic).
				Error("retrying publish message failed: ", err)

			if failureCallback != nil {
				failureCallback(message.Value, err)
			}
			return
		}
		logrus.WithField("topic", topic).
			Debug("successfully publish message")
	}()

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
		ParentNamespace:  publishBuilder.parentNamespace,
		UnionNamespace:   publishBuilder.unionNamespace,
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
	client.ReadersLock.Lock()
	defer client.ReadersLock.Unlock()
	delete(client.readers, subscribeBuilder.Slug())
	currentSubscribeCount := client.topicSubscribedCount[subscribeBuilder.topic]
	if currentSubscribeCount > 0 {
		client.topicSubscribedCount[subscribeBuilder.topic] = currentSubscribeCount - 1
	}
}

// Register register callback function and then subscribe topic
// nolint: gocognit,funlen
func (client *KafkaClient) Register(subscribeBuilder *SubscribeBuilder) error {
	if subscribeBuilder == nil {
		logrus.Error(errSubNilEvent)
		return errSubNilEvent
	}

	logrus.
		WithField("Topic Name", subscribeBuilder.topic).
		WithField("Event Name", subscribeBuilder.eventName).
		Info("register callback")

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

	loggerFields := logrus.
		WithField("Topic Name", topic).
		WithField("Event Name", subscribeBuilder.eventName)

	isRegistered := client.registerSubscriber(subscribeBuilder)
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
		client.setSubscriberReader(subscribeBuilder, reader)

		var eventProcessingFailed bool

		defer func() {

			reader.Close() // nolint: errcheck
			client.unregister(subscribeBuilder)

			if eventProcessingFailed {
				if subscribeBuilder.ctx.Err() != nil {
					// the subscription is shutting down. triggered by an external context cancellation
					loggerFields.Warn("triggered an external context cancellation. Cancelling the subscription")
					return
				}

				// current worker can't process the event and we need to unblock the event for other workers
				// as we use kafka in the explicit commit mode - we can't send the "acknowledge" and have to interrupt connection
				time.Sleep(time.Second)

				loggerFields.Warn("trying to re-register because event processing failed")
				err := client.Register(subscribeBuilder)
				if err != nil {
					loggerFields.Error(err)
				}
			}
		}()

		for {
			select {
			case <-subscribeBuilder.ctx.Done():
				// ignore error because client isn't processing events
				if subscribeBuilder.callback != nil {
					err = subscribeBuilder.callback(subscribeBuilder.ctx, nil, subscribeBuilder.ctx.Err())
				}
				if subscribeBuilder.callbackRaw != nil {
					err = subscribeBuilder.callbackRaw(subscribeBuilder.ctx, nil, subscribeBuilder.ctx.Err())
				}

				loggerFields.Warn("triggered an external context cancellation. Cancelling the subscription")

				return
			default:
				consumerMessage, errRead := reader.FetchMessage(subscribeBuilder.ctx)
				if errRead != nil {
					if errRead == context.Canceled {
						loggerFields.Infof("subscriber shut down because context cancelled")
					} else {
						loggerFields.Errorf("subscriber unable to fetch message: %v", errRead)
					}

					if subscribeBuilder.ctx.Err() != nil {
						// the subscription is shutting down. triggered by an external context cancellation
						loggerFields.Warn("triggered an external context cancellation. Cancelling the subscription")
						continue // Shutting down because ctx expired
					}

					// On read error we just retry (after slight delay).
					// Typical errors from the cluster include: consumer group is rebalancing, or leader re-election.
					// Those aren't hard errors so we should just call FetchMessage again.
					// It can also return IO errors like EOF, but not that reader automatically handles reconnecting to the cluster.
					time.Sleep(200 * time.Millisecond)
					continue
				}

				err := client.processMessage(subscribeBuilder, consumerMessage, topic)
				if err != nil {
					loggerFields.Error("unable to process the event: ", err)

					// shutdown current subscriber and mark it for restarting
					eventProcessingFailed = true

					return
				}

				err = reader.CommitMessages(subscribeBuilder.ctx, consumerMessage)
				if err != nil {
					if subscribeBuilder.ctx.Err() == nil {
						// the subscription is shutting down. triggered by an external context cancellation
						loggerFields.Warn("triggered an external context cancellation. Cancelling the subscription")
						continue
					}

					loggerFields.Error("unable to commit the event: ", err)
				}
			}
		}
	}()

	return nil
}

// registerSubscriber add callback to map with topic and eventName as a key
func (client *KafkaClient) registerSubscriber(subscribeBuilder *SubscribeBuilder) (
	isRegistered bool,
) {
	slug := subscribeBuilder.Slug() // slug contains the topic, eventName and groupID.

	client.ReadersLock.Lock()
	defer client.ReadersLock.Unlock()
	if _, exists := client.readers[slug]; exists {
		if subscribeBuilder.groupID == "" {
			return true
		} else {
			// Note: for backwards compatibility we allow multiple subscribers to the same event per pod,
			// but beside tests there's no good reason to do that.
			logrus.Warnf("multiple subscribers for %+v", subscribeBuilder)
		}
	}
	currentSubscribeCount := client.topicSubscribedCount[subscribeBuilder.topic]
	if currentSubscribeCount > 0 {
		logrus.WithField("topic", subscribeBuilder.topic).Warn("multiple subscribe for a topic")
	}
	client.topicSubscribedCount[subscribeBuilder.topic] = currentSubscribeCount + 1

	client.readers[slug] = nil //  It's registered. Later we set the actual value to the kafka.Writer.

	return false
}

func (client *KafkaClient) setSubscriberReader(subscribeBuilder *SubscribeBuilder, reader *kafka.Reader) {
	slug := subscribeBuilder.Slug()
	client.ReadersLock.Lock()
	defer client.ReadersLock.Unlock()
	client.readers[slug] = reader
}

// getWriter get a writer based on config
func (client *KafkaClient) getWriter(config kafka.WriterConfig) *kafka.Writer {
	client.WritersLock.Lock()
	defer client.WritersLock.Unlock()

	if writer, ok := client.writers[config.Topic]; ok {
		return writer
	}

	writer := kafka.NewWriter(config)
	client.writers[config.Topic] = writer

	return writer
}

// newWriter new a writer
func (client *KafkaClient) newWriter(config kafka.WriterConfig) *kafka.Writer {
	writer := kafka.NewWriter(config)

	client.WritersLock.Lock()
	defer client.WritersLock.Unlock()
	client.writers[config.Topic] = writer

	return writer
}

// deleteWriter delete writer
func (client *KafkaClient) deleteWriter(topic string) {
	client.WritersLock.Lock()
	defer client.WritersLock.Unlock()

	writer, ok := client.writers[topic]
	if ok {
		_ = writer.Close()
	}

	// we only delete the writer from the slice but no close, should close in some interval?
	delete(client.writers, topic)
}

// processMessage process a message from kafka
func (client *KafkaClient) processMessage(subscribeBuilder *SubscribeBuilder, message kafka.Message, topic string) error {
	if subscribeBuilder.callbackRaw != nil {
		return subscribeBuilder.callbackRaw(subscribeBuilder.ctx, message.Value, nil)
	}

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

func loadAuditEnv() {
	// load audit log topic
	if auditLogTopicName := loadEnv(auditLogTopicEnvKey); auditLogTopicName != "" {
		auditLogTopic = auditLogTopicName
	}
	if auditEnabledCfgStr := loadEnv(auditLogEnableEnvKey); auditEnabledCfgStr != "" {
		auditEnabledCfg, err := strconv.ParseBool(auditEnabledCfgStr)
		if err != nil {
			logrus.Error("unable to parse env audit env, err: ", err)
		}
		auditEnabled = auditEnabledCfg
	}
}

// GetWriterStats per topic
func (client *KafkaClient) GetWriterStats() (stats []kafka.WriterStats, topics []string) {
	client.WritersLock.RLock()
	defer client.WritersLock.RUnlock()

	stats = make([]kafka.WriterStats, 0, len(client.writers))
	topics = make([]string, 0, len(client.writers))
	for _, writer := range client.writers {
		if writer == nil {
			continue
		}
		topics = append(topics, writer.Topic)
		stats = append(stats, writer.Stats())
	}
	return stats, topics
}

// GetReaderStats returns stats for each subscriber, and its topic, eventName and groupID.
func (client *KafkaClient) GetReaderStats() (stats []kafka.ReaderStats, slugs []string) {
	client.ReadersLock.RLock()
	defer client.ReadersLock.RUnlock()

	stats = make([]kafka.ReaderStats, 0, len(client.readers))
	slugs = make([]string, 0, len(client.readers))
	for slug, reader := range client.readers {
		if reader == nil {
			continue
		}
		slugs = append(slugs, slug)
		stats = append(stats, reader.Stats())
	}
	return stats, slugs
}

func newPublishBackoff() *backoff.ExponentialBackOff {
	backoff := backoff.NewExponentialBackOff()
	// We increase the default multiplier, because kafka cluster operations can take quite long to complete,
	// e.g. auto-creating topics, leader re-election, or rebalancing.
	// For maxBackoffCount=4 we will get attempts: 0ms, 500ms, 2s, 8s, 16s.
	backoff.Multiplier = 4.0
	return backoff
}
