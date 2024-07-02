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
	"strconv"
	"strings"
	"sync"
	"time"

	kafkaprometheus "github.com/AccelByte/eventstream-go-sdk/v4/kafkaprometheus"
	"github.com/AccelByte/eventstream-go-sdk/v4/statistics"
	"github.com/cenkalti/backoff"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

const (
	maxBackOffCount         = 4
	defaultPublishTimeoutMs = 60000 // 60 second
	saslScramAuth           = "SASL-SCRAM"

	auditLogTopicEnvKey  = "APP_EVENT_STREAM_AUDIT_LOG_TOPIC"
	auditLogEnableEnvKey = "APP_EVENT_STREAM_AUDIT_LOG_ENABLED"
	auditLogTopicDefault = "auditLog"

	messageAdditionalSizeApprox = 2048 // in Byte. Approx data added to message that sent to kafka

	producerStatsType = "producer"
	consumerStatsType = "consumer"
)

var (
	auditLogTopic      = ""
	auditEnabled       = true
	errPubNilEvent     = errors.New("unable to publish nil event")
	errSubNilEvent     = errors.New("unable to subscribe nil event")
	ErrMessageTooLarge = errors.New("message to large")
)

// KafkaClient wraps client's functionality for Kafka
type KafkaClient struct {

	// topic prefix
	prefix string

	// enable strict validation for event fields
	strictValidation bool

	configMap *kafka.ConfigMap

	configMapLock sync.RWMutex

	// flag to indicate that auto commit with interval is enabled instead of commit per message
	autoCommitIntervalEnabled bool

	// flag to indicate commit per message before the message processed
	commitBeforeMessage bool

	// current subscribers
	readers map[string]*kafka.Consumer

	// writer
	writer *kafka.Producer

	// mutex to avoid runtime races to access subscribers map
	ReadersLock sync.RWMutex

	// current topic subscribed on the kafka client
	topicSubscribedCount map[string]int

	adminClient *kafka.AdminClient

	writerStats statistics.Stats

	writerStatsLock sync.RWMutex

	readerStats statistics.Stats

	readerStatsLock sync.RWMutex
}

func getConfig(configList []*BrokerConfig, brokers []string) (BrokerConfig, *kafka.ConfigMap, error) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":  strings.Join(brokers, ","),
		"enable.auto.commit": false,
	}

	// only uses first KafkaConfig arguments
	hasConfig := len(configList) > 0 && configList[0] != nil
	var config BrokerConfig
	if hasConfig {
		config = *configList[0]

		if config.DialTimeout != 0 {
			if err := configMap.SetKey("socket.timeout.ms", fmt.Sprintf("%d", config.DialTimeout.Milliseconds())); err != nil {
				return config, nil, err
			}
		}

		if config.SecurityConfig != nil && config.SecurityConfig.AuthenticationType == saslScramAuth {
			if err := configMap.SetKey("security.protocol", "SASL_SSL"); err != nil {
				return config, nil, err
			}
			if err := configMap.SetKey("sasl.mechanisms", "SCRAM-SHA-512"); err != nil {
				return config, nil, err
			}
			if err := configMap.SetKey("sasl.username", config.SecurityConfig.SASLUsername); err != nil {
				return config, nil, err
			}
			if err := configMap.SetKey("sasl.password", config.SecurityConfig.SASLPassword); err != nil {
				return config, nil, err
			}
		}

		if config.CACertFile != "" {
			logrus.Debug("set TLS certificate")
			if err := configMap.SetKey("security.protocol", "SSL"); err != nil {
				return config, nil, err
			}
			if err := configMap.SetKey("ssl.ca.location", config.CACertFile); err != nil {
				return config, nil, err
			}
		}
	}

	return config, configMap, nil
}

// newKafkaClient create a new instance of KafkaClient
func newKafkaClient(brokers []string, prefix string, configList ...*BrokerConfig) (*KafkaClient, error) {
	logrus.Info("create new kafka client")

	loadAuditEnv()

	config, configMap, err := getConfig(configList, brokers)
	if err != nil {
		return nil, err
	}

	client := &KafkaClient{
		prefix:               prefix,
		strictValidation:     config.StrictValidation,
		readers:              make(map[string]*kafka.Consumer),
		configMap:            configMap,
		topicSubscribedCount: make(map[string]int),
	}
	if config.AutoCommitInterval != 0 {
		client.autoCommitIntervalEnabled = true
		client.configMap.SetKey("enable.auto.commit", true)
		client.configMap.SetKey("auto.commit.interval.ms", int(config.AutoCommitInterval.Milliseconds()))
	}
	client.commitBeforeMessage = config.CommitBeforeProcessing

	client.configMap.SetKey("statistics.interval.ms", 10000)
	client.configMap.SetKey("delivery.timeout.ms", defaultPublishTimeoutMs)

	// must be the last config applied
	for k, v := range config.BaseConfig {
		err := client.configMap.SetKey(k, v)
		if err != nil {
			return nil, err
		}
	}

	if config.MetricsRegistry != nil {
		err = config.MetricsRegistry.Register(&kafkaprometheus.WriterCollector{Client: client})
		if err != nil {
			logrus.Errorf("failed to register kafka writer metrics: %v", err)
		}
		err = config.MetricsRegistry.Register(&kafkaprometheus.ReaderCollector{Client: client})
		if err != nil {
			logrus.Errorf("failed to register kafka Readers metrics: %v", err)
		}
	}

	// initialize empty stats
	client.writerStats.BrokerStats = make(map[string]statistics.BrokerStats, 0)
	client.writerStats.TopicStats = make(map[string]statistics.TopicStats, 0)
	client.writerStats.TopicPartitionStats = make(map[string]statistics.TopicPartitionStats, 0)
	client.readerStats.BrokerStats = make(map[string]statistics.BrokerStats, 0)
	client.readerStats.TopicStats = make(map[string]statistics.TopicStats, 0)
	client.readerStats.TopicPartitionStats = make(map[string]statistics.TopicPartitionStats, 0)

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

	if err = client.validateMessageSize(message); err != nil {
		return err
	}

	config := client.configMap

	if publishBuilder.timeout > 0 {
		client.configMapLock.Lock()
		err = config.SetKey("delivery.timeout.ms", int(publishBuilder.timeout.Milliseconds()))
		if err != nil {
			return err
		}
		client.configMapLock.Unlock()
	} else {
		publishBuilder.timeout = defaultPublishTimeoutMs * time.Millisecond
	}

	topic := constructTopic(client.prefix, publishBuilder.topic)
	err = client.publishEvent(topic, publishBuilder.eventName, config, message, false)
	if err != nil {
		logrus.
			WithField("Topic Name", topic).
			WithField("Event Name", publishBuilder.eventName).
			Error("giving up publishing event: ", err)

		if publishBuilder.errorCallback != nil {
			publishBuilder.errorCallback(event, err)
		}

		return nil
	}

	logrus.
		WithField("Topic Name", topic).
		WithField("Event Name", publishBuilder.eventName).
		Debug("successfully publish event")

	return nil
}

// PublishSync send an event synchronously (blocking)
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

	if err = client.validateMessageSize(message); err != nil {
		return err
	}

	config := client.configMap

	if publishBuilder.timeout > 0 {
		client.configMapLock.Lock()
		err = config.SetKey("delivery.timeout.ms", int(publishBuilder.timeout.Milliseconds()))
		if err != nil {
			return err
		}
		client.configMapLock.Unlock()
	} else {
		publishBuilder.timeout = defaultPublishTimeoutMs * time.Millisecond
	}

	topic := constructTopic(client.prefix, publishBuilder.topic)

	return client.publishEvent(topic, publishBuilder.eventName, config, message, true)
}

func (client *KafkaClient) validateMessageSize(msg *kafka.Message) error {
	maxSize := 1048576 // default size from kafka in bytes
	if client.configMap != nil {
		client.configMapLock.Lock()
		// https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
		if valInterface, ok := (*client.configMap)["message.max.bytes"]; ok {
			if intValue, ok := valInterface.(int); ok {
				maxSize = intValue
			} else if intValue, ok := valInterface.(int32); ok {
				maxSize = int(intValue)
			} else if intValue, ok := valInterface.(int64); ok {
				maxSize = int(intValue)
			}
		}
		client.configMapLock.Unlock()
	}
	maxSize -= messageAdditionalSizeApprox
	if len(msg.Key)+len(msg.Value) > maxSize {
		return ErrMessageTooLarge
	}
	return nil
}

// Publish send event to a topic
func (client *KafkaClient) publishEvent(topic, eventName string, config *kafka.ConfigMap, message *kafka.Message, sync bool) (err error) {
	writer := &kafka.Producer{}

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

			client.writer.Close()
			client.writer = nil

			err = fmt.Errorf("recover: %v", r)
		}
	}()

	writer, err = client.getWriter(config)
	if err != nil {
		return err
	}

	message.TopicPartition = kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}
	if sync {
		deliveryCh := make(chan kafka.Event)
		err = writer.Produce(message, deliveryCh)
		if err != nil {
			return err
		}

		d := <-deliveryCh
		if ev, ok := d.(*kafka.Message); ok {
			// The message delivery report, indicating success or
			// permanent failure after retries/timeout have been exhausted.
			// Application level retries won't help since the client
			// is already configured to do that.
			if ev.TopicPartition.Error != nil {
				logrus.WithField("topic", *ev.TopicPartition.Topic).
					Errorf("kafka message delivery failed: %s", ev.TopicPartition.Error.Error())
				return ev.TopicPartition.Error
			}
		}

		return nil
	}

	err = writer.Produce(message, nil)

	return err
}

func (client *KafkaClient) listenProducerEvents(producer *kafka.Producer) {
	for e := range producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			// The message delivery report, indicating success or
			// permanent failure after retries/timeout have been exhausted.
			// Application level retries won't help since the client
			// is already configured to do that.
			if ev.TopicPartition.Error != nil {
				logrus.WithField("topic", *ev.TopicPartition.Topic).
					Errorf("kafka message delivery failed: %s", ev.TopicPartition.Error.Error())
			}
		case kafka.Error:
			// Generic client instance-level errors, such as
			// broker connection failures, authentication issues, etc.
			//
			// These errors should generally be considered informational
			// as the underlying client will automatically try to
			// recover from any errors encountered, the application
			// does not need to take action on them.
			logrus.Error("kafka error: ", ev)
		case *kafka.Stats:
			go client.processStatsEvent(ev.String())
		}
	}
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
func (client *KafkaClient) publishAndRetryFailure(context context.Context, topic, eventName string, message *kafka.Message, failureCallback PublishErrorCallbackFunc) error {

	config := client.configMap
	topic = constructTopic(client.prefix, topic)

	go func() {
		err := backoff.RetryNotify(func() error {
			return client.publishEvent(topic, eventName, config, message, false)
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
func ConstructEvent(publishBuilder *PublishBuilder) (*kafka.Message, *Event, error) {
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
		return &kafka.Message{}, event, err
	}

	return &kafka.Message{
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

// Register registers callback function and then subscribe topic
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

	// librdkafka requires group ID to be set
	if subscribeBuilder.groupID == "" {
		subscribeBuilder.groupID = generateID()
	}

	topic := constructTopic(client.prefix, subscribeBuilder.topic)
	groupID := constructGroupID(client.prefix, subscribeBuilder.groupID)
	groupInstanceID := constructGroupInstanceID(client.prefix, subscribeBuilder.groupInstanceID)

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

	config := client.configMap

	err = config.SetKey("group.id", groupID)
	if err != nil {
		return err
	}
	err = config.SetKey("auto.offset.reset", "earliest")
	if err != nil {
		return err
	}
	if groupInstanceID != "" {
		err = config.SetKey("group.instance.id", groupInstanceID)
		if err != nil {
			return err
		}
	}

	reader, err := kafka.NewConsumer(config)
	if err != nil {
		return err
	}

	err = reader.SubscribeTopics([]string{topic}, func(consumer *kafka.Consumer, event kafka.Event) error {
		if (strings.HasPrefix(event.String(), "AssignedPartitions") ||
			strings.HasPrefix(event.String(), "RevokedPartitions")) &&
			(strings.Contains(event.String(), topic)) {
			client.readerStatsLock.Lock()
			defer client.readerStatsLock.Unlock()

			s := client.readerStats.TopicStats[topic]
			s.RebalanceCount++
			client.readerStats.TopicStats[topic] = s
		}
		return nil
	})
	if err != nil {
		return err
	}

	go func() {

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
				consumerMessage, err := client.readMessages(subscribeBuilder.ctx, reader)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						// the subscription is shutting down. triggered by an external context cancellation
						loggerFields.Warn("external context cancellation triggered. Cancelling the subscription")
					}
					continue
				}

				if !client.autoCommitIntervalEnabled && client.commitBeforeMessage {
					if err = client.commitMessage(consumerMessage, reader, subscribeBuilder); err != nil {
						loggerFields.Warn("error committing message: ", err.Error())
						continue
					}
				}

				err = client.processMessage(subscribeBuilder, consumerMessage, topic)
				if err != nil {
					loggerFields.Error("unable to process the event: ", err)

					// shutdown current subscriber and mark it for restarting
					eventProcessingFailed = true

					return
				}

				if !client.autoCommitIntervalEnabled && !client.commitBeforeMessage {
					if err = client.commitMessage(consumerMessage, reader, subscribeBuilder); err != nil {
						loggerFields.Warn("error committing message: ", err.Error())
						continue
					}
				}
			}
		}
	}()

	return nil
}

func (client *KafkaClient) readMessages(subscribeCtx context.Context, reader *kafka.Consumer) (*kafka.Message, error) {
	for {
		if subscribeCtx.Err() != nil {
			return nil, subscribeCtx.Err()
		}

		ev := reader.Poll(int(time.Second.Milliseconds()))
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *kafka.Message:
			if e.TopicPartition.Error != nil {
				return nil, e.TopicPartition.Error
			}
			return e, nil
		case kafka.Error:
			return nil, e
		case *kafka.Stats:
			go client.processStatsEvent(e.String())
		default:
		}
	}
}

func (client *KafkaClient) processStatsEvent(statsEvent string) {
	var kafkaStats statistics.KafkaStats
	json.Unmarshal([]byte(statsEvent), &kafkaStats)

	switch kafkaStats.Type {
	case consumerStatsType:
		client.readerStatsLock.Lock()
		defer client.readerStatsLock.Unlock()
	case producerStatsType:
		client.writerStatsLock.Lock()
		defer client.writerStatsLock.Unlock()
	}

	for k, b := range kafkaStats.Brokers {
		if k == "GroupCoordinator" {
			continue
		}
		brokerStats := statistics.BrokerStats{
			Timeouts:  b.ReqTimeouts,
			Connects:  b.Connects,
			Writes:    b.Tx,
			TxErrors:  b.Txerrs,
			TxRetries: b.Txretries,
			WriteTime: statistics.DurationSummary{
				Avg:   time.Duration(b.OutbufLatency.Avg) * time.Microsecond,
				Min:   time.Duration(b.OutbufLatency.Min) * time.Microsecond,
				Max:   time.Duration(b.OutbufLatency.Max) * time.Microsecond,
				Count: b.OutbufLatency.Cnt,
				Sum:   time.Duration(b.OutbufLatency.Sum) * time.Microsecond,
			},
			RxErrors: b.Rxerrs,
		}
		switch kafkaStats.Type {
		case consumerStatsType:
			client.readerStats.BrokerStats[k] = brokerStats
		case producerStatsType:
			client.writerStats.BrokerStats[k] = brokerStats
		}
	}

	for k, t := range kafkaStats.Topics {
		topicStats := statistics.TopicStats{
			BatchSize: statistics.Summary{
				Avg:   t.Batchcnt.Avg,
				Min:   t.Batchcnt.Min,
				Max:   t.Batchcnt.Max,
				Count: t.Batchcnt.Cnt,
				Sum:   t.Batchcnt.Sum,
			},
			BatchBytes: statistics.Summary{
				Avg:   t.Batchsize.Avg,
				Min:   t.Batchsize.Min,
				Max:   t.Batchsize.Max,
				Count: t.Batchsize.Cnt,
				Sum:   t.Batchsize.Sum,
			},
		}
		switch kafkaStats.Type {
		case consumerStatsType:
			// Keep the previous rebalance count since it should not be updated by this
			// process (otherwise it will always get assigned to zero due to json unmarshal)
			topicStats.RebalanceCount = client.readerStats.TopicStats[k].RebalanceCount
			client.readerStats.TopicStats[k] = topicStats
		case producerStatsType:
			client.writerStats.TopicStats[k] = topicStats
		}

		for pKey, p := range t.Partitions {
			if pKey == "-1" {
				continue
			}
			topicPartitionStats := statistics.TopicPartitionStats{
				TxMessages:      p.Txmsgs,
				TxBytes:         p.Txbytes,
				RxMessages:      p.Rxmsgs,
				RxBytes:         p.Rxbytes,
				CommittedOffset: p.CommittedOffset,
				Lag:             p.ConsumerLag,
				QueueLength:     p.FetchqCnt,
				QueueCapacity:   p.FetchqSize,
			}
			switch kafkaStats.Type {
			case consumerStatsType:
				client.readerStats.TopicPartitionStats[topicPartitionStatsKey(k, pKey)] = topicPartitionStats
			case producerStatsType:
				client.writerStats.TopicPartitionStats[topicPartitionStatsKey(k, pKey)] = topicPartitionStats
			}
		}
	}
}

func (client *KafkaClient) commitMessage(message *kafka.Message, reader *kafka.Consumer, subscribeBuilder *SubscribeBuilder) error {
	if subscribeBuilder.asyncCommitMessage {
		// Asynchronously commit the offset
		go asyncCommitMessage(reader, message)
	} else {
		_, err := reader.CommitMessage(message)
		if err != nil {
			if subscribeBuilder.ctx.Err() == nil {
				// the subscription is shutting down. triggered by an external context cancellation
				logrus.
					WithField("Topic Name", subscribeBuilder.topic).
					WithField("Event Name", subscribeBuilder.eventName).Warn("triggered an external context cancellation. Cancelling the subscription")
				return err
			}
			logrus.
				WithField("Topic Name", subscribeBuilder.topic).
				WithField("Event Name", subscribeBuilder.eventName).Error("unable to commit the event: ", err)
			return err
		}
	}
	return nil
}

func topicPartitionStatsKey(topic string, partition string) string {
	return fmt.Sprintf("%s@%s", topic, partition)
}

func asyncCommitMessage(consumer *kafka.Consumer, message *kafka.Message) {
	if _, err := consumer.CommitMessage(message); err != nil {
		logrus.Error("unable to async commit the event: ", err)
	}
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

func (client *KafkaClient) setSubscriberReader(subscribeBuilder *SubscribeBuilder, reader *kafka.Consumer) {
	slug := subscribeBuilder.Slug()
	client.ReadersLock.Lock()
	defer client.ReadersLock.Unlock()
	client.readers[slug] = reader
}

// getWriter get a writer based on config
func (client *KafkaClient) getWriter(config *kafka.ConfigMap) (*kafka.Producer, error) {
	if client.writer != nil {
		return client.writer, nil
	}

	writer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	client.writer = writer
	go client.listenProducerEvents(client.writer)

	return writer, nil
}

// processMessage process a message from kafka
func (client *KafkaClient) processMessage(subscribeBuilder *SubscribeBuilder, message *kafka.Message, topic string) error {
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

		// send message to DLQ topic
		if subscribeBuilder.sendErrorDLQ {
			client.publishDLQ(subscribeBuilder.ctx, topic, subscribeBuilder.eventName, message)
		}

		return nil
	}

	if subscribeBuilder.eventName != "" && subscribeBuilder.eventName != event.EventName {
		// don't send events if consumer subscribed on a non-empty event name
		// return nil to ACK the event
		return nil
	}

	return client.runCallback(subscribeBuilder, event)
}

func (client *KafkaClient) publishDLQ(ctx context.Context, topic, eventName string, message *kafka.Message) {
	dlqTopic := topic + separator + dlq
	config := client.configMap

	message.TopicPartition = kafka.TopicPartition{
		Topic:     &dlqTopic,
		Partition: kafka.PartitionAny,
	}

	err := client.publishEvent(dlqTopic, eventName, config, message, false)
	if err != nil {
		logrus.Warnf("unable to publish dlq message err : %v", err.Error())
	}
}

// unmarshal unmarshal received message into event struct
func unmarshal(message *kafka.Message) (*Event, error) {
	var event *Event

	err := json.Unmarshal(message.Value, &event)
	if err != nil {
		return &Event{}, err
	}

	event.Partition = int(message.TopicPartition.Partition)
	event.Offset = int64(message.TopicPartition.Offset)
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

// GetReaderStats returns the latest internal statistics of brokers, topics, and partitions of consumers.
// The stats values are refreshed at a fixed interval which can be configured by setting the `statistics.interval.ms` config
func (client *KafkaClient) GetReaderStats() statistics.Stats {
	client.readerStatsLock.Lock()
	defer client.readerStatsLock.Unlock()
	return client.readerStats.Copy()
}

// GetWriterStats returns the latest internal statistics of brokers, topics, and partitions of producers.
// The stats values are refreshed at a fixed interval which can be configured by setting the `statistics.interval.ms` config
func (client *KafkaClient) GetWriterStats() statistics.Stats {
	client.writerStatsLock.Lock()
	defer client.writerStatsLock.Unlock()
	return client.writerStats.Copy()
}

func newPublishBackoff() *backoff.ExponentialBackOff {
	backoff := backoff.NewExponentialBackOff()
	// We increase the default multiplier, because kafka cluster operations can take quite long to complete,
	// e.g. auto-creating topics, leader re-election, or rebalancing.
	// For maxBackoffCount=4 we will get attempts: 0ms, 500ms, 2s, 8s, 16s.
	backoff.Multiplier = 4.0
	return backoff
}

func (client *KafkaClient) getAdminClient(config *kafka.ConfigMap) (*kafka.AdminClient, error) {
	if client.adminClient != nil {
		return client.adminClient, nil
	}

	adminClient, err := kafka.NewAdminClient(config)
	if err != nil {
		return nil, err
	}
	client.adminClient = adminClient

	return client.adminClient, nil
}

func (client *KafkaClient) GetMetadata(topic string, timeout time.Duration) (*Metadata, error) {
	adminClient, err := client.getAdminClient(client.configMap)
	if err != nil {
		return nil, err
	}

	mTopic := &topic
	if topic == "" {
		mTopic = nil
	}
	metadata, err := adminClient.GetMetadata(mTopic, false, int(timeout.Milliseconds()))
	if err != nil {
		return nil, err
	}

	mBrokers := make([]BrokerMetadata, 0)
	for _, m := range metadata.Brokers {
		mBrokers = append(mBrokers, BrokerMetadata{
			ID:   m.ID,
			Host: m.Host,
			Port: m.Port,
		})
	}

	return &Metadata{
		Brokers: mBrokers,
	}, nil
}

type BrokerMetadata struct {
	ID   int32
	Host string
	Port int
}

type Metadata struct {
	Brokers []BrokerMetadata
}
