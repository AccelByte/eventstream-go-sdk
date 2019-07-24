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
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

// KafkaClient satisfies the publisher to Kafka
type KafkaClient struct {
	realm         string
	asyncProducer sarama.AsyncProducer
	syncProducer  sarama.SyncProducer
}

// KafkaConfig is Kafka configuration to wait for a successful metadata response
type KafkaConfig struct {
	DialTimeout          time.Duration
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	MetadataRetryMax     int
	MetadataRetryBackoff time.Duration
}

// NewKafkaClient creates new client to publish event to kafka
// The first producerConfiguration is the asynchronous producer configuration
// meanwhile the second is the synchronous one
func NewKafkaClient(realm string, brokerList []string, producerConfiguration ...*KafkaConfig) (*KafkaClient, error) {

	var configAsync, configSync *sarama.Config
	switch {
	case len(producerConfiguration) > 1:
		configAsync = configureKafka(producerConfiguration[0])
		configSync = configureKafka(producerConfiguration[1])
	case len(producerConfiguration) == 1:
		configAsync = configureKafka(producerConfiguration[0])
		configSync = sarama.NewConfig()
	default:
		configAsync = sarama.NewConfig()
		configSync = sarama.NewConfig()
	}

	asyncProducer, err := sarama.NewAsyncProducer(brokerList, configAsync)
	if err != nil {
		return nil, err
	}

	configSync.Producer.Return.Successes = true
	syncProducer, err := sarama.NewSyncProducer(brokerList, configSync)
	if err != nil {
		return nil, err
	}

	go listenToError(asyncProducer)

	return &KafkaClient{
		realm:         realm,
		asyncProducer: asyncProducer,
		syncProducer:  syncProducer,
	}, nil
}

func listenToError(producer sarama.AsyncProducer) {
	for err := range producer.Errors() {
		logrus.Error("unable to push event to Kafka stream: ", err)
	}
}

type eventEntry struct {
	Event

	encoded []byte
	err     error
}

func (evtEntry *eventEntry) ensureEncoded() {
	if evtEntry.encoded == nil && evtEntry.err == nil {
		evtEntry.encoded, evtEntry.err = json.Marshal(evtEntry)
	}
}

// Length returns the length of the event entry
func (evtEntry *eventEntry) Length() int {
	evtEntry.ensureEncoded()
	return len(evtEntry.encoded)
}

// Encode serialize the struct
func (evtEntry *eventEntry) Encode() ([]byte, error) {
	evtEntry.ensureEncoded()
	return evtEntry.encoded, evtEntry.err
}

// PublishEvent push an event to a Kafka topic synchronously
func (client *KafkaClient) PublishEvent(event *Event) error {
	event.Realm = client.realm
	event.Time = time.Now().UTC()

	_, _, err := client.syncProducer.SendMessage(&sarama.ProducerMessage{
		Timestamp: time.Now().UTC(),
		Value: &eventEntry{
			Event: *event,
		},
		Topic: event.Topic,
	})

	return err
}

// PublishEventAsync publish to a Kafka topic asynchronously
func (client *KafkaClient) PublishEventAsync(event *Event) {
	event.Realm = client.realm
	event.Time = time.Now().UTC()

	client.asyncProducer.Input() <- &sarama.ProducerMessage{
		Timestamp: time.Now().UTC(),
		Value: &eventEntry{
			Event: *event,
		},
		Topic: event.Topic,
	}
}

// configureKafka configure kafka
func configureKafka(kafkaConfig *KafkaConfig) (config *sarama.Config) {
	config = sarama.NewConfig()
	if kafkaConfig.DialTimeout != 0 {
		config.Net.DialTimeout = kafkaConfig.DialTimeout
	}
	if kafkaConfig.ReadTimeout != 0 {
		config.Net.ReadTimeout = kafkaConfig.ReadTimeout
	}
	if kafkaConfig.WriteTimeout != 0 {
		config.Net.WriteTimeout = kafkaConfig.WriteTimeout
	}
	if kafkaConfig.MetadataRetryMax != 0 {
		config.Metadata.Retry.Max = kafkaConfig.MetadataRetryMax
	}
	if kafkaConfig.MetadataRetryBackoff != 0 {
		config.Metadata.Retry.Backoff = kafkaConfig.MetadataRetryBackoff
	}
	return config
}
