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
	"fmt"
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

// NewKafkaClient creates new client to publish event to kafka
func NewKafkaClient(realm string, brokerList []string) (*KafkaClient, error) {
	config := sarama.NewConfig()
	asyncProducer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		return nil, err
	}
	syncProducer, err := sarama.NewSyncProducer(brokerList, config)
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
		Topic: fmt.Sprintf("topic_%d", event.EventType),
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
		Topic: fmt.Sprintf("topic_%d", event.EventType),
	}
}
