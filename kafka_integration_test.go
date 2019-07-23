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
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

func TestIntegrationPublishEventKafkaSynchronousSuccess(t *testing.T) {
	producerConf := &KafkaConfig{
		ReadTimeout:          250 * time.Millisecond,
		DialTimeout:          250 * time.Millisecond,
		WriteTimeout:         250 * time.Millisecond,
		MetadataRetryBackoff: 250 * time.Millisecond,
		MetadataRetryMax:     3,
	}
	client, _ := NewKafkaClient("test", []string{"localhost:9092"}, producerConf, producerConf)

	event := NewEvent(123, 99, 4, "iam", []string{"8dbf8e7f673242b3ad02e7cf1be90792"},
		"09cb90e74270445d9f85309b23d612a7", []string{"8dbf8e7f673242b3ad02e7cf1be90792"}, "accelbyte",
		"accelbyte", "4e4e17820f4a4b2aa19a843369033fe4", "cf1884b311e345e0b4a96988ed6b887b",
		true, "topic_name").
		WithFields(map[string]interface{}{
			"age":           12,
			"email_address": "test@example.com",
		})

	config := sarama.NewConfig()
	config.Net.DialTimeout = 250 * time.Millisecond
	config.Net.ReadTimeout = 250 * time.Millisecond
	config.Net.WriteTimeout = 250 * time.Millisecond
	config.Metadata.Retry.Backoff = 250 * time.Millisecond
	config.Metadata.Retry.Max = 3
	config.Version = sarama.V2_1_0_0

	defer func() {
		broker := sarama.NewBroker("localhost:9092")
		_ = broker.Open(config)
		_, _ = broker.DeleteTopics(&sarama.DeleteTopicsRequest{
			Topics:  []string{"topic_name"},
			Version: 3,
		})
		_ = broker.Close()
	}()

	doneChan := make(chan struct{})
	var actualEvent string

	go func(string, chan struct{}) {
		consumer, _ := sarama.NewConsumer([]string{"localhost:9092"}, config)
		defer consumer.Close()

		res, _ := consumer.ConsumePartition("topic_name", 0, 0)
		defer res.Close()

		msg := <-res.Messages()
		actualEvent = string(msg.Value)
		doneChan <- struct{}{}
	}(actualEvent, doneChan)

	err := client.PublishEvent(event)

	marshalledEvent, _ := json.Marshal(event)
	expectedEvent := string(marshalledEvent)

	<-doneChan

	assert.NoError(t, err, "error should be nil")
	assert.Equal(t, expectedEvent, actualEvent, "event should be equal")
}

func TestIntegrationPublishEventKafkaAsynchronousSuccess(t *testing.T) {
	producerConf := &KafkaConfig{
		ReadTimeout:          250 * time.Millisecond,
		DialTimeout:          250 * time.Millisecond,
		WriteTimeout:         250 * time.Millisecond,
		MetadataRetryBackoff: 250 * time.Millisecond,
		MetadataRetryMax:     3,
	}
	client, _ := NewKafkaClient("test", []string{"localhost:9092"}, producerConf, producerConf)

	event := NewEvent(123, 91, 4, "iam", []string{"8dbf8e7f673242b3ad02e7cf1be90792"},
		"09cb90e74270445d9f85309b23d612a7", []string{"8dbf8e7f673242b3ad02e7cf1be90792"}, "accelbyte",
		"accelbyte", "4e4e17820f4a4b2aa19a843369033fe4", "cf1884b311e345e0b4a96988ed6b887b",
		true, "topic_name2").
		WithFields(map[string]interface{}{
			"age":           12,
			"email_address": "test@example.com",
		})

	config := sarama.NewConfig()
	config.Net.DialTimeout = 250 * time.Millisecond
	config.Net.ReadTimeout = 250 * time.Millisecond
	config.Net.WriteTimeout = 250 * time.Millisecond
	config.Metadata.Retry.Backoff = 250 * time.Millisecond
	config.Metadata.Retry.Max = 3
	config.Version = sarama.V2_1_0_0

	defer func() {
		broker := sarama.NewBroker("localhost:9092")
		_ = broker.Open(config)
		_, _ = broker.DeleteTopics(&sarama.DeleteTopicsRequest{
			Topics:  []string{"topic_name2"},
			Version: 3,
		})
		_ = broker.Close()
	}()

	doneChan := make(chan struct{})
	var actualEvent string

	go func(string, chan struct{}) {
		consumer, _ := sarama.NewConsumer([]string{"localhost:9092"}, config)
		defer consumer.Close()

		res, _ := consumer.ConsumePartition("topic_name2", 0, 0)
		defer res.Close()

		msg := <-res.Messages()
		actualEvent = string(msg.Value)
		doneChan <- struct{}{}
	}(actualEvent, doneChan)

	client.PublishEventAsync(event)

	marshalledEvent, _ := json.Marshal(event)
	expectedEvent := string(marshalledEvent)

	<-doneChan

	assert.Equal(t, expectedEvent, actualEvent, "event should be equal")
}
