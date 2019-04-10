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
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/assert"
)

func TestNewKafkaClientError(t *testing.T) {
	client, err := NewKafkaClient("test", []string{"localhost"})
	assert.Error(t, err)
	assert.Nil(t, client)
}

func TestPublishEventKafkaAsyncSuccess(t *testing.T) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	mockProducer := mocks.NewAsyncProducer(t, config)
	mockProducer.ExpectInputAndSucceed()

	defer func() {
		if err := mockProducer.Close(); err != nil {
			t.Error(err)
		}
	}()

	client := KafkaClient{
		realm:         "test",
		asyncProducer: mockProducer,
	}

	event := NewEvent(123, 99, 4, "iam", []string{"8dbf8e7f673242b3ad02e7cf1be90792"},
		"09cb90e74270445d9f85309b23d612a7", []string{"8dbf8e7f673242b3ad02e7cf1be90792"}, "accelbyte",
		"accelbyte", "4e4e17820f4a4b2aa19a843369033fe4", "cf1884b311e345e0b4a96988ed6b887b",
		true, "topic_name").
		WithFields(map[string]interface{}{
			"age":           12,
			"email_address": "test@example.com",
		})

	client.PublishEventAsync(event)
	actualEvent := <-mockProducer.Successes()

	actual, _ := actualEvent.Value.Encode()
	expected, _ := json.Marshal(event)

	assert.Equal(t, string(expected), string(actual), "event in the producer is not equal")
	assert.Equal(t, actualEvent.Topic, fmt.Sprintf("topic_name"), "topic is not equal")
}

func TestPublishEventKafkaSynchronousSuccess(t *testing.T) {
	config := sarama.NewConfig()
	mockProducer := mocks.NewSyncProducer(t, config)
	mockProducer.ExpectSendMessageAndSucceed()

	defer func() {
		if err := mockProducer.Close(); err != nil {
			t.Error(err)
		}
	}()

	client := KafkaClient{
		realm:        "test",
		syncProducer: mockProducer,
	}

	event := NewEvent(123, 99, 4, "iam", []string{"8dbf8e7f673242b3ad02e7cf1be90792"},
		"09cb90e74270445d9f85309b23d612a7", []string{"8dbf8e7f673242b3ad02e7cf1be90792"}, "accelbyte",
		"accelbyte", "4e4e17820f4a4b2aa19a843369033fe4", "cf1884b311e345e0b4a96988ed6b887b",
		true, "topic_name").
		WithFields(map[string]interface{}{
			"age":           12,
			"email_address": "test@example.com",
		})

	err := client.PublishEvent(event)

	assert.NoError(t, err, "error should be nil")
}
