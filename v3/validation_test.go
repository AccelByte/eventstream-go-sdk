/*
 * Copyright (c) 2021 AccelByte Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package eventstream

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// nolint dupl
func TestValidatePublishEvent(t *testing.T) {
	testCases := []struct {
		input    *PublishBuilder
		expected bool
	}{
		{
			input: &PublishBuilder{
				topic:     []string{"topic1"},
				eventName: "event1",
			},
			expected: true,
		},
		{
			input: &PublishBuilder{
				topic:     []string{"Topic-1"},
				eventName: "Event-1",
			},
			expected: true,
		},
		{
			input: &PublishBuilder{
				topic:     []string{"accelbyte.dev.topic-123"},
				eventName: "event-123",
			},
			expected: true,
		},
		{
			input: &PublishBuilder{
				topic:     []string{"accelbyte.dev.topic-123", "accelbyte.dev.topic-456"},
				eventName: "event.456",
			},
			expected: true,
		},
		{
			input: &PublishBuilder{
				topic:     []string{"accelbyte-dev-topic-123", "accelbyte-dev-topic-456"},
				eventName: "event-456",
			},
			expected: true,
		},
		{
			input: &PublishBuilder{
				topic:     []string{"accelbyte_dev_topic_123", "accelbyte_dev_topic_456"},
				eventName: "event_456",
			},
			expected: true,
		},
		{
			input: &PublishBuilder{
				topic:     []string{"accelbyte.dev.topic-123", "accelbyte.dev.topic-"},
				eventName: "event.456",
			},
			expected: false,
		},
		{
			input: &PublishBuilder{
				topic:     []string{"accelbyte.dev.topic-123", "accelbyte.dev.topic-!@#"},
				eventName: "event.456",
			},
			expected: false,
		},
		{
			input: &PublishBuilder{
				topic:     []string{"accelbyte.dev.topic-123", "accelbyte.dev.topic-456"},
				eventName: "event.!@#",
			},
			expected: false,
		},
		{
			input: &PublishBuilder{
				topic:     []string{"accelbyte.dev.topic-123", "accelbyte.dev.topic-456"},
				eventName: "event-",
			},
			expected: false,
		},
		{
			input: &PublishBuilder{
				topic:     []string{"accelbyte.dev.topic-!@#", "accelbyte.dev.topic-456"},
				eventName: "event%",
			},
			expected: false,
		},
	}

	for _, testCase := range testCases {
		if testCase.expected {
			assert.Nil(t, validatePublishEvent(testCase.input, false),
				fmt.Sprintf("publisher event isn't correct. topic: %s, event: %s", testCase.input.topic,
					testCase.input.eventName))
		} else {
			assert.Error(t, validatePublishEvent(testCase.input, false),
				fmt.Sprintf("publisher event isn't correct. topic: %s, event: %s", testCase.input.topic,
					testCase.input.eventName))
		}
	}
}

// nolint dupl
func TestValidateSubscriberEvent(t *testing.T) {

	callbackFunc := func(ctx context.Context, event *Event, err error) error {
		return nil
	}

	testCases := []struct {
		input    *SubscribeBuilder
		expected bool
	}{
		{
			input: &SubscribeBuilder{
				topic:     "topic1",
				eventName: "event1",
				callback:  callbackFunc,
			},
			expected: true,
		},
		{
			input: &SubscribeBuilder{
				topic:     "Topic-1",
				eventName: "Event-1",
				callback:  callbackFunc,
			},
			expected: true,
		},
		{
			input: &SubscribeBuilder{
				topic:     "Topic_1",
				eventName: "Event_1",
				callback:  callbackFunc,
			},
			expected: true,
		},
		{
			input: &SubscribeBuilder{
				topic:     "accelbyte.dev.topic-123",
				eventName: "event-123",
				callback:  callbackFunc,
			},
			expected: true,
		},
		{
			input: &SubscribeBuilder{
				topic:     "accelbyte.dev.topic-123",
				eventName: "event.456",
				callback:  callbackFunc,
			},
			expected: true,
		},
		{
			input: &SubscribeBuilder{
				topic:     "accelbyte.dev.topic-",
				eventName: "event.456",
				callback:  callbackFunc,
			},
			expected: false,
		},
		{
			input: &SubscribeBuilder{
				topic:     "accelbyte.dev.topic-!@#",
				eventName: "event.456",
				callback:  callbackFunc,
			},
			expected: false,
		},
		{
			input: &SubscribeBuilder{
				topic:     "accelbyte.dev.topic-123",
				eventName: "event.!@#",
				callback:  callbackFunc,
			},
			expected: false,
		},
		{
			input: &SubscribeBuilder{
				topic:     "accelbyte.dev.topic-123",
				eventName: "event-",
				callback:  callbackFunc,
			},
			expected: false,
		},
		{
			input: &SubscribeBuilder{
				topic:     "accelbyte.dev.topic-!@#",
				eventName: "event%",
				callback:  callbackFunc,
			},
			expected: false,
		},
		{
			input: &SubscribeBuilder{
				topic:     "accelbyte.dev.topic-123",
				eventName: "event-123",
				callback:  nil,
			},
			expected: false,
		},
		{
			input: &SubscribeBuilder{
				topic:     "accelbyte.dev.topic-123",
				eventName: "",
				callback:  callbackFunc,
			},
			expected: true,
		},
	}

	for _, testCase := range testCases {
		if testCase.expected {
			assert.Nil(t, validateSubscribeEvent(testCase.input),
				fmt.Sprintf("publisher event isn't correct. topic: %s, event: %s", testCase.input.topic,
					testCase.input.eventName))
		} else {
			assert.Error(t, validateSubscribeEvent(testCase.input),
				fmt.Sprintf("publisher event isn't correct. topic: %s, event: %s", testCase.input.topic,
					testCase.input.eventName))
		}
	}
}

func TestValidateTopicEvent(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{input: "topic", expected: true},
		{input: "topic123", expected: true},
		{input: "topic_123", expected: true},
		{input: "accelbyte.dev.topic", expected: true},
		{input: "3671efef-876b-4133-897d-79134e936cd9", expected: true},
		{input: "3671efef876b4133897d79134e936cd9", expected: true},
		{input: "accelbyte.dev.topic-123", expected: true},
		{input: "accelbyte.dev.topic-abc", expected: true},
		{input: "accelbyte.dev.topic_123", expected: true},
		{input: "accelbyte.dev.topic_abx", expected: true},
		{input: "accelbyte.dev.topic.123", expected: true},
		{input: "accelbyte.dev.topic.abc", expected: true},
		{input: "topic-", expected: false},
		{input: "topic!@#", expected: false},
		{input: "topic!@#", expected: false},
		{input: "accelbyte.dev.topic.", expected: false},
		{input: "accelbyte.dev.topic-", expected: false},
		{input: "accelbyte.dev.topic_", expected: false},
		{input: "accelbyte.dev.topic-!", expected: false},
		{input: "accelbyte.dev.topic-#", expected: false},
		{input: "accelbyte.dev.topic-1,", expected: false},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expected, validateTopicEvent(testCase.input),
			"topic or event name validation isn't correct")
	}
}
