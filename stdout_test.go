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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStdoutClient(t *testing.T) {
	client := NewStdoutClient("test")
	expectedClient := &StdoutClient{
		realm: "test",
	}
	assert.Equal(t, expectedClient, client, "client should be equal")
}

func TestPublishEventStdoutAsyncSuccess(t *testing.T) {
	client := StdoutClient{
		realm: "test",
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
}

func TestPublishEventStdoutSyncSuccess(t *testing.T) {
	client := StdoutClient{
		realm: "test",
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
	assert.NoError(t, err, "should be no error")
}

func TestPublishEventStdoutSyncNil(t *testing.T) {
	client := StdoutClient{
		realm: "test",
	}

	err := client.PublishEvent(nil)
	assert.Error(t, err, "should be error")
}
