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

func TestNewBlackholeClient(t *testing.T) {
	client := NewBlackholeClient()
	expectedClient := &BlackholeClient{}
	assert.Equal(t, expectedClient, client, "client should be equal")
}

func TestPublishEventBlackholeAsync(t *testing.T) {
	client := &BlackholeClient{}
	client.PublishEventAsync(nil)
}

func TestPublishEventBlackholeSync(t *testing.T) {
	client := &BlackholeClient{}

	err := client.PublishEvent(nil)
	assert.NoError(t, err, "should be error")
}
