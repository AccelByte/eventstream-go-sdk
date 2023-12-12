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

// BlackholeClient satisfies the publisher for mocking
type BlackholeClient struct{}

// newBlackholeClient creates new telemetry client
func newBlackholeClient() *BlackholeClient {
	return &BlackholeClient{}
}

func (client *BlackholeClient) Publish(publishBuilder *PublishBuilder) error {
	// do nothing
	return nil
}

func (client *BlackholeClient) Register(subscribeBuilder *SubscribeBuilder) error {
	// do nothing
	return nil
}

func (client *BlackholeClient) PublishSync(publishBuidler *PublishBuilder) error {
	// do nothing
	return nil
}

func (client *BlackholeClient) PublishAuditLog(auditLogBuilder *AuditLogBuilder) error {
	// do nothing
	return nil
}
