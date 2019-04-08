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

	"github.com/sirupsen/logrus"
)

// StdoutClient satisfies the publisher for mocking
type StdoutClient struct {
	realm string
}

// NewStdoutClient creates new telemetry client
func NewStdoutClient(realm string) *StdoutClient {
	return &StdoutClient{
		realm: realm,
	}
}

func (client *StdoutClient) PublishEvent(event *Event) error {
	if event == nil {
		return fmt.Errorf("event can't be nil")
	}
	event.Realm = client.realm
	event.Time = time.Now().UTC()

	eventByte, err := json.Marshal(event)
	if err != nil {
		return err
	}
	fmt.Println(string(eventByte))
	return nil
}

func (client *StdoutClient) PublishEventAsync(event *Event) {
	go func() {
		err := client.PublishEvent(event)
		if err != nil {
			logrus.Error(err)
		}
	}()
}
