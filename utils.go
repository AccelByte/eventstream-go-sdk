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
	"encoding/json"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// constructTopic construct topic name
func constructTopic(prefix, topic string) string {
	if prefix != "" {
		return prefix + separator + topic
	}

	return topic
}

// constructGroupID construct groupID or queue group name
func constructGroupID(prefix, groupID string) string {
	if groupID == "" {
		return ""
	}

	return prefix + separator + groupID
}

// generateID returns UUID without dash
func generateID() string {
	id := uuid.New()
	return strings.Replace(id.String(), "-", "", -1)
}

// marshal marshal event into json []byte
func marshal(event *Event) ([]byte, error) {
	bytes, err := json.Marshal(&event)
	if err != nil {
		logrus.Errorf("unable to marshal event : %s , error : %v", event.EventName, err)
		return nil, err
	}

	return bytes, nil
}

func loadEnv(key string) string {
	if s, exist := os.LookupEnv(key); exist && s != "" {
		return s
	}
	return ""
}
