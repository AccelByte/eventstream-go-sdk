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

package main

import (
	"context"
	"fmt"

	"github.com/AccelByte/eventstream-go-sdk"
	"github.com/sirupsen/logrus"
)

func main() {
	prefix := "example"
	client, err := eventstream.NewStdoutClient(prefix)
	if err != nil {
		logrus.Error(err)
	}

	client.Register(
		eventstream.NewSubscribe().
			EventName("eventName").
			Topic("topic").
			Context(context.Background()).
			Callback(func(event *eventstream.Event, err error) {
				if err != nil {
					logrus.Error(err)
				}
				fmt.Printf("%+v", event)
			}))

	client.Publish(
		eventstream.NewPublish().
			Topic("topic").
			EventName("eventName").
			Namespace("namespace").
			ClientID("clientId").
			UserID("userId").
			TraceID("traceId").
			Version("version").
			Context(context.Background()).
			Payload(map[string]interface{}{
				"payload1": struct {
					Field1 string
					Field2 string
				}{
					Field1: "value1",
					Field2: "value2",
				},
				"payload2": struct {
					Field3 string
					Field4 string
				}{
					Field3: "value3",
					Field4: "value4",
				},
			}))

}
