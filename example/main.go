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
	"github.com/AccelByte/eventstream-go-sdk/v3"
	"github.com/sirupsen/logrus"
	"time"
)

// nolint: funlen
func main() {

	logrus.SetLevel(logrus.DebugLevel)

	config := &eventstream.BrokerConfig{
		StrictValidation: true,
		DialTimeout:      0,
		ReadTimeout:      0,
		WriteTimeout:     0,
	}

	prefix := "example"

	client, err := eventstream.NewClient(prefix, "kafka", []string{"localhost:9092"}, config)
	if err != nil {
		logrus.Error(err)
	}

	err = client.Register(
		eventstream.NewSubscribe().
			EventName("eventName").
			Topic("topic").
			Context(context.Background()).
			Callback(func(ctx context.Context, event *eventstream.Event, err error) error {
				if err != nil {
					logrus.Error(err)
				}
				fmt.Printf("%+v", event)

				return nil
			}))

	if err != nil {
		logrus.Error(err)
	}

	time.Sleep(time.Hour * 4)

}
