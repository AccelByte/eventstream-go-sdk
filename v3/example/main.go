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
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"strings"
)

// nolint: funlen
func main() {
	config := &eventstream.BrokerConfig{
		StrictValidation: true,
		DialTimeout:      0,
		ReadTimeout:      0,
		WriteTimeout:     0,
	}

	prefix := "example"

	client, err := eventstream.NewClient(prefix, "stdout", nil, config)
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

	err = client.Publish(
		eventstream.NewPublish().
			Topic("topic").
			EventName("eventName").
			Namespace("namespace").
			ClientID("682af5a46e934a42b798bb4afb9a973e").
			UserID("e635e94c2afb408c9427f143b293a3c7").
			SessionID("9428c3dd028849cf84c1a763e1b7ea71").
			TraceID("f75368ef5603402ca98af501304949c0").
			Version(1). // nolint: gomnd
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

	if err != nil {
		logrus.Error(err)
	}

	err = client.Register(
		eventstream.NewSubscribe().
			EventName("auditLog").
			Topic("auditLog").
			Context(context.Background()).
			CallbackRaw(func(ctx context.Context, msgValue []byte, err error) error {
				if err != nil {
					logrus.Error(err)
				}
				fmt.Println("-----------------------audit log received-----------------------")
				fmt.Printf("%+v", string(msgValue))

				return nil
			}))

	if err != nil {
		logrus.Error(err)
	}

	auditLogContent := make(map[string]interface{})
	auditLogContent["platformId"] = "steam"
	auditLogContent["secret"] = "steam_secret"
	auditLogDiff := eventstream.AuditLogDiff{}
	auditLogDiff.After = make(map[string]interface{})
	auditLogDiff.Before = make(map[string]interface{})
	auditLogDiff.Before["before"] = "before"
	auditLogDiff.After["after"] = "after"
	err = client.PublishAuditLog(
		eventstream.NewAuditLogBuilder().
			Category("test_category").
			ActionName("test_action").
			IP("127.0.0.1").
			Actor(strings.Replace(uuid.New().String(), "-", "", -1)).
			IsActorTypeUser(true).
			ClientID(strings.Replace(uuid.New().String(), "-", "", -1)).
			ActorNamespace("test_namespace").
			ObjectID(strings.Replace(uuid.New().String(), "-", "", -1)).
			ObjectType("User").
			ObjectNamespace("test_object_namespace").
			TargetUserID(strings.Replace(uuid.New().String(), "-", "", -1)).
			DeviceID("test_device").
			Content(auditLogContent).
			Diff(&auditLogDiff).
			ErrorCallback(func(message []byte, err error) {
				fmt.Printf("message: %s", string(message))
			}),
	)
	if err != nil {
		logrus.Error(err)
	}

}
