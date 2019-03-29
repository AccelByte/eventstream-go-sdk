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
	"time"

	"github.com/AccelByte/eventstream-go-sdk"
)

func main() {
	client := eventpublisher.NewStdoutClient("alpha")

	event := ConstructUserRegistrationEvent(13, 12, 1, "IAM", []string{"70daa46c789640cf8ae0a7a4027ee006"},
		"a5584584badf42bba5a9954434b27ef4", []string{"a5584584badf42bba5a9954434b27ef4"}, "accelbyte",
		"accelbyte", "a349286b153f469ca311763ada8d1635", "4b9cbf8f61fb4b7eb60d1591ca7e90a5",
		true, "test display", "email@example.com", 12)

	client.PublishEventAsync(event)

	time.Sleep(time.Second)
}

func ConstructUserRegistrationEvent(eventID int,
	eventType int,
	eventLevel int,
	service string,
	clientIDs []string,
	userID string,
	targetUserIDs []string,
	namespace string,
	targetNamespace string,
	traceID string,
	sessionID string,
	privacy bool,
	displayName string,
	emailAddress string,
	age int) *eventpublisher.Event {

	return eventpublisher.NewEvent(eventID, eventType, eventLevel, service, clientIDs, userID, targetUserIDs,
		namespace, targetNamespace, traceID, sessionID, privacy).
		WithFields(map[string]interface{}{
			"display_name":  displayName,
			"email_address": emailAddress,
			"age":           age})
}
