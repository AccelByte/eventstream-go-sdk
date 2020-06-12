/*
 * Copyright (c) 2020 AccelByte Inc
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
	"errors"

	validator "github.com/AccelByte/justice-input-validation-go"
	"github.com/sirupsen/logrus"
)

var (
	errInvalidPubStruct = errors.New("publish struct isn't valid")
	errInvalidSubStruct = errors.New("subscribe struct isn't valid")
	errInvalidUserID    = errors.New("userID isn't valid")
	errInvalidClientID  = errors.New("clientID isn't valid")
	errInvalidSessionID = errors.New("sessionID isn't valid")
	errInvalidTraceID   = errors.New("traceID isn't valid")
	errInvalidCallback  = errors.New("callback should not be nil")
)

// validatePublishEvent validate published event
func validatePublishEvent(publishBuilder *PublishBuilder, strictValidation bool) error {
	publishEvent := struct {
		Topic     []string `valid:"required"`
		EventName string   `valid:"alphanum,stringlength(1|256),required"`
		Namespace string   `valid:"alphanum,stringlength(1|256),required"`
		ClientID  string
		UserID    string
		SessionID string
		TraceID   string
	}{
		Topic:     publishBuilder.topic,
		EventName: publishBuilder.eventName,
		Namespace: publishBuilder.namespace,
		ClientID:  publishBuilder.clientID,
		TraceID:   publishBuilder.traceID,
		UserID:    publishBuilder.userID,
		SessionID: publishBuilder.sessionID,
	}

	valid, err := validator.ValidateStruct(publishEvent)
	if err != nil {
		logrus.Errorf("unable to validate publish event. error : %v", err)
		return errInvalidPubStruct
	}

	if !valid {
		return errInvalidPubStruct
	}

	// only additional validation that included to strictValidation
	if strictValidation {
		if publishEvent.UserID != "" && !validator.IsUUID4WithoutHyphens(publishEvent.UserID) {
			return errInvalidUserID
		}

		if publishEvent.ClientID != "" && !validator.IsUUID4WithoutHyphens(publishEvent.ClientID) {
			return errInvalidClientID
		}

		if publishEvent.SessionID != "" && !validator.IsUUID4WithoutHyphens(publishEvent.SessionID) {
			return errInvalidSessionID
		}

		if publishEvent.TraceID != "" && !validator.IsUUID4WithoutHyphens(publishEvent.TraceID) {
			return errInvalidTraceID
		}
	}

	return nil
}

// validateSubscribeEvent validate subscribe event
func validateSubscribeEvent(subscribeBuilder *SubscribeBuilder) error {
	subscribeEvent := struct {
		Topic     string `valid:"required"`
		EventName string `valid:"alphanum,stringlength(1|256),required"`
		GroupID   string `valid:"alphanum,stringlength(1|256),required"`
		Callback  func(ctx context.Context, event *Event, err error) error
	}{
		Topic:     subscribeBuilder.topic,
		EventName: subscribeBuilder.eventName,
		GroupID:   subscribeBuilder.groupID,
		Callback:  subscribeBuilder.callback,
	}

	_, err := validator.ValidateStruct(subscribeEvent)
	if err != nil {
		logrus.Errorf("unable to validate subscribe event. error : %v", err)
		return errInvalidSubStruct
	}

	if subscribeEvent.Callback == nil {
		return errInvalidCallback
	}

	return nil
}
