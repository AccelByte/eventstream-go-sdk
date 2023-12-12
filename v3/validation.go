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
	"regexp"

	validator "github.com/AccelByte/justice-input-validation-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const TopicEventPattern = "^[a-zA-Z0-9]+((['_.-][a-zA-Z0-9])?[a-zA-Z0-9]*)*$"

var (
	errInvalidPubStruct       = errors.New("publish struct isn't valid")
	errInvalidTopicFormat     = errors.New("topic format isn't valid")
	errInvalidEventNameFormat = errors.New("eventname format isn't valid")
	errInvalidUserID          = errors.New("userID isn't valid")
	errInvalidClientID        = errors.New("clientID isn't valid")
	errInvalidSessionID       = errors.New("sessionID isn't valid")
	errInvalidTraceID         = errors.New("traceID isn't valid")
	errInvalidCallback        = errors.New("callback should not be nil")
)

var topicRegex *regexp.Regexp = nil

func init() {
	validRegex, err := regexp.Compile(TopicEventPattern)
	if err != nil {
		panic(err)
	}
	topicRegex = validRegex
}

// validatePublishEvent validate published event
func validatePublishEvent(publishBuilder *PublishBuilder, strictValidation bool) error {

	for _, topic := range publishBuilder.topic {
		if isTopicValid := validateTopicEvent(topic); !isTopicValid {
			logrus.
				WithField("Topic Name", topic).
				WithField("Event Name", publishBuilder.eventName).
				Errorf("unable to validate publisher event. error: invalid topic format")
			return errInvalidTopicFormat
		}
	}

	if isEventNameValid := validateTopicEvent(publishBuilder.eventName); !isEventNameValid {
		logrus.
			WithField("Topic Name", publishBuilder.topic).
			WithField("Event Name", publishBuilder.eventName).
			Errorf("unable to validate publisher event. error: invalid event name format")
		return errInvalidEventNameFormat
	}

	publishEvent := struct {
		Topic     []string `valid:"required"`
		EventName string   `valid:"required"`
		Namespace string
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
		logrus.
			WithField("Topic Name", publishEvent.Topic).
			WithField("Event Name", publishEvent.EventName).
			Errorf("unable to validate publisher event. error : %v", err)
		return err
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

	if isTopicValid := validateTopicEvent(subscribeBuilder.topic); !isTopicValid {
		logrus.
			WithField("Topic Name", subscribeBuilder.topic).
			WithField("Event Name", subscribeBuilder.eventName).
			Errorf("unable to validate subscribe event. error: invalid topic format")
		return errInvalidTopicFormat
	}

	if subscribeBuilder.eventName != "" {
		if isEventNameValid := validateTopicEvent(subscribeBuilder.eventName); !isEventNameValid {
			logrus.
				WithField("Topic Name", subscribeBuilder.topic).
				WithField("Event Name", subscribeBuilder.eventName).
				Errorf("unable to validate subscribe event. error: invalid event name format")
			return errInvalidEventNameFormat
		}
	}

	subscribeEvent := struct {
		Topic       string `valid:"required"`
		EventName   string
		GroupID     string
		Callback    func(ctx context.Context, event *Event, err error) error
		CallbackRaw func(ctx context.Context, msg []byte, err error) error
	}{
		Topic:       subscribeBuilder.topic,
		EventName:   subscribeBuilder.eventName,
		GroupID:     subscribeBuilder.groupID,
		Callback:    subscribeBuilder.callback,
		CallbackRaw: subscribeBuilder.callbackRaw,
	}

	_, err := validator.ValidateStruct(subscribeEvent)
	if err != nil {
		logrus.
			WithField("Topic Name", subscribeBuilder.topic).
			WithField("Event Name", subscribeBuilder.eventName).
			Errorf("unable to validate subscribe event. error : %v", err)
		return err
	}

	if subscribeEvent.Callback == nil && subscribeEvent.CallbackRaw == nil {
		return errInvalidCallback
	}

	return nil
}

func validateTopicEvent(value string) bool {
	return topicRegex.MatchString(value)
}
