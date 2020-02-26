// Copyright (c) 2019 AccelByte Inc. All Rights Reserved.
// This is licensed software from AccelByte Inc, for limitations
// and restrictions contact your company contract manager.

package eventstream

import (
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
		GroupID   string
		Callback  func(event *Event, err error)
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
