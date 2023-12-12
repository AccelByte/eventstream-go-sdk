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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nolint:funlen
func TestMultipleSubscriptionsEventuallyProcessAllEvents(t *testing.T) {
	testTimeoutDuration := time.Duration(timeoutTest) * time.Second * 4 //nolint:gomnd

	ctx, done := context.WithTimeout(context.Background(), testTimeoutDuration)
	defer done()

	doneChan := make(chan bool, 100) // nolint:gomnd

	client := createKafkaClient(t)

	topicName := constructTopicTest()

	var mockPayload = make(map[string]interface{})
	mockPayload[testPayload] = "testPayload" // nolint:goconst

	mockAdditionalFields := map[string]interface{}{
		"summary": "user:_failed",
	}

	eventName := "TestMultipleSubscriptionsEventuallyProcessAllEvents"

	mockEvent := Event{
		EventName:        eventName,
		Namespace:        "event",
		ClientID:         "7d480ce0e8624b02901bd80d9ba9817c",
		TraceID:          "01c34ec3b07f4bfaa59ba0184a3de14d",
		SpanContext:      "test-span-context",
		UserID:           "e95b150043ff4a2c88427a6eb25e5bc8",
		EventID:          0,   // nolint:gomnd
		EventType:        301, // nolint:gomnd
		EventLevel:       3,   //nolint:gomnd
		ServiceName:      "test",
		ClientIDs:        []string{"7d480ce0e8624b02901bd80d9ba9817c"},
		TargetUserIDs:    []string{"1fe7f425a0e049d29d87ca3d32e45b5a"},
		TargetNamespace:  "publisher",
		Privacy:          true,
		AdditionalFields: mockAdditionalFields,
		Version:          defaultVersion,
		Payload:          mockPayload,
	}

	// init event counters
	processedEvents := make(map[int]int64) // map[event-id]count. used to count all processed events

	var processedEventsMutex sync.Mutex

	groupID := generateID()

	// create few workers
	for i := 0; i < 4; i++ {
		func(i int) {
			workerID := i
			workerEventCounter := 0

			err := client.Register(
				NewSubscribe().
					Topic(topicName).
					EventName(mockEvent.EventName).
					GroupID(groupID).
					Context(ctx).
					Offset(kafka.FirstOffset).
					Callback(func(ctx context.Context, event *Event, err error) error {
						if ctx.Err() != nil {
							return ctx.Err()
						}

						if err != nil {
							return err
						}

						// nothing to process
						if event == nil {
							return nil
						}

						workerEventCounter++

						processedEventsMutex.Lock()
						defer processedEventsMutex.Unlock()

						if workerEventCounter%25 == 0 { // fail processing each 25-th event
							logrus.Infof("worker: %v, EventID %v failed (%d)", workerID, event.EventID, workerEventCounter)

							return fmt.Errorf("worker: %v, Event %v failed", workerID, event.EventID)
						}

						processedEvents[event.EventID]++

						go func() {
							doneChan <- true
						}()

						return nil
					}))
			require.NoError(t, err)
		}(i)
	}

	numberOfProducer := 2
	numberOfEventsPerProducer := 100
	numberOfEvents := numberOfProducer * numberOfEventsPerProducer

	// publish few thousand of events
	for i := 0; i < numberOfProducer; i++ {
		go func(producer int) {
			for j := 0; j < numberOfEventsPerProducer; j++ {
				eventToSend := mockEvent
				eventToSend.EventID = producer*numberOfEventsPerProducer + j

				err := client.Publish(
					NewPublish().
						Topic(topicName).
						EventName(eventToSend.EventName).
						Namespace(eventToSend.Namespace).
						ClientID(eventToSend.ClientID).
						UserID(eventToSend.UserID).
						SessionID(eventToSend.SessionID).
						TraceID(eventToSend.TraceID).
						SpanContext(eventToSend.SpanContext).
						EventID(eventToSend.EventID).
						EventType(eventToSend.EventType).
						EventLevel(eventToSend.EventLevel).
						ServiceName(eventToSend.ServiceName).
						ClientIDs(eventToSend.ClientIDs).
						TargetUserIDs(eventToSend.TargetUserIDs).
						TargetNamespace(eventToSend.TargetNamespace).
						Privacy(eventToSend.Privacy).
						AdditionalFields(eventToSend.AdditionalFields).
						Context(context.Background()).
						Payload(mockPayload))
				require.NoError(t, err)
			}
		}(i)
	}

	for {
		select {
		case <-doneChan:
			processedEventsMutex.Lock()

			// check that all events processed
			if len(processedEvents) == numberOfEvents {
				return
			}

			logrus.Printf("Total processed %v", len(processedEvents))

			processedEventsMutex.Unlock()
		case <-ctx.Done():
			assert.FailNow(t, errorTimeout)
		}
	}
}
