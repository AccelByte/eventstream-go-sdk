/*
 * Copyright 2023 AccelByte Inc
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

package kafkaprometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// ReaderCollector implements prometheus' Collector interface, for kafka reader.
type ReaderCollector struct {
	Client KafkaStatCollector
}

var (
	labels              = []string{"topic", "event"}
	readerDials         = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "dials", Help: "Total number of dial attempts made by the reader."}, labels)
	readerFetches       = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "fetches", Help: "Total number of fetch attempts made by the reader."}, labels)
	readerMessages      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "messages", Help: "Total number of messages read by the reader."}, labels)
	readerBytes         = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "message_bytes", Help: "Total number of bytes read by the reader."}, labels)
	readerRebalances    = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "rebalances", Help: "Total number of times the reader has been rebalanced."}, labels)
	readerTimeouts      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "timeouts", Help: "Total number of timeouts that occurred while reading."}, labels)
	readerErrors        = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "error", Help: "Total number of errors encountered by the reader."}, labels)
	readerDialTimeAvg   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "dial_seconds_avg", Help: "Average duration of dial attempts made by the reader."}, labels)
	readerDialTimeMax   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "dial_seconds_max", Help: "Max duration of dial attempts made by the reader."}, labels)
	readerReadTimeAvg   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "read_seconds_avg", Help: "Average duration of read attempts made by the reader."}, labels)
	readerReadTimeMax   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "read_seconds_max", Help: "Max duration of read attempts made by the reader."}, labels)
	readerWaitTimeAvg   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "wait_seconds_avg", Help: "Average duration of wait time for messages by the reader."}, labels)
	readerWaitTimeMax   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "wait_seconds_max", Help: "Max duration of wait time for messages by the reader."}, labels)
	readerFetchSizeAvg  = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "fetch_size_avg", Help: "Average fetch size used by the reader."}, labels)
	readerFetchSizeMax  = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "fetch_size_max", Help: "Max fetch size used by the reader."}, labels)
	readerFetchBytesAvg = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "fetch_bytes_avg", Help: "Average number of bytes fetched per fetch attempt by the reader."}, labels)
	readerFetchBytesMax = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "fetch_bytes_max", Help: "Max number of bytes fetched per fetch attempt by the reader."}, labels)
	readerOffset        = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "offset", Help: "Current offset of the reader."}, labels)
	readerLag           = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "lag", Help: "Current lag of the reader (difference between latest message offset and current offset)."}, labels)
	readerQueueLength   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "queue_length", Help: "Current length of the reader queue."}, labels)
	readerQueueCapacity = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "queue_capacity", Help: "Capacity of the reader queue."}, labels)
)

func (r *ReaderCollector) Collect(metrics chan<- prometheus.Metric) {
	// First collect the stats for all topic writers
	stats, slugs := r.Client.GetReaderStats()

	uniqueReaders := map[string]struct{}{}

	// Then send those stats to Prometheus
	for i, slug := range slugs {
		topic, eventName, _ := SplitSlug(slug)

		if topic == "" || eventName == "" {
			logrus.Warnf("ignoring empty stat %s", slug)
			continue
		}

		if _, ok := uniqueReaders[topic+eventName]; ok {
			// For metric labels we only use one per topic + eventName.
			// We ignore groupID to avoid high cardinality prometheus labels.
			// Typically, one pod wouldn't have more than one subscriber for a given topic+eventName.
			logrus.Infof("ignoring stat %s", slug)
			continue
		} else {
			uniqueReaders[topic+eventName] = struct{}{}
		}

		stat := stats[i]
		if topic == "" {
			logrus.Errorf("metric topic empty, stat.Topic: %v, topic: %v", stat.Topic, topic)
			continue
		}

		// (note, `stat.Topic` includes the prefix, `topic` doesn't)

		labels := []string{topic, eventName}

		metrics <- counter(readerDials, stat.Dials, labels...)
		metrics <- counter(readerFetches, stat.Fetches, labels...)
		metrics <- counter(readerMessages, stat.Messages, labels...)
		metrics <- counter(readerBytes, stat.Bytes, labels...)
		metrics <- counter(readerRebalances, stat.Rebalances, labels...)
		metrics <- counter(readerTimeouts, stat.Timeouts, labels...)
		metrics <- counter(readerErrors, stat.Errors, labels...)

		metrics <- gauge(readerDialTimeAvg, stat.DialTime.Avg.Seconds(), labels...)
		metrics <- gauge(readerDialTimeMax, stat.DialTime.Max.Seconds(), labels...)
		metrics <- gauge(readerReadTimeAvg, stat.ReadTime.Avg.Seconds(), labels...)
		metrics <- gauge(readerReadTimeMax, stat.ReadTime.Max.Seconds(), labels...)
		metrics <- gauge(readerWaitTimeAvg, stat.WaitTime.Avg.Seconds(), labels...)
		metrics <- gauge(readerWaitTimeMax, stat.WaitTime.Max.Seconds(), labels...)
		metrics <- gauge(readerFetchSizeAvg, float64(stat.FetchSize.Avg), labels...)
		metrics <- gauge(readerFetchSizeMax, float64(stat.FetchSize.Max), labels...)
		metrics <- gauge(readerFetchBytesAvg, float64(stat.FetchBytes.Avg), labels...)
		metrics <- gauge(readerFetchBytesMax, float64(stat.FetchBytes.Max), labels...)

		metrics <- gauge(readerOffset, float64(stat.Offset), labels...)
		metrics <- gauge(readerLag, float64(stat.Lag), labels...)
		metrics <- gauge(readerQueueLength, float64(stat.QueueLength), labels...)
		metrics <- gauge(readerQueueCapacity, float64(stat.QueueCapacity), labels...)
	}
}

func (r *ReaderCollector) Describe(c chan<- *prometheus.Desc) {
	readerDials.Describe(c)
	readerFetches.Describe(c)
	readerMessages.Describe(c)
	readerBytes.Describe(c)
	readerRebalances.Describe(c)
	readerTimeouts.Describe(c)
	readerErrors.Describe(c)
	readerDialTimeAvg.Describe(c)
	readerDialTimeMax.Describe(c)
	readerReadTimeAvg.Describe(c)
	readerReadTimeMax.Describe(c)
	readerWaitTimeAvg.Describe(c)
	readerWaitTimeMax.Describe(c)
	readerFetchSizeAvg.Describe(c)
	readerFetchSizeMax.Describe(c)
	readerFetchBytesAvg.Describe(c)
	readerFetchBytesMax.Describe(c)
	readerOffset.Describe(c)
	readerLag.Describe(c)
	readerQueueLength.Describe(c)
	readerQueueCapacity.Describe(c)
}
