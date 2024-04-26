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
	"strings"
)

// ReaderCollector implements prometheus' Collector interface, for kafka reader.
type ReaderCollector struct {
	Client       KafkaStatCollector
	counterStats readerCounterStats
}

type readerCounterStats struct {
	readerDials      map[string]int64
	readerTimeouts   map[string]int64
	readerErrors     map[string]int64
	readerRebalances map[string]int64
	readerMessages   map[string]int64
	readerBytes      map[string]int64
}

type ReaderStat struct {
}

var (
	readerDials         = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "dials", Help: "Total number of dial attempts made by the reader."}, brokerLabels)
	readerMessages      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "messages", Help: "Total number of messages read by the reader."}, topicPartitionLabels)
	readerBytes         = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "message_bytes", Help: "Total number of bytes read by the reader."}, topicPartitionLabels)
	readerRebalances    = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "rebalances", Help: "Total number of times the reader has been rebalanced."}, topicLabels)
	readerTimeouts      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "timeouts", Help: "Total number of timeouts that occurred while reading."}, brokerLabels)
	readerErrors        = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "error", Help: "Total number of errors encountered by the reader."}, brokerLabels)
	readerOffset        = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "offset", Help: "Current offset of the reader."}, topicPartitionLabels)
	readerLag           = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "lag", Help: "Current lag of the reader (difference between latest message offset and current offset)."}, topicPartitionLabels)
	readerQueueLength   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "queue_length", Help: "Current length of the reader queue."}, topicPartitionLabels)
	readerQueueCapacity = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "queue_capacity", Help: "Capacity of the reader queue."}, topicPartitionLabels)
)

func (r *ReaderCollector) Collect(metrics chan<- prometheus.Metric) {
	stats := r.Client.GetReaderStats()

	if r.counterStats.readerDials == nil {
		r.counterStats.readerDials = make(map[string]int64)
		r.counterStats.readerTimeouts = make(map[string]int64)
		r.counterStats.readerErrors = make(map[string]int64)
		r.counterStats.readerRebalances = make(map[string]int64)
		r.counterStats.readerMessages = make(map[string]int64)
		r.counterStats.readerBytes = make(map[string]int64)
	}

	for broker, b := range stats.BrokerStats {
		if b.Connects > r.counterStats.readerDials[broker] {
			metrics <- counter(readerDials, b.Connects-r.counterStats.readerDials[broker], broker)
			r.counterStats.readerDials[broker] = b.Connects
		}

		if b.Timeouts > r.counterStats.readerTimeouts[broker] {
			metrics <- counter(readerTimeouts, b.Timeouts-r.counterStats.readerTimeouts[broker], broker)
			r.counterStats.readerTimeouts[broker] = b.Timeouts
		}

		if b.RxErrors > r.counterStats.readerErrors[broker] {
			metrics <- counter(readerErrors, b.RxErrors-r.counterStats.readerErrors[broker], broker)
			r.counterStats.readerErrors[broker] = b.RxErrors
		}
	}

	for topic, t := range stats.TopicStats {
		if t.RebalanceCount > r.counterStats.readerRebalances[topic] {
			metrics <- counter(readerRebalances, t.RebalanceCount-r.counterStats.readerRebalances[topic], topic)
			r.counterStats.readerRebalances[topic] = t.RebalanceCount
		}
	}

	for topicPartition, p := range stats.TopicPartitionStats {
		split := strings.Split(topicPartition, "@")
		if len(split) != 2 {
			continue
		}
		topic := split[0]
		partition := split[1]

		if p.RxMessages > r.counterStats.readerMessages[topicPartition] {
			metrics <- counter(readerMessages, p.RxMessages-r.counterStats.readerMessages[topicPartition], topic, partition)
			r.counterStats.readerMessages[topicPartition] = p.RxMessages
		}

		if p.RxBytes > r.counterStats.readerBytes[topicPartition] {
			metrics <- counter(readerBytes, p.RxBytes-r.counterStats.readerBytes[topicPartition], topic, partition)
			r.counterStats.readerBytes[topicPartition] = p.RxBytes
		}
		metrics <- gauge(readerOffset, float64(p.CommittedOffset), topic, partition)
		metrics <- gauge(readerLag, float64(p.Lag), topic, partition)
		metrics <- gauge(readerQueueLength, float64(p.QueueLength), topic, partition)
		metrics <- gauge(readerQueueCapacity, float64(p.QueueCapacity), topic, partition)
	}
}

func (r *ReaderCollector) Describe(c chan<- *prometheus.Desc) {
	readerDials.Describe(c)
	readerMessages.Describe(c)
	readerBytes.Describe(c)
	readerRebalances.Describe(c)
	readerTimeouts.Describe(c)
	readerErrors.Describe(c)
	readerOffset.Describe(c)
	readerLag.Describe(c)
	readerQueueLength.Describe(c)
	readerQueueCapacity.Describe(c)
}
