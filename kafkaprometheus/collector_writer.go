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

// WriterCollector implements prometheus' Collector interface, for kafka writer.
type WriterCollector struct {
	Client       KafkaStatCollector
	counterStats writerCounterStats
}

type writerCounterStats struct {
	writerWrites   map[string]int64
	writerErrors   map[string]int64
	writerRetries  map[string]int64
	writerMessages map[string]int64
	writerBytes    map[string]int64
}

var (
	brokerLabels            = []string{"broker"}
	topicLabels             = []string{"topic"}
	topicPartitionLabels    = []string{"topic", "partition"}
	writerWrites            = prometheus.NewCounterVec(prometheus.CounterOpts{Name: writerPrefix + "writes", Help: "Total number of write attempts made by the Kafka writer."}, brokerLabels)
	writerMessages          = prometheus.NewCounterVec(prometheus.CounterOpts{Name: writerPrefix + "messages", Help: "Total number of messages written by the Kafka writer."}, topicPartitionLabels)
	writerBytes             = prometheus.NewCounterVec(prometheus.CounterOpts{Name: writerPrefix + "message_bytes", Help: "Total number of bytes written by the Kafka writer."}, topicPartitionLabels)
	writerErrors            = prometheus.NewCounterVec(prometheus.CounterOpts{Name: writerPrefix + "errors", Help: "Total number of errors encountered by the Kafka writer."}, brokerLabels)
	writerRetries           = prometheus.NewCounterVec(prometheus.CounterOpts{Name: writerPrefix + "retries_count_summary", Help: "Summary of retries made by the Kafka writer."}, brokerLabels)
	writerWriteTime         = prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: writerPrefix + "write_seconds", Help: "Summary of time taken for write operations by the Kafka writer."}, brokerLabels)
	writerBatchSizeSummary  = prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: writerPrefix + "batch_size_summary", Help: "Summary of batch sizes used by the Kafka writer."}, topicLabels)
	writerBatchBytesSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: writerPrefix + "batch_bytes_summary", Help: "Summary of bytes written per batch by the Kafka writer."}, topicLabels)
)

func (w *WriterCollector) Collect(metrics chan<- prometheus.Metric) {
	stats := w.Client.GetWriterStats()

	if w.counterStats.writerWrites == nil {
		w.counterStats.writerWrites = make(map[string]int64)
		w.counterStats.writerErrors = make(map[string]int64)
		w.counterStats.writerRetries = make(map[string]int64)
		w.counterStats.writerMessages = make(map[string]int64)
		w.counterStats.writerBytes = make(map[string]int64)
	}

	for broker, b := range stats.BrokerStats {
		if b.Writes > w.counterStats.writerWrites[broker] {
			metrics <- counter(writerWrites, b.Writes-w.counterStats.writerWrites[broker], broker)
			w.counterStats.writerWrites[broker] = b.Writes
		}

		if b.TxErrors > w.counterStats.writerErrors[broker] {
			metrics <- counter(writerErrors, b.TxErrors-w.counterStats.writerErrors[broker], broker)
			w.counterStats.writerErrors[broker] = b.TxErrors
		}

		if b.TxRetries > w.counterStats.writerRetries[broker] {
			metrics <- counter(writerRetries, b.TxRetries-w.counterStats.writerRetries[broker], broker)
			w.counterStats.writerRetries[broker] = b.TxRetries
		}

		metrics <- summaryDuration(writerWriteTime, DurationStats(b.WriteTime), broker)
	}

	for topic, t := range stats.TopicStats {
		metrics <- summaryCount(writerBatchSizeSummary, SummaryStats(t.BatchSize), topic)
		metrics <- summaryCount(writerBatchBytesSummary, SummaryStats(t.BatchBytes), topic)
	}

	for topicPartition, p := range stats.TopicPartitionStats {
		split := strings.Split(topicPartition, "@")
		if len(split) != 2 {
			continue
		}
		topic := split[0]
		partition := split[1]
		if p.TxMessages > w.counterStats.writerMessages[topicPartition] {
			metrics <- counter(writerMessages, p.TxMessages-w.counterStats.writerMessages[topicPartition], topic, partition)
			w.counterStats.writerMessages[topicPartition] = p.TxMessages
		}

		if p.TxBytes > w.counterStats.writerBytes[topicPartition] {
			metrics <- counter(writerBytes, p.TxBytes-w.counterStats.writerBytes[topicPartition], topic, partition)
			w.counterStats.writerBytes[topicPartition] = p.TxBytes
		}
	}
}

func (w *WriterCollector) Describe(c chan<- *prometheus.Desc) {
	writerWrites.Describe(c)
	writerMessages.Describe(c)
	writerBytes.Describe(c)
	writerErrors.Describe(c)
	writerRetries.Describe(c)
	writerWriteTime.Describe(c)
	writerBatchSizeSummary.Describe(c)
	writerBatchBytesSummary.Describe(c)
}
