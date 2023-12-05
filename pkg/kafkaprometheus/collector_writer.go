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
)

// WriterCollector implements prometheus' Collector interface, for kafka writer.
type WriterCollector struct {
	Client KafkaStatCollector
}

var (
	writerLabels            = []string{"topic"}
	writerWrites            = prometheus.NewCounterVec(prometheus.CounterOpts{Name: writerPrefix + "writes", Help: "Total number of write attempts made by the Kafka writer."}, writerLabels)
	writerMessages          = prometheus.NewCounterVec(prometheus.CounterOpts{Name: writerPrefix + "messages", Help: "Total number of messages written by the Kafka writer."}, writerLabels)
	writerBytes             = prometheus.NewCounterVec(prometheus.CounterOpts{Name: writerPrefix + "message_bytes", Help: "Total number of bytes written by the Kafka writer."}, writerLabels)
	writerErrors            = prometheus.NewCounterVec(prometheus.CounterOpts{Name: writerPrefix + "errors", Help: "Total number of errors encountered by the Kafka writer."}, writerLabels)
	writerRetries           = prometheus.NewCounterVec(prometheus.CounterOpts{Name: writerPrefix + "retries_count_summary", Help: "Summary of retries made by the Kafka writer."}, writerLabels)
	writerBatchTime         = prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: writerPrefix + "batch_seconds", Help: "Summary of batch time taken by the Kafka writer."}, writerLabels)
	writerBatchQueueTime    = prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: writerPrefix + "batch_queue_seconds", Help: "Summary of time taken in the batch queue by the Kafka writer."}, writerLabels)
	writerWriteTime         = prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: writerPrefix + "write_seconds", Help: "Summary of time taken for write operations by the Kafka writer."}, writerLabels)
	writerWaitTime          = prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: writerPrefix + "wait_seconds", Help: "Summary of wait time before write operations by the Kafka writer."}, writerLabels)
	writerBatchSizeSummary  = prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: writerPrefix + "batch_size_summary", Help: "Summary of batch sizes used by the Kafka writer."}, writerLabels)
	writerBatchBytesSummary = prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: writerPrefix + "batch_bytes_summary", Help: "Summary of bytes written per batch by the Kafka writer."}, writerLabels)
)

func (w *WriterCollector) Collect(metrics chan<- prometheus.Metric) {
	// First collect the stats for all topic writers
	stats, topics := w.Client.GetWriterStats()

	// Then send those stats to Prometheus
	for i, topic := range topics {
		s := stats[i]
		/*if s.Writes == 0 && s.Messages == 0 && s.Bytes == 0 && s.Errors == 0 && s.Retries == 0 {
			continue
		}*/

		// librdkafka statistics fields reference: https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md
		metrics <- counter(writerWrites, s["Writes"].(int64), topic)     // tx?
		metrics <- counter(writerMessages, s["Messages"].(int64), topic) // txmsgs
		metrics <- counter(writerBytes, s["Bytes"].(int64), topic)       // txmsg_bytes
		metrics <- counter(writerErrors, s["Errors"].(int64), topic)     // txerrs
		metrics <- counter(writerRetries, s["Retries"].(int64), topic)   // txretries

		metrics <- summaryDuration(writerBatchTime, s["BatchTime"].(DurationStats), topic)           // rtt
		metrics <- summaryDuration(writerBatchQueueTime, s["BatchQueueTime"].(DurationStats), topic) // throttle
		metrics <- summaryDuration(writerWriteTime, s["WriteTime"].(DurationStats), topic)           // outbuf_latency
		metrics <- summaryDuration(writerWaitTime, s["WaitTime"].(DurationStats), topic)             // int_latency
		metrics <- summaryCount(writerBatchSizeSummary, s["BatchSize"].(SummaryStats), topic)        // topics.batchcnt
		metrics <- summaryCount(writerBatchBytesSummary, s["BatchBytes"].(SummaryStats), topic)      // topics.batchsize
	}
}

func (w *WriterCollector) Describe(c chan<- *prometheus.Desc) {
	writerWrites.Describe(c)
	writerMessages.Describe(c)
	writerBytes.Describe(c)
	writerErrors.Describe(c)
	writerRetries.Describe(c)
	writerBatchTime.Describe(c)
	writerBatchQueueTime.Describe(c)
	writerWriteTime.Describe(c)
	writerWaitTime.Describe(c)
	writerBatchSizeSummary.Describe(c)
	writerBatchBytesSummary.Describe(c)
}
