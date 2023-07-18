package kafkaprometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
)

// WriterCollector implements prometheus' Collector interface, for kafka writer.
type WriterCollector struct {
	Writer *kafka.Writer
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
	writerMaxAttempts       = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: writerPrefix + "attempts_max", Help: "Maximum number of attempts allowed by the Kafka writer."}, writerLabels)
	writerMaxBatchSize      = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: writerPrefix + "batch_max", Help: "Maximum batch size allowed by the Kafka writer."}, writerLabels)
	writerBatchTimeout      = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: writerPrefix + "batch_timeout", Help: "Timeout for batch operations by the Kafka writer."}, writerLabels)
	writerReadTimeout       = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: writerPrefix + "read_timeout", Help: "Timeout for read operations by the Kafka writer."}, writerLabels)
	writerWriteTimeout      = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: writerPrefix + "write_timeout", Help: "Timeout for write operations by the Kafka writer."}, writerLabels)
)

func (w *WriterCollector) Collect(metrics chan<- prometheus.Metric) {
	stats := w.Writer.Stats()
	topic := stats.Topic
	metrics <- counter(writerWrites, float64(stats.Writes), topic)
	metrics <- counter(writerMessages, float64(stats.Messages), topic)
	metrics <- counter(writerBytes, float64(stats.Bytes), topic)
	metrics <- counter(writerErrors, float64(stats.Errors), topic)
	metrics <- counter(writerRetries, float64(stats.Retries), topic)

	// Collect DurationStats as prometheus.NewConstSummary summaries
	metrics <- summaryDuration(writerBatchTime, stats.BatchTime, topic)
	metrics <- summaryDuration(writerBatchQueueTime, stats.BatchQueueTime, topic)
	metrics <- summaryDuration(writerWriteTime, stats.WriteTime, topic)
	metrics <- summaryDuration(writerWaitTime, stats.WaitTime, topic)

	// Collect SummaryStats as prometheus.NewConstSummary summaries
	metrics <- summaryCount(writerBatchSizeSummary, stats.BatchSize, topic)
	metrics <- summaryCount(writerBatchBytesSummary, stats.BatchBytes, topic)

	metrics <- gauge(writerMaxAttempts, float64(stats.MaxAttempts), topic)
	metrics <- gauge(writerMaxBatchSize, float64(stats.MaxBatchSize), topic)
	metrics <- gauge(writerBatchTimeout, stats.BatchTimeout.Seconds(), topic)
	metrics <- gauge(writerReadTimeout, stats.ReadTimeout.Seconds(), topic)
	metrics <- gauge(writerWriteTimeout, stats.WriteTimeout.Seconds(), topic)
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
	writerMaxAttempts.Describe(c)
	writerMaxBatchSize.Describe(c)
	writerBatchTimeout.Describe(c)
	writerReadTimeout.Describe(c)
	writerWriteTimeout.Describe(c)
}
