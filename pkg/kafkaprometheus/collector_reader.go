package kafkaprometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
)

// ReaderCollector implements prometheus' Collector interface, for kafka reader.
type ReaderCollector struct {
	Reader *kafka.Reader
}

var (
	labels              = []string{"client_id", "topic", "partition"}
	readerDials         = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "dials", Help: "Total number of dial attempts made by the reader."}, labels)
	readerFetches       = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "fetches", Help: "Total number of fetch attempts made by the reader."}, labels)
	readerMessages      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "messages", Help: "Total number of messages read by the reader."}, labels)
	readerBytes         = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "message_bytes", Help: "Total number of bytes read by the reader."}, labels)
	readerRebalances    = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "rebalances", Help: "Total number of times the reader has been rebalanced."}, labels)
	readerTimeouts      = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "timeouts", Help: "Total number of timeouts that occurred while reading."}, labels)
	readerErrors        = prometheus.NewCounterVec(prometheus.CounterOpts{Name: readerPrefix + "error", Help: "Total number of errors encountered by the reader."}, labels)
	readerDialTimeAvg   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "dial_seconds_avg", Help: "Average duration of dial attempts made by the reader."}, labels)
	readerReadTimeAvg   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "read_seconds_avg", Help: "Average duration of read attempts made by the reader."}, labels)
	readerWaitTimeAvg   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "wait_seconds_avg", Help: "Average duration of wait time for messages by the reader."}, labels)
	readerFetchSizeAvg  = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "fetch_size_avg", Help: "Average fetch size used by the reader."}, labels)
	readerFetchBytesAvg = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "fetch_bytes_avg", Help: "Average number of bytes fetched per fetch attempt by the reader."}, labels)
	readerOffset        = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "offset", Help: "Current offset of the reader."}, labels)
	readerLag           = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "lag", Help: "Current lag of the reader (difference between latest message offset and current offset)."}, labels)
	readerMinBytes      = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "config_fetch_bytes_min", Help: "Minimum byte configuration value for fetch requests."}, labels)
	readerMaxBytes      = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "config_fetch_bytes_max", Help: "Maximum byte configuration value for fetch requests."}, labels)
	readerMaxWait       = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "fetch_wait_max", Help: "Maximum wait time for a fetch request."}, labels)
	readerQueueLength   = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "queue_length", Help: "Current length of the reader queue."}, labels)
	readerQueueCapacity = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: readerPrefix + "queue_capacity", Help: "Capacity of the reader queue."}, labels)
)

func (r *ReaderCollector) Collect(metrics chan<- prometheus.Metric) {
	stats := r.Reader.Stats()

	// We need a partition label to prevent collisions in the metrics,
	// because each partition gets its own reader, meaning we have multiple readers per topic per service.
	topic, partition := stats.Topic, stats.Partition
	metrics <- counter(readerDials, float64(stats.Dials), topic, partition)
	metrics <- counter(readerFetches, float64(stats.Fetches), topic, partition)
	metrics <- counter(readerMessages, float64(stats.Messages), topic, partition)
	metrics <- counter(readerBytes, float64(stats.Bytes), topic, partition)
	metrics <- counter(readerRebalances, float64(stats.Rebalances), topic, partition)
	metrics <- counter(readerTimeouts, float64(stats.Timeouts), topic, partition)
	metrics <- counter(readerErrors, float64(stats.Errors), topic, partition)

	metrics <- gauge(readerDialTimeAvg, stats.DialTime.Avg.Seconds(), topic, partition)
	metrics <- gauge(readerReadTimeAvg, stats.ReadTime.Avg.Seconds(), topic, partition)
	metrics <- gauge(readerWaitTimeAvg, stats.WaitTime.Avg.Seconds(), topic, partition)
	metrics <- gauge(readerFetchSizeAvg, float64(stats.FetchSize.Avg), topic, partition)
	metrics <- gauge(readerFetchBytesAvg, float64(stats.FetchBytes.Avg), topic, partition)

	metrics <- gauge(readerOffset, float64(stats.Offset), topic, partition)
	metrics <- gauge(readerLag, float64(stats.Lag), topic, partition)
	metrics <- gauge(readerMinBytes, float64(stats.MinBytes), topic, partition)
	metrics <- gauge(readerMaxBytes, float64(stats.MaxBytes), topic, partition)
	metrics <- gauge(readerMaxWait, float64(stats.MaxWait), topic, partition)
	metrics <- gauge(readerQueueLength, float64(stats.QueueLength), topic, partition)
	metrics <- gauge(readerQueueCapacity, float64(stats.QueueCapacity), topic, partition)
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
	readerReadTimeAvg.Describe(c)
	readerWaitTimeAvg.Describe(c)
	readerFetchSizeAvg.Describe(c)
	readerFetchBytesAvg.Describe(c)
	readerOffset.Describe(c)
	readerLag.Describe(c)
	readerMinBytes.Describe(c)
	readerMaxBytes.Describe(c)
	readerMaxWait.Describe(c)
	readerQueueLength.Describe(c)
	readerQueueCapacity.Describe(c)
}
