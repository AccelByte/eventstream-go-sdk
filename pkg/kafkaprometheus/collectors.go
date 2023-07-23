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

/*
	Use this package to report kafka stats to prometheus.
	To enable, set the MetricsRegistry field on BrokerConfig when creating a kafka client.
*/
import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"strings"
)

const (
	writerPrefix = "ab_eventstream_kafka_writer_"
	readerPrefix = "ab_eventstream_kafka_reader_"
)

const SlugSeparator = "$" // SlugSeparator is excluded by topicRegex.

type KafkaStatCollector interface {
	GetWriterStats() ([]kafka.WriterStats, []string)
	GetReaderStats() ([]kafka.ReaderStats, []string)
}

func summaryCount(s *prometheus.SummaryVec, ss kafka.SummaryStats, labels ...string) prometheus.Metric {
	return summary(s, ss.Count, float64(ss.Sum), float64(ss.Avg), float64(ss.Min), float64(ss.Max), labels...)
}

func summaryDuration(s *prometheus.SummaryVec, ds kafka.DurationStats, labels ...string) prometheus.Metric {
	return summary(s, ds.Count, ds.Sum.Seconds(), ds.Avg.Seconds(), ds.Min.Seconds(), ds.Max.Seconds(), labels...)
}

func summary(s prometheus.Collector, count int64, sum, avg, min, max float64, labels ...string) prometheus.Metric {
	descChan := make(chan *prometheus.Desc, 1)
	s.Describe(descChan)
	desc := <-descChan

	summary, err := prometheus.NewConstSummary(
		desc,
		uint64(count),
		sum,
		map[float64]float64{
			// Note: 0.1, 0.5, 0.9 percentiles shouldn't be the min, avg and max,
			// but I use these fields because a prometheus summary doesn't track min, avg and max.
			0.1: min,
			0.5: avg,
			0.9: max,
		},
		labels...,
	)
	if err != nil {
		logrus.Warnf("failed to create summary: %v", err)
	}
	return summary
}

func counter(counter *prometheus.CounterVec, value int64, labels ...string) prometheus.Counter {
	m := counter.WithLabelValues(labels...)
	m.Add(float64(value))
	return m
}

func gauge(gauge *prometheus.GaugeVec, value float64, labels ...string) prometheus.Metric {
	m := gauge.WithLabelValues(labels...)
	if value != 0 {
		m.Set(value)
	}
	return m
}

// ParseSlug returns topic, eventName, and groupID
func SplitSlug(s string) (topic, eventName, groupID string) {
	split := strings.SplitN(s, SlugSeparator, 3)
	if len(split) == 3 {
		return split[0], split[1], split[2]
	}
	if len(split) == 2 {
		return split[0], split[1], ""
	}
	if len(split) == 1 {
		return split[0], "", ""
	}
	return "", "", ""
}
