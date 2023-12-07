/*
 * Copyright (c) 2023 AccelByte Inc
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

package statistics

import (
	"time"
)

type Stats struct {
	TopLevelStats       TopLevelStats
	BrokerStats         map[string]BrokerStats
	TopicStats          map[string]TopicStats
	TopicPartitionStats map[string]TopicPartitionStats
}

type TopLevelStats struct {
	RebalanceCount int64
}

type BrokerStats struct {
	Timeouts int64 // req_timeouts
	Connects int64 // connects

	// producer
	Writes    int64           // tx
	TxErrors  int64           // txerrs
	TxRetries int64           // txretries
	WriteTime DurationSummary // outbuf_latency

	// consumer
	RxErrors int64 // rxerrs
}

type TopicStats struct {
	BatchSize  Summary // batchcnt
	BatchBytes Summary // batchsize
}

type TopicPartitionStats struct {
	// producer
	TxMessages int64 // txmsgs
	TxBytes    int64 // txbytes

	// consumer
	RxMessages      int64 // rxmsgs
	RxBytes         int64 // rxbytes
	CommittedOffset int64 // committed_offset
	Lag             int64 // consumer_lag
	QueueLength     int64 // fetchq_cnt
	QueueCapacity   int64 // fetchq_size
}

func (s *Stats) Copy() Stats {
	// deep copy stats
	stats := Stats{
		TopLevelStats: TopLevelStats{
			RebalanceCount: s.TopLevelStats.RebalanceCount,
		},
		BrokerStats:         make(map[string]BrokerStats, 0),
		TopicStats:          make(map[string]TopicStats, 0),
		TopicPartitionStats: make(map[string]TopicPartitionStats, 0),
	}

	for k, b := range s.BrokerStats {
		stats.BrokerStats[k] = BrokerStats{
			Timeouts:  b.Timeouts,
			Connects:  b.Connects,
			Writes:    b.Writes,
			TxErrors:  b.TxErrors,
			TxRetries: b.TxRetries,
			WriteTime: DurationSummary{
				Avg:   b.WriteTime.Avg,
				Min:   b.WriteTime.Min,
				Max:   b.WriteTime.Max,
				Count: b.WriteTime.Count,
				Sum:   b.WriteTime.Sum,
			},
			RxErrors: b.RxErrors,
		}
	}

	for k, t := range s.TopicStats {
		stats.TopicStats[k] = TopicStats{
			BatchSize: Summary{
				Avg:   t.BatchSize.Avg,
				Min:   t.BatchSize.Min,
				Max:   t.BatchSize.Max,
				Count: t.BatchSize.Count,
				Sum:   t.BatchSize.Sum,
			},
			BatchBytes: Summary{
				Avg:   t.BatchBytes.Avg,
				Min:   t.BatchBytes.Min,
				Max:   t.BatchBytes.Max,
				Count: t.BatchBytes.Count,
				Sum:   t.BatchBytes.Sum,
			},
		}
	}

	for k, p := range s.TopicPartitionStats {
		stats.TopicPartitionStats[k] = TopicPartitionStats{
			TxMessages:      p.TxMessages,
			TxBytes:         p.TxBytes,
			RxMessages:      p.RxMessages,
			RxBytes:         p.RxBytes,
			CommittedOffset: p.CommittedOffset,
			Lag:             p.Lag,
			QueueLength:     p.QueueLength,
			QueueCapacity:   p.QueueCapacity,
		}
	}

	return stats
}

type DurationSummary struct {
	Avg   time.Duration
	Min   time.Duration
	Max   time.Duration
	Count int64
	Sum   time.Duration
}

type Summary struct {
	Avg   int64
	Min   int64
	Max   int64
	Count int64
	Sum   int64
}

type KafkaBrokerStats struct {
	Name          string `json:"name"`
	Nodeid        int64  `json:"nodeid"`
	Nodename      string `json:"nodename"`
	Source        string `json:"source"`
	State         string `json:"state"`
	Connects      int64  `json:"connects"`
	Stateage      int64  `json:"stateage"`
	OutbufCnt     int64  `json:"outbuf_cnt"`
	OutbufMsgCnt  int64  `json:"outbuf_msg_cnt"`
	OutbufLatency struct {
		Min        int64 `json:"min"`
		Max        int64 `json:"max"`
		Avg        int64 `json:"avg"`
		Sum        int64 `json:"sum"`
		Stddev     int64 `json:"stddev"`
		P50        int64 `json:"p50"`
		P75        int64 `json:"p75"`
		P90        int64 `json:"p90"`
		P95        int64 `json:"p95"`
		P99        int64 `json:"p99"`
		P9999      int64 `json:"p99_99"`
		Outofrange int64 `json:"outofrange"`
		Hdrsize    int64 `json:"hdrsize"`
		Cnt        int64 `json:"cnt"`
	} `json:"outbuf_latency"`
	WaitrespCnt    int64 `json:"waitresp_cnt"`
	WaitrespMsgCnt int64 `json:"waitresp_msg_cnt"`
	Tx             int64 `json:"tx"`
	Txbytes        int64 `json:"txbytes"`
	Txerrs         int64 `json:"txerrs"`
	Txretries      int64 `json:"txretries"`
	ReqTimeouts    int64 `json:"req_timeouts"`
	Rx             int64 `json:"rx"`
	Rxbytes        int64 `json:"rxbytes"`
	Rxerrs         int64 `json:"rxerrs"`
	Rxcorriderrs   int64 `json:"rxcorriderrs"`
	Rxpartial      int64 `json:"rxpartial"`
	ZbufGrow       int64 `json:"zbuf_grow"`
	BufGrow        int64 `json:"buf_grow"`
	Wakeups        int64 `json:"wakeups"`
	IntLatency     struct {
		Min        int64 `json:"min"`
		Max        int64 `json:"max"`
		Avg        int64 `json:"avg"`
		Sum        int64 `json:"sum"`
		Stddev     int64 `json:"stddev"`
		P50        int64 `json:"p50"`
		P75        int64 `json:"p75"`
		P90        int64 `json:"p90"`
		P95        int64 `json:"p95"`
		P99        int64 `json:"p99"`
		P9999      int64 `json:"p99_99"`
		Outofrange int64 `json:"outofrange"`
		Hdrsize    int64 `json:"hdrsize"`
		Cnt        int64 `json:"cnt"`
	} `json:"int_latency"`
	Rtt struct {
		Min        int64 `json:"min"`
		Max        int64 `json:"max"`
		Avg        int64 `json:"avg"`
		Sum        int64 `json:"sum"`
		Stddev     int64 `json:"stddev"`
		P50        int64 `json:"p50"`
		P75        int64 `json:"p75"`
		P90        int64 `json:"p90"`
		P95        int64 `json:"p95"`
		P99        int64 `json:"p99"`
		P9999      int64 `json:"p99_99"`
		Outofrange int64 `json:"outofrange"`
		Hdrsize    int64 `json:"hdrsize"`
		Cnt        int64 `json:"cnt"`
	} `json:"rtt"`
	Throttle struct {
		Min        int64 `json:"min"`
		Max        int64 `json:"max"`
		Avg        int64 `json:"avg"`
		Sum        int64 `json:"sum"`
		Stddev     int64 `json:"stddev"`
		P50        int64 `json:"p50"`
		P75        int64 `json:"p75"`
		P90        int64 `json:"p90"`
		P95        int64 `json:"p95"`
		P99        int64 `json:"p99"`
		P9999      int64 `json:"p99_99"`
		Outofrange int64 `json:"outofrange"`
		Hdrsize    int64 `json:"hdrsize"`
		Cnt        int64 `json:"cnt"`
	} `json:"throttle"`
	Toppars struct {
	} `json:"toppars"`
}

type KafkaPartitionStats struct {
	Partition       int64  `json:"partition"`
	Broker          int64  `json:"broker"`
	Leader          int64  `json:"leader"`
	Desired         bool   `json:"desired"`
	Unknown         bool   `json:"unknown"`
	MsgqCnt         int64  `json:"msgq_cnt"`
	MsgqBytes       int64  `json:"msgq_bytes"`
	XmitMsgqCnt     int64  `json:"xmit_msgq_cnt"`
	XmitMsgqBytes   int64  `json:"xmit_msgq_bytes"`
	FetchqCnt       int64  `json:"fetchq_cnt"`
	FetchqSize      int64  `json:"fetchq_size"`
	FetchState      string `json:"fetch_state"`
	QueryOffset     int64  `json:"query_offset"`
	NextOffset      int64  `json:"next_offset"`
	AppOffset       int64  `json:"app_offset"`
	StoredOffset    int64  `json:"stored_offset"`
	CommitedOffset  int64  `json:"commited_offset"`
	CommittedOffset int64  `json:"committed_offset"`
	EofOffset       int64  `json:"eof_offset"`
	LoOffset        int64  `json:"lo_offset"`
	HiOffset        int64  `json:"hi_offset"`
	ConsumerLag     int64  `json:"consumer_lag"`
	Txmsgs          int64  `json:"txmsgs"`
	Txbytes         int64  `json:"txbytes"`
	Rxmsgs          int64  `json:"rxmsgs"`
	Rxbytes         int64  `json:"rxbytes"`
	Msgs            int64  `json:"msgs"`
	RxVerDrops      int64  `json:"rx_ver_drops"`
}

type KafkaTopicStats struct {
	Topic       string `json:"topic"`
	MetadataAge int64  `json:"metadata_age"`
	Batchsize   struct {
		Min        int64 `json:"min"`
		Max        int64 `json:"max"`
		Avg        int64 `json:"avg"`
		Sum        int64 `json:"sum"`
		Stddev     int64 `json:"stddev"`
		P50        int64 `json:"p50"`
		P75        int64 `json:"p75"`
		P90        int64 `json:"p90"`
		P95        int64 `json:"p95"`
		P99        int64 `json:"p99"`
		P9999      int64 `json:"p99_99"`
		Outofrange int64 `json:"outofrange"`
		Hdrsize    int64 `json:"hdrsize"`
		Cnt        int64 `json:"cnt"`
	} `json:"batchsize"`
	Batchcnt struct {
		Min        int64 `json:"min"`
		Max        int64 `json:"max"`
		Avg        int64 `json:"avg"`
		Sum        int64 `json:"sum"`
		Stddev     int64 `json:"stddev"`
		P50        int64 `json:"p50"`
		P75        int64 `json:"p75"`
		P90        int64 `json:"p90"`
		P95        int64 `json:"p95"`
		P99        int64 `json:"p99"`
		P9999      int64 `json:"p99_99"`
		Outofrange int64 `json:"outofrange"`
		Hdrsize    int64 `json:"hdrsize"`
		Cnt        int64 `json:"cnt"`
	} `json:"batchcnt"`
	Partitions map[string]KafkaPartitionStats `json:"partitions"`
}

type KafkaStats struct {
	Name             string                      `json:"name"`
	ClientId         string                      `json:"client_id"`
	Type             string                      `json:"type"`
	Ts               int64                       `json:"ts"`
	Time             int64                       `json:"time"`
	Replyq           int64                       `json:"replyq"`
	MsgCnt           int64                       `json:"msg_cnt"`
	MsgSize          int64                       `json:"msg_size"`
	MsgMax           int64                       `json:"msg_max"`
	MsgSizeMax       int64                       `json:"msg_size_max"`
	SimpleCnt        int64                       `json:"simple_cnt"`
	MetadataCacheCnt int64                       `json:"metadata_cache_cnt"`
	Brokers          map[string]KafkaBrokerStats `json:"brokers"`
	Topics           map[string]KafkaTopicStats  `json:"topics"`
	Tx               int64                       `json:"tx"`
	TxBytes          int64                       `json:"tx_bytes"`
	Rx               int64                       `json:"rx"`
	RxBytes          int64                       `json:"rx_bytes"`
	Txmsgs           int64                       `json:"txmsgs"`
	TxmsgBytes       int64                       `json:"txmsg_bytes"`
	Rxmsgs           int64                       `json:"rxmsgs"`
	RxmsgBytes       int64                       `json:"rxmsg_bytes"`
	Cgrp             struct {
		State           string `json:"state"`
		Stateage        int64  `json:"stateage"`
		JoinState       string `json:"join_state"`
		RebalanceAge    int64  `json:"rebalance_age"`
		RebalanceCnt    int64  `json:"rebalance_cnt"`
		RebalanceReason string `json:"rebalance_reason"`
		AssignmentSize  int64  `json:"assignment_size"`
	} `json:"cgrp"`
}
