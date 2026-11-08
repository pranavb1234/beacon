// Package metrics declares all Prometheus instruments for a beacon broker node.
//
// Centralising every metric registration here achieves two things:
//  1. The full observability surface is visible in one file — easy to audit.
//  2. Other packages receive a *Metrics value; they never import prometheus
//     directly, which prevents duplicate-registration panics in tests.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds every Prometheus instrument used by the broker.
// Pass a *Metrics around by pointer; never copy.
type Metrics struct {
	// MessagesPublished counts Publish RPCs accepted by this node.
	// Labelled by topic so we can spot hot topics.
	MessagesPublished *prometheus.CounterVec

	// MessagesDelivered counts messages pushed to ≥1 subscriber.
	// Delivery < Published implies a subscriber-side gap worth investigating.
	MessagesDelivered *prometheus.CounterVec

	// MessagesDropped counts messages evicted due to backpressure.
	// Labelled by reason: "buffer_full" or "subscriber_slow".
	MessagesDropped *prometheus.CounterVec

	// ActiveSubscribers is a gauge that goes up on Subscribe and down on disconnect.
	ActiveSubscribers prometheus.Gauge

	// ClusterSize tracks the number of live broker nodes in the hash ring.
	ClusterSize prometheus.Gauge

	// PublishLatency observes the wall-clock duration of each Publish RPC.
	PublishLatency prometheus.Histogram

	// DeliveryLatency observes the time from Publish acceptance to subscriber delivery.
	// High values indicate a slow fan-out or backpressure buildup.
	DeliveryLatency prometheus.Histogram
}

// New registers all metrics with reg and returns the populated struct.
// Pass prometheus.NewRegistry() in tests so each test gets isolated counters
// and avoid duplicate-registration panics from the global default registry.
func New(reg prometheus.Registerer) *Metrics {
	f := promauto.With(reg)

	return &Metrics{
		MessagesPublished: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "beacon",
			Name:      "messages_published_total",
			Help:      "Total messages accepted by this node's Publish RPC.",
		}, []string{"topic"}),

		MessagesDelivered: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "beacon",
			Name:      "messages_delivered_total",
			Help:      "Total messages pushed to at least one subscriber.",
		}, []string{"topic"}),

		MessagesDropped: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: "beacon",
			Name:      "messages_dropped_total",
			Help:      "Total messages discarded due to backpressure.",
		}, []string{"topic", "reason"}),

		ActiveSubscribers: f.NewGauge(prometheus.GaugeOpts{
			Namespace: "beacon",
			Name:      "active_subscribers",
			Help:      "Number of currently connected subscribers.",
		}),

		ClusterSize: f.NewGauge(prometheus.GaugeOpts{
			Namespace: "beacon",
			Name:      "cluster_size",
			Help:      "Number of broker nodes currently in the consistent hash ring.",
		}),

		PublishLatency: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: "beacon",
			Name:      "publish_latency_seconds",
			Help:      "Wall-clock duration of each Publish RPC.",
			// 14 buckets from 100µs to ~800ms covers typical in-memory latencies.
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
		}),

		DeliveryLatency: f.NewHistogram(prometheus.HistogramOpts{
			Namespace: "beacon",
			Name:      "delivery_latency_seconds",
			Help:      "Time from Publish acceptance to first subscriber delivery.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 14),
		}),
	}
}
