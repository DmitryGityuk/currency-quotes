// Package metrics exposes the service's Prometheus metrics. Metrics implements
// the observer interfaces of the other packages, so they report what happens
// without depending on Prometheus themselves.
//
// Label values come from small closed sets (routes, six pairs, a handful of
// statuses), never from ids or raw paths, to keep the number of series small.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
)

const namespace = "quotes"

// Metrics holds the collectors of one process on its own registry.
type Metrics struct {
	registry *prometheus.Registry

	httpRequests *prometheus.CounterVec
	httpDuration *prometheus.HistogramVec

	updateRequests  *prometheus.CounterVec
	updatesFinished *prometheus.CounterVec
	updateDuration  *prometheus.HistogramVec
	updateAttempts  prometheus.Histogram

	providerCalls    *prometheus.CounterVec
	providerDuration prometheus.Histogram

	outboxPublished prometheus.Counter
	outboxFailures  prometheus.Counter
	outboxBacklog   prometheus.Gauge
	outboxOldest    prometheus.Gauge

	kafkaMessages *prometheus.CounterVec
}

// New returns Metrics with every collector registered, plus the Go runtime
// and process collectors.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),

		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "http_requests_total",
			Help: "HTTP requests by method, route pattern and status code.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "http_request_duration_seconds",
			Help:    "HTTP request latency by method and route pattern.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"method", "route"}),

		updateRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "update_requests_total",
			Help: "Accepted update requests by pair and outcome: created, joined or replayed.",
		}, []string{"pair", "outcome"}),
		updatesFinished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "updates_finished_total",
			Help: "Updates that reached a final status, by pair and status.",
		}, []string{"pair", "status"}),
		updateDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "update_duration_seconds",
			Help:    "Time from the POST to the final status: what the client waits for.",
			Buckets: []float64{.1, .25, .5, 1, 2, 5, 10, 30, 60},
		}, []string{"status"}),
		updateAttempts: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "update_attempts",
			Help:    "Provider calls an update needed before its final status.",
			Buckets: []float64{1, 2, 3, 4, 5},
		}),

		providerCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "provider_requests_total",
			Help: "Calls to the rates provider by outcome.",
		}, []string{"outcome"}),
		providerDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace, Name: "provider_request_duration_seconds",
			Help:    "Latency of calls to the rates provider.",
			Buckets: []float64{.05, .1, .25, .5, 1, 2, 5},
		}),

		outboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "outbox_published_messages_total",
			Help: "Outbox messages acknowledged by Kafka.",
		}),
		outboxFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "outbox_publish_failures_total",
			Help: "Relay batches that stayed in the outbox because publishing failed.",
		}),
		outboxBacklog: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "outbox_backlog_messages",
			Help: "Messages waiting in the outbox. Growing means Kafka or the relay is down.",
		}),
		outboxOldest: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "outbox_oldest_message_age_seconds",
			Help: "Age of the oldest message in the outbox; 0 when it is empty.",
		}),

		kafkaMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "kafka_messages_total",
			Help: "Consumed messages by outcome: processed, dlq or retried.",
		}, []string{"outcome"}),
	}

	m.registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.httpRequests, m.httpDuration,
		m.updateRequests, m.updatesFinished, m.updateDuration, m.updateAttempts,
		m.providerCalls, m.providerDuration,
		m.outboxPublished, m.outboxFailures, m.outboxBacklog, m.outboxOldest,
		m.kafkaMessages,
	)
	return m
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// ObserveHTTP implements httpapi.HTTPObserver.
func (m *Metrics) ObserveHTTP(method, route string, status int, took time.Duration) {
	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(took.Seconds())
}

// UpdateRequested implements httpapi.UpdateObserver.
func (m *Metrics) UpdateRequested(pair quotes.Pair, outcome string) {
	m.updateRequests.WithLabelValues(pair.String(), outcome).Inc()
}

// ProviderCall implements worker.Observer.
func (m *Metrics) ProviderCall(outcome string, took time.Duration) {
	m.providerCalls.WithLabelValues(outcome).Inc()
	m.providerDuration.Observe(took.Seconds())
}

// UpdateFinished implements worker.Observer.
func (m *Metrics) UpdateFinished(pair quotes.Pair, status quotes.Status, attempts int, sinceRequested time.Duration) {
	m.updatesFinished.WithLabelValues(pair.String(), string(status)).Inc()
	m.updateDuration.WithLabelValues(string(status)).Observe(sinceRequested.Seconds())
	m.updateAttempts.Observe(float64(attempts))
}

// Published implements outbox.Observer.
func (m *Metrics) Published(n int) { m.outboxPublished.Add(float64(n)) }

// PublishFailed implements outbox.Observer.
func (m *Metrics) PublishFailed() { m.outboxFailures.Inc() }

// MessageHandled implements kafka.Observer.
func (m *Metrics) MessageHandled(outcome string) { m.kafkaMessages.WithLabelValues(outcome).Inc() }

// SetOutbox records the current state of the outbox.
func (m *Metrics) SetOutbox(messages int, oldest time.Duration) {
	m.outboxBacklog.Set(float64(messages))
	m.outboxOldest.Set(oldest.Seconds())
}
