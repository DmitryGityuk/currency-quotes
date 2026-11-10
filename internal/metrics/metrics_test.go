package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/DmitryGityuk/currency-quotes/internal/httpapi"
	"github.com/DmitryGityuk/currency-quotes/internal/kafka"
	"github.com/DmitryGityuk/currency-quotes/internal/outbox"
	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
	"github.com/DmitryGityuk/currency-quotes/internal/worker"
)

// Metrics must satisfy every observer interface it is wired into.
var (
	_ httpapi.HTTPObserver   = (*Metrics)(nil)
	_ httpapi.UpdateObserver = (*Metrics)(nil)
	_ worker.Observer        = (*Metrics)(nil)
	_ outbox.Observer        = (*Metrics)(nil)
	_ kafka.Observer         = (*Metrics)(nil)
)

var eurMXN = quotes.Pair{Base: "EUR", Quote: "MXN"}

func TestObservations(t *testing.T) {
	m := New()
	m.ObserveHTTP("POST", "/api/v1/quotes/updates", 202, 3*time.Millisecond)
	m.UpdateRequested(eurMXN, "created")
	m.UpdateRequested(eurMXN, "joined")
	m.ProviderCall("ok", 120*time.Millisecond)
	m.ProviderCall("temporary_error", time.Second)
	m.UpdateFinished(eurMXN, quotes.StatusDone, 2, 1500*time.Millisecond)
	m.Published(3)
	m.PublishFailed()
	m.MessageHandled("processed")
	m.MessageHandled("dlq")
	m.SetOutbox(4, 90*time.Second)
	m.UpdatesRequeued(2)

	checks := []struct {
		name string
		got  float64
		want float64
	}{
		{"http requests", testutil.ToFloat64(m.httpRequests.WithLabelValues("POST", "/api/v1/quotes/updates", "202")), 1},
		{"created", testutil.ToFloat64(m.updateRequests.WithLabelValues("EUR/MXN", "created")), 1},
		{"joined", testutil.ToFloat64(m.updateRequests.WithLabelValues("EUR/MXN", "joined")), 1},
		{"provider ok", testutil.ToFloat64(m.providerCalls.WithLabelValues("ok")), 1},
		{"provider temporary", testutil.ToFloat64(m.providerCalls.WithLabelValues("temporary_error")), 1},
		{"finished done", testutil.ToFloat64(m.updatesFinished.WithLabelValues("EUR/MXN", "done")), 1},
		{"outbox published", testutil.ToFloat64(m.outboxPublished), 3},
		{"outbox failures", testutil.ToFloat64(m.outboxFailures), 1},
		{"outbox backlog", testutil.ToFloat64(m.outboxBacklog), 4},
		{"outbox oldest", testutil.ToFloat64(m.outboxOldest), 90},
		{"kafka dlq", testutil.ToFloat64(m.kafkaMessages.WithLabelValues("dlq")), 1},
		{"requeued", testutil.ToFloat64(m.updatesRequeued), 2},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if n := testutil.CollectAndCount(m.updateDuration); n != 1 {
		t.Errorf("update duration series = %d, want 1", n)
	}
}

func TestHandlerServesTextFormat(t *testing.T) {
	m := New()
	m.UpdateRequested(eurMXN, "created")

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", http.NoBody))
	body, _ := io.ReadAll(rec.Body)

	for _, want := range []string{
		`quotes_update_requests_total{outcome="created",pair="EUR/MXN"} 1`,
		"# HELP quotes_outbox_backlog_messages",
		"go_goroutines",                 // Go runtime collector
		"process_resident_memory_bytes", // process collector
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics does not contain %q", want)
		}
	}
}
