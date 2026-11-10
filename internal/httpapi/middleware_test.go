package httpapi

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestChainOrder(t *testing.T) {
	var order []string
	mark := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}
	h := Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		order = append(order, "handler")
	}), mark("a"), mark("b"))

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody))

	if got, want := strings.Join(order, ","), "a,b,handler"; got != want {
		t.Errorf("order = %s, want %s", got, want)
	}
}

func TestRequestID(t *testing.T) {
	tests := []struct {
		name     string
		incoming string
		wantSame bool // whether the incoming ID must be kept
	}{
		{name: "generated when missing", incoming: "", wantSame: false},
		{name: "kept when valid", incoming: "req-123", wantSame: true},
		{name: "replaced when it contains a newline", incoming: "a\nb", wantSame: false},
		{name: "replaced when too long", incoming: strings.Repeat("x", 65), wantSame: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var seen string
			h := RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				seen = RequestIDFrom(r.Context())
			}))

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody)
			if tt.incoming != "" {
				req.Header.Set(RequestIDHeader, tt.incoming)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if seen == "" {
				t.Fatal("request ID is missing from the context")
			}
			if got := rec.Header().Get(RequestIDHeader); got != seen {
				t.Errorf("response header = %q, context = %q; want them equal", got, seen)
			}
			if tt.wantSame && seen != tt.incoming {
				t.Errorf("request ID = %q, want %q", seen, tt.incoming)
			}
			if !tt.wantSame && seen == tt.incoming {
				t.Errorf("request ID %q was kept, want a new one", seen)
			}
		})
	}
}

func TestRecoverReturns500AndLogs(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))

	h := Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}), RequestID, Recover(logger))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", ct)
	}
	var p problem
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if p.Status != http.StatusInternalServerError {
		t.Errorf("problem status = %d, want %d", p.Status, http.StatusInternalServerError)
	}
	if !strings.Contains(logs.String(), "boom") {
		t.Errorf("log does not mention the panic: %s", logs.String())
	}
}

func TestLoggingRecordsStatus(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))

	h := Logging(logger)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/quotes", http.NoBody))

	var rec struct {
		Method string `json:"method"`
		Path   string `json:"path"`
		Status int    `json:"status"`
	}
	if err := json.Unmarshal(logs.Bytes(), &rec); err != nil {
		t.Fatalf("decode log record %q: %v", logs.String(), err)
	}
	if rec.Method != http.MethodPost || rec.Path != "/quotes" || rec.Status != http.StatusTeapot {
		t.Errorf("log record = %+v, want POST /quotes 418", rec)
	}
}

type recordingHTTPObserver struct {
	routes   []string
	statuses []int
}

func (o *recordingHTTPObserver) ObserveHTTP(method, route string, status int, _ time.Duration) {
	o.routes = append(o.routes, method+" "+route)
	o.statuses = append(o.statuses, status)
}

func TestMetricsLabelsByRoutePattern(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/quotes/updates/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	o := &recordingHTTPObserver{}
	h := Chain(mux, RequestID, Metrics(o))

	for _, path := range []string{"/api/v1/quotes/updates/1", "/api/v1/quotes/updates/2", "/nowhere"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
	}

	want := []string{
		"GET /api/v1/quotes/updates/{id}", // ids do not leak into labels
		"GET /api/v1/quotes/updates/{id}",
		"GET unmatched",
	}
	if strings.Join(o.routes, ",") != strings.Join(want, ",") {
		t.Errorf("routes = %v, want %v", o.routes, want)
	}
	if o.statuses[0] != http.StatusNotFound {
		t.Errorf("status = %d, want the status the handler wrote", o.statuses[0])
	}
}

func TestRecoverAfterTheResponseStarted(t *testing.T) {
	h := Recover(slog.New(slog.DiscardHandler))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"id":`))
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody))

	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want the 202 the handler already sent", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "internal") {
		t.Errorf("a problem body was appended to a started response: %s", rec.Body.String())
	}
}
