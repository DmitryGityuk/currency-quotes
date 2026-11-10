package rates

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
)

var eurMXN = quotes.Pair{Base: "EUR", Quote: "MXN"}

func TestFrankfurterRate(t *testing.T) {
	var gotPath, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotAccept = r.URL.Path, r.Header.Get("Accept")
		w.Header().Set("Content-Type", "application/json")
		// More digits than float64 keeps exactly: the client must not round them.
		_, _ = w.Write([]byte(`{"date": "2026-11-05", "base": "EUR", "quote": "MXN", "rate": 21.451234567890123456}`))
	}))
	defer srv.Close()

	got, err := NewFrankfurter(srv.URL+"/", srv.Client()).Rate(t.Context(), eurMXN)
	if err != nil {
		t.Fatalf("Rate() error = %v", err)
	}
	if gotPath != "/v2/rate/EUR/MXN" {
		t.Errorf("request path = %q, want /v2/rate/EUR/MXN", gotPath)
	}
	if gotAccept != "application/json" {
		t.Errorf("Accept = %q", gotAccept)
	}
	if got.Price != "21.451234567890123456" {
		t.Errorf("Price = %q, want every digit of the JSON number", got.Price)
	}
	if want := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC); !got.Date.Equal(want) {
		t.Errorf("Date = %v, want %v", got.Date, want)
	}
}

func TestFrankfurterErrors(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		header         map[string]string
		body           string
		wantTemporary  bool
		wantRetryAfter time.Duration
	}{
		{name: "unknown currency", status: http.StatusUnprocessableEntity, body: `{"message":"not found"}`},
		{name: "not found", status: http.StatusNotFound},
		{name: "server error", status: http.StatusServiceUnavailable, wantTemporary: true},
		{
			name: "rate limited", status: http.StatusTooManyRequests, header: map[string]string{"Retry-After": "7"},
			wantTemporary: true, wantRetryAfter: 7 * time.Second,
		},
		{
			name: "retry-after as a date is ignored", status: http.StatusTooManyRequests,
			header: map[string]string{"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}, wantTemporary: true,
		},
		{name: "not json", status: http.StatusOK, body: `<html>`},
		{name: "another pair", status: http.StatusOK, body: `{"date":"2026-11-05","base":"USD","quote":"MXN","rate":18.4}`},
		{name: "negative rate", status: http.StatusOK, body: `{"date":"2026-11-05","base":"EUR","quote":"MXN","rate":-1}`},
		{name: "exponent", status: http.StatusOK, body: `{"date":"2026-11-05","base":"EUR","quote":"MXN","rate":2.1e1}`},
		{name: "bad date", status: http.StatusOK, body: `{"date":"05.11.2026","base":"EUR","quote":"MXN","rate":21.45}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for k, v := range tt.header {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			_, err := NewFrankfurter(srv.URL, srv.Client()).Rate(t.Context(), eurMXN)
			if err == nil {
				t.Fatal("Rate() error = nil, want error")
			}
			if got := IsTemporary(err); got != tt.wantTemporary {
				t.Errorf("IsTemporary(%v) = %v, want %v", err, got, tt.wantTemporary)
			}
			if got := RetryAfter(err); got != tt.wantRetryAfter {
				t.Errorf("RetryAfter() = %v, want %v", got, tt.wantRetryAfter)
			}
		})
	}
}

func TestFrankfurterNetworkErrors(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer slow.Close()

	t.Run("timeout is temporary", func(t *testing.T) {
		client := &http.Client{Timeout: 50 * time.Millisecond}
		_, err := NewFrankfurter(slow.URL, client).Rate(t.Context(), eurMXN)
		if !IsTemporary(err) {
			t.Errorf("IsTemporary(%v) = false, want true", err)
		}
	})

	t.Run("connection refused is temporary", func(t *testing.T) {
		closed := httptest.NewServer(http.NotFoundHandler())
		addr := closed.URL
		closed.Close()
		_, err := NewFrankfurter(addr, http.DefaultClient).Rate(t.Context(), eurMXN)
		if !IsTemporary(err) {
			t.Errorf("IsTemporary(%v) = false, want true", err)
		}
	})

	t.Run("cancelled context is passed through", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(20*time.Millisecond, cancel)
		_, err := NewFrankfurter(slow.URL, slow.Client()).Rate(ctx, eurMXN)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Rate() error = %v, want context.Canceled", err)
		}
		if IsTemporary(err) {
			t.Error("a cancelled call must not be retried")
		}
	})
}

func TestFakeRate(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 11, 6, 23, 30, 0, 0, time.FixedZone("UTC+5", 5*3600)) }
	f := &Fake{Now: now}

	for _, pair := range []quotes.Pair{eurMXN, {Base: "MXN", Quote: "EUR"}, {Base: "USD", Quote: "EUR"}} {
		r, err := f.Rate(t.Context(), pair)
		if err != nil {
			t.Fatalf("Rate(%s) error = %v", pair, err)
		}
		if _, err := quotes.ParsePrice(string(r.Price)); err != nil {
			t.Errorf("Rate(%s) price %q is not a valid price: %v", pair, r.Price, err)
		}
		if want := time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC); !r.Date.Equal(want) {
			t.Errorf("Rate(%s) date = %v, want %v", pair, r.Date, want)
		}
	}

	if _, err := f.Rate(t.Context(), quotes.Pair{Base: "GBP", Quote: "USD"}); err == nil || IsTemporary(err) {
		t.Errorf("unknown currency: error = %v, want a permanent error", err)
	}

	failing := &Fake{FailureRate: 1}
	if _, err := failing.Rate(t.Context(), eurMXN); !IsTemporary(err) {
		t.Errorf("FailureRate 1: error = %v, want a temporary error", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := (&Fake{Delay: time.Hour}).Rate(ctx, eurMXN); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled during delay: error = %v, want context.Canceled", err)
	}
	if _, err := (&Fake{Delay: time.Millisecond}).Rate(t.Context(), eurMXN); err != nil {
		t.Errorf("short delay: error = %v", err)
	}
}

func TestIsTemporaryPlainErrors(t *testing.T) {
	if IsTemporary(errors.New("boom")) {
		t.Error("an unknown error must not be retried")
	}
	if !IsTemporary(context.DeadlineExceeded) {
		t.Error("a deadline is worth retrying")
	}
	if RetryAfter(errors.New("boom")) != 0 {
		t.Error("RetryAfter of a plain error must be 0")
	}
}
