package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
)

var (
	testID      = uuid.MustParse("01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e")
	requestedAt = time.Date(2026, 11, 6, 10, 15, 0, 123000000, time.UTC)
	eurMXN      = quotes.Pair{Base: "EUR", Quote: "MXN"}
)

// fakeQuoteStore records requests and returns canned results.
type fakeQuoteStore struct {
	mu      sync.Mutex
	reqs    []quotes.UpdateRequest
	result  quotes.UpdateRequestResult
	update  quotes.Update
	quote   quotes.Quote
	err     error
	lastID  uuid.UUID
	lastPar quotes.Pair
}

func (s *fakeQuoteStore) RequestUpdate(_ context.Context, req quotes.UpdateRequest) (quotes.UpdateRequestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqs = append(s.reqs, req)
	return s.result, s.err
}

func (s *fakeQuoteStore) Update(_ context.Context, id uuid.UUID) (quotes.Update, error) {
	s.lastID = id
	return s.update, s.err
}

func (s *fakeQuoteStore) LatestQuote(_ context.Context, p quotes.Pair) (quotes.Quote, error) {
	s.lastPar = p
	return s.quote, s.err
}

func pendingUpdate() quotes.Update {
	return quotes.Update{ID: testID, Pair: eurMXN, Status: quotes.StatusPending, RequestedAt: requestedAt}
}

// serve runs one request through the same middleware chain as production.
func serve(t *testing.T, store QuoteStore, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	NewQuotes(store, quotes.MustCurrencies("USD", "EUR", "MXN"), slog.New(slog.DiscardHandler)).Register(mux)
	h := Chain(mux, RequestID)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func post(t *testing.T, body string, headers ...string) *http.Request {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, updatesPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(RequestIDHeader, "req-42")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	return req
}

func get(t *testing.T, target string) *http.Request {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, target, http.NoBody)
	req.Header.Set(RequestIDHeader, "req-42")
	return req
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %s: %v", rec.Body.String(), err)
	}
	return v
}

// wantProblem checks an RFC 9457 error response.
func wantProblem(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) problem {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body %s", rec.Code, status, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json", ct)
	}
	p := decode[problem](t, rec)
	if p.Code != code || p.Status != status || p.Type != "about:blank" || p.Title != http.StatusText(status) {
		t.Errorf("problem = %+v, want code %q and status %d", p, code, status)
	}
	if p.RequestID != "req-42" {
		t.Errorf("request_id = %q, want the X-Request-ID of the request", p.RequestID)
	}
	return p
}

func TestCreateUpdateAccepted(t *testing.T) {
	store := &fakeQuoteStore{result: quotes.UpdateRequestResult{Update: pendingUpdate()}}
	rec := serve(t, store, post(t, `{"pair": "eur/mxn"}`, idempotencyKeyHeader, "key-1"))

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Location"); got != updatesPath+"/"+testID.String() {
		t.Errorf("Location = %q", got)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1 for an unfinished update", got)
	}
	if got := rec.Header().Get("Idempotent-Replayed"); got != "" {
		t.Errorf("Idempotent-Replayed = %q on a first request", got)
	}
	want := `{"id":"01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e","pair":"EUR/MXN","status":"pending","requested_at":"2026-11-06T10:15:00.123Z"}`
	if got := strings.TrimSpace(rec.Body.String()); got != want {
		t.Errorf("body = %s\nwant   %s", got, want)
	}

	if len(store.reqs) != 1 {
		t.Fatalf("store got %d requests, want 1", len(store.reqs))
	}
	req := store.reqs[0]
	if req.Pair != eurMXN || req.IdempotencyKey != "key-1" || req.RequestID != "req-42" {
		t.Errorf("store request = %+v, want the normalized pair, the key and the request id", req)
	}
	if req.RequestHash != requestHash(eurMXN) {
		t.Error("request hash is not the hash of the normalized request")
	}
}

func TestCreateUpdateReplayedAndJoined(t *testing.T) {
	t.Run("replayed by Idempotency-Key", func(t *testing.T) {
		done := pendingUpdate()
		done.Status, done.Price, done.RateDate, done.UpdatedAt = quotes.StatusDone, "21.4512", requestedAt, requestedAt
		store := &fakeQuoteStore{result: quotes.UpdateRequestResult{Update: done, Replayed: true}}
		rec := serve(t, store, post(t, `{"pair":"EUR/MXN"}`, idempotencyKeyHeader, `"key-1"`))

		if rec.Code != http.StatusAccepted || rec.Header().Get("Idempotent-Replayed") != "true" {
			t.Errorf("status %d, Idempotent-Replayed %q; want 202 and true", rec.Code, rec.Header().Get("Idempotent-Replayed"))
		}
		if rec.Header().Get("Retry-After") != "" {
			t.Error("a finished update must not carry Retry-After")
		}
		if store.reqs[0].IdempotencyKey != "key-1" {
			t.Errorf("key = %q, want the quotes of the structured-field form removed", store.reqs[0].IdempotencyKey)
		}
	})

	t.Run("joined the unfinished update of the pair", func(t *testing.T) {
		store := &fakeQuoteStore{result: quotes.UpdateRequestResult{Update: pendingUpdate(), Joined: true}}
		rec := serve(t, store, post(t, `{"pair":"EUR/MXN"}`))
		if rec.Code != http.StatusAccepted || decode[updateResponse](t, rec).ID != testID.String() {
			t.Errorf("status %d, body %s; want 202 with the existing id", rec.Code, rec.Body.String())
		}
		if store.reqs[0].IdempotencyKey != "" {
			t.Errorf("key = %q, want none", store.reqs[0].IdempotencyKey)
		}
	})
}

func TestCreateUpdateRejectsBadRequests(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		key        string
		wantStatus int
		wantCode   string
	}{
		{name: "not json", body: `pair=EUR/MXN`, wantStatus: 400, wantCode: codeInvalidJSON},
		{name: "unknown field", body: `{"pair":"EUR/MXN","currency":"EUR"}`, wantStatus: 400, wantCode: codeInvalidJSON},
		{name: "two objects", body: `{"pair":"EUR/MXN"}{"pair":"USD/MXN"}`, wantStatus: 400, wantCode: codeInvalidJSON},
		{name: "pair is a number", body: `{"pair":42}`, wantStatus: 400, wantCode: codeInvalidJSON},
		{name: "empty body", body: ``, wantStatus: 400, wantCode: codeInvalidJSON},
		{name: "too large", body: `{"pair":"` + strings.Repeat("A", 2000) + `"}`, wantStatus: 400, wantCode: codeInvalidJSON},
		{name: "no pair", body: `{}`, wantStatus: 400, wantCode: codeInvalidPair},
		{name: "bad pair format", body: `{"pair":"EURMXN"}`, wantStatus: 400, wantCode: codeInvalidPair},
		{name: "unsupported currency", body: `{"pair":"GBP/USD"}`, wantStatus: 422, wantCode: codeUnsupportedCurrency},
		{name: "same currency", body: `{"pair":"EUR/EUR"}`, wantStatus: 422, wantCode: codeSameCurrency},
		{name: "key too long", body: `{"pair":"EUR/MXN"}`, key: strings.Repeat("k", 256), wantStatus: 400, wantCode: codeInvalidIdempotencyKey},
		{name: "key with a space", body: `{"pair":"EUR/MXN"}`, key: "my key", wantStatus: 400, wantCode: codeInvalidIdempotencyKey},
		{name: "key is empty quotes", body: `{"pair":"EUR/MXN"}`, key: `""`, wantStatus: 400, wantCode: codeInvalidIdempotencyKey},
		{name: "key is not ascii", body: `{"pair":"EUR/MXN"}`, key: "ключ", wantStatus: 400, wantCode: codeInvalidIdempotencyKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeQuoteStore{}
			var headers []string
			if tt.key != "" {
				headers = []string{idempotencyKeyHeader, tt.key}
			}
			rec := serve(t, store, post(t, tt.body, headers...))
			wantProblem(t, rec, tt.wantStatus, tt.wantCode)
			if len(store.reqs) != 0 {
				t.Error("an invalid request reached the store")
			}
		})
	}
}

func TestCreateUpdateStoreErrors(t *testing.T) {
	t.Run("key reused with another body", func(t *testing.T) {
		store := &fakeQuoteStore{err: quotes.ErrIdempotencyKeyReused}
		rec := serve(t, store, post(t, `{"pair":"USD/MXN"}`, idempotencyKeyHeader, "key-1"))
		wantProblem(t, rec, http.StatusUnprocessableEntity, codeIdempotencyKeyReused)
	})

	t.Run("database down", func(t *testing.T) {
		store := &fakeQuoteStore{err: errors.New("dial tcp 10.0.0.5:5432: connection refused")}
		rec := serve(t, store, post(t, `{"pair":"USD/MXN"}`))
		p := wantProblem(t, rec, http.StatusInternalServerError, codeInternal)
		if p.Detail != "" || strings.Contains(rec.Body.String(), "10.0.0.5") {
			t.Errorf("500 response leaks internals: %s", rec.Body.String())
		}
	})
}

func TestRequestHashIgnoresLetterCase(t *testing.T) {
	lower, _ := quotes.ParsePair("eur/mxn")
	if requestHash(lower) != requestHash(eurMXN) {
		t.Error("the same pair in another letter case must hash the same")
	}
	if requestHash(quotes.Pair{Base: "USD", Quote: "MXN"}) == requestHash(eurMXN) {
		t.Error("different pairs must hash differently")
	}
}

func TestGetUpdate(t *testing.T) {
	done := pendingUpdate()
	done.Status, done.Price, done.Attempts = quotes.StatusDone, "21.4512", 1
	done.RateDate = time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	done.UpdatedAt = time.Date(2026, 11, 6, 15, 15, 0, 734000000, time.FixedZone("UTC+5", 5*3600))

	failed := pendingUpdate()
	failed.Status, failed.Error = quotes.StatusFailed, "rates provider is unavailable: 3 attempts failed"

	processing := pendingUpdate()
	processing.Status = quotes.StatusProcessing

	tests := []struct {
		name           string
		update         quotes.Update
		wantBody       string
		wantRetryAfter string
	}{
		{
			name:           "pending",
			update:         pendingUpdate(),
			wantBody:       `{"id":"01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e","pair":"EUR/MXN","status":"pending","requested_at":"2026-11-06T10:15:00.123Z"}`,
			wantRetryAfter: "1",
		},
		{
			name:           "processing",
			update:         processing,
			wantBody:       `{"id":"01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e","pair":"EUR/MXN","status":"processing","requested_at":"2026-11-06T10:15:00.123Z"}`,
			wantRetryAfter: "1",
		},
		{
			name:     "done, time in UTC",
			update:   done,
			wantBody: `{"id":"01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e","pair":"EUR/MXN","status":"done","requested_at":"2026-11-06T10:15:00.123Z","price":"21.4512","rate_date":"2026-11-05","updated_at":"2026-11-06T10:15:00.734Z"}`,
		},
		{
			name:     "failed",
			update:   failed,
			wantBody: `{"id":"01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e","pair":"EUR/MXN","status":"failed","requested_at":"2026-11-06T10:15:00.123Z","error":"rates provider is unavailable: 3 attempts failed"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeQuoteStore{update: tt.update}
			rec := serve(t, store, get(t, updatesPath+"/"+testID.String()))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, body %s", rec.Code, rec.Body.String())
			}
			if got := strings.TrimSpace(rec.Body.String()); got != tt.wantBody {
				t.Errorf("body = %s\nwant   %s", got, tt.wantBody)
			}
			if got := rec.Header().Get("Retry-After"); got != tt.wantRetryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tt.wantRetryAfter)
			}
			if store.lastID != testID {
				t.Errorf("store asked for %s", store.lastID)
			}
		})
	}
}

func TestGetUpdateErrors(t *testing.T) {
	wantProblem(t, serve(t, &fakeQuoteStore{}, get(t, updatesPath+"/not-a-uuid")), http.StatusBadRequest, codeInvalidID)
	wantProblem(t, serve(t, &fakeQuoteStore{err: quotes.ErrNotFound}, get(t, updatesPath+"/"+testID.String())),
		http.StatusNotFound, codeUpdateNotFound)
	wantProblem(t, serve(t, &fakeQuoteStore{err: errors.New("boom")}, get(t, updatesPath+"/"+testID.String())),
		http.StatusInternalServerError, codeInternal)
}

func TestLatestQuote(t *testing.T) {
	store := &fakeQuoteStore{quote: quotes.Quote{
		Pair:      eurMXN,
		Price:     "21.4512",
		RateDate:  time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 11, 6, 10, 15, 0, 734000000, time.UTC),
		UpdateID:  testID,
	}}
	// The slash may come encoded or not; both mean the same pair.
	for _, target := range []string{latestPath + "?pair=EUR/MXN", latestPath + "?pair=eur%2Fmxn"} {
		rec := serve(t, store, get(t, target))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s: status = %d, body %s", target, rec.Code, rec.Body.String())
		}
		want := `{"pair":"EUR/MXN","price":"21.4512","rate_date":"2026-11-05","updated_at":"2026-11-06T10:15:00.734Z","update_id":"01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e"}`
		if got := strings.TrimSpace(rec.Body.String()); got != want {
			t.Errorf("GET %s: body = %s\nwant %s", target, got, want)
		}
		if store.lastPar != eurMXN {
			t.Errorf("store asked for %s", store.lastPar)
		}
	}
}

func TestLatestQuoteErrors(t *testing.T) {
	tests := []struct {
		target     string
		store      *fakeQuoteStore
		wantStatus int
		wantCode   string
	}{
		{target: latestPath, store: &fakeQuoteStore{}, wantStatus: 400, wantCode: codeInvalidPair},
		{target: latestPath + "?pair=EURMXN", store: &fakeQuoteStore{}, wantStatus: 400, wantCode: codeInvalidPair},
		{target: latestPath + "?pair=GBP/USD", store: &fakeQuoteStore{}, wantStatus: 422, wantCode: codeUnsupportedCurrency},
		{target: latestPath + "?pair=USD/USD", store: &fakeQuoteStore{}, wantStatus: 422, wantCode: codeSameCurrency},
		{target: latestPath + "?pair=EUR/MXN", store: &fakeQuoteStore{err: quotes.ErrNotFound}, wantStatus: 404, wantCode: codeQuoteNotFound},
		{target: latestPath + "?pair=EUR/MXN", store: &fakeQuoteStore{err: errors.New("boom")}, wantStatus: 500, wantCode: codeInternal},
	}
	for _, tt := range tests {
		t.Run(tt.target+" "+tt.wantCode, func(t *testing.T) {
			wantProblem(t, serve(t, tt.store, get(t, tt.target)), tt.wantStatus, tt.wantCode)
		})
	}
}

func TestQuoteRoutesRejectOtherMethods(t *testing.T) {
	for _, req := range []*http.Request{
		httptest.NewRequestWithContext(t.Context(), http.MethodGet, updatesPath, http.NoBody),
		httptest.NewRequestWithContext(t.Context(), http.MethodDelete, updatesPath+"/"+testID.String(), http.NoBody),
		httptest.NewRequestWithContext(t.Context(), http.MethodPost, latestPath, bytes.NewReader(nil)),
	} {
		if rec := serve(t, &fakeQuoteStore{}, req); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s %s: status = %d, want 405", req.Method, req.URL.Path, rec.Code)
		}
	}
}

type recordingUpdateObserver struct{ outcomes []string }

func (o *recordingUpdateObserver) UpdateRequested(p quotes.Pair, outcome string) {
	o.outcomes = append(o.outcomes, p.String()+" "+outcome)
}

func TestCreateUpdateReportsOutcome(t *testing.T) {
	for _, tt := range []struct {
		result quotes.UpdateRequestResult
		want   string
	}{
		{quotes.UpdateRequestResult{Update: pendingUpdate()}, "EUR/MXN created"},
		{quotes.UpdateRequestResult{Update: pendingUpdate(), Joined: true}, "EUR/MXN joined"},
		{quotes.UpdateRequestResult{Update: pendingUpdate(), Replayed: true}, "EUR/MXN replayed"},
	} {
		o := &recordingUpdateObserver{}
		mux := http.NewServeMux()
		NewQuotes(&fakeQuoteStore{result: tt.result}, quotes.MustCurrencies("EUR", "MXN"), slog.New(slog.DiscardHandler)).
			WithObserver(o).Register(mux)
		mux.ServeHTTP(httptest.NewRecorder(), post(t, `{"pair":"EUR/MXN"}`))
		if len(o.outcomes) != 1 || o.outcomes[0] != tt.want {
			t.Errorf("outcomes = %v, want [%s]", o.outcomes, tt.want)
		}
	}
}
