package demo

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/DmitryGityuk/currency-quotes/internal/httpapi"
	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
)

// memStore follows the PostgreSQL store's rules in memory and finishes every
// update shortly after it is created, like a fast worker.
type memStore struct {
	mu       sync.Mutex
	updates  map[uuid.UUID]*quotes.Update
	inFlight map[quotes.Pair]uuid.UUID
	keys     map[string]struct {
		hash [32]byte
		id   uuid.UUID
	}
	latest map[quotes.Pair]quotes.Quote
}

func newMemStore() *memStore {
	return &memStore{
		updates:  map[uuid.UUID]*quotes.Update{},
		inFlight: map[quotes.Pair]uuid.UUID{},
		keys: map[string]struct {
			hash [32]byte
			id   uuid.UUID
		}{},
		latest: map[quotes.Pair]quotes.Quote{},
	}
}

func (s *memStore) RequestUpdate(_ context.Context, req quotes.UpdateRequest) (quotes.UpdateRequestResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.IdempotencyKey != "" {
		if k, ok := s.keys[req.IdempotencyKey]; ok {
			if k.hash != req.RequestHash {
				return quotes.UpdateRequestResult{}, quotes.ErrIdempotencyKeyReused
			}
			return quotes.UpdateRequestResult{Update: *s.updates[k.id], Replayed: true}, nil
		}
	}
	res := quotes.UpdateRequestResult{}
	if id, ok := s.inFlight[req.Pair]; ok {
		res = quotes.UpdateRequestResult{Update: *s.updates[id], Joined: true}
	} else {
		u := &quotes.Update{ID: uuid.NewV7(), Pair: req.Pair, Status: quotes.StatusPending, RequestedAt: time.Now()}
		s.updates[u.ID], s.inFlight[req.Pair] = u, u.ID
		res.Update = *u
		time.AfterFunc(50*time.Millisecond, func() { s.finish(u.ID) })
	}
	if req.IdempotencyKey != "" {
		s.keys[req.IdempotencyKey] = struct {
			hash [32]byte
			id   uuid.UUID
		}{req.RequestHash, res.Update.ID}
	}
	return res, nil
}

func (s *memStore) finish(id uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.updates[id]
	u.Status, u.Price, u.RateDate, u.UpdatedAt = quotes.StatusDone, "21.4512", time.Now(), time.Now()
	delete(s.inFlight, u.Pair)
	s.latest[u.Pair] = quotes.Quote{Pair: u.Pair, Price: u.Price, RateDate: u.RateDate, UpdatedAt: u.UpdatedAt, UpdateID: id}
}

func (s *memStore) Update(_ context.Context, id uuid.UUID) (quotes.Update, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u, ok := s.updates[id]; ok {
		return *u, nil
	}
	return quotes.Update{}, quotes.ErrNotFound
}

func (s *memStore) LatestQuote(_ context.Context, p quotes.Pair) (quotes.Quote, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if q, ok := s.latest[p]; ok {
		return q, nil
	}
	return quotes.Quote{}, quotes.ErrNotFound
}

func newRunner(url string, out *bytes.Buffer) *Runner {
	return &Runner{BaseURL: url, Client: http.DefaultClient, Out: out, PollEvery: 10 * time.Millisecond, Timeout: 2 * time.Second}
}

func TestRunAgainstTheRealHandlers(t *testing.T) {
	mux := http.NewServeMux()
	httpapi.NewQuotes(newMemStore(), quotes.MustCurrencies("USD", "EUR", "MXN"), slog.New(slog.DiscardHandler)).Register(mux)
	srv := httptest.NewServer(httpapi.Chain(mux, httpapi.RequestID))
	defer srv.Close()

	var out bytes.Buffer
	if err := newRunner(srv.URL, &out).Run(t.Context()); err != nil {
		t.Fatalf("Run() error = %v\n%s", err, out.String())
	}
	for _, want := range []string{
		"← 202 Accepted",
		"← done через",
		"Idempotent-Replayed: true",
		"idempotency_key_reused",
		"1 уникальный id",
		"unsupported_currency",
		"Всё работает",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output does not mention %q:\n%s", want, out.String())
		}
	}
}

func TestRunReportsABrokenAPI(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	var out bytes.Buffer
	err := newRunner(srv.URL, &out).Run(t.Context())
	if err == nil || !strings.Contains(err.Error(), "want 202") {
		t.Errorf("Run() error = %v, want a failed first step", err)
	}
	if !strings.Contains(out.String(), "✗") {
		t.Errorf("output does not mark the failure:\n%s", out.String())
	}
}

func TestRunReportsAnUnreachableAPI(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	var out bytes.Buffer
	if err := newRunner(url, &out).Run(t.Context()); err == nil {
		t.Error("Run() error = nil for an API that is down")
	}
}
