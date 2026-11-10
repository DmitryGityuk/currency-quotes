package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/DmitryGityuk/currency-quotes/internal/events"
	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
	"github.com/DmitryGityuk/currency-quotes/internal/rates"
)

var (
	eurMXN   = quotes.Pair{Base: "EUR", Quote: "MXN"}
	updateID = uuid.MustParse("01972f4e-8b3a-7c21-9d4e-5f6a7b8c9d0e")
	goodRate = rates.Rate{Price: "21.4512", Date: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)}
	now      = time.Date(2026, 11, 6, 10, 15, 0, 0, time.UTC)
)

// fakeStore keeps updates in memory and follows the same rules as the
// PostgreSQL store: finished updates are never changed.
type fakeStore struct {
	mu      sync.Mutex
	updates map[uuid.UUID]*quotes.Update
	quotes  map[quotes.Pair]quotes.Quote
	err     error // returned by every call when set
}

func newFakeStore(upd quotes.Update) *fakeStore {
	return &fakeStore{
		updates: map[uuid.UUID]*quotes.Update{upd.ID: &upd},
		quotes:  map[quotes.Pair]quotes.Quote{},
	}
}

func (s *fakeStore) StartAttempt(_ context.Context, id uuid.UUID) (quotes.Update, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return quotes.Update{}, s.err
	}
	u, ok := s.updates[id]
	if !ok {
		return quotes.Update{}, quotes.ErrNotFound
	}
	if !u.Status.Finished() {
		u.Status = quotes.StatusProcessing
		u.Attempts++
	}
	return *u, nil
}

func (s *fakeStore) Complete(_ context.Context, id uuid.UUID, r rates.Rate, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	u := s.updates[id]
	u.Status, u.Price, u.RateDate, u.UpdatedAt = quotes.StatusDone, r.Price, r.Date, at
	s.quotes[u.Pair] = quotes.Quote{Pair: u.Pair, Price: r.Price, RateDate: r.Date, UpdatedAt: at, UpdateID: id}
	return nil
}

func (s *fakeStore) Fail(_ context.Context, id uuid.UUID, reason string, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	u := s.updates[id]
	u.Status, u.Error = quotes.StatusFailed, reason
	return nil
}

func (s *fakeStore) get(id uuid.UUID) quotes.Update {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.updates[id]
}

// scriptedProvider returns the given results in order, then repeats the last.
type scriptedProvider struct {
	results []error // nil means success with goodRate
	calls   int
}

func (p *scriptedProvider) Rate(_ context.Context, _ quotes.Pair) (rates.Rate, error) {
	err := p.results[min(p.calls, len(p.results)-1)]
	p.calls++
	if err != nil {
		return rates.Rate{}, err
	}
	return goodRate, nil
}

var (
	errTemporary = &rates.Error{Provider: "test", Temporary: true, Err: errors.New("503")}
	errPermanent = &rates.Error{Provider: "test", Err: errors.New("422 unknown currency")}
)

func newProcessor(store Store, provider rates.Provider) (*Processor, *[]time.Duration) {
	p := NewProcessor(store, provider, slog.New(slog.DiscardHandler), Config{
		MaxAttempts: 3,
		CallTimeout: time.Second,
		Backoff:     Backoff{Base: 500 * time.Millisecond, Max: 5 * time.Second},
	})
	var slept []time.Duration
	p.now = func() time.Time { return now }
	p.sleep = func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		return nil
	}
	return p, &slept
}

func message(t *testing.T) Message {
	t.Helper()
	b, err := events.UpdateRequested{UpdateID: updateID, Pair: "EUR/MXN", RequestedAt: now}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return Message{Key: "EUR/MXN", Value: b, Headers: map[string]string{events.HeaderRequestID: "req-1"}}
}

func pending() quotes.Update {
	return quotes.Update{ID: updateID, Pair: eurMXN, Status: quotes.StatusPending, RequestedAt: now}
}

func TestHandleStoresTheRate(t *testing.T) {
	store := newFakeStore(pending())
	p, slept := newProcessor(store, &scriptedProvider{results: []error{nil}})

	if err := p.Handle(t.Context(), message(t)); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	got := store.get(updateID)
	if got.Status != quotes.StatusDone || got.Price != goodRate.Price || got.Attempts != 1 || !got.UpdatedAt.Equal(now) {
		t.Errorf("update = %+v, want done with price %s after 1 attempt", got, goodRate.Price)
	}
	if q := store.quotes[eurMXN]; q.Price != goodRate.Price || q.UpdateID != updateID {
		t.Errorf("latest quote = %+v, want the new price", q)
	}
	if len(*slept) != 0 {
		t.Errorf("slept %v, want no pauses", *slept)
	}
}

func TestHandleRetriesTemporaryErrors(t *testing.T) {
	store := newFakeStore(pending())
	provider := &scriptedProvider{results: []error{errTemporary, errTemporary, nil}}
	p, slept := newProcessor(store, provider)

	if err := p.Handle(t.Context(), message(t)); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := store.get(updateID); got.Status != quotes.StatusDone || got.Attempts != 3 {
		t.Errorf("update = %+v, want done after 3 attempts", got)
	}
	if len(*slept) != 2 {
		t.Fatalf("pauses = %v, want 2", *slept)
	}
	// Equal jitter: attempt 1 waits 250-500ms, attempt 2 waits 500ms-1s.
	if d := (*slept)[0]; d < 250*time.Millisecond || d > 500*time.Millisecond {
		t.Errorf("first pause = %v, want 250ms..500ms", d)
	}
	if d := (*slept)[1]; d < 500*time.Millisecond || d > time.Second {
		t.Errorf("second pause = %v, want 500ms..1s", d)
	}
}

func TestHandleFailsAfterMaxAttempts(t *testing.T) {
	store := newFakeStore(pending())
	provider := &scriptedProvider{results: []error{errTemporary}}
	p, _ := newProcessor(store, provider)

	if err := p.Handle(t.Context(), message(t)); err != nil {
		t.Fatalf("Handle() error = %v, want nil: a failed update is still processed", err)
	}
	got := store.get(updateID)
	if got.Status != quotes.StatusFailed || !strings.Contains(got.Error, "after 3 attempts") {
		t.Errorf("update = %+v, want failed after 3 attempts", got)
	}
	if provider.calls != 3 {
		t.Errorf("provider calls = %d, want 3", provider.calls)
	}
}

func TestHandleDoesNotRetryPermanentErrors(t *testing.T) {
	store := newFakeStore(pending())
	provider := &scriptedProvider{results: []error{errPermanent}}
	p, slept := newProcessor(store, provider)

	if err := p.Handle(t.Context(), message(t)); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := store.get(updateID); got.Status != quotes.StatusFailed || !strings.Contains(got.Error, "unknown currency") {
		t.Errorf("update = %+v, want failed with the provider's reason", got)
	}
	if provider.calls != 1 || len(*slept) != 0 {
		t.Errorf("calls = %d, pauses = %v; want one call and no pauses", provider.calls, *slept)
	}
}

func TestHandleHonoursRetryAfter(t *testing.T) {
	store := newFakeStore(pending())
	limited := &rates.Error{Provider: "test", Temporary: true, RetryAfter: 3 * time.Second, Err: errors.New("429")}
	p, slept := newProcessor(store, &scriptedProvider{results: []error{limited, nil}})

	if err := p.Handle(t.Context(), message(t)); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(*slept) != 1 || (*slept)[0] != 3*time.Second {
		t.Errorf("pauses = %v, want [3s] from Retry-After", *slept)
	}
}

func TestHandleSkipsFinishedUpdates(t *testing.T) {
	done := pending()
	done.Status, done.Price, done.Attempts = quotes.StatusDone, "20.0000", 1
	store := newFakeStore(done)
	provider := &scriptedProvider{results: []error{nil}}
	p, _ := newProcessor(store, provider)

	if err := p.Handle(t.Context(), message(t)); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if provider.calls != 0 {
		t.Errorf("provider calls = %d, want 0 for a duplicate delivery", provider.calls)
	}
	if got := store.get(updateID); got.Price != "20.0000" {
		t.Errorf("price = %s, a finished update must not change", got.Price)
	}
}

func TestHandleGivesUpAfterCrashLoop(t *testing.T) {
	// Three earlier deliveries crashed during the provider call.
	crashed := pending()
	crashed.Status, crashed.Attempts = quotes.StatusProcessing, 3
	store := newFakeStore(crashed)
	provider := &scriptedProvider{results: []error{nil}}
	p, _ := newProcessor(store, provider)

	if err := p.Handle(t.Context(), message(t)); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := store.get(updateID); got.Status != quotes.StatusFailed || !strings.Contains(got.Error, "gave up") {
		t.Errorf("update = %+v, want failed without another call", got)
	}
	if provider.calls != 0 {
		t.Errorf("provider calls = %d, want 0", provider.calls)
	}
}

func TestHandlePoisonMessages(t *testing.T) {
	p, _ := newProcessor(newFakeStore(pending()), &scriptedProvider{results: []error{nil}})

	if err := p.Handle(t.Context(), Message{Value: []byte("not json")}); !errors.Is(err, ErrPoison) {
		t.Errorf("malformed payload: error = %v, want ErrPoison", err)
	}

	b, _ := events.UpdateRequested{UpdateID: uuid.NewV7(), Pair: "EUR/MXN"}.Encode()
	if err := p.Handle(t.Context(), Message{Value: b}); !errors.Is(err, ErrPoison) {
		t.Errorf("unknown update: error = %v, want ErrPoison", err)
	}
}

func TestHandleStoreErrorsAreRetriedLater(t *testing.T) {
	dbDown := errors.New("connection refused")

	t.Run("start", func(t *testing.T) {
		store := newFakeStore(pending())
		store.err = dbDown
		p, _ := newProcessor(store, &scriptedProvider{results: []error{nil}})
		err := p.Handle(t.Context(), message(t))
		if !errors.Is(err, dbDown) || errors.Is(err, ErrPoison) {
			t.Errorf("error = %v, want the database error and no DLQ", err)
		}
	})

	t.Run("complete", func(t *testing.T) {
		store := newFakeStore(pending())
		provider := &failingAfterCall{store: store, err: dbDown}
		p, _ := newProcessor(store, provider)
		if err := p.Handle(t.Context(), message(t)); !errors.Is(err, dbDown) {
			t.Errorf("error = %v, want the database error", err)
		}
	})

	t.Run("fail", func(t *testing.T) {
		store := newFakeStore(pending())
		provider := &failingAfterCall{store: store, err: dbDown, providerErr: errPermanent}
		p, _ := newProcessor(store, provider)
		if err := p.Handle(t.Context(), message(t)); !errors.Is(err, dbDown) {
			t.Errorf("error = %v, want the database error", err)
		}
	})
}

// failingAfterCall breaks the store while the provider is being called, as if
// the database went down in the middle of processing.
type failingAfterCall struct {
	store       *fakeStore
	err         error
	providerErr error
}

func (f *failingAfterCall) Rate(context.Context, quotes.Pair) (rates.Rate, error) {
	f.store.mu.Lock()
	f.store.err = f.err
	f.store.mu.Unlock()
	if f.providerErr != nil {
		return rates.Rate{}, f.providerErr
	}
	return goodRate, nil
}

func TestHandleStopsOnShutdown(t *testing.T) {
	t.Run("during the pause", func(t *testing.T) {
		store := newFakeStore(pending())
		p, _ := newProcessor(store, &scriptedProvider{results: []error{errTemporary}})
		p.sleep = func(context.Context, time.Duration) error { return context.Canceled }

		if err := p.Handle(t.Context(), message(t)); !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
		if got := store.get(updateID); got.Status != quotes.StatusProcessing {
			t.Errorf("status = %s, want processing: the message will be redelivered", got.Status)
		}
	})

	t.Run("during the call", func(t *testing.T) {
		store := newFakeStore(pending())
		ctx, cancel := context.WithCancel(t.Context())
		p, _ := newProcessor(store, providerFunc(func(context.Context, quotes.Pair) (rates.Rate, error) {
			cancel()
			return rates.Rate{}, context.Canceled
		}))
		if err := p.Handle(ctx, message(t)); !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
		if got := store.get(updateID); got.Status != quotes.StatusProcessing {
			t.Errorf("status = %s, want processing", got.Status)
		}
	})
}

type providerFunc func(context.Context, quotes.Pair) (rates.Rate, error)

func (f providerFunc) Rate(ctx context.Context, p quotes.Pair) (rates.Rate, error) { return f(ctx, p) }

func TestBackoffDelay(t *testing.T) {
	b := Backoff{Base: 500 * time.Millisecond, Max: 5 * time.Second}
	tests := []struct {
		attempt  int
		min, max time.Duration
	}{
		{attempt: 1, min: 250 * time.Millisecond, max: 500 * time.Millisecond},
		{attempt: 2, min: 500 * time.Millisecond, max: time.Second},
		{attempt: 3, min: time.Second, max: 2 * time.Second},
		{attempt: 5, min: 2500 * time.Millisecond, max: 5 * time.Second},  // 8s capped at 5s
		{attempt: 64, min: 2500 * time.Millisecond, max: 5 * time.Second}, // no overflow
		{attempt: 0, min: 2500 * time.Millisecond, max: 5 * time.Second},
	}
	for _, tt := range tests {
		for range 100 {
			if d := b.Delay(tt.attempt); d < tt.min || d > tt.max {
				t.Fatalf("Delay(%d) = %v, want %v..%v", tt.attempt, d, tt.min, tt.max)
			}
		}
	}
}

func TestSleepCtx(t *testing.T) {
	if err := sleepCtx(t.Context(), time.Millisecond); err != nil {
		t.Errorf("sleepCtx() error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := sleepCtx(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Errorf("sleepCtx() error = %v, want context.Canceled", err)
	}
}

type recordingObserver struct {
	calls    []string
	finished []string
}

func (o *recordingObserver) ProviderCall(outcome string, _ time.Duration) {
	o.calls = append(o.calls, outcome)
}

func (o *recordingObserver) UpdateFinished(p quotes.Pair, s quotes.Status, attempts int, since time.Duration) {
	o.finished = append(o.finished, fmt.Sprintf("%s %s %d %s", p, s, attempts, since))
}

func TestHandleReportsToObserver(t *testing.T) {
	t.Run("retried, then done", func(t *testing.T) {
		upd := pending()
		upd.RequestedAt = now.Add(-2 * time.Second)
		o := &recordingObserver{}
		p, _ := newProcessor(newFakeStore(upd), &scriptedProvider{results: []error{errTemporary, nil}})
		p.WithObserver(o)
		if err := p.Handle(t.Context(), message(t)); err != nil {
			t.Fatal(err)
		}
		if strings.Join(o.calls, ",") != "temporary_error,ok" {
			t.Errorf("provider calls = %v", o.calls)
		}
		if len(o.finished) != 1 || o.finished[0] != "EUR/MXN done 2 2s" {
			t.Errorf("finished = %v, want one done update after 2 attempts, 2s after the request", o.finished)
		}
	})

	t.Run("failed", func(t *testing.T) {
		o := &recordingObserver{}
		p, _ := newProcessor(newFakeStore(pending()), &scriptedProvider{results: []error{errPermanent}})
		p.WithObserver(o)
		_ = p.Handle(t.Context(), message(t))
		if strings.Join(o.calls, ",") != "permanent_error" || len(o.finished) != 1 || !strings.Contains(o.finished[0], "failed") {
			t.Errorf("calls = %v, finished = %v", o.calls, o.finished)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		o := &recordingObserver{}
		ctx, cancel := context.WithCancel(t.Context())
		p, _ := newProcessor(newFakeStore(pending()), providerFunc(func(context.Context, quotes.Pair) (rates.Rate, error) {
			cancel()
			return rates.Rate{}, context.Canceled
		}))
		p.WithObserver(o)
		_ = p.Handle(ctx, message(t))
		if strings.Join(o.calls, ",") != "canceled" || len(o.finished) != 0 {
			t.Errorf("calls = %v, finished = %v", o.calls, o.finished)
		}
	})
}
