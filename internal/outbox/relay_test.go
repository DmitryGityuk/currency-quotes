package outbox

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memStore is an outbox in memory with the same contract as the database.
type memStore struct {
	mu   sync.Mutex
	rows []Message
	err  error // returned by the store itself
}

func (s *memStore) PublishBatch(ctx context.Context, limit int, publish func(context.Context, []Message) error) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return 0, s.err
	}
	n := min(limit, len(s.rows))
	if n == 0 {
		return 0, nil
	}
	batch := append([]Message(nil), s.rows[:n]...)
	if err := publish(ctx, batch); err != nil {
		return 0, err // "rollback": the rows stay
	}
	s.rows = s.rows[n:]
	return n, nil
}

func (s *memStore) left() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

type memPublisher struct {
	mu       sync.Mutex
	got      []Message
	failures int // the first calls fail
	calls    int
}

func (p *memPublisher) Publish(_ context.Context, msgs []Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.calls <= p.failures {
		return errors.New("broker unavailable")
	}
	p.got = append(p.got, msgs...)
	return nil
}

func (p *memPublisher) published() []Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Message(nil), p.got...)
}

func rows(n int) []Message {
	out := make([]Message, n)
	for i := range out {
		out[i] = Message{ID: int64(i + 1), Topic: "t", Key: "EUR/MXN", Value: []byte("{}")}
	}
	return out
}

// eventually waits until cond holds or fails the test.
func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func runRelay(t *testing.T, r *Relay) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run() error = %v, want nil on shutdown", err)
			}
		case <-time.After(time.Second):
			t.Error("Run() did not return after cancel")
		}
	}
}

func TestRelayPublishesInOrderAndDrainsBacklog(t *testing.T) {
	store := &memStore{rows: rows(25)}
	pub := &memPublisher{}
	// A long interval: draining 25 rows in batches of 10 must not wait for it.
	stop := runRelay(t, NewRelay(store, pub, slog.New(slog.DiscardHandler), time.Hour, 10))
	defer stop()

	eventually(t, func() bool { return store.left() == 0 })
	got := pub.published()
	for i, m := range got {
		if m.ID != int64(i+1) {
			t.Fatalf("message %d has id %d, want the outbox order", i, m.ID)
		}
	}
}

func TestRelayKeepsRowsWhileTheBrokerIsDown(t *testing.T) {
	store := &memStore{rows: rows(3)}
	pub := &memPublisher{failures: 2}
	r := NewRelay(store, pub, slog.New(slog.DiscardHandler), time.Millisecond, 10)
	r.maxPause = 4 * time.Millisecond
	stop := runRelay(t, r)
	defer stop()

	eventually(t, func() bool { return store.left() == 0 })
	if got := len(pub.published()); got != 3 {
		t.Errorf("published %d messages, want all 3 once the broker is back", got)
	}
}

func TestRelaySurvivesStoreErrors(t *testing.T) {
	store := &memStore{rows: rows(1), err: errors.New("database down")}
	r := NewRelay(store, &memPublisher{}, slog.New(slog.DiscardHandler), time.Millisecond, 10)
	r.maxPause = 2 * time.Millisecond
	stop := runRelay(t, r)
	time.Sleep(20 * time.Millisecond)
	stop()
	if store.left() != 1 {
		t.Error("rows must stay in the outbox while the store fails")
	}
}

func TestRelayStopsDuringPause(t *testing.T) {
	stop := runRelay(t, NewRelay(&memStore{}, &memPublisher{}, slog.New(slog.DiscardHandler), time.Hour, 10))
	stop() // must return promptly, not after an hour
}

type recordingObserver struct {
	mu        sync.Mutex
	published int
	failures  int
}

func (o *recordingObserver) Published(n int) { o.mu.Lock(); o.published += n; o.mu.Unlock() }
func (o *recordingObserver) PublishFailed()  { o.mu.Lock(); o.failures++; o.mu.Unlock() }

func (o *recordingObserver) counts() (int, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.published, o.failures
}

func TestRelayReportsToObserver(t *testing.T) {
	store := &memStore{rows: rows(3)}
	o := &recordingObserver{}
	r := NewRelay(store, &memPublisher{failures: 1}, slog.New(slog.DiscardHandler), time.Millisecond, 10).WithObserver(o)
	r.maxPause = 2 * time.Millisecond
	stop := runRelay(t, r)
	eventually(t, func() bool { published, _ := o.counts(); return published == 3 })
	stop()
	if _, failures := o.counts(); failures != 1 {
		t.Errorf("failures = %d, want 1", failures)
	}
}

// hangingPublisher is a broker that accepts the connection and never answers.
type hangingPublisher struct{ calls atomic.Int32 }

func (p *hangingPublisher) Publish(ctx context.Context, _ []Message) error {
	p.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func TestRelayGivesUpOnAHangingBroker(t *testing.T) {
	store := &memStore{rows: rows(1)}
	pub := &hangingPublisher{}
	r := NewRelay(store, pub, slog.New(slog.DiscardHandler), time.Millisecond, 10)
	r.publishTimeout, r.maxPause = 20*time.Millisecond, 5*time.Millisecond
	stop := runRelay(t, r)
	// Several attempts in a short time mean each one was cut off.
	eventually(t, func() bool { return pub.calls.Load() >= 3 })
	stop()
	if store.left() != 1 {
		t.Error("the row must stay in the outbox when publishing times out")
	}
}
