//go:build integration

// Integration tests against a real PostgreSQL. Run with
//
//	TEST_DATABASE_URL=postgres://quotes:quotes@localhost:55432/quotes?sslmode=disable \
//	    go test -tags integration ./internal/postgres
//
// Every run creates its own schema, applies migrations/ to it and drops it
// at the end, so runs do not see each other's data.
package postgres

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DmitryGityuk/currency-quotes/internal/events"
	"github.com/DmitryGityuk/currency-quotes/internal/outbox"
	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
	"github.com/DmitryGityuk/currency-quotes/internal/rates"
)

var (
	eurMXN = quotes.Pair{Base: "EUR", Quote: "MXN"}
	usdMXN = quotes.Pair{Base: "USD", Quote: "MXN"}
)

// newStore returns a Store on a fresh schema with the migrations applied.
func newStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx := t.Context()
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())

	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		conn, err := pgx.Connect(context.Background(), url)
		if err == nil {
			_, _ = conn.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
			_ = conn.Close(context.Background())
		}
	})

	files, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("find migrations: %v", err)
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", filepath.Base(f), err)
		}
	}
	return New(pool), pool
}

func request(pair quotes.Pair, key string) quotes.UpdateRequest {
	return quotes.UpdateRequest{
		Pair:           pair,
		IdempotencyKey: key,
		RequestHash:    sha256.Sum256([]byte(pair.String())),
		RequestID:      "req-1",
	}
}

func outboxCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM outbox").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRequestUpdateCreatesUpdateAndOutboxMessage(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()

	res, err := s.RequestUpdate(ctx, request(eurMXN, ""))
	if err != nil {
		t.Fatalf("RequestUpdate() error = %v", err)
	}
	u := res.Update
	if u.Status != quotes.StatusPending || u.Pair != eurMXN || res.Joined || res.Replayed || u.RequestedAt.IsZero() {
		t.Errorf("result = %+v, want a new pending update", res)
	}

	var (
		topic, key string
		payload    []byte
		headers    map[string]string
	)
	err = pool.QueryRow(ctx, "SELECT topic, message_key, payload, headers FROM outbox").Scan(&topic, &key, &payload, &headers)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	ev, err := events.DecodeUpdateRequested(payload)
	if err != nil {
		t.Fatalf("decode outbox payload %s: %v", payload, err)
	}
	if topic != events.TopicUpdateRequested || key != "EUR/MXN" || ev.UpdateID != u.ID || headers[events.HeaderRequestID] != "req-1" {
		t.Errorf("outbox = %s %s %+v %v, want a message about update %s", topic, key, ev, headers, u.ID)
	}

	got, err := s.Update(ctx, u.ID)
	if err != nil || got.ID != u.ID || got.Status != quotes.StatusPending {
		t.Errorf("Update() = %+v, %v", got, err)
	}
}

func TestRequestUpdateJoinsTheUnfinishedUpdateOfThePair(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()

	first, _ := s.RequestUpdate(ctx, request(eurMXN, ""))
	second, err := s.RequestUpdate(ctx, request(eurMXN, ""))
	if err != nil {
		t.Fatalf("RequestUpdate() error = %v", err)
	}
	if !second.Joined || second.Update.ID != first.Update.ID {
		t.Errorf("second request = %+v, want it joined to %s", second, first.Update.ID)
	}
	if n := outboxCount(t, pool); n != 1 {
		t.Errorf("outbox has %d messages, want 1: a joined request sends nothing", n)
	}

	other, _ := s.RequestUpdate(ctx, request(usdMXN, ""))
	if other.Joined || other.Update.ID == first.Update.ID {
		t.Error("another pair must get its own update")
	}

	// Once the update finishes, the next request starts a new one.
	if err := s.Fail(ctx, first.Update.ID, "boom", time.Now()); err != nil {
		t.Fatal(err)
	}
	third, _ := s.RequestUpdate(ctx, request(eurMXN, ""))
	if third.Joined || third.Update.ID == first.Update.ID {
		t.Errorf("request after the update finished = %+v, want a new update", third)
	}
}

func TestRequestUpdateIdempotencyKey(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()

	first, err := s.RequestUpdate(ctx, request(eurMXN, "key-1"))
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := s.RequestUpdate(ctx, request(eurMXN, "key-1"))
	if err != nil {
		t.Fatalf("replay error = %v", err)
	}
	if !replayed.Replayed || replayed.Update.ID != first.Update.ID {
		t.Errorf("replay = %+v, want the original update", replayed)
	}

	_, err = s.RequestUpdate(ctx, request(usdMXN, "key-1"))
	if !errors.Is(err, quotes.ErrIdempotencyKeyReused) {
		t.Errorf("same key, another body: error = %v, want ErrIdempotencyKeyReused", err)
	}

	// A replay after the update finished returns its result.
	if err := s.Complete(ctx, first.Update.ID, rates.Rate{Price: "21.45", Date: time.Now()}, time.Now()); err != nil {
		t.Fatal(err)
	}
	done, _ := s.RequestUpdate(ctx, request(eurMXN, "key-1"))
	if done.Update.Status != quotes.StatusDone || done.Update.Price != "21.45" {
		t.Errorf("replay of a finished update = %+v", done.Update)
	}
}

func TestRequestUpdateUnderConcurrency(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()

	run := func(n int, req func(i int) quotes.UpdateRequest) []quotes.UpdateRequestResult {
		results := make([]quotes.UpdateRequestResult, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() { results[i], errs[i] = s.RequestUpdate(ctx, req(i)) })
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("request %d: %v", i, err)
			}
		}
		return results
	}

	t.Run("many requests for one pair", func(t *testing.T) {
		results := run(20, func(int) quotes.UpdateRequest { return request(eurMXN, "") })
		for _, r := range results {
			if r.Update.ID != results[0].Update.ID {
				t.Fatalf("got different updates %s and %s for one pair", r.Update.ID, results[0].Update.ID)
			}
		}
		if n := outboxCount(t, pool); n != 1 {
			t.Errorf("outbox has %d messages, want exactly 1", n)
		}
	})

	t.Run("many retries with one key", func(t *testing.T) {
		results := run(20, func(int) quotes.UpdateRequest { return request(usdMXN, "retry-key") })
		replays := 0
		for _, r := range results {
			if r.Update.ID != results[0].Update.ID {
				t.Fatalf("got different updates for one key")
			}
			if r.Replayed {
				replays++
			}
		}
		if replays == 0 {
			t.Error("no request was answered as a replay")
		}
	})
}

func TestWorkerFlow(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	res, _ := s.RequestUpdate(ctx, request(eurMXN, ""))
	id := res.Update.ID

	u, err := s.StartAttempt(ctx, id)
	if err != nil || u.Status != quotes.StatusProcessing || u.Attempts != 1 {
		t.Fatalf("StartAttempt() = %+v, %v; want processing, attempt 1", u, err)
	}
	u, _ = s.StartAttempt(ctx, id)
	if u.Attempts != 2 {
		t.Errorf("attempts = %d, want 2", u.Attempts)
	}

	// More digits than float64 can hold: they must survive the round trip.
	rate := rates.Rate{Price: "21.451234567890123456", Date: time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)}
	at := time.Date(2026, 11, 6, 10, 15, 0, 734000000, time.UTC)
	if err := s.Complete(ctx, id, rate, at); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}

	u, _ = s.Update(ctx, id)
	if u.Status != quotes.StatusDone || u.Price != rate.Price || !u.UpdatedAt.Equal(at) || !u.RateDate.Equal(rate.Date) {
		t.Errorf("done update = %+v", u)
	}
	q, err := s.LatestQuote(ctx, eurMXN)
	if err != nil || q.Price != rate.Price || q.UpdateID != id || !q.UpdatedAt.Equal(at) {
		t.Errorf("LatestQuote() = %+v, %v", q, err)
	}

	// A duplicate delivery changes nothing.
	u, _ = s.StartAttempt(ctx, id)
	if u.Status != quotes.StatusDone || u.Attempts != 2 {
		t.Errorf("StartAttempt on a finished update = %+v, want it unchanged", u)
	}
	if err := s.Complete(ctx, id, rates.Rate{Price: "1", Date: rate.Date}, at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.Fail(ctx, id, "late failure", at); err != nil {
		t.Fatal(err)
	}
	if u, _ = s.Update(ctx, id); u.Status != quotes.StatusDone || u.Price != rate.Price {
		t.Errorf("finished update was changed: %+v", u)
	}
}

func TestLatestQuoteDoesNotGoBackInTime(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	day := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)

	a, _ := s.RequestUpdate(ctx, request(eurMXN, ""))
	_ = s.Complete(ctx, a.Update.ID, rates.Rate{Price: "21.50", Date: day}, newer)

	// An update that obtained its price earlier finishes later.
	b, _ := s.RequestUpdate(ctx, request(eurMXN, ""))
	_ = s.Complete(ctx, b.Update.ID, rates.Rate{Price: "21.00", Date: day}, newer.Add(-time.Minute))

	q, _ := s.LatestQuote(ctx, eurMXN)
	if q.Price != "21.50" || q.UpdateID != a.Update.ID {
		t.Errorf("latest quote = %+v, want the newer price 21.50", q)
	}
	if u, _ := s.Update(ctx, b.Update.ID); u.Price != "21.00" {
		t.Errorf("the older update must still keep its own price, got %+v", u)
	}
}

func TestNotFound(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	if _, err := s.Update(ctx, uuid.NewV7()); !errors.Is(err, quotes.ErrNotFound) {
		t.Errorf("Update(unknown) error = %v", err)
	}
	if _, err := s.StartAttempt(ctx, uuid.NewV7()); !errors.Is(err, quotes.ErrNotFound) {
		t.Errorf("StartAttempt(unknown) error = %v", err)
	}
	if _, err := s.LatestQuote(ctx, eurMXN); !errors.Is(err, quotes.ErrNotFound) {
		t.Errorf("LatestQuote(no data) error = %v", err)
	}
}

func TestPublishBatch(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()
	for _, p := range []quotes.Pair{eurMXN, usdMXN, {Base: "EUR", Quote: "USD"}} {
		if _, err := s.RequestUpdate(ctx, request(p, "")); err != nil {
			t.Fatal(err)
		}
	}

	failed := errors.New("broker down")
	if _, err := s.PublishBatch(ctx, 10, func(context.Context, []outbox.Message) error { return failed }); !errors.Is(err, failed) {
		t.Fatalf("PublishBatch() error = %v, want the publish error", err)
	}
	if n := outboxCount(t, pool); n != 3 {
		t.Fatalf("outbox has %d rows after a failed publish, want 3", n)
	}

	var got []outbox.Message
	n, err := s.PublishBatch(ctx, 2, func(_ context.Context, msgs []outbox.Message) error {
		got = msgs
		return nil
	})
	if err != nil || n != 2 || len(got) != 2 {
		t.Fatalf("PublishBatch() = %d, %v; got %d messages", n, err, len(got))
	}
	if got[0].Key != "EUR/MXN" || got[1].Key != "USD/MXN" || got[0].ID > got[1].ID {
		t.Errorf("batch = %+v, want the first two rows in order", got)
	}
	if got[0].Headers[events.HeaderRequestID] != "req-1" || !strings.Contains(string(got[0].Value), "update_id") {
		t.Errorf("message = %+v, want payload and headers", got[0])
	}
	if n := outboxCount(t, pool); n != 1 {
		t.Errorf("outbox has %d rows, want 1 left", n)
	}
}

func TestPublishBatchSkipsLockedRows(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	for _, p := range []quotes.Pair{eurMXN, usdMXN} {
		_, _ = s.RequestUpdate(ctx, request(p, ""))
	}

	firstHolds := make(chan struct{})
	release := make(chan struct{})
	var firstKey string
	done := make(chan error, 1)
	go func() {
		_, err := s.PublishBatch(ctx, 1, func(_ context.Context, msgs []outbox.Message) error {
			firstKey = msgs[0].Key
			close(firstHolds)
			<-release
			return nil
		})
		done <- err
	}()
	<-firstHolds

	var secondKey string
	_, err := s.PublishBatch(ctx, 10, func(_ context.Context, msgs []outbox.Message) error {
		if len(msgs) != 1 {
			return fmt.Errorf("second relay got %d rows, want only the unlocked one", len(msgs))
		}
		secondKey = msgs[0].Key
		return nil
	})
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if firstKey == secondKey {
		t.Errorf("both relays published %s", firstKey)
	}
}

func TestOutboxStats(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()
	if n, age, err := s.OutboxStats(ctx); err != nil || n != 0 || age != 0 {
		t.Fatalf("empty outbox: %d, %v, %v", n, age, err)
	}
	_, _ = s.RequestUpdate(ctx, request(eurMXN, ""))
	_, _ = s.RequestUpdate(ctx, request(usdMXN, ""))
	if _, err := pool.Exec(ctx, "UPDATE outbox SET created_at = now() - interval '90 seconds' WHERE message_key = 'EUR/MXN'"); err != nil {
		t.Fatal(err)
	}
	n, age, err := s.OutboxStats(ctx)
	if err != nil || n != 2 || age < 89*time.Second || age > 100*time.Second {
		t.Errorf("OutboxStats() = %d, %v, %v; want 2 messages, the oldest about 90s old", n, age, err)
	}
}

func TestRequeueStale(t *testing.T) {
	s, pool := newStore(t)
	ctx := t.Context()
	usdEUR := quotes.Pair{Base: "USD", Quote: "EUR"}

	// Lost: the worker took it and crashed, and Kafka lost the message.
	res, _ := s.RequestUpdate(ctx, request(eurMXN, ""))
	lost := res.Update.ID
	if _, err := s.StartAttempt(ctx, lost); err != nil {
		t.Fatal(err)
	}
	// Finished long ago: nothing to requeue.
	res, _ = s.RequestUpdate(ctx, request(usdEUR, ""))
	done := res.Update.ID
	_, _ = s.StartAttempt(ctx, done)
	if err := s.Complete(ctx, done, rates.Rate{Price: "0.92", Date: time.Now()}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Fresh: the worker may still be on it.
	_, _ = s.RequestUpdate(ctx, request(usdMXN, ""))

	// The relay published everything, and two of the updates are old.
	if _, err := pool.Exec(ctx, "DELETE FROM outbox"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE quote_updates SET created_at = now() - interval '10 minutes' WHERE id IN ($1, $2)", lost, done); err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().Add(-time.Minute)

	n, err := s.RequeueStale(ctx, cutoff)
	if err != nil || n != 1 {
		t.Fatalf("RequeueStale() = %d, %v; want 1: only the lost update", n, err)
	}
	var payload []byte
	if err := pool.QueryRow(ctx, "SELECT payload FROM outbox").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	ev, err := events.DecodeUpdateRequested(payload)
	if err != nil || ev.UpdateID != lost || ev.Pair != "EUR/MXN" {
		t.Errorf("requeued message = %+v, %v; want update %s", ev, err, lost)
	}

	// While the new message waits in the outbox, nothing is added.
	if n, err := s.RequeueStale(ctx, cutoff); err != nil || n != 0 {
		t.Errorf("second RequeueStale() = %d, %v; want 0", n, err)
	}

	// Once it finishes, the pair takes new requests again.
	if err := s.Complete(ctx, lost, rates.Rate{Price: "21.4", Date: time.Now()}, time.Now()); err != nil {
		t.Fatal(err)
	}
	res, _ = s.RequestUpdate(ctx, request(eurMXN, ""))
	if res.Joined || res.Update.ID == lost {
		t.Error("a new request for the pair must create a new update")
	}
}

func TestDeleteIdempotencyKeys(t *testing.T) {
	s, _ := newStore(t)
	ctx := t.Context()
	_, _ = s.RequestUpdate(ctx, request(eurMXN, "old-key"))

	if n, err := s.DeleteIdempotencyKeys(ctx, time.Now().Add(-time.Hour)); err != nil || n != 0 {
		t.Errorf("deleted %d, %v; want 0: the key is fresh", n, err)
	}
	if n, err := s.DeleteIdempotencyKeys(ctx, time.Now().Add(time.Minute)); err != nil || n != 1 {
		t.Errorf("deleted %d, %v; want 1", n, err)
	}
	// The key is gone, so the same key now starts a new request.
	res, _ := s.RequestUpdate(ctx, request(eurMXN, "old-key"))
	if res.Replayed {
		t.Error("an expired key must not be replayed")
	}
}

func TestPing(t *testing.T) {
	s, _ := newStore(t)
	if err := s.Ping(t.Context()); err != nil {
		t.Errorf("Ping() error = %v", err)
	}
}

func TestNewPool(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	pool, err := NewPool(t.Context(), url, "currency-quotes-test")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var app string
	if err := pool.QueryRow(t.Context(), "SHOW application_name").Scan(&app); err != nil || app != "currency-quotes-test" {
		t.Errorf("application_name = %q, %v", app, err)
	}

	if _, err := NewPool(t.Context(), "postgres://u:s3cret@host:port/db", "x"); err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Errorf("NewPool(bad url) error = %v, want an error without the password", err)
	}
}
