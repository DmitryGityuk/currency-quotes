package kafka

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DmitryGityuk/currency-quotes/internal/events"
	"github.com/DmitryGityuk/currency-quotes/internal/outbox"
	"github.com/DmitryGityuk/currency-quotes/internal/worker"
)

// The tests run against kfake, an in-process Kafka implementation from
// franz-go: real protocol, real consumer groups, no Docker.

func newCluster(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(3, events.TopicUpdateRequested),
		kfake.SeedTopics(1, events.TopicUpdateRequestedDLQ),
	)
	if err != nil {
		t.Fatalf("start kfake: %v", err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func newWorkerClient(t *testing.T, brokers []string) *kgo.Client {
	t.Helper()
	cl, err := NewClient(brokers, "test-group", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

// readAll returns the records of topic, reading from the start without a group.
func readAll(t *testing.T, brokers []string, topic string, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer Close(t.Context(), cl, time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var got []*kgo.Record
	for len(got) < want {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records from %s", len(got), want, topic)
		}
		got = append(got, fs.Records()...)
	}
	return got
}

func produce(t *testing.T, brokers []string, values ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer Close(t.Context(), cl, time.Second)
	for _, v := range values {
		// One key, so all records land on one partition in this order.
		r := &kgo.Record{Topic: events.TopicUpdateRequested, Key: []byte("EUR/MXN"), Value: []byte(v)}
		if err := cl.ProduceSync(t.Context(), r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProducerPublish(t *testing.T) {
	brokers := newCluster(t)
	cl := newWorkerClient(t, brokers)
	defer Close(t.Context(), cl, time.Second)

	err := NewProducer(cl).Publish(t.Context(), []outbox.Message{
		{ID: 1, Topic: events.TopicUpdateRequested, Key: "EUR/MXN", Value: []byte(`{"n":1}`),
			Headers: map[string]string{events.HeaderRequestID: "req-1", "b": "2"}},
		{ID: 2, Topic: events.TopicUpdateRequested, Key: "EUR/MXN", Value: []byte(`{"n":2}`)},
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	got := readAll(t, brokers, events.TopicUpdateRequested, 2)
	if string(got[0].Key) != "EUR/MXN" || string(got[0].Value) != `{"n":1}` || string(got[1].Value) != `{"n":2}` {
		t.Errorf("records = %s %s, want both messages in order", got[0].Value, got[1].Value)
	}
	if h := headerMap(got[0].Headers); h[events.HeaderRequestID] != "req-1" || h["b"] != "2" {
		t.Errorf("headers = %v", h)
	}
	if got[0].Headers[0].Key != "b" {
		t.Error("headers must be sorted by key so records are reproducible")
	}
}

// scriptedHandler fails some values the first times it sees them.
type scriptedHandler struct {
	mu       sync.Mutex
	seen     []string
	failures map[string][]error // value -> errors to return before succeeding
}

func (h *scriptedHandler) handle(_ context.Context, msg worker.Message) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	v := string(msg.Value)
	h.seen = append(h.seen, v)
	if errs := h.failures[v]; len(errs) > 0 {
		h.failures[v] = errs[1:]
		return errs[0]
	}
	return nil
}

func (h *scriptedHandler) count(v string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, s := range h.seen {
		if s == v {
			n++
		}
	}
	return n
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runConsumer(t *testing.T, c *Consumer) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run() did not return after cancel")
		}
	}
}

func TestConsumerCommitsRetriesAndSendsPoisonToDLQ(t *testing.T) {
	brokers := newCluster(t)
	produce(t, brokers, "ok-1", "poison", "flaky", "ok-2")

	h := &scriptedHandler{failures: map[string][]error{
		"poison": {errors.Join(worker.ErrPoison, errors.New("malformed"))},
		"flaky":  {errors.New("database down"), errors.New("database down")},
	}}
	cl := newWorkerClient(t, brokers)
	o := &recordingObserver{counts: map[string]int{}}
	c := NewConsumer(cl, h.handle, slog.New(slog.DiscardHandler)).WithObserver(o)
	c.retryDelay = 10 * time.Millisecond
	stop := runConsumer(t, c)

	eventually(t, func() bool { return h.count("ok-2") == 1 })
	stop()
	Close(t.Context(), cl, time.Second)

	if got := o.snapshot(); got["processed"] != 3 || got["dlq"] != 1 || got["retried"] != 2 {
		t.Errorf("observed %v, want 3 processed, 1 dlq, 2 retried", got)
	}

	if h.count("flaky") != 3 {
		t.Errorf("flaky was handled %d times, want 3: two failures, then success", h.count("flaky"))
	}
	if h.count("ok-1") != 1 || h.count("poison") != 1 {
		t.Errorf("seen = %v, want every other record once", h.seen)
	}

	dlq := readAll(t, brokers, events.TopicUpdateRequestedDLQ, 1)
	headers := headerMap(dlq[0].Headers)
	if string(dlq[0].Value) != "poison" || headers[events.HeaderDLQReason] == "" || headers["source_topic"] != events.TopicUpdateRequested {
		t.Errorf("DLQ record = %s %v, want the poison message with the reason and its origin", dlq[0].Value, headers)
	}

	// Everything was committed: a new member of the group starts after
	// "ok-2" and sees only the record produced now.
	produce(t, brokers, "after")
	h2 := &scriptedHandler{}
	cl2 := newWorkerClient(t, brokers)
	stop2 := runConsumer(t, NewConsumer(cl2, h2.handle, slog.New(slog.DiscardHandler)))
	eventually(t, func() bool { return h2.count("after") == 1 })
	stop2()
	Close(t.Context(), cl2, time.Second)
	if len(h2.seen) != 1 {
		t.Errorf("second consumer saw %v, want only the new record", h2.seen)
	}
}

func TestConsumerDoesNotCommitOnShutdown(t *testing.T) {
	brokers := newCluster(t)
	produce(t, brokers, "slow")

	started := make(chan struct{})
	var once sync.Once
	cl := newWorkerClient(t, brokers)
	c := NewConsumer(cl, func(ctx context.Context, _ worker.Message) error {
		once.Do(func() { close(started) })
		<-ctx.Done() // the worker is stopped in the middle of processing
		return ctx.Err()
	}, slog.New(slog.DiscardHandler))
	stop := runConsumer(t, c)
	<-started
	stop()
	Close(t.Context(), cl, time.Second)

	// The record was not committed, so the group gets it again.
	h := &scriptedHandler{}
	cl2 := newWorkerClient(t, brokers)
	stop2 := runConsumer(t, NewConsumer(cl2, h.handle, slog.New(slog.DiscardHandler)))
	eventually(t, func() bool { return h.count("slow") == 1 })
	stop2()
	Close(t.Context(), cl2, time.Second)
}

// countingHandler counts log records by message.
type countingHandler struct {
	mu     sync.Mutex
	counts map[string]int
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *countingHandler) WithGroup(string) slog.Handler            { return h }
func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counts[r.Message]++
	return nil
}

func (h *countingHandler) count(msg string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counts[msg]
}

func freePort(t *testing.T) int {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// Regression: when the broker came back with recreated topics, the consumer
// spun without pausing and wrote over a gigabyte of logs in two minutes.
func TestConsumerBacksOffAndExitsWhenTheTopicIsRecreated(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the topic grace period; skipped with -short")
	}
	port := freePort(t)
	start := func() *kfake.Cluster {
		c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.Ports(port),
			kfake.SeedTopics(3, events.TopicUpdateRequested), kfake.SeedTopics(1, events.TopicUpdateRequestedDLQ))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	first := start()
	brokers := first.ListenAddrs()
	produce(t, brokers, "before")

	logs := &countingHandler{counts: map[string]int{}}
	h := &scriptedHandler{}
	cl, err := NewClient(brokers, "test-group", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer Close(t.Context(), cl, time.Second)
	c := NewConsumer(cl, h.handle, slog.New(logs))
	c.topicGrace = 2 * time.Second

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	eventually(t, func() bool { return h.count("before") == 1 })

	// The broker restarts and loses its state: same topic names, new ids.
	first.Close()
	second := start()
	defer second.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrTopicRecreated) {
			t.Fatalf("Run() error = %v, want ErrTopicRecreated", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Run() did not give up on the recreated topic")
	}
	// With pauses of 100ms, 200ms, 400ms, ... a few seconds produce a
	// handful of lines, not tens of thousands.
	if n := logs.count("kafka fetch failed"); n == 0 || n > 20 {
		t.Errorf("logged %d fetch failures, want a few", n)
	}
}

func TestCheckTopicIDs(t *testing.T) {
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	c := &Consumer{topicGrace: 30 * time.Second, now: func() time.Time { return now }}
	unknown := []kgo.FetchError{{Topic: "t", Err: kerr.UnknownTopicID}}
	other := []kgo.FetchError{{Topic: "t", Err: errors.New("connection refused")}}

	if err := c.checkTopicIDs(unknown); err != nil {
		t.Fatalf("first UNKNOWN_TOPIC_ID: error = %v, want a grace period", err)
	}
	now = now.Add(10 * time.Second)
	if err := c.checkTopicIDs(unknown); err != nil {
		t.Fatalf("within the grace period: error = %v", err)
	}
	if err := c.checkTopicIDs(other); err != nil || !c.unknownTopicAt.IsZero() {
		t.Fatalf("another error must reset the timer, got %v", err)
	}
	_ = c.checkTopicIDs(unknown)
	now = now.Add(31 * time.Second)
	if err := c.checkTopicIDs(unknown); !errors.Is(err, ErrTopicRecreated) {
		t.Errorf("after the grace period: error = %v, want ErrTopicRecreated", err)
	}
}

func TestSlogLogger(t *testing.T) {
	l := slogLogger{slog.New(slog.DiscardHandler)}
	if l.Level() != kgo.LogLevelWarn {
		t.Error("franz-go should only log warnings and errors")
	}
	l.Log(kgo.LogLevelError, "boom", "broker", 1)
	l.Log(kgo.LogLevelWarn, "hmm")
}

type recordingObserver struct {
	mu     sync.Mutex
	counts map[string]int
}

func (o *recordingObserver) MessageHandled(outcome string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.counts[outcome]++
}

func (o *recordingObserver) snapshot() map[string]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return maps.Clone(o.counts)
}
