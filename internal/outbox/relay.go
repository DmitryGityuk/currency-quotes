// Package outbox moves messages from the outbox table to the message broker.
//
// The API writes a message into the outbox in the same transaction as the
// update it describes. The relay publishes those rows and deletes them only
// after the broker has acknowledged them, so a message is never lost; it can
// be published twice (if the process dies between the ack and the delete),
// which the idempotent consumer tolerates.
package outbox

import (
	"context"
	"log/slog"
	"time"
)

// Message is one outbox row.
type Message struct {
	ID      int64
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

// Store gives the relay exclusive access to a batch of outbox rows.
type Store interface {
	// PublishBatch locks up to limit rows that no other relay holds, calls
	// publish with them and deletes them if publish succeeds, in one
	// transaction. It returns how many rows were published.
	PublishBatch(ctx context.Context, limit int, publish func(context.Context, []Message) error) (int, error)
}

// Publisher sends messages to the broker and returns once they are
// acknowledged.
type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// Observer receives what the relay does, for metrics.
type Observer interface {
	// Published reports messages that reached the broker.
	Published(n int)
	// PublishFailed reports a batch that stayed in the outbox.
	PublishFailed()
}

type nopObserver struct{}

func (nopObserver) Published(int)  {}
func (nopObserver) PublishFailed() {}

// Relay polls the outbox and publishes what it finds.
type Relay struct {
	store     Store
	publisher Publisher
	logger    *slog.Logger
	observer  Observer
	interval  time.Duration
	batchSize int
	maxPause  time.Duration
	// publishTimeout bounds one publish. The rows stay locked in an open
	// transaction while the broker answers, so a broker that accepts
	// connections but never replies must not hold them forever.
	publishTimeout time.Duration
}

// NewRelay returns a relay that polls every interval and publishes up to
// batchSize rows at a time.
func NewRelay(store Store, publisher Publisher, logger *slog.Logger, interval time.Duration, batchSize int) *Relay {
	return &Relay{
		store:     store,
		publisher: publisher,
		logger:    logger,
		observer:  nopObserver{},
		interval:  interval,
		batchSize: batchSize,
		maxPause:  5 * time.Second,

		publishTimeout: 10 * time.Second,
	}
}

// WithObserver makes the relay report to o and returns the relay.
func (r *Relay) WithObserver(o Observer) *Relay {
	r.observer = o
	return r
}

// Run publishes the outbox until ctx is cancelled. A full batch means more
// rows are probably waiting, so the next batch goes immediately; an error
// (broker or database down) doubles the pause, up to maxPause, so a long
// outage does not flood the logs.
func (r *Relay) Run(ctx context.Context) error {
	pause := r.interval
	for {
		n, err := r.store.PublishBatch(ctx, r.batchSize, r.publish)
		switch {
		case ctx.Err() != nil:
			return nil //nolint:nilerr // shutdown is a normal exit, not a failure
		case err != nil:
			r.observer.PublishFailed()
			pause = min(pause*2, r.maxPause)
			r.logger.WarnContext(ctx, "outbox relay failed, rows stay in the outbox",
				slog.Any("error", err), slog.Duration("retry_in", pause))
		case n > 0:
			r.observer.Published(n)
			pause = r.interval
			r.logger.DebugContext(ctx, "outbox published", slog.Int("messages", n))
			if n == r.batchSize {
				continue
			}
		default:
			pause = r.interval
		}

		t := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

func (r *Relay) publish(ctx context.Context, msgs []Message) error {
	ctx, cancel := context.WithTimeout(ctx, r.publishTimeout)
	defer cancel()
	return r.publisher.Publish(ctx, msgs)
}
