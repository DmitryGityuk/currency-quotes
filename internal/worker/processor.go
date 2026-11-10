// Package worker turns "update requested" messages into stored quotes.
//
// The processor knows nothing about Kafka or PostgreSQL: it works through the
// Store and rates.Provider interfaces, and the consumer decides what to do
// with the offset from the error it returns. See docs/diagrams/sequence-failures.puml.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
	"uuid"

	"github.com/DmitryGityuk/currency-quotes/internal/events"
	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
	"github.com/DmitryGityuk/currency-quotes/internal/rates"
)

// Store is what the processor needs from the database.
type Store interface {
	// StartAttempt moves an unfinished update to processing, increments its
	// attempts and returns it. A finished update is returned unchanged, so a
	// duplicate delivery is harmless. An unknown id yields quotes.ErrNotFound.
	StartAttempt(ctx context.Context, id uuid.UUID) (quotes.Update, error)
	// Complete marks the update done and stores the price as the latest quote
	// of the pair, in one transaction.
	Complete(ctx context.Context, id uuid.UUID, rate rates.Rate, at time.Time) error
	// Fail marks the update failed with a reason.
	Fail(ctx context.Context, id uuid.UUID, reason string, at time.Time) error
}

// Message is a Kafka record as the processor sees it.
type Message struct {
	Key     string
	Value   []byte
	Headers map[string]string
}

// ErrPoison marks a message that can never be processed. The consumer sends
// it to the DLQ and commits its offset instead of retrying forever.
var ErrPoison = errors.New("poison message")

// Config tunes the processor.
type Config struct {
	// MaxAttempts is how many times the provider is asked before the update
	// fails. Attempts are stored in the database, so they also count calls
	// made before a crash and redelivery.
	MaxAttempts int
	// CallTimeout bounds one provider call.
	CallTimeout time.Duration
	// Backoff is the pause between attempts.
	Backoff Backoff
}

// Observer receives what the processor does, for metrics.
type Observer interface {
	// ProviderCall reports one call to the rates provider: "ok",
	// "temporary_error", "permanent_error" or "canceled".
	ProviderCall(outcome string, took time.Duration)
	// UpdateFinished reports an update that reached done or failed;
	// sinceRequested is the time from the POST to the result.
	UpdateFinished(pair quotes.Pair, status quotes.Status, attempts int, sinceRequested time.Duration)
}

type nopObserver struct{}

func (nopObserver) ProviderCall(string, time.Duration)                            {}
func (nopObserver) UpdateFinished(quotes.Pair, quotes.Status, int, time.Duration) {}

// Processor handles UpdateRequested messages.
type Processor struct {
	store    Store
	provider rates.Provider
	logger   *slog.Logger
	cfg      Config
	observer Observer

	now   func() time.Time
	sleep func(context.Context, time.Duration) error
}

// NewProcessor returns a processor that reads rates from provider.
func NewProcessor(store Store, provider rates.Provider, logger *slog.Logger, cfg Config) *Processor {
	return &Processor{
		store:    store,
		provider: provider,
		logger:   logger,
		cfg:      cfg,
		observer: nopObserver{},
		now:      time.Now,
		sleep:    sleepCtx,
	}
}

// WithObserver makes the processor report to o and returns the processor.
func (p *Processor) WithObserver(o Observer) *Processor {
	p.observer = o
	return p
}

// Handle processes one message. A nil error means the offset can be
// committed; ErrPoison means the message goes to the DLQ; any other error
// means "not processed, try again later" (database down, shutdown).
func (p *Processor) Handle(ctx context.Context, msg Message) error {
	ev, err := events.DecodeUpdateRequested(msg.Value)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPoison, err)
	}
	logger := p.logger.With(
		slog.String("update_id", ev.UpdateID.String()),
		slog.String("pair", ev.Pair),
		slog.String("request_id", msg.Headers[events.HeaderRequestID]),
	)

	for {
		upd, err := p.store.StartAttempt(ctx, ev.UpdateID)
		switch {
		case errors.Is(err, quotes.ErrNotFound):
			return fmt.Errorf("%w: update %s does not exist", ErrPoison, ev.UpdateID)
		case err != nil:
			return fmt.Errorf("start attempt: %w", err)
		case upd.Status.Finished():
			logger.Info("update is already finished, skipping a duplicate delivery", slog.String("status", string(upd.Status)))
			return nil
		case upd.Attempts > p.cfg.MaxAttempts:
			// Earlier deliveries crashed mid-call; do not loop forever.
			return p.fail(ctx, logger, upd, fmt.Sprintf("gave up after %d attempts", p.cfg.MaxAttempts))
		}

		rate, err := p.fetch(ctx, upd.Pair)
		if err == nil {
			now := p.now()
			if err := p.store.Complete(ctx, upd.ID, rate, now); err != nil {
				return fmt.Errorf("complete update: %w", err)
			}
			p.observer.UpdateFinished(upd.Pair, quotes.StatusDone, upd.Attempts, now.Sub(upd.RequestedAt))
			logger.Info("quote updated",
				slog.String("price", string(rate.Price)),
				slog.String("rate_date", rate.Date.Format(time.DateOnly)),
				slog.Int("attempts", upd.Attempts),
			)
			return nil
		}
		if ctx.Err() != nil {
			// Shutting down: leave the update in processing; the message is
			// not committed and will be delivered again.
			return ctx.Err()
		}
		if !rates.IsTemporary(err) {
			return p.fail(ctx, logger, upd, err.Error())
		}
		if upd.Attempts >= p.cfg.MaxAttempts {
			return p.fail(ctx, logger, upd, fmt.Sprintf("%v (after %d attempts)", err, upd.Attempts))
		}

		delay := min(max(p.cfg.Backoff.Delay(upd.Attempts), rates.RetryAfter(err)), p.cfg.Backoff.Max)
		logger.Warn("rates provider call failed, retrying",
			slog.Int("attempt", upd.Attempts),
			slog.Duration("delay", delay),
			slog.Any("error", err),
		)
		if err := p.sleep(ctx, delay); err != nil {
			return err
		}
	}
}

func (p *Processor) fetch(ctx context.Context, pair quotes.Pair) (rates.Rate, error) {
	callCtx, cancel := context.WithTimeout(ctx, p.cfg.CallTimeout)
	defer cancel()
	start := p.now()
	rate, err := p.provider.Rate(callCtx, pair)

	outcome := "ok"
	switch {
	case err == nil:
	case ctx.Err() != nil:
		outcome = "canceled"
	case rates.IsTemporary(err):
		outcome = "temporary_error"
	default:
		outcome = "permanent_error"
	}
	p.observer.ProviderCall(outcome, p.now().Sub(start))
	return rate, err
}

func (p *Processor) fail(ctx context.Context, logger *slog.Logger, upd quotes.Update, reason string) error {
	now := p.now()
	if err := p.store.Fail(ctx, upd.ID, reason, now); err != nil {
		return fmt.Errorf("fail update: %w", err)
	}
	p.observer.UpdateFinished(upd.Pair, quotes.StatusFailed, upd.Attempts, now.Sub(upd.RequestedAt))
	logger.Warn("quote update failed", slog.String("reason", reason), slog.Int("attempts", upd.Attempts))
	return nil
}

// Backoff computes pauses between retries.
type Backoff struct {
	Base time.Duration // pause before the second attempt
	Max  time.Duration // upper bound for any pause
}

// Delay returns the pause after attempt number n (1-based): Base·2^(n-1),
// capped at Max, with "equal jitter" — half fixed, half random — so workers
// that failed together do not retry in lockstep.
func (b Backoff) Delay(n int) time.Duration {
	d := b.Max
	if n >= 1 && n < 32 {
		if exp := b.Base << (n - 1); exp > 0 && exp < b.Max {
			d = exp
		}
	}
	half := d / 2
	return half + rand.N(half+1) //nolint:gosec // jitter, not security
}

// sleepCtx waits for d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
