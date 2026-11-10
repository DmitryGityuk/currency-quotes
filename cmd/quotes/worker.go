package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/DmitryGityuk/currency-quotes/internal/config"
	"github.com/DmitryGityuk/currency-quotes/internal/httpapi"
	"github.com/DmitryGityuk/currency-quotes/internal/kafka"
	"github.com/DmitryGityuk/currency-quotes/internal/metrics"
	"github.com/DmitryGityuk/currency-quotes/internal/outbox"
	"github.com/DmitryGityuk/currency-quotes/internal/postgres"
	"github.com/DmitryGityuk/currency-quotes/internal/rates"
	"github.com/DmitryGityuk/currency-quotes/internal/worker"
)

// runWorker runs the background part of the service until ctx is cancelled:
//
//   - the outbox relay publishes accepted requests to Kafka;
//   - the consumer fetches the rate for each request and stores it;
//   - a janitor deletes expired idempotency keys;
//   - a sampler records the outbox backlog for metrics;
//   - an HTTP server answers the health probes and serves /metrics.
//
// If one of them fails, errgroup cancels the others and the process exits,
// so Docker or Kubernetes restarts it instead of leaving it half alive.
func runWorker(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL, "currency-quotes-worker")
	if err != nil {
		return err
	}
	defer pool.Close()
	store := postgres.New(pool)

	cl, err := kafka.NewClient(cfg.KafkaBrokers, cfg.KafkaGroup, logger)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	// Deferred after pool.Close, so it runs first: leave the group before
	// the database connections go away, without hanging on a dead broker.
	defer kafka.Close(ctx, cl, 5*time.Second)

	m := metrics.New()
	processor := worker.NewProcessor(store, newProvider(cfg), logger, worker.Config{
		MaxAttempts: cfg.MaxAttempts,
		CallTimeout: cfg.ProviderTimeout,
		Backoff:     worker.Backoff{Base: cfg.RetryBaseDelay, Max: cfg.RetryMaxDelay},
	}).WithObserver(m)
	relay := outbox.NewRelay(store, kafka.NewProducer(cl), logger, cfg.RelayInterval, cfg.RelayBatchSize).WithObserver(m)
	consumer := kafka.NewConsumer(cl, processor.Handle, logger).WithObserver(m)

	mux := http.NewServeMux()
	health := httpapi.NewHealth()
	health.Add("postgres", httpapi.CheckerFunc(store.Ping))
	health.Add("kafka", httpapi.CheckerFunc(cl.Ping))
	health.Register(mux)
	mux.Handle("GET /metrics", m.Handler())

	logger.Info("worker started",
		slog.String("rates_provider", cfg.RatesProvider),
		slog.String("kafka_group", cfg.KafkaGroup),
		slog.Any("kafka_brokers", cfg.KafkaBrokers),
	)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return relay.Run(ctx) })
	g.Go(func() error { return consumer.Run(ctx) })
	g.Go(func() error { return deleteExpiredKeys(ctx, store, cfg.IdempotencyKeyTTL, logger) })
	g.Go(func() error { return sampleOutbox(ctx, store, m, logger) })
	g.Go(func() error {
		return listenAndServe(ctx, cfg, logger, httpapi.Chain(mux, httpapi.Recover(logger), httpapi.Metrics(m)))
	})
	return g.Wait()
}

// newProvider returns the configured rates provider.
func newProvider(cfg config.Config) rates.Provider {
	if cfg.RatesProvider == config.ProviderFake {
		return &rates.Fake{Delay: cfg.FakeDelay, FailureRate: cfg.FakeFailureRate}
	}
	return rates.NewFrankfurter(cfg.FrankfurterURL, &http.Client{Timeout: cfg.ProviderTimeout})
}

// deleteExpiredKeys removes idempotency keys older than ttl, at start and
// then every hour. Several workers may do it at once; that is harmless.
func deleteExpiredKeys(ctx context.Context, store *postgres.Store, ttl time.Duration, logger *slog.Logger) error {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		n, err := store.DeleteIdempotencyKeys(ctx, time.Now().Add(-ttl))
		switch {
		case err != nil && ctx.Err() == nil:
			logger.WarnContext(ctx, "delete expired idempotency keys", slog.Any("error", err))
		case n > 0:
			logger.InfoContext(ctx, "deleted expired idempotency keys", slog.Int64("count", n))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// sampleOutbox records the outbox backlog every few seconds. A growing
// backlog is the first sign that Kafka or the relay is down, while POST still
// answers 202 and nothing else looks wrong.
func sampleOutbox(ctx context.Context, store *postgres.Store, m *metrics.Metrics, logger *slog.Logger) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		n, oldest, err := store.OutboxStats(ctx)
		switch {
		case err == nil:
			m.SetOutbox(n, oldest)
		case ctx.Err() == nil:
			logger.DebugContext(ctx, "sample outbox", slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
