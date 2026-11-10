package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"

	"github.com/DmitryGityuk/currency-quotes/api"
	"github.com/DmitryGityuk/currency-quotes/internal/config"
	"github.com/DmitryGityuk/currency-quotes/internal/httpapi"
	"github.com/DmitryGityuk/currency-quotes/internal/metrics"
	"github.com/DmitryGityuk/currency-quotes/internal/postgres"
)

// runAPI serves the HTTP API until ctx is cancelled. The API needs only
// PostgreSQL: a request is registered together with its outbox message, and
// the worker does the rest, so POST keeps working while Kafka is down.
func runAPI(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL, "currency-quotes-api")
	if err != nil {
		return err
	}
	defer pool.Close()
	store := postgres.New(pool)
	m := metrics.New()

	mux := http.NewServeMux()
	health := httpapi.NewHealth()
	health.Add("postgres", httpapi.CheckerFunc(store.Ping))
	health.Register(mux)
	mux.Handle("GET /metrics", m.Handler())
	httpapi.RegisterDocs(mux, api.OpenAPI)
	httpapi.NewQuotes(store, cfg.Currencies, logger).WithObserver(m).Register(mux)

	handler := httpapi.Chain(mux,
		httpapi.RequestID,
		httpapi.Logging(logger),
		httpapi.Recover(logger),
		httpapi.Metrics(m), // last: it needs the route pattern set by the mux
	)
	logger.Info("api started", slog.String("currencies", cfg.Currencies.String()))
	return listenAndServe(ctx, cfg, logger, handler)
}

// listenAndServe opens the listener first, so a busy port fails fast with
// a clear error, and then serves until ctx is cancelled.
func listenAndServe(ctx context.Context, cfg config.Config, logger *slog.Logger, h http.Handler) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.HTTPAddr, err)
	}

	logger.Info("http server started", "addr", ln.Addr().String())
	err = httpapi.Serve(ctx, httpapi.NewServer(h, logger), ln, cfg.ShutdownTimeout)
	logger.Info("http server stopped")
	return err
}
