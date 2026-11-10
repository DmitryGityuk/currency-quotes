// Command quotes is the currency quotes service.
//
// One binary runs in several modes, so the API and the worker can be
// deployed and scaled independently:
//
//	quotes api          HTTP API; registers update requests in PostgreSQL
//	quotes worker       outbox relay and Kafka consumer; fetches and stores rates
//	quotes healthcheck  probes /readyz of the local instance; used by Docker
//	quotes demo         walks through the assignment against a running API
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/DmitryGityuk/currency-quotes/internal/config"
)

const usage = `usage: quotes <command>

commands:
  api          run the HTTP API
  worker       run the background worker
  healthcheck  exit 0 if the local instance is ready, 1 otherwise
  demo         run the assignment scenario against DEMO_API_URL`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 {
		return errors.New(usage)
	}

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if args[0] == "healthcheck" {
		return healthcheck(cfg)
	}

	logger := newLogger(cfg)

	// Docker sends SIGTERM on stop; Ctrl+C sends SIGINT. Either one cancels
	// ctx, and every component shuts down from there.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "api":
		return runAPI(ctx, cfg, logger.With("mode", "api"))
	case "worker":
		return runWorker(ctx, cfg, logger.With("mode", "worker"))
	case "demo":
		return runDemo(ctx)
	default:
		return fmt.Errorf("unknown command %q\n\n%s", args[0], usage)
	}
}

func newLogger(cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	var h slog.Handler = slog.NewJSONHandler(os.Stdout, opts)
	if cfg.LogFormat == config.LogFormatText {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(h).With("service", "currency-quotes")
}
