// Package httpapi is the HTTP layer of the service: server setup,
// middleware, health probes, docs and the quotes handlers.
package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// NewServer returns an http.Server with timeouts. The zero http.Server has
// none, so a slow client could hold a connection open forever. Errors that
// net/http logs itself (bad requests, handshake failures) go to logger
// instead of plain text on stderr.
func NewServer(h http.Handler, logger *slog.Logger) *http.Server {
	return &http.Server{
		Handler:           h,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

// Serve runs srv on ln until ctx is cancelled, then shuts down gracefully:
// it stops accepting new connections and waits up to shutdownTimeout for
// in-flight requests to finish.
func Serve(ctx context.Context, srv *http.Server, ln net.Listener, shutdownTimeout time.Duration) error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case err := <-errCh:
		// Serve only returns before Shutdown on a real failure.
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	// ctx is already cancelled, so the shutdown deadline needs a context
	// that keeps ctx's values but not its cancellation.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http server shutdown: %w", err)
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}
