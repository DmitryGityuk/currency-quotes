package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/DmitryGityuk/currency-quotes/internal/config"
)

// healthcheck asks the local instance whether it is ready. The runtime
// image is built from scratch and has no curl or wget, so the binary checks
// itself.
func healthcheck(cfg config.Config) error {
	_, port, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("parse HTTP_ADDR %q: %w", cfg.HTTPAddr, err)
	}
	url := "http://" + net.JoinHostPort("127.0.0.1", port) + "/readyz"

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: %s returned %d", url, resp.StatusCode)
	}
	return nil
}
