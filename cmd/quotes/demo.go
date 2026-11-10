package main

import (
	"context"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/DmitryGityuk/currency-quotes/internal/demo"
)

// runDemo walks through the assignment against the API at DEMO_API_URL
// (http://localhost:8080 by default) and prints what happens.
func runDemo(ctx context.Context) error {
	url := os.Getenv("DEMO_API_URL")
	if url == "" {
		url = "http://localhost:8080"
	}
	r := &demo.Runner{
		BaseURL:   strings.TrimRight(url, "/"),
		Client:    &http.Client{Timeout: 10 * time.Second},
		Out:       os.Stdout,
		PollEvery: 200 * time.Millisecond,
		// Enough for the real provider with a couple of retries.
		Timeout: time.Minute,
	}
	return r.Run(ctx)
}
