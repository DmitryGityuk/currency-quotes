package rates

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
)

// DefaultFrankfurterURL is the public Frankfurter instance. It needs no API
// key and publishes daily rates of central banks, including USD, EUR and MXN.
const DefaultFrankfurterURL = "https://api.frankfurter.dev"

// maxResponseSize caps how much of a response body is read.
const maxResponseSize = 64 << 10

// Frankfurter is a client of the Frankfurter API v2.
type Frankfurter struct {
	baseURL string
	client  *http.Client
}

// NewFrankfurter returns a client for the instance at baseURL. The client's
// timeout bounds each call; the caller's context can cut it shorter.
func NewFrankfurter(baseURL string, client *http.Client) *Frankfurter {
	return &Frankfurter{baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

// rateResponse is the body of GET /v2/rate/{base}/{quote}, for example
// {"date": "2026-11-05", "base": "EUR", "quote": "MXN", "rate": 21.4512}.
type rateResponse struct {
	Date  string      `json:"date"`
	Base  string      `json:"base"`
	Quote string      `json:"quote"`
	Rate  json.Number `json:"rate"` // read as text: no float64 rounding
}

// Rate implements Provider.
func (f *Frankfurter) Rate(ctx context.Context, pair quotes.Pair) (Rate, error) {
	const name = "frankfurter"
	endpoint := f.baseURL + "/v2/rate/" + url.PathEscape(string(pair.Base)) + "/" + url.PathEscape(string(pair.Quote))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return Rate{}, permanent(name, "build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "currency-quotes/1.0 (+https://github.com/DmitryGityuk/currency-quotes)")

	resp, err := f.client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return Rate{}, err
		}
		return Rate{}, temporary(name, 0, "GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return Rate{}, temporary(name, 0, "read response: %w", err)
	}

	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError:
		return Rate{}, temporary(name, parseRetryAfter(resp.Header.Get("Retry-After")),
			"GET %s: status %d: %s", endpoint, resp.StatusCode, snippet(body))
	default:
		// 404, 422 for an unknown currency and so on: retrying will not help.
		return Rate{}, permanent(name, "GET %s: status %d: %s", endpoint, resp.StatusCode, snippet(body))
	}

	var r rateResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return Rate{}, permanent(name, "decode response: %w", err)
	}
	if !strings.EqualFold(r.Base, string(pair.Base)) || !strings.EqualFold(r.Quote, string(pair.Quote)) {
		return Rate{}, permanent(name, "asked for %s, got %s/%s", pair, r.Base, r.Quote)
	}
	price, err := quotes.ParsePrice(r.Rate.String())
	if err != nil {
		return Rate{}, permanent(name, "rate: %w", err)
	}
	date, err := time.Parse(time.DateOnly, r.Date)
	if err != nil {
		return Rate{}, permanent(name, "date: %w", err)
	}
	return Rate{Price: price, Date: date}, nil
}

// parseRetryAfter understands the delay-seconds form of Retry-After, which is
// what rate limiters send; an HTTP date or garbage yields 0.
func parseRetryAfter(v string) time.Duration {
	secs, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || secs <= 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}

// snippet shortens a response body for an error message.
func snippet(b []byte) string {
	const limit = 200
	s := strings.TrimSpace(string(b))
	if len(s) > limit {
		s = s[:limit] + "…"
	}
	return fmt.Sprintf("%q", s)
}
