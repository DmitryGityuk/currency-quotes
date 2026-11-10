// Package rates fetches exchange rates from external providers.
package rates

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
)

// Rate is a price published by a provider.
type Rate struct {
	Price quotes.Price
	// Date is the day the provider published the rate. Central banks
	// publish once per working day, so this is often yesterday.
	Date time.Time
}

// Provider returns the current rate of a pair.
type Provider interface {
	Rate(ctx context.Context, pair quotes.Pair) (Rate, error)
}

// Error is a failed provider call. Temporary errors (timeouts, 5xx, 429)
// are worth retrying; the others (unknown currency, a response the client
// cannot parse) will fail the same way again.
type Error struct {
	Provider  string
	Temporary bool
	// RetryAfter is the provider's own hint from a Retry-After header.
	RetryAfter time.Duration
	Err        error
}

func (e *Error) Error() string { return e.Provider + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// IsTemporary reports whether retrying the call that returned err may help.
func IsTemporary(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false // we are shutting down; the message will be redelivered
	}
	var pe *Error
	if errors.As(err, &pe) {
		return pe.Temporary
	}
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, context.DeadlineExceeded)
}

// RetryAfter returns the provider's Retry-After hint carried by err, or 0.
func RetryAfter(err error) time.Duration {
	var pe *Error
	if errors.As(err, &pe) {
		return pe.RetryAfter
	}
	return 0
}

func temporary(provider string, retryAfter time.Duration, format string, args ...any) error {
	return &Error{Provider: provider, Temporary: true, RetryAfter: retryAfter, Err: fmt.Errorf(format, args...)}
}

func permanent(provider, format string, args ...any) error {
	return &Error{Provider: provider, Err: fmt.Errorf(format, args...)}
}
