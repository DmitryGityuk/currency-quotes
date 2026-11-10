package quotes

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"uuid"
)

// Status is the state of a quote update. See docs/diagrams/state-update.puml.
type Status string

const (
	// StatusPending: the request is accepted and waits for the worker.
	StatusPending Status = "pending"
	// StatusProcessing: the worker is fetching the rate.
	StatusProcessing Status = "processing"
	// StatusDone: the rate is stored; the update has a price.
	StatusDone Status = "done"
	// StatusFailed: the rate could not be fetched; the update has a reason.
	StatusFailed Status = "failed"
)

// Finished reports whether s is a final status that never changes again.
func (s Status) Finished() bool { return s == StatusDone || s == StatusFailed }

// Price is a positive decimal number kept as text, such as "21.4512".
//
// It never passes through float64: the provider's JSON number is read as
// text, stored in a numeric column and sent to clients as a string, so no
// digit is ever rounded away.
type Price string

// ErrInvalidPrice is returned by ParsePrice.
var ErrInvalidPrice = errors.New("price must be a positive decimal number")

// ParsePrice validates s as a positive decimal without sign or exponent.
func ParsePrice(s string) (Price, error) {
	intPart, frac, hasDot := strings.Cut(s, ".")
	if intPart == "" || !digitsOnly(intPart) || (hasDot && (frac == "" || !digitsOnly(frac))) {
		return "", fmt.Errorf("%w, got %q", ErrInvalidPrice, s)
	}
	if strings.Trim(intPart+frac, "0") == "" {
		return "", fmt.Errorf("%w, got %q", ErrInvalidPrice, s)
	}
	return Price(s), nil
}

func digitsOnly(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// Update is one request to refresh the quote of a pair, together with its
// result once it is finished.
type Update struct {
	ID          uuid.UUID
	Pair        Pair
	Status      Status
	Attempts    int       // times the worker asked the provider
	RequestedAt time.Time // when the API accepted the request

	// Set only when Status is StatusDone.
	Price     Price
	RateDate  time.Time // the day the provider published the rate
	UpdatedAt time.Time // when the service obtained the rate

	// Set only when Status is StatusFailed.
	Error string
}

// Quote is the latest known price of a pair.
type Quote struct {
	Pair      Pair
	Price     Price
	RateDate  time.Time
	UpdatedAt time.Time
	UpdateID  uuid.UUID // the update that obtained this price
}

// Errors returned by stores.
var (
	ErrNotFound             = errors.New("not found")
	ErrIdempotencyKeyReused = errors.New("idempotency key was already used with a different request")
)

// UpdateRequest is what the API asks the store to register.
type UpdateRequest struct {
	Pair Pair
	// IdempotencyKey is the Idempotency-Key header, or "" if there was none.
	IdempotencyKey string
	// RequestHash is the SHA-256 of the normalized request body; a repeated
	// key with a different hash is rejected with ErrIdempotencyKeyReused.
	RequestHash [32]byte
	// RequestID goes into the Kafka message headers, for tracing.
	RequestID string
}

// UpdateRequestResult says which update the request ended up with.
type UpdateRequestResult struct {
	Update Update
	// Replayed is true when the Idempotency-Key was seen before and the
	// original update was returned.
	Replayed bool
	// Joined is true when the pair already had an unfinished update and the
	// request was attached to it instead of starting a new one.
	Joined bool
}
