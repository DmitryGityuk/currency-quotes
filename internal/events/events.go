// Package events defines the Kafka messages the service exchanges.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"uuid"
)

// Topic names. kafka-init in compose.yaml creates them.
const (
	// TopicUpdateRequested carries UpdateRequested; the key is the pair, so all
	// updates of one pair land on the same partition.
	TopicUpdateRequested = "quote.update.requested"
	// TopicUpdateRequestedDLQ receives messages the worker cannot process.
	TopicUpdateRequestedDLQ = "quote.update.requested.dlq"
)

// Header names used on Kafka messages.
const (
	// HeaderRequestID links a message to the HTTP request that caused it.
	HeaderRequestID = "request_id"
	// HeaderDLQReason explains why a message was sent to the DLQ.
	HeaderDLQReason = "dlq_reason"
)

// UpdateRequested says that the API accepted a request to update a quote.
// The worker treats the database row as the source of truth and uses the
// message only as a trigger, so the payload is deliberately small.
type UpdateRequested struct {
	UpdateID    uuid.UUID `json:"update_id"`
	Pair        string    `json:"pair"`
	RequestedAt time.Time `json:"requested_at"`
}

// Key returns the Kafka message key.
func (e UpdateRequested) Key() string { return e.Pair }

// Encode returns the JSON payload of the message.
func (e UpdateRequested) Encode() ([]byte, error) {
	return json.Marshal(e)
}

// ErrMalformed is returned by DecodeUpdateRequested for payloads that can
// never be processed; the worker sends such messages to the DLQ.
var ErrMalformed = errors.New("malformed message")

// DecodeUpdateRequested parses a payload. Unknown fields are ignored, so an
// older worker keeps working when a newer API adds fields.
func DecodeUpdateRequested(b []byte) (UpdateRequested, error) {
	var e UpdateRequested
	if err := json.Unmarshal(b, &e); err != nil {
		return UpdateRequested{}, fmt.Errorf("%w: %w", ErrMalformed, err)
	}
	if e.UpdateID == uuid.Nil() {
		return UpdateRequested{}, fmt.Errorf("%w: update_id is missing", ErrMalformed)
	}
	return e, nil
}
