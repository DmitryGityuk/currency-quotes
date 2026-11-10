package httpapi

import (
	"encoding/json"
	"net/http"
)

// writeJSON writes v as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The status line is already sent, so an encoding error (usually a
	// client that went away) cannot be reported to the client anyway.
	_ = json.NewEncoder(w).Encode(v)
}

// Error codes in problem responses, listed in api/openapi.yaml. Clients can
// branch on them without parsing the human-readable detail.
const (
	codeInvalidJSON           = "invalid_json"
	codeInvalidPair           = "invalid_pair"
	codeInvalidID             = "invalid_id"
	codeInvalidIdempotencyKey = "invalid_idempotency_key"
	codeUnsupportedCurrency   = "unsupported_currency"
	codeSameCurrency          = "same_currency"
	codeIdempotencyKeyReused  = "idempotency_key_reused"
	codeUpdateNotFound        = "update_not_found"
	codeQuoteNotFound         = "quote_not_found"
	codeInternal              = "internal"
)

// problem is an error response body in the RFC 9457 "problem details" format.
// code and request_id are extension members.
type problem struct {
	Type      string `json:"type"`
	Title     string `json:"title"`
	Status    int    `json:"status"`
	Detail    string `json:"detail,omitempty"`
	Code      string `json:"code"`
	RequestID string `json:"request_id,omitempty"`
}

// writeProblem writes an RFC 9457 error response.
func writeProblem(w http.ResponseWriter, r *http.Request, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{
		Type:      "about:blank",
		Title:     http.StatusText(status),
		Status:    status,
		Detail:    detail,
		Code:      code,
		RequestID: RequestIDFrom(r.Context()),
	})
}
