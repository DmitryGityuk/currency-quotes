package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
	"uuid"

	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
)

// QuoteStore is what the quote handlers need from the database. The handlers
// never call the rates provider or Kafka: registering a request is all the
// API does, the update itself happens in the worker.
type QuoteStore interface {
	// RequestUpdate registers an update request and returns the update the
	// request ended up with: a new one, the one an Idempotency-Key points to,
	// or the unfinished update of the same pair.
	RequestUpdate(ctx context.Context, req quotes.UpdateRequest) (quotes.UpdateRequestResult, error)
	// Update returns an update by id, or quotes.ErrNotFound.
	Update(ctx context.Context, id uuid.UUID) (quotes.Update, error)
	// LatestQuote returns the latest quote of a pair, or quotes.ErrNotFound.
	LatestQuote(ctx context.Context, pair quotes.Pair) (quotes.Quote, error)
}

const (
	updatesPath = "/api/v1/quotes/updates"
	latestPath  = "/api/v1/quotes/latest"

	idempotencyKeyHeader = "Idempotency-Key"
	maxIdempotencyKeyLen = 255
	// maxBodySize is far more than {"pair":"EUR/MXN"} needs.
	maxBodySize = 1 << 10
	// retryAfter tells a polling client when to ask again; the worker usually
	// finishes well within a second.
	retryAfter = "1"
)

// UpdateObserver receives the outcome of every accepted update request:
// "created", "joined" (the pair already had an unfinished update) or
// "replayed" (a repeated Idempotency-Key).
type UpdateObserver interface {
	UpdateRequested(pair quotes.Pair, outcome string)
}

type nopUpdateObserver struct{}

func (nopUpdateObserver) UpdateRequested(quotes.Pair, string) {}

// Quotes serves the quote endpoints described in api/openapi.yaml.
type Quotes struct {
	store      QuoteStore
	currencies quotes.Currencies
	logger     *slog.Logger
	observer   UpdateObserver
}

// NewQuotes returns the quote handlers.
func NewQuotes(store QuoteStore, currencies quotes.Currencies, logger *slog.Logger) *Quotes {
	return &Quotes{store: store, currencies: currencies, logger: logger, observer: nopUpdateObserver{}}
}

// WithObserver makes the handlers report to o and returns them.
func (h *Quotes) WithObserver(o UpdateObserver) *Quotes {
	h.observer = o
	return h
}

// Register mounts the endpoints on mux.
func (h *Quotes) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST "+updatesPath, h.createUpdate)
	mux.HandleFunc("GET "+updatesPath+"/{id}", h.getUpdate)
	mux.HandleFunc("GET "+latestPath, h.latestQuote)
}

// apiError is a client error with its HTTP status and problem code.
type apiError struct {
	status int
	code   string
	detail string
}

func badRequest(code, format string, args ...any) *apiError {
	return &apiError{status: http.StatusBadRequest, code: code, detail: fmt.Sprintf(format, args...)}
}

func (h *Quotes) createUpdate(w http.ResponseWriter, r *http.Request) {
	pair, key, apiErr := h.parseCreateUpdate(w, r)
	if apiErr != nil {
		writeProblem(w, r, apiErr.status, apiErr.code, apiErr.detail)
		return
	}
	h.registerUpdate(w, r, pair, key)
}

// parseCreateUpdate validates everything the client sent before the store is
// touched: the Idempotency-Key header, the body and the currencies.
func (h *Quotes) parseCreateUpdate(w http.ResponseWriter, r *http.Request) (quotes.Pair, string, *apiError) {
	key, apiErr := idempotencyKey(r.Header.Get(idempotencyKeyHeader))
	if apiErr != nil {
		return quotes.Pair{}, "", apiErr
	}
	pair, apiErr := decodeUpdateRequest(w, r)
	if apiErr != nil {
		return quotes.Pair{}, "", apiErr
	}
	if apiErr := h.validate(pair); apiErr != nil {
		return quotes.Pair{}, "", apiErr
	}
	return pair, key, nil
}

func (h *Quotes) registerUpdate(w http.ResponseWriter, r *http.Request, pair quotes.Pair, key string) {
	res, err := h.store.RequestUpdate(r.Context(), quotes.UpdateRequest{
		Pair:           pair,
		IdempotencyKey: key,
		RequestHash:    requestHash(pair),
		RequestID:      RequestIDFrom(r.Context()),
	})
	if errors.Is(err, quotes.ErrIdempotencyKeyReused) {
		writeProblem(w, r, http.StatusUnprocessableEntity, codeIdempotencyKeyReused,
			"Idempotency-Key was already used with a different request body")
		return
	}
	if err != nil {
		h.internalError(w, r, fmt.Errorf("request update: %w", err))
		return
	}

	outcome := "created"
	switch {
	case res.Replayed:
		outcome = "replayed"
	case res.Joined:
		outcome = "joined"
	}
	h.observer.UpdateRequested(pair, outcome)
	h.logger.InfoContext(r.Context(), "quote update requested",
		slog.String("update_id", res.Update.ID.String()),
		slog.String("pair", pair.String()),
		slog.Bool("replayed", res.Replayed),
		slog.Bool("joined", res.Joined),
		slog.String("request_id", RequestIDFrom(r.Context())),
	)
	w.Header().Set("Location", updatesPath+"/"+res.Update.ID.String())
	if res.Replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeUpdate(w, http.StatusAccepted, res.Update)
}

func (h *Quotes) getUpdate(w http.ResponseWriter, r *http.Request) {
	raw := r.PathValue("id")
	id, err := uuid.Parse(raw)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, codeInvalidID, fmt.Sprintf("update id must be a UUID, got %q", raw))
		return
	}
	upd, err := h.store.Update(r.Context(), id)
	if errors.Is(err, quotes.ErrNotFound) {
		writeProblem(w, r, http.StatusNotFound, codeUpdateNotFound, fmt.Sprintf("update %s not found", id))
		return
	}
	if err != nil {
		h.internalError(w, r, fmt.Errorf("get update %s: %w", id, err))
		return
	}
	writeUpdate(w, http.StatusOK, upd)
}

func (h *Quotes) latestQuote(w http.ResponseWriter, r *http.Request) {
	raw := r.URL.Query().Get("pair")
	if raw == "" {
		writeProblem(w, r, http.StatusBadRequest, codeInvalidPair, "query parameter pair is required, e.g. ?pair=EUR/MXN")
		return
	}
	pair, err := quotes.ParsePair(raw)
	if err != nil {
		writeProblem(w, r, http.StatusBadRequest, codeInvalidPair, err.Error())
		return
	}
	if apiErr := h.validate(pair); apiErr != nil {
		writeProblem(w, r, apiErr.status, apiErr.code, apiErr.detail)
		return
	}

	q, err := h.store.LatestQuote(r.Context(), pair)
	if errors.Is(err, quotes.ErrNotFound) {
		writeProblem(w, r, http.StatusNotFound, codeQuoteNotFound,
			fmt.Sprintf("no quote for %s yet; request an update with POST %s", pair, updatesPath))
		return
	}
	if err != nil {
		h.internalError(w, r, fmt.Errorf("latest quote %s: %w", pair, err))
		return
	}
	writeJSON(w, http.StatusOK, quoteResponse{
		Pair:      q.Pair.String(),
		Price:     string(q.Price),
		RateDate:  q.RateDate.Format(time.DateOnly),
		UpdatedAt: q.UpdatedAt.UTC(),
		UpdateID:  q.UpdateID.String(),
	})
}

// validate checks that the service supports both currencies of pair.
func (h *Quotes) validate(pair quotes.Pair) *apiError {
	err := h.currencies.Validate(pair)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, quotes.ErrSameCurrency):
		return &apiError{status: http.StatusUnprocessableEntity, code: codeSameCurrency, detail: err.Error()}
	default:
		return &apiError{status: http.StatusUnprocessableEntity, code: codeUnsupportedCurrency, detail: err.Error()}
	}
}

// internalError logs err and answers 500 without exposing details; the
// client can quote request_id to find the log record.
func (h *Quotes) internalError(w http.ResponseWriter, r *http.Request, err error) {
	h.logger.ErrorContext(r.Context(), "request failed",
		slog.Any("error", err),
		slog.String("request_id", RequestIDFrom(r.Context())),
	)
	writeProblem(w, r, http.StatusInternalServerError, codeInternal, "")
}

// decodeUpdateRequest reads {"pair": "EUR/MXN"} and nothing else.
func decodeUpdateRequest(w http.ResponseWriter, r *http.Request) (quotes.Pair, *apiError) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodySize))
	dec.DisallowUnknownFields()

	var body struct {
		Pair *string `json:"pair"`
	}
	if err := dec.Decode(&body); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return quotes.Pair{}, badRequest(codeInvalidJSON, "request body is larger than %d bytes", maxBodySize)
		}
		return quotes.Pair{}, badRequest(codeInvalidJSON, "request body: %v", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return quotes.Pair{}, badRequest(codeInvalidJSON, "request body must contain a single JSON object")
	}
	if body.Pair == nil {
		return quotes.Pair{}, badRequest(codeInvalidPair, "pair is required, e.g. {\"pair\": \"EUR/MXN\"}")
	}
	pair, err := quotes.ParsePair(*body.Pair)
	if err != nil {
		return quotes.Pair{}, badRequest(codeInvalidPair, "%v", err)
	}
	return pair, nil
}

// idempotencyKey validates the Idempotency-Key header; "" means no key. The
// IETF draft defines the value as a structured-field string ("abc"), while
// many clients send it bare, so both forms are accepted.
func idempotencyKey(v string) (string, *apiError) {
	if v == "" {
		return "", nil
	}
	if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		v = v[1 : len(v)-1]
	}
	if v == "" || len(v) > maxIdempotencyKeyLen {
		return "", badRequest(codeInvalidIdempotencyKey, "%s must be 1 to %d characters long", idempotencyKeyHeader, maxIdempotencyKeyLen)
	}
	for i := range len(v) {
		if v[i] < '!' || v[i] > '~' {
			return "", badRequest(codeInvalidIdempotencyKey, "%s must be printable ASCII without spaces", idempotencyKeyHeader)
		}
	}
	return v, nil
}

// requestHash fingerprints the normalized request, so "eur/mxn" and
// "EUR/MXN" sent with the same key count as the same request.
func requestHash(p quotes.Pair) [32]byte {
	return sha256.Sum256([]byte(`{"pair":"` + p.String() + `"}`))
}

type updateResponse struct {
	ID          string     `json:"id"`
	Pair        string     `json:"pair"`
	Status      string     `json:"status"`
	RequestedAt time.Time  `json:"requested_at"`
	Price       string     `json:"price,omitempty"`
	RateDate    string     `json:"rate_date,omitempty"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty"`
	Error       string     `json:"error,omitempty"`
}

type quoteResponse struct {
	Pair      string    `json:"pair"`
	Price     string    `json:"price"`
	RateDate  string    `json:"rate_date"`
	UpdatedAt time.Time `json:"updated_at"`
	UpdateID  string    `json:"update_id"`
}

// writeUpdate writes an update with exactly the fields its status promises,
// and Retry-After while it is unfinished.
func writeUpdate(w http.ResponseWriter, status int, u quotes.Update) {
	resp := updateResponse{
		ID:          u.ID.String(),
		Pair:        u.Pair.String(),
		Status:      string(u.Status),
		RequestedAt: u.RequestedAt.UTC(),
	}
	switch u.Status {
	case quotes.StatusDone:
		updatedAt := u.UpdatedAt.UTC()
		resp.Price = string(u.Price)
		resp.RateDate = u.RateDate.Format(time.DateOnly)
		resp.UpdatedAt = &updatedAt
	case quotes.StatusFailed:
		resp.Error = u.Error
	default:
		w.Header().Set("Retry-After", retryAfter)
	}
	writeJSON(w, status, resp)
}
