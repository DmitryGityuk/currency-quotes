// Package postgres stores quote updates, quotes, the outbox and idempotency
// keys in PostgreSQL. The schema is in migrations/.
//
// Store implements httpapi.QuoteStore for the API, worker.Store for the
// worker and outbox.Store for the relay.
package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DmitryGityuk/currency-quotes/internal/events"
	"github.com/DmitryGityuk/currency-quotes/internal/outbox"
	"github.com/DmitryGityuk/currency-quotes/internal/quotes"
	"github.com/DmitryGityuk/currency-quotes/internal/rates"
)

// NewPool returns a connection pool. It does not connect yet: the service
// starts even while the database is down and reports it through /readyz.
// appName shows up in pg_stat_activity, which helps to tell the API from
// the worker.
func NewPool(ctx context.Context, url, appName string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, errors.New("parse DATABASE_URL") // the error would contain the password
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = appName
	return pgxpool.NewWithConfig(ctx, cfg)
}

// Store is the PostgreSQL implementation of the service's stores.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store on top of pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Ping checks that the database answers; used by /readyz.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// updateColumns is the select list that scanUpdate reads. Prices are read
// as text, so they are never converted to float.
const updateColumns = `id, base_currency, quote_currency, status, attempts, created_at,
	price::text, rate_date, completed_at, last_error`

func scanUpdate(row pgx.Row) (quotes.Update, error) {
	var (
		u           quotes.Update
		base, quote string
		status      string
		price       *string
		rateDate    *time.Time
		completedAt *time.Time
		lastError   *string
	)
	err := row.Scan(&u.ID, &base, &quote, &status, &u.Attempts, &u.RequestedAt,
		&price, &rateDate, &completedAt, &lastError)
	if err != nil {
		return quotes.Update{}, err
	}
	u.Pair = quotes.Pair{Base: quotes.Currency(base), Quote: quotes.Currency(quote)}
	u.Status = quotes.Status(status)
	switch u.Status {
	case quotes.StatusDone:
		u.Price, u.RateDate, u.UpdatedAt = quotes.Price(*price), *rateDate, *completedAt
	case quotes.StatusFailed:
		u.Error = *lastError
	}
	return u, nil
}

// errContention means a concurrent request changed what this one was relying
// on; RequestUpdate starts over.
var errContention = errors.New("concurrent request, try again")

// RequestUpdate registers an update request in one transaction: the update
// row, its outbox message and the idempotency key. See
// docs/diagrams/sequence-idempotency.puml.
func (s *Store) RequestUpdate(ctx context.Context, req quotes.UpdateRequest) (quotes.UpdateRequestResult, error) {
	for range 5 {
		res, err := s.requestUpdate(ctx, req)
		if !errors.Is(err, errContention) {
			return res, err
		}
	}
	return quotes.UpdateRequestResult{}, fmt.Errorf("request update: %w", errContention)
}

func (s *Store) requestUpdate(ctx context.Context, req quotes.UpdateRequest) (res quotes.UpdateRequestResult, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op after a commit

	if req.IdempotencyKey != "" {
		upd, found, err := replay(ctx, tx, req)
		if err != nil || found {
			return quotes.UpdateRequestResult{Update: upd, Replayed: found}, err
		}
	}

	// A new update, unless the pair already has an unfinished one: the
	// partial unique index turns the insert into a no-op then.
	upd, err := scanUpdate(tx.QueryRow(ctx, `
		INSERT INTO quote_updates (id, base_currency, quote_currency)
		VALUES ($1, $2, $3)
		ON CONFLICT (base_currency, quote_currency) WHERE status IN ('pending', 'processing')
		DO NOTHING
		RETURNING `+updateColumns,
		uuid.NewV7(), req.Pair.Base, req.Pair.Quote))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		upd, err = scanUpdate(tx.QueryRow(ctx, `
			SELECT `+updateColumns+` FROM quote_updates
			WHERE base_currency = $1 AND quote_currency = $2 AND status IN ('pending', 'processing')`,
			req.Pair.Base, req.Pair.Quote))
		if errors.Is(err, pgx.ErrNoRows) {
			return res, errContention // it finished between the two statements
		}
		if err != nil {
			return res, fmt.Errorf("find unfinished update: %w", err)
		}
		res.Joined = true
	case err != nil:
		return res, fmt.Errorf("insert update: %w", err)
	default:
		if err := insertOutbox(ctx, tx, upd, req.RequestID); err != nil {
			return res, err
		}
	}

	if req.IdempotencyKey != "" {
		tag, err := tx.Exec(ctx, `
			INSERT INTO idempotency_keys (idempotency_key, request_hash, update_id)
			VALUES ($1, $2, $3)
			ON CONFLICT (idempotency_key) DO NOTHING`,
			req.IdempotencyKey, req.RequestHash[:], upd.ID)
		if err != nil {
			return res, fmt.Errorf("insert idempotency key: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// A parallel request with the same key committed first. Roll
			// back and start over: the replay branch will answer.
			return res, errContention
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit: %w", err)
	}
	res.Update = upd
	return res, nil
}

// replay returns the update an Idempotency-Key already points to.
func replay(ctx context.Context, tx pgx.Tx, req quotes.UpdateRequest) (quotes.Update, bool, error) {
	var (
		hash []byte
		id   uuid.UUID
	)
	err := tx.QueryRow(ctx, `SELECT request_hash, update_id FROM idempotency_keys WHERE idempotency_key = $1`,
		req.IdempotencyKey).Scan(&hash, &id)
	if errors.Is(err, pgx.ErrNoRows) {
		return quotes.Update{}, false, nil
	}
	if err != nil {
		return quotes.Update{}, false, fmt.Errorf("find idempotency key: %w", err)
	}
	if !bytes.Equal(hash, req.RequestHash[:]) {
		return quotes.Update{}, false, quotes.ErrIdempotencyKeyReused
	}
	upd, err := scanUpdate(tx.QueryRow(ctx, `SELECT `+updateColumns+` FROM quote_updates WHERE id = $1`, id))
	if err != nil {
		return quotes.Update{}, false, fmt.Errorf("get replayed update: %w", err)
	}
	return upd, true, nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, upd quotes.Update, requestID string) error {
	ev := events.UpdateRequested{UpdateID: upd.ID, Pair: upd.Pair.String(), RequestedAt: upd.RequestedAt.UTC()}
	payload, err := ev.Encode()
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}
	headers := map[string]string{}
	if requestID != "" {
		headers[events.HeaderRequestID] = requestID
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (topic, message_key, payload, headers) VALUES ($1, $2, $3, $4)`,
		events.TopicUpdateRequested, ev.Key(), payload, headers)
	if err != nil {
		return fmt.Errorf("insert outbox message: %w", err)
	}
	return nil
}

// Update returns an update by id.
func (s *Store) Update(ctx context.Context, id uuid.UUID) (quotes.Update, error) {
	upd, err := scanUpdate(s.pool.QueryRow(ctx, `SELECT `+updateColumns+` FROM quote_updates WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return quotes.Update{}, quotes.ErrNotFound
	}
	return upd, err
}

// LatestQuote returns the latest quote of a pair.
func (s *Store) LatestQuote(ctx context.Context, pair quotes.Pair) (quotes.Quote, error) {
	q := quotes.Quote{Pair: pair}
	var price string
	err := s.pool.QueryRow(ctx, `
		SELECT price::text, rate_date, updated_at, update_id FROM quotes
		WHERE base_currency = $1 AND quote_currency = $2`,
		pair.Base, pair.Quote).Scan(&price, &q.RateDate, &q.UpdatedAt, &q.UpdateID)
	if errors.Is(err, pgx.ErrNoRows) {
		return quotes.Quote{}, quotes.ErrNotFound
	}
	if err != nil {
		return quotes.Quote{}, fmt.Errorf("get latest quote: %w", err)
	}
	q.Price = quotes.Price(price)
	return q, nil
}

// StartAttempt implements worker.Store.
func (s *Store) StartAttempt(ctx context.Context, id uuid.UUID) (quotes.Update, error) {
	upd, err := scanUpdate(s.pool.QueryRow(ctx, `
		UPDATE quote_updates SET status = 'processing', attempts = attempts + 1
		WHERE id = $1 AND status IN ('pending', 'processing')
		RETURNING `+updateColumns, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return s.Update(ctx, id) // finished already, or unknown
	}
	return upd, err
}

// Complete implements worker.Store: the update becomes done and its price
// becomes the latest quote of the pair, unless a newer one is already there.
func (s *Store) Complete(ctx context.Context, id uuid.UUID, rate rates.Rate, at time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var base, quote string
	err = tx.QueryRow(ctx, `
		UPDATE quote_updates
		SET status = 'done', price = $2::text::numeric, rate_date = $3, completed_at = $4
		WHERE id = $1 AND status IN ('pending', 'processing')
		RETURNING base_currency, quote_currency`,
		id, string(rate.Price), rate.Date, at).Scan(&base, &quote)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // a duplicate delivery finished it first
	}
	if err != nil {
		return fmt.Errorf("mark update done: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO quotes (base_currency, quote_currency, price, rate_date, updated_at, update_id)
		VALUES ($1, $2, $3::text::numeric, $4, $5, $6)
		ON CONFLICT (base_currency, quote_currency) DO UPDATE
		SET price = EXCLUDED.price, rate_date = EXCLUDED.rate_date,
		    updated_at = EXCLUDED.updated_at, update_id = EXCLUDED.update_id
		WHERE quotes.updated_at < EXCLUDED.updated_at`,
		base, quote, string(rate.Price), rate.Date, at, id)
	if err != nil {
		return fmt.Errorf("upsert latest quote: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Fail implements worker.Store.
func (s *Store) Fail(ctx context.Context, id uuid.UUID, reason string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE quote_updates SET status = 'failed', last_error = $2, completed_at = $3
		WHERE id = $1 AND status IN ('pending', 'processing')`,
		id, reason, at)
	if err != nil {
		return fmt.Errorf("mark update failed: %w", err)
	}
	return nil
}

// PublishBatch implements outbox.Store. FOR UPDATE SKIP LOCKED lets several
// relays run at once: each takes rows the others have not locked.
func (s *Store) PublishBatch(ctx context.Context, limit int, publish func(context.Context, []outbox.Message) error) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, topic, message_key, payload, headers FROM outbox
		ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, fmt.Errorf("lock outbox rows: %w", err)
	}
	msgs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (outbox.Message, error) {
		var m outbox.Message
		err := row.Scan(&m.ID, &m.Topic, &m.Key, &m.Value, &m.Headers)
		return m, err
	})
	if err != nil {
		return 0, fmt.Errorf("read outbox rows: %w", err)
	}
	if len(msgs) == 0 {
		return 0, nil
	}

	if err := publish(ctx, msgs); err != nil {
		return 0, fmt.Errorf("publish: %w", err)
	}

	ids := make([]int64, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	if _, err := tx.Exec(ctx, `DELETE FROM outbox WHERE id = ANY($1)`, ids); err != nil {
		return 0, fmt.Errorf("delete published rows: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}
	return len(msgs), nil
}

// OutboxStats returns how many messages wait in the outbox and the age of
// the oldest one, for the backlog metrics.
func (s *Store) OutboxStats(ctx context.Context) (int, time.Duration, error) {
	var (
		n      int
		oldest float64
	)
	err := s.pool.QueryRow(ctx, `
		SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::float8 FROM outbox`).Scan(&n, &oldest)
	if err != nil {
		return 0, 0, fmt.Errorf("outbox stats: %w", err)
	}
	return n, time.Duration(oldest * float64(time.Second)), nil
}

// DeleteIdempotencyKeys removes keys created before cutoff.
func (s *Store) DeleteIdempotencyKeys(ctx context.Context, cutoff time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE created_at < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("delete idempotency keys: %w", err)
	}
	return tag.RowsAffected(), nil
}
