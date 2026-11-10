-- Initial schema. Applied by golang-migrate (the migrate service in
-- compose.yaml). golang-migrate does not wrap a file in a transaction, so the
-- file does it itself: the schema is created completely or not at all.
BEGIN;

-- One row per update request: the background job and, once it finishes, its
-- result. GET /api/v1/quotes/updates/{id} reads this table, so an old update
-- keeps returning the price it got even after newer updates.
CREATE TABLE quote_updates (
    id             uuid        PRIMARY KEY,
    base_currency  text        NOT NULL CHECK (base_currency ~ '^[A-Z]{3}$'),
    quote_currency text        NOT NULL CHECK (quote_currency ~ '^[A-Z]{3}$'),
    status         text        NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending', 'processing', 'done', 'failed')),
    -- Times the worker took the update; grows on redelivery and retries.
    attempts       integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    -- numeric without a scale keeps the price exactly as the provider sent it.
    price          numeric     CHECK (price > 0),
    -- Date the provider published the rate for; central banks publish daily.
    rate_date      date,
    last_error     text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    -- When the update reached done or failed.
    completed_at   timestamptz,

    CONSTRAINT quote_updates_pair_check CHECK (base_currency <> quote_currency),
    -- Every status carries exactly the fields it promises to the API.
    CONSTRAINT quote_updates_result_check CHECK (
        CASE status
            WHEN 'done' THEN price IS NOT NULL AND rate_date IS NOT NULL
                         AND completed_at IS NOT NULL
            WHEN 'failed' THEN price IS NULL AND last_error IS NOT NULL
                           AND completed_at IS NOT NULL
            ELSE price IS NULL AND rate_date IS NULL AND completed_at IS NULL
        END
    )
);

-- At most one unfinished update per pair. A repeated POST for a pair that is
-- already being updated joins that update and gets its id instead of starting
-- another fetch. The API relies on this index in
-- INSERT ... ON CONFLICT (base_currency, quote_currency) WHERE status IN (...).
CREATE UNIQUE INDEX quote_updates_in_flight_uidx
    ON quote_updates (base_currency, quote_currency)
    WHERE status IN ('pending', 'processing');

-- Latest known quote per pair, for GET /api/v1/quotes/latest. Written in the
-- same transaction that marks an update done.
CREATE TABLE quotes (
    base_currency  text        NOT NULL,
    quote_currency text        NOT NULL,
    price          numeric     NOT NULL CHECK (price > 0),
    rate_date      date        NOT NULL,
    -- When the service obtained the price: the "update time" of the quote.
    updated_at     timestamptz NOT NULL,
    -- The update that produced this price.
    update_id      uuid        NOT NULL REFERENCES quote_updates (id),

    PRIMARY KEY (base_currency, quote_currency)
);

-- Transactional outbox. The API writes the Kafka message here in the same
-- transaction as the update row, so a request is never accepted without its
-- message, even if Kafka is down. The relay in the worker publishes rows in id
-- order and deletes them once Kafka has acknowledged them.
CREATE TABLE outbox (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic       text        NOT NULL,
    -- Kafka message key; the currency pair, so one pair stays on one partition.
    message_key text        NOT NULL,
    payload     jsonb       NOT NULL,
    -- Kafka headers, e.g. the X-Request-ID of the HTTP request for tracing.
    headers     jsonb       NOT NULL DEFAULT '{}',
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Idempotency-Key header values. A retried POST with the same key and body
-- gets the original update id back; the same key with a different body is
-- rejected. Keys older than the retention period are deleted by the worker.
CREATE TABLE idempotency_keys (
    idempotency_key text        PRIMARY KEY
                                CHECK (char_length(idempotency_key) BETWEEN 1 AND 255),
    -- SHA-256 of the normalized request body.
    request_hash    bytea       NOT NULL CHECK (octet_length(request_hash) = 32),
    update_id       uuid        NOT NULL REFERENCES quote_updates (id) ON DELETE CASCADE,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- For the periodic cleanup: DELETE ... WHERE created_at < now() - retention.
CREATE INDEX idempotency_keys_created_at_idx ON idempotency_keys (created_at);

COMMIT;
