-- Reverts 000001_init.up.sql. Tables are dropped in reverse dependency order.
BEGIN;

DROP TABLE IF EXISTS idempotency_keys;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS quotes;
DROP TABLE IF EXISTS quote_updates;

COMMIT;
