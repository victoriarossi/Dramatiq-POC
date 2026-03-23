-- Run this as a superuser (e.g. postgres):
--   psql -U postgres -f create_db.sql

CREATE DATABASE mydb;

\c mydb

-- ── tasks: single source of truth ────────────────────────────────────────────
--
-- status lifecycle:  QUEUED → RUNNING → DONE | ERROR | CANCELLED
--
-- Sequential execution is enforced by RabbitMQ prefetch_count=1:
-- the worker receives the next message only after it acks the current one.
-- A SAP failure cascades: remaining QUEUED rows → CANCELLED, RabbitMQ queue purged.

DROP TABLE IF EXISTS tasks;

CREATE TABLE tasks (
  id         BIGSERIAL    PRIMARY KEY,          -- insertion order = execution order
  task_id    VARCHAR(64)  NOT NULL UNIQUE,
  client_id  VARCHAR(512) NOT NULL,
  intent     TEXT         NOT NULL,
  status     VARCHAR(32)  NOT NULL DEFAULT 'QUEUED',
  message_id VARCHAR(512) NULL,                 -- Dramatiq message ID, set on enqueue
  result     JSONB        NULL,                 -- populated on DONE or ERROR
  created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX ON tasks (client_id, status, id);