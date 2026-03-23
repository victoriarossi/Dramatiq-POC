-- Run this as a superuser (e.g. postgres):
--   psql -U postgres -f create_db.sql

CREATE DATABASE mydb;

\c mydb

-- ── tasks: Dramatiq result store ─────────────────────────────────────────────
-- One row per dispatched message.  status: 0=SUCCESS  1=PENDING  2=ERROR

DROP TABLE IF EXISTS task_queue;
DROP TABLE IF EXISTS tasks;

CREATE TABLE tasks (
  key        VARCHAR(512) PRIMARY KEY,
  status     INT          NOT NULL,
  intent     VARCHAR(512) NOT NULL,
  client_id  VARCHAR(512) NOT NULL,
  result     JSONB        NULL,
  created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ── task_queue: user-visible logical queue ────────────────────────────────────
-- status lifecycle: QUEUED → DONE | ERROR | CANCELLED
-- (RUNNING is transient; kept for observability and crash recovery)
--
-- Sequential execution is enforced by RabbitMQ prefetch_count=1:
-- the worker receives the next message only after it acks the current one.
-- A SAP failure cascades: remaining QUEUED rows → CANCELLED, RabbitMQ queue purged.

CREATE TABLE task_queue (
    id         BIGSERIAL    PRIMARY KEY,          -- auto-increment; order = execution order
    client_id  VARCHAR(512) NOT NULL,
    task_id    VARCHAR(64)  NOT NULL UNIQUE,
    intent     TEXT         NOT NULL,
    status     VARCHAR(32)  NOT NULL DEFAULT 'QUEUED',
    message_id VARCHAR(512) NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX ON task_queue (client_id, status, id);