-- -- Run this as a superuser (e.g. postgres):
-- -- psql -U postgres -f create_db.sql

CREATE DATABASE mydb;

\c mydb

DROP TABLE IF EXISTS tasks;

CREATE TABLE tasks (
  key        VARCHAR(512) PRIMARY KEY,
  status     INT          NOT NULL,   -- 0=SUCCESS, 1=PENDING, 2=ERROR
  intent     VARCHAR(512) NOT NULL,
  client_id  VARCHAR(512) NOT NULL,
  result     JSONB        NULL,
  created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE task_queue (
    id         INT    PRIMARY KEY,
    client_id  VARCHAR(512) NOT NULL,
    task_id    VARCHAR(64)  NOT NULL UNIQUE,
    intent     TEXT         NOT NULL,
    status     VARCHAR(32)  NOT NULL DEFAULT 'QUEUED',
    message_id VARCHAR(512) NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Efficient lookup: "give me the next QUEUED row for client X"
CREATE INDEX ON task_queue (client_id, status, id);