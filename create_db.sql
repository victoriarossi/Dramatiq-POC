-- Run this as a superuser (e.g. postgres):
-- psql -U postgres -f create_db.sql

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