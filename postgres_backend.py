"""
postgres_backend.py — All persistence against a single `tasks` table.

Schema (see create_db.sql):

  CREATE TABLE tasks (
    id         BIGSERIAL    PRIMARY KEY,
    task_id    VARCHAR(64)  NOT NULL UNIQUE,
    client_id  VARCHAR(512) NOT NULL,
    intent     TEXT         NOT NULL,
    status     VARCHAR(32)  NOT NULL DEFAULT 'QUEUED',
    message_id VARCHAR(512) NULL,
    result     JSONB        NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
  );

status lifecycle:  QUEUED → RUNNING → DONE | ERROR | CANCELLED

Environment variables:
  PG_HOST      (default: localhost)
  PG_PORT      (default: 5432)
  PG_DB        (default: mydb)
  PG_USER      (default: postgres)
  PG_PASSWORD  (default: "")
"""

import os
import time

import psycopg2
import psycopg2.extras
from dramatiq.results.backend import ResultBackend
from dramatiq.results.errors import ResultMissing, ResultTimeout

# ── Status constants ──────────────────────────────────────────────────────────

STATUS_QUEUED    = "QUEUED"
STATUS_RUNNING   = "RUNNING"
STATUS_DONE      = "DONE"
STATUS_ERROR     = "ERROR"
STATUS_CANCELLED = "CANCELLED"

_TERMINAL_STATUSES = {STATUS_DONE, STATUS_ERROR, STATUS_CANCELLED}

# ── DB connection ─────────────────────────────────────────────────────────────

def _pg_connect():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        dbname=os.getenv("PG_DB", "mydb"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
    )


# ── Dramatiq ResultBackend ────────────────────────────────────────────────────

class PostgresBackend(ResultBackend):
    """
    Implements the Dramatiq ResultBackend interface against `tasks`.

    Instead of maintaining a separate key/namespace scheme, results are stored
    directly on the row identified by message_id.
    """

    def __init__(self, *, namespace: str = "dramatiq"):
        super().__init__(namespace=namespace)

    # Dramatiq expects a namespaced key for internal lookups.
    def _key(self, message_id: str) -> str:
        return f"{self.namespace}:{message_id}"

    # ── Write path ────────────────────────────────────────────────────────────

    def store_result(self, message, result, ttl: int):
        """Called by the worker on successful task completion."""
        is_error = isinstance(result, Exception)
        status   = STATUS_ERROR if is_error else STATUS_DONE
        payload  = {"error": str(result)} if is_error else result
        self._write_result(message.message_id, status, payload)

    def store_exception(self, message, exception, ttl: int):
        """Called by Dramatiq after_nack when retries are exhausted."""
        self._write_result(
            message.message_id,
            STATUS_ERROR,
            {"error": str(exception)},
        )

    def _store(self, message_key: str, result, ttl: int):
        """
        Low-level hook called by Dramatiq's base class on some code paths
        (e.g. newer versions route the nack path here).  result is already a
        wrapped dict at this point.
        """
        is_error = isinstance(result, Exception) or (
            isinstance(result, dict) and (
                "error" in result
                or result.get("type") == "django.core.exceptions.ImproperlyConfigured"
            )
        )
        status  = STATUS_ERROR if is_error else STATUS_DONE
        payload = result if isinstance(result, dict) else {"error": str(result)}

        # message_key is "namespace:message_id" — strip the prefix.
        message_id = message_key.split(":", 1)[-1]
        self._write_result(message_id, status, payload)

    def _write_result(self, message_id: str, status: str, payload: dict):
        """Persist result + status onto the tasks row identified by message_id."""
        sql = """
            UPDATE tasks
            SET status     = %(status)s,
                result     = %(result)s,
                updated_at = NOW()
            WHERE message_id = %(message_id)s
        """
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "message_id": message_id,
                    "status":     status,
                    "result":     psycopg2.extras.Json(payload),
                })

    # ── Read path ─────────────────────────────────────────────────────────────

    def get_result(self, message, *, block: bool = False, timeout: int | None = None):
        """Poll `tasks` until status is terminal, then return the result payload."""
        deadline      = (time.monotonic() + timeout / 1000.0) if (block and timeout is not None) else None
        poll_interval = 1.0

        while True:
            row = self._fetch_row(message.message_id)
            if row is not None:
                status, payload = row
                if status in _TERMINAL_STATUSES:
                    return payload

            if not block:
                raise ResultMissing(message)
            if deadline is not None and time.monotonic() >= deadline:
                raise ResultTimeout(message)
            time.sleep(poll_interval)

    def _fetch_row(self, message_id: str):
        """Return (status, result_dict) from `tasks` by message_id, or None."""
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status, result FROM tasks WHERE message_id = %s",
                    (message_id,),
                )
                return cur.fetchone()

    def delete_result(self, message, ttl: int):
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM tasks WHERE message_id = %s",
                    (message.message_id,),
                )


# ── Task helpers (used by client.py and task_queue.py) ───────────────────────

def task_create(client_id: str, task_id: str, intent: str, message_id: str) -> int:
    """
    Insert a new QUEUED row and return its auto-increment `id`.
    message_id is set immediately because we publish to RabbitMQ before inserting.
    """
    sql = """
        INSERT INTO tasks (task_id, client_id, intent, status, message_id)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
    """
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (task_id, client_id, intent, STATUS_QUEUED, message_id))
            return cur.fetchone()[0]


def task_get_by_task_id(task_id: str) -> dict | None:
    """Return the tasks row for *task_id*, or None."""
    sql = """
        SELECT id, client_id, task_id, intent, status, message_id, result, created_at
        FROM tasks
        WHERE task_id = %s
    """
    with _pg_connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (task_id,))
            return cur.fetchone()


def task_mark_running(task_id: str):
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tasks SET status=%s, updated_at=NOW() WHERE task_id=%s",
                (STATUS_RUNNING, task_id),
            )


def task_mark_done(task_id: str, result: dict):
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tasks SET status=%s, result=%s, updated_at=NOW() WHERE task_id=%s",
                (STATUS_DONE, psycopg2.extras.Json(result), task_id),
            )


def task_mark_error(task_id: str, error: str):
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tasks SET status=%s, result=%s, updated_at=NOW() WHERE task_id=%s",
                (STATUS_ERROR, psycopg2.extras.Json({"error": error}), task_id),
            )


def task_cancel_queued(client_id: str) -> int:
    """Cancel every QUEUED row for this client. Returns the count cancelled."""
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tasks SET status=%s, updated_at=NOW() "
                "WHERE client_id=%s AND status=%s",
                (STATUS_CANCELLED, client_id, STATUS_QUEUED),
            )
            return cur.rowcount


def task_all(client_id: str) -> list[dict]:
    """All tasks rows for this client, ordered by insertion order."""
    sql = """
        SELECT id, task_id, intent, status, message_id, result, created_at
        FROM tasks
        WHERE client_id = %s
        ORDER BY id ASC
    """
    with _pg_connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (client_id,))
            return cur.fetchall()