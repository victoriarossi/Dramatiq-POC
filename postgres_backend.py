"""
postgres_backend.py — Dramatiq result backend backed by PostgreSQL.

Schema
------
  tasks table (unchanged):

    CREATE TABLE tasks (
      key        VARCHAR(512) PRIMARY KEY,
      status     INT          NOT NULL,          -- 0=SUCCESS, 1=PENDING, 2=ERROR
      intent     VARCHAR(512) NOT NULL,
      client_id  VARCHAR(512) NOT NULL,
      result     JSONB        NULL,
      created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    );

  task_queue table (run once, see create_db.sql):

    CREATE TABLE task_queue (
      id         BIGSERIAL    PRIMARY KEY,
      client_id  VARCHAR(512) NOT NULL,
      task_id    VARCHAR(64)  NOT NULL UNIQUE,
      intent     TEXT         NOT NULL,
      status     VARCHAR(32)  NOT NULL DEFAULT 'QUEUED',
      message_id VARCHAR(512) NULL,
      created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    );
    CREATE INDEX ON task_queue (client_id, status, id);

tasks.status constants   → STATUS_*
task_queue.status strings → QS_*

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

# ── tasks.status constants ────────────────────────────────────────────────────

STATUS_SUCCESS = 0
STATUS_PENDING = 1
STATUS_ERROR   = 2

# ── task_queue.status strings ─────────────────────────────────────────────────

QS_QUEUED    = "QUEUED"
QS_RUNNING   = "RUNNING"
QS_DONE      = "DONE"
QS_ERROR     = "ERROR"
QS_CANCELLED = "CANCELLED"


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
    Persists Dramatiq task results to the `tasks` table.

    key       = namespace:message_id
    client_id = extracted from the actor's queue name (sap_tasks.<client_id>)
    """

    def __init__(self, *, namespace: str = "dramatiq"):
        super().__init__(namespace=namespace)

    def _key(self, message_id: str) -> str:
        return f"{self.namespace}:{message_id}"

    @staticmethod
    def _client_id_from_message(message) -> str:
        q: str = getattr(message, "queue_name", "") or ""
        return q[len("sap_tasks."):] if q.startswith("sap_tasks.") else (q or "unknown")

    # ── ResultBackend interface ───────────────────────────────────────────────

    def store_result(self, message, result, ttl: int):
        """Called by the worker on task completion (success or exception)."""
        key       = self._key(message.message_id)
        client_id = self._client_id_from_message(message)
        is_error  = isinstance(result, Exception)
        status    = STATUS_ERROR if is_error else STATUS_SUCCESS
        payload   = {"error": str(result)} if is_error else result

        sql = """
            INSERT INTO tasks (key, status, client_id, intent, result)
            VALUES (%(key)s, %(status)s, %(client_id)s, %(intent)s, %(result)s)
            ON CONFLICT (key) DO UPDATE
                SET status     = EXCLUDED.status,
                    result     = EXCLUDED.result,
                    updated_at = NOW()
        """
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "key":       key,
                    "status":    status,
                    "client_id": client_id,
                    "intent":    "",
                    "result":    psycopg2.extras.Json(payload),
                })

    def _store(self, message_key: str, result, ttl: int):
        """
        Low-level write called by Dramatiq's base class for both store_result
        and store_exception.  Newer Dramatiq versions route the nack/exception
        path through here instead of store_result, so we must implement it.
        result is already a wrapped dict at this point (not a raw Exception).
        """
        is_error = isinstance(result, Exception) or (
            isinstance(result, dict) and result.get("type") == "django.core.exceptions.ImproperlyConfigured"
        ) or (isinstance(result, dict) and "error" in result)

        status  = STATUS_ERROR if is_error else STATUS_SUCCESS
        payload = result if isinstance(result, dict) else {"error": str(result)}

        sql = """
            INSERT INTO tasks (key, status, client_id, intent, result)
            VALUES (%(key)s, %(status)s, %(client_id)s, %(intent)s, %(result)s)
            ON CONFLICT (key) DO UPDATE
                SET status     = EXCLUDED.status,
                    result     = EXCLUDED.result,
                    updated_at = NOW()
        """
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "key":       message_key,
                    "status":    status,
                    "client_id": "unknown",   # message object not available here
                    "intent":    "",
                    "result":    psycopg2.extras.Json(payload),
                })

    def store_exception(self, message, exception, ttl: int):
        """
        Called by Dramatiq after_nack when retries are exhausted.
        Stores the exception as an ERROR row so the result is always queryable.
        """
        key       = self._key(message.message_id)
        client_id = self._client_id_from_message(message)
        payload   = {"error": str(exception)}

        sql = """
            INSERT INTO tasks (key, status, client_id, intent, result)
            VALUES (%(key)s, %(status)s, %(client_id)s, %(intent)s, %(result)s)
            ON CONFLICT (key) DO UPDATE
                SET status     = EXCLUDED.status,
                    result     = EXCLUDED.result,
                    updated_at = NOW()
        """
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "key":       key,
                    "status":    STATUS_ERROR,
                    "client_id": client_id,
                    "intent":    "",
                    "result":    psycopg2.extras.Json(payload),
                })

    def get_result(self, message, *, block: bool = False, timeout: int | None = None):
        """Poll `tasks` until status != PENDING."""
        key           = self._key(message.message_id)
        deadline      = (time.monotonic() + timeout / 1000.0) if (block and timeout is not None) else None
        poll_interval = 1.0

        while True:
            row = self._fetch_row(key)
            if row is not None:
                status, payload = row
                if status != STATUS_PENDING:
                    return payload

            if not block:
                raise ResultMissing(message)
            if deadline is not None and time.monotonic() >= deadline:
                raise ResultTimeout(message)
            time.sleep(poll_interval)

    def _fetch_row(self, key: str):
        """Return (status, result_dict) from `tasks`, or None if absent."""
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status, result FROM tasks WHERE key = %s", (key,))
                return cur.fetchone()

    def delete_result(self, message, ttl: int):
        key = self._key(message.message_id)
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM tasks WHERE key = %s", (key,))

    def create_pending(self, message_id: str, client_id: str, intent: str):
        """Insert a PENDING row so the task is visible in `tasks` before the worker picks it up."""
        key = self._key(message_id)
        sql = """
            INSERT INTO tasks (key, status, client_id, intent)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (key) DO NOTHING
        """
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (key, STATUS_PENDING, client_id, intent))

    def all_tasks_for_client(self, client_id: str) -> list[dict]:
        sql = """
            SELECT key, status, result, intent, created_at
            FROM tasks
            WHERE client_id = %s
            ORDER BY created_at ASC
        """
        with _pg_connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (client_id,))
                return cur.fetchall()


# ── task_queue helpers ────────────────────────────────────────────────────────

def tq_get_result(message_id: str | None) -> dict | None:
    """
    Return the result payload from the `tasks` table for a given message_id,
    or None if not found or message_id is None.
    Used by the client watcher to display SAP result details on completion.
    """
    if not message_id:
        return None
    key = f"dramatiq:{message_id}"
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT result FROM tasks WHERE key = %s", (key,))
            row = cur.fetchone()
            return dict(row[0]) if row and row[0] else None

def tq_push(client_id: str, task_id: str, intent: str) -> int:
    """
    Append a QUEUED entry for this client and immediately publish it to
    RabbitMQ via enqueue_task().

    Returns the new row's `id` (insertion order = logical execution order).
    """
    sql = """
        INSERT INTO task_queue (client_id, task_id, intent, status)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (client_id, task_id, intent, QS_QUEUED))
            row_id = cur.fetchone()[0]

    # Publish to RabbitMQ — import here to avoid circular import at module load.
    from task_queue import enqueue_task
    message = enqueue_task(client_id, task_id, intent)

    # Record the Dramatiq message_id so we can look it up if needed.
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE task_queue SET message_id=%s WHERE id=%s",
                (message.message_id, row_id),
            )

    return row_id


def tq_get_by_task_id(task_id: str) -> dict | None:
    """Return the task_queue row for *task_id*, or None if not found."""
    sql = """
        SELECT id, client_id, task_id, intent, status, message_id, created_at
        FROM task_queue
        WHERE task_id = %s
    """
    with _pg_connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (task_id,))
            return cur.fetchone()


def tq_mark_running(conn, row_id: int, message_id: str):
    """Flip a QUEUED row to RUNNING (must be called inside an open transaction)."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE task_queue SET status=%s, message_id=%s, updated_at=NOW() WHERE id=%s",
            (QS_RUNNING, message_id, row_id),
        )


def tq_mark_done(row_id: int):
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE task_queue SET status=%s, updated_at=NOW() WHERE id=%s",
                (QS_DONE, row_id),
            )


def tq_mark_error(row_id: int):
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE task_queue SET status=%s, updated_at=NOW() WHERE id=%s",
                (QS_ERROR, row_id),
            )


def tq_cancel_queued(client_id: str) -> int:
    """
    Cancel every QUEUED row for this client.
    Returns the count of rows cancelled.
    """
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE task_queue SET status=%s, updated_at=NOW() "
                "WHERE client_id=%s AND status=%s",
                (QS_CANCELLED, client_id, QS_QUEUED),
            )
            return cur.rowcount


def tq_get_running(client_id: str) -> dict | None:
    """Return the single RUNNING row for this client, or None."""
    sql = """
        SELECT id, task_id, intent, message_id
        FROM task_queue
        WHERE client_id = %s AND status = %s
        LIMIT 1
    """
    with _pg_connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (client_id, QS_RUNNING))
            return cur.fetchone()


def tq_all(client_id: str) -> list[dict]:
    """All task_queue rows for this client, ordered by insertion order."""
    sql = """
        SELECT id, task_id, intent, status, message_id, created_at
        FROM task_queue
        WHERE client_id = %s
        ORDER BY id ASC
    """
    with _pg_connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (client_id,))
            return cur.fetchall()


# Kept for backward compatibility — no longer used by tq_push.
def tq_pop_next(conn, client_id: str) -> dict | None:
    """
    Within an already-open transaction, lock and return the oldest QUEUED row.
    Retained for any callers that still use it directly.
    """
    sql = """
        SELECT id, task_id, intent
        FROM task_queue
        WHERE client_id = %s AND status = %s
        ORDER BY id ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (client_id, QS_QUEUED))
        return cur.fetchone()