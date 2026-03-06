"""
postgres_backend.py — Dramatiq result backend backed by PostgreSQL.

Uses the `tasks` table:

  CREATE TABLE tasks (
    key        VARCHAR(512) PRIMARY KEY,
    status     INT          NOT NULL,          -- 0=SUCCESS, 1=PENDING, 2=ERROR
    client_id  VARCHAR(512) NOT NULL,
    result     JSONB        NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
  );

Status constants:
  0 = SUCCESS
  1 = PENDING
  2 = ERROR

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

STATUS_SUCCESS = 0
STATUS_PENDING = 1
STATUS_ERROR   = 2


# ── DB connection ─────────────────────────────────────────────────────────────

def _pg_connect():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        dbname=os.getenv("PG_DB", "mydb"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
    )


# ── Backend ───────────────────────────────────────────────────────────────────

class PostgresBackend(ResultBackend):
    """
    Dramatiq ResultBackend that persists results to the PostgreSQL `tasks` table.

    The `key` column holds the Dramatiq message_id (prefixed with namespace).
    The `client_id` column is extracted from the actor's queue name (sap_tasks.<client_id>).
    """

    def __init__(self, *, namespace: str = "dramatiq"):
        super().__init__(namespace=namespace)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _key(self, message_id: str) -> str:
        return f"{self.namespace}:{message_id}"

    @staticmethod
    def _client_id_from_message(message) -> str:
        """Extract the client_id from the actor's queue name (sap_tasks.<client_id>)."""
        queue: str = getattr(message, "queue_name", "") or ""
        if queue.startswith("sap_tasks."):
            return queue[len("sap_tasks."):]
        return queue or "unknown"

    # ── ResultBackend interface ───────────────────────────────────────────────

    def store_result(self, message, result, ttl: int):
        """
        Called by the worker when a task finishes (success or failure).
        Upserts the row: sets status=SUCCESS/ERROR and writes the result payload.
        """
        key       = self._key(message.message_id)
        client_id = self._client_id_from_message(message)

        is_error = isinstance(result, Exception)
        status   = STATUS_ERROR if is_error else STATUS_SUCCESS
        payload  = {"error": str(result)} if is_error else result

        sql = """
            INSERT INTO tasks (key, status, client_id, result)
            VALUES (%(key)s, %(status)s, %(client_id)s, %(result)s)
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
                    "result":    psycopg2.extras.Json(payload),
                })

    def get_result(self, message, *, block: bool = False, timeout: int | None = None):
        """
        Retrieve the result for *message*.

        Polls the `tasks` table until status is no longer PENDING.

        Args:
            block:   If True, keep polling until a result appears or timeout elapses.
            timeout: Milliseconds to wait when block=True (None → wait forever).

        Raises:
            ResultMissing: Row absent or still PENDING (non-blocking call).
            ResultTimeout: block=True and timeout elapsed with no result.
        """
        key           = self._key(message.message_id)
        deadline      = (time.monotonic() + timeout / 1000.0) if (block and timeout is not None) else None
        poll_interval = 1.0  # seconds

        while True:
            row = self._fetch_row(key)

            if row is not None:
                status, result_payload = row
                if status != STATUS_PENDING:
                    return result_payload

            # Row missing or still PENDING
            if not block:
                raise ResultMissing(message)

            if deadline is not None and time.monotonic() >= deadline:
                raise ResultTimeout(message)

            time.sleep(poll_interval)

    def _fetch_row(self, key: str):
        """Returns (status, result_dict) if the row exists, otherwise None."""
        sql = "SELECT status, result FROM tasks WHERE key = %s"
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (key,))
                return cur.fetchone()  # None if not found

    def delete_result(self, message, ttl: int):
        """Remove the result row (called by Dramatiq on ack after retrieval)."""
        key = self._key(message.message_id)
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM tasks WHERE key = %s", (key,))

    # ── Convenience: create a PENDING row when a task is first enqueued ───────

    def create_pending(self, message_id: str, client_id: str, intent: str):
        """
        Insert a PENDING row immediately when a task is dispatched.
        This makes the task visible in the DB before the worker picks it up.
        Call this from backend.py's enqueue_task() after actor.send().
        """
        key = self._key(message_id)
        sql = """
            INSERT INTO tasks (key, status, client_id, intent)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (key) DO NOTHING
        """
        with _pg_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (key, STATUS_PENDING, client_id, intent))