"""
queue_manager.py — Per-client durable queue manager.

Run one instance per client alongside the Dramatiq worker:

    python queue_manager.py --client-id alice
    python queue_manager.py --client-id bob

Responsibilities
----------------
  1. Pull the next QUEUED task from `task_queue` (oldest id first, in-order).
  2. Dispatch it to Dramatiq via enqueue_task() — atomically marks it RUNNING.
  3. Poll `tasks` for the Dramatiq result.
  4. On SUCCESS  → mark row DONE, loop back to step 1.
  5. On FAILURE  → mark row ERROR, cancel ALL remaining QUEUED rows for this
                   client (so nothing executes until the client submits new work).

Crash recovery
--------------
If the manager is killed while a task is RUNNING, on restart it detects the
orphaned RUNNING row, re-attaches to the in-flight Dramatiq message, and
resumes waiting — no task is double-dispatched or silently dropped.

Concurrency
-----------
tq_pop_next() uses SELECT FOR UPDATE SKIP LOCKED, so it is safe to run
multiple manager processes for the same client_id (only one will win the lock).
In practice, one manager per client is enough.
"""

import argparse
import time

import psycopg2
import psycopg2.extras

from postgres_backend import (
    _pg_connect,
    PostgresBackend,
    STATUS_SUCCESS,
    STATUS_ERROR,
    STATUS_PENDING,
    QS_RUNNING,
    tq_pop_next,
    tq_mark_running,
    tq_mark_done,
    tq_mark_error,
    tq_cancel_queued,
    tq_get_running,
)
from task_queue import enqueue_task

# How long to sleep when the queue is empty (seconds)
IDLE_POLL_INTERVAL = 2

# How long to wait for a single task result before treating it as a timeout-failure
RESULT_TIMEOUT_SEC = 180

_backend = PostgresBackend()


# ── Entry point ───────────────────────────────────────────────────────────────

def run(client_id: str) -> None:
    print(f"[QueueManager:{client_id}] Started.")

    # ── Crash recovery: re-attach to any task that was RUNNING when we died ──
    running = tq_get_running(client_id)
    if running:
        print(
            f"[QueueManager:{client_id}] Recovering orphaned RUNNING task "
            f"{running['task_id']} (message {running['message_id']})"
        )
        _resume_running(client_id, running)

    # ── Main dispatch loop ────────────────────────────────────────────────────
    while True:
        dispatched = _try_dispatch_next(client_id)
        if not dispatched:
            time.sleep(IDLE_POLL_INTERVAL)


# ── Dispatch ──────────────────────────────────────────────────────────────────

def _try_dispatch_next(client_id: str) -> bool:
    """
    Attempt to pop and dispatch the next QUEUED task.
    Returns True if a task was dispatched, False if the queue was empty.

    The SELECT FOR UPDATE and the status→RUNNING update happen in the same
    transaction, so no other manager process can dispatch the same row.
    """
    conn = _pg_connect()
    try:
        row = tq_pop_next(conn, client_id)
        if row is None:
            conn.rollback()
            return False

        row_id  = row["id"]
        task_id = row["task_id"]
        intent  = row["intent"]

        # Dispatch to Dramatiq — this sends the message to RabbitMQ and writes
        # a PENDING row to `tasks`.
        message = enqueue_task(client_id, task_id, intent)

        # Atomically flip status to RUNNING and record the message_id.
        tq_mark_running(conn, row_id, message.message_id)
        conn.commit()

        print(f"[QueueManager:{client_id}] Dispatched task {task_id!r} → message {message.message_id}")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    _wait_for_result(client_id, row_id, task_id, message)
    return True


# ── Result waiting ────────────────────────────────────────────────────────────

def _wait_for_result(client_id: str, row_id: int, task_id: str, message) -> None:
    """
    Block until the Dramatiq result is written to `tasks`, then advance or
    fail the queue accordingly.
    """
    key      = _backend._key(message.message_id)
    deadline = time.monotonic() + RESULT_TIMEOUT_SEC
    interval = 2.0

    print(f"[QueueManager:{client_id}] Waiting for task {task_id!r}…")

    while True:
        if time.monotonic() > deadline:
            print(
                f"[QueueManager:{client_id}] Task {task_id!r} TIMED OUT "
                f"after {RESULT_TIMEOUT_SEC}s — treating as failure."
            )
            _on_failure(client_id, row_id, task_id)
            return

        row = _backend._fetch_row(key)
        if row is not None:
            status, payload = row
            if status == STATUS_SUCCESS:
                tq_mark_done(row_id)
                dur = (payload or {}).get("duration", "?")
                print(f"[QueueManager:{client_id}] Task {task_id!r} DONE (duration={dur}s).")
                return
            elif status == STATUS_ERROR:
                err = (payload or {}).get("error", "unknown")
                print(f"[QueueManager:{client_id}] Task {task_id!r} FAILED: {err}")
                _on_failure(client_id, row_id, task_id)
                return
            # STATUS_PENDING → keep polling

        time.sleep(interval)


def _on_failure(client_id: str, row_id: int, task_id: str) -> None:
    """Mark the failed task ERROR and cancel every QUEUED task behind it."""
    tq_mark_error(row_id)
    n = tq_cancel_queued(client_id)
    print(
        f"[QueueManager:{client_id}] Task {task_id!r} marked ERROR — "
        f"{n} downstream task(s) CANCELLED."
    )


# ── Crash recovery helper ─────────────────────────────────────────────────────

def _resume_running(client_id: str, running: dict) -> None:
    """
    Re-attach to an orphaned RUNNING task after a manager restart.
    Constructs a minimal message proxy so _wait_for_result can poll `tasks`.
    """
    class _MessageProxy:
        """Minimal shim so _wait_for_result can call _backend._key(message.message_id)."""
        def __init__(self, message_id: str):
            self.message_id = message_id

    proxy = _MessageProxy(running["message_id"])
    _wait_for_result(client_id, running["id"], running["task_id"], proxy)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Per-client SAP task queue manager")
    parser.add_argument("--client-id", required=True, help="Client ID to manage (e.g. alice)")
    args = parser.parse_args()
    run(args.client_id)