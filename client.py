"""
SAP Integration Client — multi-instance version.

Each client gets its own isolated RabbitMQ queue named after its client-id.
Task state is read from the `tasks` table in Postgres — no local state dict.

Usage:
    python client.py --client-id alice
    python client.py --client-id bob
"""

import argparse
import uuid
import threading
import time
import queue

from task_queue import enqueue_task
from postgres_backend import PostgresBackend, STATUS_PENDING, STATUS_SUCCESS, STATUS_ERROR
from dramatiq.results.errors import ResultTimeout

POLL_INTERVAL = 5    # seconds between each poll attempt
SAP_TIMEOUT   = 180  # 3 minutes — report timeout beyond this

_STATUS_LABEL = {
    STATUS_SUCCESS: "DONE",
    STATUS_PENDING: "PENDING",
    STATUS_ERROR:   "ERROR",
}

# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SAP Integration Client")
    parser.add_argument(
        "--client-id",
        required=True,
        help="Unique name for this client (e.g. alice, bob, client-1).",
    )
    return parser.parse_args()


# ── Client ────────────────────────────────────────────────────────────────────

class SAPClient:
    def __init__(self, client_id: str):
        self.client_id    = client_id
        self.local_queue  = queue.Queue()
        # Minimal local state: just task_id → input string, so we can print
        # the original input alongside DB-sourced status. Nothing else is stored locally.
        self._inputs: dict  = {}
        self._inputs_lock   = threading.Lock()
        self._db            = PostgresBackend()

    # ── DB helpers ────────────────────────────────────────────────────────────

    def _db_status(self, message_id: str) -> int | None:
        """Return the raw int status from the tasks table, or None if not found."""
        row = self._db._fetch_row(self._db._key(message_id))
        return row[0] if row else None

    def _db_result(self, message_id: str) -> dict | None:
        """Return the result payload from the tasks table, or None if not found."""
        row = self._db._fetch_row(self._db._key(message_id))
        return row[1] if row else None

    def _db_all_tasks(self) -> list[dict]:
        """Return all task rows for this client_id. """
        import psycopg2.extras
        from postgres_backend import _pg_connect
        sql = """
            SELECT key, status, result, intent, created_at
            FROM tasks
            WHERE client_id = %s
            ORDER BY created_at ASC
        """
        with _pg_connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (self.client_id,))
                return cur.fetchall()

    # ── Polling ───────────────────────────────────────────────────────────────

    def poll_until_done(self, task_id: str, message) -> None:
        start_time       = time.time()
        pending_printed  = False
        timeout_deadline = start_time + SAP_TIMEOUT

        with self._inputs_lock:
            input_preview = self._inputs.get(task_id, "?")

        while True:
            elapsed = time.time() - start_time

            if time.time() > timeout_deadline:
                # Drain and cancel queued tasks
                cancelled = []
                while not self.local_queue.empty():
                    try:
                        queued_id, queued_input = self.local_queue.get_nowait()
                        cancelled.append((queued_id, queued_input))
                        self.local_queue.task_done()
                    except queue.Empty:
                        break

                print(f"\n[✗] [{self.client_id}] Task {task_id} | {input_preview!r} | TIMEOUT")
                if cancelled:
                    print(f"    {len(cancelled)} queued task(s) cancelled:")
                    for cid, cinput in cancelled:
                        print(f"      • {cid} | {cinput!r}")
                print()
                return

            # Print PENDING warning after 30s, pulling status from DB
            if not pending_printed and elapsed >= 30:
                db_status = self._db_status(message.message_id)
                label = _STATUS_LABEL.get(db_status, "UNKNOWN")
                print(f"\n[…] [{self.client_id}] Task {task_id} | {input_preview!r} | {label}...\n")
                pending_printed = True

            try:
                result = message.get_result(block=True, timeout=POLL_INTERVAL * 1000)
                self._print_result(task_id, result)
                return

            except ResultTimeout:
                continue

            except Exception as e:
                print(f"\n[✗] [{self.client_id}] Task {task_id} FAILED: {e}\n")
                return

    def _print_result(self, task_id: str, result: dict) -> None:
        with self._inputs_lock:
            input_preview = self._inputs.get(task_id, "?")
        print(
            f"\n[✔] [{self.client_id}] Task {task_id} DONE"
            f"\n    Input   : {input_preview!r}"
            f"\n    Status  : {result.get('status')}"
            f"\n    Duration: {result.get('duration')}s\n"
        )

    # ── Dispatcher ────────────────────────────────────────────────────────────

    def dispatcher(self) -> None:
        while True:
            task_id, user_input = self.local_queue.get()
            message = enqueue_task(self.client_id, task_id, user_input)
            self._print_queue_position(task_id)
            self.poll_until_done(task_id, message)
            self.local_queue.task_done()

    def _print_queue_position(self, task_id: str) -> None:
        waiting = self.local_queue.qsize()
        with self._inputs_lock:
            input_preview = self._inputs.get(task_id, "?")
        if waiting:
            print(f"[Queue:{self.client_id}] Executing {task_id} | {input_preview!r} | {waiting} waiting.\n")
        else:
            print(f"[Queue:{self.client_id}] Executing {task_id} | {input_preview!r} | No tasks waiting.\n")

    # ── Status ────────────────────────────────────────────────────────────────

    def print_status(self) -> None:
        rows = self._db_all_tasks()
        if not rows:
            print(f"[Status:{self.client_id}] No tasks found in DB.")
            return

        print(f"\n[Status:{self.client_id}] {len(rows)} task(s):")
        for row in rows:
            # Strip namespace prefix from key to get message_id for display
            key    = row["key"]
            status = _STATUS_LABEL.get(row["status"], "UNKNOWN")
            result = row["result"] or {}
            intent = row["intent"]

            if row["status"] == STATUS_SUCCESS:
                dur = result.get("duration", "?")
                print(f" [{intent}] [{ status}]  {key} | duration={dur}s")
            elif row["status"] == STATUS_ERROR:
                err = result.get("error", "?")
                print(f" [{intent}] [{status}]  {key} | {err}")
            else:
                print(f" [{intent}] [{status}] {key}")
        print()

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        print(f"SAP Integration Client: {self.client_id!r}")
        print(f"  • Queue: sap_tasks.{self.client_id}")
        print("  • Tasks are executed strictly one at a time, in submission order.")
        print("  • Type any text and press Enter to enqueue a task.")
        print("  • Type 'status' to see all tasks.")
        print("  • Type 'quit' to exit.\n")

        t = threading.Thread(target=self.dispatcher, daemon=True, name=f"dispatcher-{self.client_id}")
        t.start()

        while True:
            try:
                user_input = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nExiting.")
                break

            if not user_input:
                continue
            if user_input.lower() == "quit":
                print("Goodbye.")
                break
            if user_input.lower() == "status":
                self.print_status()
                continue

            task_id = str(uuid.uuid4())[:8]
            with self._inputs_lock:
                self._inputs[task_id] = user_input

            self.local_queue.put((task_id, user_input))

            position = self.local_queue.qsize()
            if position == 0:
                print(f"[Client:{self.client_id}] Task {task_id} accepted — executing now.\n")
            else:
                print(f"[Client:{self.client_id}] Task {task_id} accepted — position {position} in queue.\n")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()
    SAPClient(client_id=args.client_id).run()