"""
SAP Integration Client.

Each client enqueues tasks by:
  1. Publishing to RabbitMQ via enqueue_task() (returns a message_id).
  2. Writing a single QUEUED row to `tasks` with that message_id attached.

A background watcher polls `tasks` every few seconds and prints a notification
whenever a task reaches a terminal status (DONE / ERROR / CANCELLED).

Usage
-----
    # Terminal 1 — worker
    python task_queue.py --client-id alice

    # Terminal 2 — client REPL
    python client.py --client-id alice

Multiple clients are fully independent:
    python task_queue.py --client-id bob
    python client.py --client-id bob
"""

import argparse
import threading
import time
import uuid

from postgres_backend import (
    task_create,
    task_all,
    STATUS_QUEUED,
    STATUS_RUNNING,
    STATUS_DONE,
    STATUS_ERROR,
    STATUS_CANCELLED,
)
from task_queue import enqueue_task

WATCHER_POLL_INTERVAL = 3  # seconds between DB polls

_TERMINAL = {STATUS_DONE, STATUS_ERROR, STATUS_CANCELLED}

_STATUS_LABEL = {
    STATUS_QUEUED:    "QUEUED   ",
    STATUS_RUNNING:   "RUNNING  ",
    STATUS_DONE:      "DONE     ",
    STATUS_ERROR:     "ERROR    ",
    STATUS_CANCELLED: "CANCELLED",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SAP Integration Client")
    parser.add_argument("--client-id", required=True)
    return parser.parse_args()


class SAPClient:
    def __init__(self, client_id: str):
        self.client_id   = client_id
        self._known: dict[str, str] = {}
        self._known_lock = threading.Lock()

    # ── Background watcher ────────────────────────────────────────────────────

    def _watcher(self) -> None:
        """Poll tasks every WATCHER_POLL_INTERVAL seconds; print on status change."""
        while True:
            time.sleep(WATCHER_POLL_INTERVAL)
            try:
                rows = task_all(self.client_id)
            except Exception:
                continue

            with self._known_lock:
                for row in rows:
                    task_id = row["task_id"]
                    status  = row["status"]

                    if self._known.get(task_id) == status:
                        continue
                    self._known[task_id] = status

                    if status == STATUS_DONE:
                        result   = row.get("result") or {}
                        duration = result.get("duration", "?")
                        sap_id   = result.get("id", "?")
                        print(
                            f"\n[✔] Task {task_id!r} DONE  |  {row['intent']!r}"
                            f"\n    SAP id={sap_id}  duration={duration}s\n> ",
                            end="", flush=True,
                        )
                    elif status == STATUS_ERROR:
                        result = row.get("result") or {}
                        reason = result.get("error", "unknown")
                        print(
                            f"\n[✗] Task {task_id!r} FAILED  |  {row['intent']!r}"
                            f"\n    reason: {reason}\n> ",
                            end="", flush=True,
                        )
                    elif status == STATUS_CANCELLED:
                        print(
                            f"\n[–] Task {task_id!r} CANCELLED  |  {row['intent']!r}\n> ",
                            end="", flush=True,
                        )

    def _start_watcher(self) -> None:
        # Seed known statuses so we don't fire spurious notifications on startup.
        rows = task_all(self.client_id)
        with self._known_lock:
            for row in rows:
                self._known[row["task_id"]] = row["status"]

        t = threading.Thread(
            target=self._watcher, daemon=True, name=f"watcher-{self.client_id}"
        )
        t.start()

    # ── Status display ────────────────────────────────────────────────────────

    def print_status(self) -> None:
        rows = task_all(self.client_id)
        if not rows:
            print(f"[Status:{self.client_id}] No tasks found.\n")
            return
        print(f"\n[Status:{self.client_id}] {len(rows)} task(s):\n")
        for row in rows:
            label = _STATUS_LABEL.get(row["status"], row["status"])
            print(f"  [{label}]  {row['task_id']}  |  {row['intent']!r}")
        print()

    # ── Enqueue ───────────────────────────────────────────────────────────────

    def submit(self, user_input: str) -> str:
        """
        Publish to RabbitMQ, then write a single QUEUED row to `tasks`.
        Returns the task_id.
        """
        task_id = str(uuid.uuid4())[:8]

        # 1. Publish to RabbitMQ first — get back the Dramatiq message_id.
        message = enqueue_task(self.client_id, task_id, user_input)

        # 2. Write one row to tasks with message_id already attached.
        row_id = task_create(self.client_id, task_id, user_input, message.message_id)

        with self._known_lock:
            self._known[task_id] = STATUS_QUEUED

        print(
            f"[Client:{self.client_id}] Task {task_id!r} queued "
            f"(position #{row_id}): {user_input!r}\n"
        )
        return task_id

    # ── Main REPL ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        print(f"\nSAP Integration Client: {self.client_id!r}")
        print(f"  Worker   : python task_queue.py --client-id {self.client_id}")
        print("  Commands : type any text to enqueue | 'status' | 'quit'\n")
        print("  Note     : tasks execute sequentially (RabbitMQ prefetch=1).")
        print("             A SAP failure cancels all queued tasks behind it.\n")

        self._start_watcher()

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

            self.submit(user_input)


if __name__ == "__main__":
    args = parse_args()
    SAPClient(client_id=args.client_id).run()