"""
SAP Integration Client — scalable multi-client version.

Each client writes tasks to the `task_queue` table in Postgres.
The queue_manager.py process for this client_id owns dispatch and ordering.
This process only handles user input and status display — no local queues,
no polling threads, no in-memory task state.

Usage:
    # Terminal 1 — worker
    python task_queue.py --client-id alice

    # Terminal 2 — queue manager
    python queue_manager.py --client-id alice

    # Terminal 3 — client REPL
    python client.py --client-id alice
"""

import argparse
import threading
import time
import uuid

from postgres_backend import (
    tq_push,
    tq_all,
    QS_QUEUED,
    QS_RUNNING,
    QS_DONE,
    QS_ERROR,
    QS_CANCELLED,)

WATCHER_POLL_INTERVAL = 3  # seconds between DB polls in the watcher thread

# Statuses that are terminal — once a task reaches one of these we stop watching it.
_TERMINAL = {QS_DONE, QS_ERROR, QS_CANCELLED}

# ── Status display labels ─────────────────────────────────────────────────────

_QUEUE_LABEL = {
    QS_QUEUED:    "QUEUED",
    QS_RUNNING:   "RUNNING",
    QS_DONE:      "DONE   ",
    QS_ERROR:     "ERROR  ",
    QS_CANCELLED: "CANCELLED",
}

# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SAP Integration Client")
    parser.add_argument(
        "--client-id",
        required=True,
        help="Unique name for this client instance (e.g. alice, bob).",
    )
    return parser.parse_args()


# ── Client ────────────────────────────────────────────────────────────────────

class SAPClient:
    def __init__(self, client_id: str):
        self.client_id = client_id
        # task_id → last known status; seeded on startup so we don't re-announce
        # tasks that already finished before this session started.
        self._known: dict[str, str] = {}
        self._known_lock = threading.Lock()

    # ── Background watcher ────────────────────────────────────────────────────

    def _watcher(self) -> None:
        """
        Polls task_queue every WATCHER_POLL_INTERVAL seconds.
        Prints a notification whenever a task's status changes to a terminal state.
        Removes tasks from the watch set once they reach a terminal state so the
        dict stays small even across long sessions.
        """
        while True:
            time.sleep(WATCHER_POLL_INTERVAL)
            try:
                rows = tq_all(self.client_id)
            except Exception:
                continue  # transient DB hiccup — just retry next cycle

            with self._known_lock:
                for row in rows:
                    task_id = row["task_id"]
                    status  = row["status"]
                    prev    = self._known.get(task_id)

                    if prev == status:
                        continue  # no change

                    self._known[task_id] = status

                    # Only announce transitions *into* a terminal state so the
                    # user isn't spammed on every QUEUED→RUNNING intermediate step.
                    if status == QS_DONE:
                        print(
                            f"\n[✔] Task {task_id!r} DONE  |  {row['intent']!r}\n> ",
                            end="", flush=True,
                        )
                    elif status == QS_ERROR:
                        print(
                            f"\n[✗] Task {task_id!r} FAILED  |  {row['intent']!r}\n> ",
                            end="", flush=True,
                        )
                    elif status == QS_CANCELLED:
                        print(
                            f"\n[–] Task {task_id!r} CANCELLED  |  {row['intent']!r}\n> ",
                            end="", flush=True,
                        )

    def _start_watcher(self) -> None:
        """Seed _known with current DB state, then launch the watcher thread."""
        rows = tq_all(self.client_id)
        with self._known_lock:
            for row in rows:
                self._known[row["task_id"]] = row["status"]

        t = threading.Thread(target=self._watcher, daemon=True, name=f"watcher-{self.client_id}")
        t.start()

    # ── Status ────────────────────────────────────────────────────────────────

    def print_status(self) -> None:
        rows = tq_all(self.client_id)
        if not rows:
            print(f"[Status:{self.client_id}] No tasks found.\n")
            return

        print(f"\n[Status:{self.client_id}] {len(rows)} task(s):\n")
        for row in rows:
            label = _QUEUE_LABEL.get(row["status"], row["status"])
            print(f"  [{label}]  {row['task_id']}  |  {row['intent']!r}")
        print()

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        print(f"\nSAP Integration Client: {self.client_id!r}")
        print(f"  Queue manager : python queue_manager.py --client-id {self.client_id}")
        print(f"  Worker        : python task_queue.py --client-id {self.client_id}")
        print("  Commands      : type any text to enqueue | 'status' | 'quit'\n")

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

            # Enqueue — just write a QUEUED row; queue_manager handles the rest.
            task_id = str(uuid.uuid4())[:8]
            row_id  = tq_push(self.client_id, task_id, user_input)
            print(f"[Client:{self.client_id}] Task {task_id!r} queued (position #{row_id}): {user_input!r}\n")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()
    SAPClient(client_id=args.client_id).run()