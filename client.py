"""
SAP Integration Client — scalable multi-client version.

Each client writes tasks directly to the `task_queue` table in Postgres AND
immediately publishes them to RabbitMQ (via tq_push → enqueue_task).

RabbitMQ enforces sequential execution via prefetch_count=1: the worker only
receives the next message after it has fully processed and ack'd the current one.

queue_manager.py is no longer required — you only need two processes per client:

    # Terminal 1 — worker (consumes from RabbitMQ, updates Postgres)
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
    tq_push,
    tq_all,
    tq_get_result,
    QS_QUEUED,
    QS_RUNNING,
    QS_DONE,
    QS_ERROR,
    QS_CANCELLED,
)

WATCHER_POLL_INTERVAL = 3  # seconds between DB polls in the watcher thread

_TERMINAL = {QS_DONE, QS_ERROR, QS_CANCELLED}

_QUEUE_LABEL = {
    QS_QUEUED:    "QUEUED   ",
    QS_RUNNING:   "RUNNING  ",
    QS_DONE:      "DONE     ",
    QS_ERROR:     "ERROR    ",
    QS_CANCELLED: "CANCELLED",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SAP Integration Client")
    parser.add_argument("--client-id", required=True)
    return parser.parse_args()


class SAPClient:
    def __init__(self, client_id: str):
        self.client_id  = client_id
        self._known: dict[str, str] = {}
        self._known_lock = threading.Lock()

    # ── Background watcher ────────────────────────────────────────────────────

    def _watcher(self) -> None:
        """
        Polls task_queue every WATCHER_POLL_INTERVAL seconds.
        Prints a notification on any terminal status transition.
        """
        while True:
            time.sleep(WATCHER_POLL_INTERVAL)
            try:
                rows = tq_all(self.client_id)
            except Exception:
                continue

            with self._known_lock:
                for row in rows:
                    task_id = row["task_id"]
                    status  = row["status"]
                    prev    = self._known.get(task_id)

                    if prev == status:
                        continue
                    self._known[task_id] = status

                    if status == QS_DONE:
                        result = tq_get_result(row["message_id"]) or {}
                        duration = result.get("duration", "?")
                        sap_id   = result.get("id", "?")
                        print(
                            f"\n[✔] Task {task_id!r} DONE  |  {row['intent']!r}"
                            f"\n    SAP id={sap_id}  duration={duration}s\n> ",
                            end="", flush=True,
                        )
                    elif status == QS_ERROR:
                        result = tq_get_result(row["message_id"]) or {}
                        reason = result.get("error", "unknown")
                        print(
                            f"\n[✗] Task {task_id!r} FAILED  |  {row['intent']!r}"
                            f"\n    reason: {reason}\n> ",
                            end="", flush=True,
                        )
                    elif status == QS_CANCELLED:
                        print(
                            f"\n[–] Task {task_id!r} CANCELLED  |  {row['intent']!r}\n> ",
                            end="", flush=True,
                        )

    def _start_watcher(self) -> None:
        rows = tq_all(self.client_id)
        with self._known_lock:
            for row in rows:
                self._known[row["task_id"]] = row["status"]

        t = threading.Thread(
            target=self._watcher, daemon=True, name=f"watcher-{self.client_id}"
        )
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

            # tq_push writes the Postgres row AND publishes to RabbitMQ.
            task_id = str(uuid.uuid4())[:8]
            row_id  = tq_push(self.client_id, task_id, user_input)
            print(
                f"[Client:{self.client_id}] Task {task_id!r} queued "
                f"(position #{row_id}): {user_input!r}\n"
            )


if __name__ == "__main__":
    args = parse_args()
    SAPClient(client_id=args.client_id).run()