"""
task_queue.py — Worker entry point with RabbitMQ-enforced sequential execution.

Architecture
------------
prefetch_count=1 is set on the RabbitMQ channel so the broker never delivers
a second message to a worker until the first one is fully ack'd.  This makes
RabbitMQ the sequencer — no external polling loop is needed for ordering.

Each client gets its own queue:  sap_tasks.<client_id>

Failure cascade
---------------
If the SAP call fails, the worker:
  1. Marks the task row ERROR in Postgres.
  2. Cancels every remaining QUEUED row for that client in Postgres.
  3. Purges the client's RabbitMQ queue so those messages are never delivered.

This guarantees a single SAP failure stops all downstream work for that client
without leaving stale messages in the broker.

Usage
-----
    python task_queue.py --client-id alice
    python task_queue.py --client-id bob   # independent queue, same guarantees
"""

import logging
import os
import sys

import dramatiq
import pika
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.results import Results

logging.getLogger("dramatiq.worker.WorkerThread").setLevel(logging.CRITICAL)
logging.getLogger("dramatiq.middleware.retries.Retries").setLevel(logging.CRITICAL)

from postgres_backend import (
    PostgresBackend,
    task_get_by_task_id,
    task_mark_running,
    task_mark_done,
    task_mark_error,
    task_cancel_queued,
    STATUS_CANCELLED,
)
from mcp_server import sync_to_sap

# ── Config ────────────────────────────────────────────────────────────────────

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672")

# ── Broker + result backend ───────────────────────────────────────────────────

result_backend = PostgresBackend()

broker = RabbitmqBroker(url=RABBITMQ_URL)
broker.add_middleware(Results(backend=result_backend))
dramatiq.set_broker(broker)

# Enforce prefetch_count=1 — the single most important line for sequential execution.
_orig_consume = broker.consume


def _consume_with_prefetch1(queue_name, prefetch=1, timeout=5000):
    return _orig_consume(queue_name, prefetch=1, timeout=timeout)


broker.consume = _consume_with_prefetch1

# ── Actor registry ────────────────────────────────────────────────────────────

_actor_registry: dict = {}


def make_actor(client_id: str):
    """
    Return the Dramatiq actor for *client_id*, creating it at most once.

    On success  → mark DONE, ack (implicit on return), RabbitMQ delivers next.
    On failure  → mark ERROR, cascade cancel + purge, re-raise.
    """
    if client_id in _actor_registry:
        return _actor_registry[client_id]

    queue_name = f"sap_tasks.{client_id}"

    @dramatiq.actor(
        store_results=True,
        max_retries=0,
        queue_name=queue_name,
        actor_name=f"process_integration_task_{client_id}",
    )
    def process_integration_task(task_id: str, input_data: str):
        print(f"[Worker:{client_id}] Picked up task {task_id!r}: {input_data!r}")

        row = task_get_by_task_id(task_id)

        # Guard: skip tasks already cancelled by a previous failure cascade.
        if row and row["status"] == STATUS_CANCELLED:
            print(f"[Worker:{client_id}] Task {task_id!r} already CANCELLED — skipping.")
            return {"task_id": task_id, "skipped": True}

        task_mark_running(task_id)

        try:
            result = sync_to_sap(input_data)
            result["task_id"] = task_id
            print(f"[Worker:{client_id}] Task {task_id!r} DONE: {result}")
            task_mark_done(task_id, result)
            return result

        except Exception as exc:
            print(f"[Worker:{client_id}] FAILED task {task_id!r}: {exc}")
            task_mark_error(task_id, str(exc))
            _cascade_failure(client_id, queue_name)
            raise

    _actor_registry[client_id] = process_integration_task
    return process_integration_task


def _cascade_failure(client_id: str, queue_name: str) -> None:
    """
    Cancel all remaining QUEUED tasks in Postgres and purge the RabbitMQ queue.
    """
    n = task_cancel_queued(client_id)
    print(f"[Worker:{client_id}] Cascade: {n} task(s) cancelled in Postgres.")

    try:
        params   = pika.URLParameters(RABBITMQ_URL)
        pika_con = pika.BlockingConnection(params)
        ch       = pika_con.channel()
        result   = ch.queue_purge(queue=queue_name)
        pika_con.close()
        msg_count = result.method.message_count
        print(
            f"[Worker:{client_id}] Cascade: purged {queue_name!r} "
            f"({msg_count} message(s) removed from RabbitMQ)."
        )
    except Exception as purge_exc:
        # Non-fatal: Postgres is already consistent. Messages that slip through
        # will find their rows CANCELLED and be skipped by the guard above.
        print(
            f"[Worker:{client_id}] WARNING: could not purge RabbitMQ queue "
            f"{queue_name!r}: {purge_exc}"
        )


# ── Public dispatcher (used by client.py) ─────────────────────────────────────

def enqueue_task(client_id: str, task_id: str, user_input: str) -> object:
    """
    Publish one message to this client's RabbitMQ queue.
    Returns the Dramatiq message object (caller uses message.message_id).
    """
    actor   = make_actor(client_id)
    message = actor.send(task_id, user_input)
    print(f"[Dispatcher:{client_id}] Task {task_id!r} → message {message.message_id}")
    return message


# ── Module-level actor registration (required for Dramatiq forked workers) ────

_client_id_env = os.environ.get("DRAMATIQ_CLIENT_ID")
if _client_id_env:
    make_actor(_client_id_env)


# ── Worker entry point ────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SAP task worker")
    parser.add_argument("--client-id", required=True)
    args = parser.parse_args()

    os.environ["DRAMATIQ_CLIENT_ID"] = args.client_id
    actor = make_actor(args.client_id)

    print(f"[Worker] Client={args.client_id!r}  queue={actor.queue_name}")
    print("[Worker] prefetch_count=1 · processes=1 · threads=1 → strict sequential execution")

    sys.argv = [
        "dramatiq", "task_queue",
        "--queues",    actor.queue_name,
        "--processes", "1",
        "--threads",   "1",
    ]
    from dramatiq.cli import main
    main()