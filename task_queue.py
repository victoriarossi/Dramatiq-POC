"""
task_queue.py — Worker entry point with RabbitMQ-enforced sequential execution.

Architecture
------------
prefetch_count=1 is set on the RabbitMQ channel so the broker never delivers
a second message to a worker until the first one is fully ack'd.  This makes
RabbitMQ the sequencer — the queue_manager.py polling loop is no longer needed
for ordering.

Each client gets its own queue:  sap_tasks.<client_id>

Failure cascade
---------------
If the SAP call fails (after all retries are exhausted), the worker:
  1. Marks the task_queue row ERROR in Postgres.
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

# Dramatiq logs ERROR + traceback on every actor exception and WARNING on retry
# exhaustion. These are expected when SAP fails and the cascade fires — our own
# print statements already communicate what happened, so suppress the noise.
logging.getLogger("dramatiq.worker.WorkerThread").setLevel(logging.CRITICAL)
logging.getLogger("dramatiq.middleware.retries.Retries").setLevel(logging.CRITICAL)

from postgres_backend import (
    PostgresBackend,
    tq_mark_done,
    tq_mark_error,
    tq_cancel_queued,
    tq_get_by_task_id,
    QS_CANCELLED,
)
from mcp_server import sync_to_sap

# ── Config ────────────────────────────────────────────────────────────────────

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672")

# ── Broker + result backend ───────────────────────────────────────────────────
#
# We override `consume` to always pass prefetch=1.  Dramatiq's RabbitmqBroker
# calls channel.basic_qos(prefetch_count=prefetch) inside its consume() method,
# so intercepting at that level is the cleanest hook point.

result_backend = PostgresBackend()

broker = RabbitmqBroker(url=RABBITMQ_URL)
broker.add_middleware(Results(backend=result_backend))
dramatiq.set_broker(broker)

# Enforce prefetch_count=1 — the single most important line for sequential execution.
_orig_consume = broker.consume


def _consume_with_prefetch1(queue_name, prefetch=1, timeout=5000):
    """Wrap broker.consume to hard-code prefetch=1."""
    return _orig_consume(queue_name, prefetch=1, timeout=timeout)


broker.consume = _consume_with_prefetch1

# ── Actor registry ────────────────────────────────────────────────────────────

_actor_registry: dict = {}


def make_actor(client_id: str):
    """
    Return the Dramatiq actor for *client_id*, creating it at most once.

    The actor performs the SAP call and owns the failure cascade:
      • on success → mark DONE, ack (implicit on return), RabbitMQ delivers next
      • on failure → mark ERROR, cancel + purge remaining queue, re-raise
    """
    if client_id in _actor_registry:
        return _actor_registry[client_id]

    queue_name = f"sap_tasks.{client_id}"

    @dramatiq.actor(
        store_results=True,
        max_retries=0,          # no silent retries — a failure cascades immediately
        queue_name=queue_name,
        actor_name=f"process_integration_task_{client_id}",
    )
    def process_integration_task(task_id: str, input_data: str):
        print(f"[Worker:{client_id}] Picked up task {task_id!r}: {input_data!r}")

        row    = tq_get_by_task_id(task_id)
        row_id = row["id"] if row else None

        # Guard: if this row was already cancelled by a previous failure cascade,
        # skip the SAP call entirely and ack silently. This closes the race window
        # between _cascade_failure() cancelling Postgres rows and queue_purge()
        # removing the RabbitMQ messages — a fast worker could pick up a message
        # in that gap, so we must check here as a second line of defence.
        if row and row["status"] == QS_CANCELLED:
            print(f"[Worker:{client_id}] Task {task_id!r} already CANCELLED — skipping.")
            return {"task_id": task_id, "skipped": True}

        try:
            result = sync_to_sap(input_data)
            result["task_id"] = task_id
            print(f"[Worker:{client_id}] Task {task_id!r} DONE: {result}")

            if row_id is not None:
                tq_mark_done(row_id)

            # Implicit ack on clean return → RabbitMQ delivers next message.
            return result

        except Exception as exc:
            print(f"[Worker:{client_id}] FAILED task {task_id!r}: {exc}")

            if row_id is not None:
                tq_mark_error(row_id)

            _cascade_failure(client_id, queue_name)

            # Re-raise → Dramatiq records error result and nacks the message.
            raise

    _actor_registry[client_id] = process_integration_task
    return process_integration_task


def _cascade_failure(client_id: str, queue_name: str) -> None:
    """
    Cancel all remaining QUEUED tasks in Postgres and purge the RabbitMQ queue.
    Called inside the actor on any SAP failure.
    """
    # 1 — Postgres: mark every QUEUED row for this client CANCELLED.
    n = tq_cancel_queued(client_id)
    print(f"[Worker:{client_id}] Cascade: {n} task(s) cancelled in Postgres.")

    # 2 — RabbitMQ: purge the queue so those messages are never delivered.
    #     We open a fresh blocking pika connection to avoid touching Dramatiq's
    #     internal channel state.
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
        # Non-fatal: Postgres is already consistent.  The messages will be
        # delivered but the worker will find their task_queue rows CANCELLED
        # and skip them.
        print(
            f"[Worker:{client_id}] WARNING: could not purge RabbitMQ queue "
            f"{queue_name!r}: {purge_exc}"
        )


# ── Public dispatcher (used by client.py) ─────────────────────────────────────

def enqueue_task(client_id: str, task_id: str, user_input: str) -> object:
    """
    Publish one message to this client's RabbitMQ queue and record a PENDING
    row in `tasks`.

    With prefetch_count=1 the broker holds back this message until the worker
    has ack'd every message ahead of it — giving strict FIFO sequential
    execution with zero polling overhead.
    """
    actor   = make_actor(client_id)
    message = actor.send(task_id, user_input)
    result_backend.create_pending(message.message_id, client_id, user_input)
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

    # --processes 1 --threads 1: exactly one consumer in flight at a time.
    # Combined with prefetch_count=1 this means at most one SAP call per client.
    sys.argv = [
        "dramatiq", "task_queue",
        "--queues",    actor.queue_name,
        "--processes", "1",
        "--threads",   "1",
    ]
    from dramatiq.cli import main
    main()