"""
task_queue.py — Merged actor registry, task dispatcher, and worker entry point.

Usage (worker):
    python task_queue.py --client-id alice

Responsibilities:
  - Configure the Dramatiq broker and result backend (once on import)
  - Register per-client actors lazily (cached in _actor_registry)
  - Expose enqueue_task() for use by queue_manager.py
  - Serve as the module Dramatiq imports when forking worker processes

Note: enqueue_task() no longer calls create_pending(). The queue_manager
owns all task_queue lifecycle; create_pending() is kept on PostgresBackend
for any direct callers that still want it.
"""

import os
import sys

import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.results import Results

from postgres_backend import PostgresBackend
from mcp_server import sync_to_sap

# ── Broker + backend (runs once on import) ────────────────────────────────────

result_backend = PostgresBackend()
broker = RabbitmqBroker(url="amqp://guest:guest@localhost:5672")
broker.add_middleware(Results(backend=result_backend))
dramatiq.set_broker(broker)

# ── Actor registry ────────────────────────────────────────────────────────────

_actor_registry: dict = {}


def make_actor(client_id: str):
    """
    Return the Dramatiq actor for *client_id*, creating it only once.
    Re-using the same instance ensures worker and sender share the same
    registered function name and queue.
    """
    if client_id in _actor_registry:
        return _actor_registry[client_id]

    queue_name = f"sap_tasks.{client_id}"

    @dramatiq.actor(
        store_results=True,
        max_retries=3,
        queue_name=queue_name,
        actor_name=f"process_integration_task_{client_id}",
    )
    def process_integration_task(task_id: str, input_data: str):
        print(f"[Worker:{client_id}] Picked up task {task_id}: {input_data}")
        try:
            result = sync_to_sap(input_data)
            result["task_id"] = task_id
            print(f"[Worker:{client_id}] Task {task_id} complete: {result}")
            return result
        except Exception as e:
            print(f"[Worker:{client_id}] ERROR on task {task_id}: {e}. Retrying…")
            raise

    _actor_registry[client_id] = process_integration_task
    return process_integration_task


# ── Task dispatcher ───────────────────────────────────────────────────────────

def enqueue_task(client_id: str, task_id: str, user_input: str) -> object:
    """
    Send a message to this client's Dramatiq queue and return the message
    object so the caller (queue_manager) can track it.

    The queue_manager is responsible for writing / updating task_queue rows.
    The `tasks` PENDING row is written by PostgresBackend.create_pending(),
    which the manager calls after this function returns.
    """
    actor   = make_actor(client_id)
    message = actor.send(task_id, user_input)
    result_backend.create_pending(message.message_id, client_id, user_input)
    print(f"[Dispatcher:{client_id}] Task {task_id} → message {message.message_id}")
    return message


# ── Module-level actor registration for forked worker processes ───────────────
# When Dramatiq forks workers it re-imports this module. The actor must be
# registered at import time so forked processes know how to handle messages.

_client_id = os.environ.get("DRAMATIQ_CLIENT_ID")
if _client_id:
    make_actor(_client_id)


# ── Worker entry point ────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SAP task worker")
    parser.add_argument("--client-id", required=True, help="Client ID to consume tasks for")
    args = parser.parse_args()

    os.environ["DRAMATIQ_CLIENT_ID"] = args.client_id
    actor = make_actor(args.client_id)

    print(f"[Worker] Registered actor for {args.client_id!r} on queue: {actor.queue_name}")

    sys.argv = ["dramatiq", "task_queue", "--queues", actor.queue_name]
    from dramatiq.cli import main
    main()