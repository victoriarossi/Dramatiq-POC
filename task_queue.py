"""
task_queue.py — Merged actor registry, task dispatcher, and worker entry point.

Usage:
    python task_queue.py --client-id alice

Replaces actors.py and backend.py.

Responsibilities:
  - Configure the Dramatiq broker and result backend (once)
  - Register per-client actors (lazily, cached)
  - Expose enqueue_task() for use by client.py
  - Used by worker.py to register the actor before consuming
"""

import os

import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.results import Results

from postgres_backend import PostgresBackend
from mcp_server import sync_to_sap

# ── Broker + backend setup (runs once on import) ──────────────────────────────

result_backend = PostgresBackend()
broker = RabbitmqBroker(url="amqp://guest:guest@localhost:5672")
broker.add_middleware(Results(backend=result_backend))
dramatiq.set_broker(broker)

# ── Actor registry ────────────────────────────────────────────────────────────

_actor_registry: dict = {}


def make_actor(client_id: str):
    """
    Returns the Dramatiq actor for this client, creating it only once.
    Re-using the same actor instance ensures the worker and the sender
    reference the same registered function name and queue.
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
            print(f"[Worker:{client_id}] ERROR on task {task_id}: {e}. Retrying...")
            raise

    _actor_registry[client_id] = process_integration_task
    return process_integration_task


# ── Task dispatcher ───────────────────────────────────────────────────────────

def enqueue_task(client_id: str, task_id: str, user_input: str) -> object:
    """
    Dispatch a task to this client's dedicated RabbitMQ queue.
    Immediately writes a PENDING row to the DB, then returns the Dramatiq
    message object so the caller can poll for results.
    """
    actor = make_actor(client_id)
    print(f"[Backend:{client_id}] Enqueueing task {task_id}: {user_input}")
    message = actor.send(task_id, user_input)

    result_backend.create_pending(message.message_id, client_id, user_input)

    print(f"[Backend:{client_id}] Task {task_id} queued with message ID: {message.message_id}")
    return message


# ── Module-level actor registration (runs in main + forked worker processes) ──
# When Dramatiq forks workers, it re-imports this module. The actor must be
# registered at import time so forked processes know how to handle messages.

_client_id = os.environ.get("DRAMATIQ_CLIENT_ID")
if _client_id:
    make_actor(_client_id)


# ── Worker entry point ────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description="SAP task worker")
    parser.add_argument("--client-id", required=True, help="Client ID to consume tasks for")
    args = parser.parse_args()

    os.environ["DRAMATIQ_CLIENT_ID"] = args.client_id
    actor = make_actor(args.client_id)

    print(f"[Worker] Registered actor for client {args.client_id!r} on queue: {actor.queue_name}")

    sys.argv = ["dramatiq", "task_queue", "--queues", actor.queue_name]
    from dramatiq.cli import main
    main()