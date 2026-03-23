"""
queue_manager.py — DEPRECATED.

This file is kept for reference only.  It is no longer needed.

Previously this process acted as a software sequencer: it would pop one task
at a time from Postgres, dispatch it to Dramatiq, poll for the result, then
dispatch the next.

Sequential execution is now enforced by RabbitMQ itself via prefetch_count=1
on the worker channel (see task_queue.py).  The broker holds back messages
until the worker acks the current one, so no external polling loop is required.

The failure cascade (cancel + purge on SAP error) is handled inside the
Dramatiq actor in task_queue.py.

You only need two processes per client:

    python task_queue.py --client-id alice   # worker
    python client.py     --client-id alice   # REPL

For multiple independent clients, start a worker per client:

    python task_queue.py --client-id alice
    python task_queue.py --client-id bob
"""

import sys

if __name__ == "__main__":
    print(__doc__)
    sys.exit(0)