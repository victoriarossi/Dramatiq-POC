# SAP Integration System

An asynchronous, non-blocking task queue for dispatching requests to SAP API. Clients submit tasks freely and continue working while execution happens in the background — results are surfaced automatically as they arrive.

---

## Overview

This project connects a CLI client to an SAP ERP system through a message queue. Tasks are picked up by whichever worker is available first and executed in no guaranteed order. Multiple clients can run simultaneously, all sharing the same worker pool.

```
client_1.py  →  backend.py  →  RabbitMQ  →  actors.py  →  mcp_server.py  →  sap_api.py
                                                  ↓
                                               Redis  ←  (client polls for result)
```

---

## Components

| File | Role |
|---|---|
| `client_1.py` | CLI interface, background polling, result display |
| `client_2.py` | CLI interface, background polling, result display |
| `backend.py` | Non-blocking task dispatch |
| `actors.py` | Dramatiq worker definition and broker setup |
| `mcp_server.py` | Standardised MCP tool interface to SAP |
| `sap_api.py` | SAP ERP simulator (random delay, 0–6 min) |

---

## How It Works

1. The user types input at the `client_1.py` prompt.
2. The client calls `backend.enqueue_task()`, which places the task on the RabbitMQ queue via Dramatiq's `.send()` and returns immediately.
3. The first available worker picks up the task — **order of execution is not guaranteed**.
4. The worker calls `mcp_server.sync_to_sap()`, which forwards the request to `sap_api.call_sap_erp()`.
5. The SAP API simulates processing with a random delay between 0 and 5 minutes.
6. Once done, the worker stores the result in **Redis**, keyed by message ID.
7. A background thread in the client polls Redis every 5 seconds for that message ID and prints the result as soon as it arrives.

The client remains fully interactive throughout — new tasks can be submitted at any time regardless of what is currently executing.

---

## Result Storage (Redis)

Results are currently stored in **Redis** using Dramatiq's `RedisBackend`.

> **Note:** The Redis backend can be swapped out for your own database with minimal changes. Dramatiq supports custom result backends — implementing one requires a class that extends `dramatiq.results.backend.Backend` and defines `get_result` and `store_result`. This would allow results to be persisted in PostgreSQL, MongoDB, or any other store your infrastructure already uses.

---

## Client Notifications

| Event | Behaviour |
|---|---|
| Task submitted | Acknowledged immediately; prompt returns |
| 30 seconds elapsed | Prints `PENDING` once |
| Result arrives | Prints `DONE` with status and duration |
| 3 minutes elapsed | Prints `TIMEOUT` and marks task as `ERROR` |
| Worker exception | Prints `FAILED`; Dramatiq retries up to 3 times |

Type `status` at any time to see the current state of all submitted tasks.

---

## Running the Project

Start RabbitMQ and Redis via Docker, then start the worker and client:

```bash
# Start RabbitMQ (with management UI on port 15672)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Start Redis
docker run -d --name redis -p 6379:6379 redis

# Terminal 1 — start the Dramatiq worker
python3 -m dramatiq actors --watch . -v

# Terminal 2 — start client 1
python client_1.py

# Terminal 2 — start client 2
python client_2.py
```

### Client commands

| Input | Action |
|---|---|
| Any text | Enqueue a new task |
| `status` | Print the state of all tasks |
| `quit` | Exit the client |

---

## Configuration

| Parameter | Default | Location |
|---|---|---|
| RabbitMQ URL | `amqp://guest:guest@localhost:5672` | `actors.py` |
| Redis host | `localhost` | `actors.py` |
| SAP simulated delay | 0–300 seconds | `sap_api.py` |
| Poll interval | 5 seconds | `client_1.py` |
| Pending notification threshold | 30 seconds | `client_1.py` |
| Client-side timeout | 180 seconds | `client_1.py` |
| Max worker retries | 3 | `actors.py` |

---

## Possible Extensions

### Ordered Execution

In this version, tasks are picked up by whichever worker is free — if two tasks are submitted close together, the second may complete before the first. If ordered, sequential execution is needed, this can be achieved in two complementary ways:

- **Per-client queues:** Each client publishes to its own named RabbitMQ queue. A dedicated worker per queue ensures no two tasks from the same client run at the same time.
- **Dramatiq pipelines:** Tasks are chained at enqueue time using `dramatiq.pipeline()`. Each task in the chain is only dispatched after the previous one completes, enforcing strict submission order regardless of worker count.

Both approaches can be combined so that clients are isolated from each other while their own tasks always execute in order.
