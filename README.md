# SAP Integration Orchestrator

A multi-client task queue system for integrating with SAP ERP. Each client gets an isolated RabbitMQ queue, tasks are executed serially per client, and results are persisted to PostgreSQL.

---

## Architecture

```
client.py
   │  submits tasks
   ▼
task_queue.py  ──► RabbitMQ (per-client queue: sap_tasks.<client_id>)
   │                             │
   │                             ▼
   │                       worker process
   │                             │
   │                       mcp_server.py
   │                             │
   │                        sap_api.py
   │                             │
   ▼                             ▼
postgres_backend.py  ◄───── result stored
   │
   ▼
client polls DB for status
```

**Key design decisions:**

- **One queue per client** — tasks are isolated between clients and run strictly in submission order.
- **Dramatiq + RabbitMQ** — handles retries, message durability, and worker lifecycle.
- **PostgreSQL result backend** — tasks are visible in the DB from the moment they're enqueued (as `PENDING`), before the worker picks them up.
- **MCP tool layer** — SAP calls go through a FastMCP server (`sync_to_sap`), making the SAP integration swappable.

---

## File Overview

| File | Responsibility |
|---|---|
| `client.py` | Interactive CLI; accepts user input, enqueues tasks, polls for results |
| `task_queue.py` | Broker setup, per-client actor registry, `enqueue_task()` dispatcher, worker entry point |
| `mcp_server.py` | Server connecting to `sync_to_sap` tool |
| `sap_api.py` | SAP ERP stub (simulates variable latency and occasional failures) |
| `postgres_backend.py` | Dramatiq `ResultBackend` backed by PostgreSQL `tasks` table |

---

## Prerequisites

- Python 3.11+
- RabbitMQ running on `localhost:5672` (default guest credentials)
- PostgreSQL with the `tasks` table (see schema below)

### Python dependencies

```bash
pip install dramatiq[rabbitmq] psycopg2-binary
```

### Database schema

```sql
CREATE TABLE tasks (
    key        VARCHAR(512) PRIMARY KEY,
    status     INT          NOT NULL,   -- 0=SUCCESS, 1=PENDING, 2=ERROR
    client_id  VARCHAR(512) NOT NULL,
    intent     TEXT,
    result     JSONB        NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `PG_HOST` | `localhost` | PostgreSQL host |
| `PG_PORT` | `5432` | PostgreSQL port |
| `PG_DB` | `mydb` | Database name |
| `PG_USER` | `postgres` | Database user |
| `PG_PASSWORD` | *(empty)* | Database password |

---

## Running

### 1. Start the worker

Each client ID requires its own worker process consuming its dedicated queue:

```bash
python task_queue.py --client-id alice
python task_queue.py --client-id bob   # separate terminal, separate client
```

### 2. Start the client

In another terminal, run the interactive client for the same client ID:

```bash
python client.py --client-id alice
```

### 3. Submit tasks

Once the client is running, type any text and press Enter to enqueue a task:

```
> postpone
> w/o
> status        # show all tasks for this client from DB
> quit
```

---

## Task Lifecycle

1. User types input → `client.py` generates a UUID task ID and puts it on the local queue.
2. The dispatcher thread calls `enqueue_task()`, which:
   - Sends the message to RabbitMQ via the client's Dramatiq actor.
   - Immediately writes a `PENDING` row to PostgreSQL.
3. The worker picks up the message, calls `sync_to_sap()` via the MCP server, and stores the result (`SUCCESS` or `ERROR`) back to PostgreSQL.
4. The client polls for the result every 5 seconds. After 30 seconds it prints a `PENDING...` warning. After 3 minutes it reports a timeout and cancels any queued tasks.

### Retry behaviour

Failed tasks are automatically retried up to **3 times** by Dramatiq before being marked `ERROR`.

---

## Status output

Type `status` in the client to see all tasks for your client ID pulled directly from the database:

```
[Status:alice] 3 task(s):
 [postpone] [DONE]     dramatiq:abc123 | duration=47s
 [w/o] [PENDING]  dramatiq:def456
 [transfer] [ERROR]    dramatiq:ghi789 | SAP execution failed
```

---

## Notes

- Tasks run **one at a time per client**. Multiple simultaneous clients each have their own queue and worker and do not interfere with each other.