import uuid
import threading
import time
import queue
from backend import enqueue_task
from dramatiq.results.errors import ResultTimeout

# ── Shared state ────────────────────────────────────────────────────────────
# { task_id: {"input": str, "status": "PENDING"|"QUEUED"|"DONE"|"ERROR", "result": dict|None} }
tasks: dict = {}
tasks_lock = threading.Lock()

# Local FIFO queue — holds (task_id, user_input) tuples not yet sent to RabbitMQ
local_queue: queue.Queue = queue.Queue()

POLL_INTERVAL = 5    # seconds between each Redis poll attempt
SAP_TIMEOUT   = 180  # 3 minutes — report timeout beyond this

# ── Result polling ───────────────────────────────────────────────────────────

def poll_until_done(task_id: str, message) -> None:
    """
    Blocks the dispatcher thread until the task is done, errored, or timed out.
    Returns only when a terminal state is reached.
    """
    start_time       = time.time()
    pending_printed  = False
    timeout_deadline = start_time + SAP_TIMEOUT

    while True:
        elapsed = time.time() - start_time

        # ── 3-minute hard timeout ──────────────────────────────────────────
        if time.time() > timeout_deadline:
            with tasks_lock:
                tasks[task_id]["status"] = "ERROR"
                tasks[task_id]["result"] = {"error": "SAP timeout"}
                input_preview = tasks[task_id]["input"]
            print(f"\n[✗] Task {task_id} | {input_preview!r} | TIMEOUT — SAP took longer than 3 minutes.\n")
            return

        # ── Print PENDING once after 30 seconds ───────────────────────────
        if not pending_printed and elapsed >= 30:
            with tasks_lock:
                input_preview = tasks[task_id]["input"]
            print(f"\n[…] Task {task_id} | {input_preview!r} | PENDING (still processing...)\n")
            pending_printed = True

        # ── Poll Redis ────────────────────────────────────────────────────
        try:
            result = message.get_result(block=True, timeout=POLL_INTERVAL * 1000)

            with tasks_lock:
                tasks[task_id]["status"] = "DONE"
                tasks[task_id]["result"] = result

            _print_result(task_id, result)
            return

        except ResultTimeout:
            continue  # not ready yet — loop back

        except Exception as e:
            with tasks_lock:
                tasks[task_id]["status"] = "ERROR"
                tasks[task_id]["result"] = {"error": str(e)}
            print(f"\n[✗] Task {task_id} FAILED: {e}\n")
            return


def _print_result(task_id: str, result: dict) -> None:
    with tasks_lock:
        input_preview = tasks[task_id]["input"]
    print(
        f"\n[✔] Task {task_id} DONE"
        f"\n    Input   : {input_preview!r}"
        f"\n    Status  : {result.get('status')}"
        f"\n    Duration: {result.get('duration')}s\n"
    )


# ── Sequential dispatcher ────────────────────────────────────────────────────

def dispatcher() -> None:
    """
    Single background thread that drains the local queue one task at a time.
    A task is only sent to RabbitMQ after the previous one has fully completed.
    """
    while True:
        task_id, user_input = local_queue.get()  # blocks until work arrives

        # ── Enqueue to RabbitMQ ────────────────────────────────────────────
        message = enqueue_task(task_id, user_input)
        with tasks_lock:
            tasks[task_id]["status"] = "PENDING"

        _print_queue_position(task_id)

        # ── Block until this task finishes ────────────────────────────────
        poll_until_done(task_id, message)

        local_queue.task_done()


def _print_queue_position(task_id: str) -> None:
    """Shows how many tasks are still waiting behind the active one."""
    waiting = local_queue.qsize()
    with tasks_lock:
        input_preview = tasks[task_id]["input"]
    if waiting:
        print(f"[Queue] Now executing task {task_id} | {input_preview!r} | {waiting} task(s) waiting behind it.\n")
    else:
        print(f"[Queue] Now executing task {task_id} | {input_preview!r} | No tasks waiting.\n")


# ── Status command ───────────────────────────────────────────────────────────

def print_status() -> None:
    with tasks_lock:
        if not tasks:
            print("[Status] No tasks submitted yet.")
            return
        print(f"\n[Status] {len(tasks)} task(s):")
        for tid, info in tasks.items():
            status = info["status"]
            label  = info["input"]
            if status == "DONE":
                dur = info["result"].get("duration", "?")
                print(f"  [{status}]  {tid} | {label!r} | duration={dur}s")
            elif status == "ERROR":
                err = info["result"].get("error", "?")
                print(f"  [{status}]  {tid} | {label!r} | {err}")
            elif status == "QUEUED":
                # Count position in the local queue (approximate)
                print(f"  [{status}]  {tid} | {label!r} | waiting in local queue")
            else:
                print(f"  [{status}] {tid} | {label!r}")
        print()


# ── Main loop ────────────────────────────────────────────────────────────────

def main() -> None:
    print("SAP Integration Client 1")
    print("  • Tasks are executed strictly one at a time, in submission order.")
    print("  • Type any text and press Enter to enqueue a task.")
    print("  • Type 'status' to see all tasks.")
    print("  • Type 'quit' to exit.\n")

    # Start the single dispatcher thread
    t = threading.Thread(target=dispatcher, daemon=True, name="dispatcher")
    t.start()

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
            print_status()
            continue

        # Register task locally (QUEUED state — not yet sent to RabbitMQ)
        task_id = str(uuid.uuid4())[:8]
        with tasks_lock:
            tasks[task_id] = {"input": user_input, "status": "QUEUED", "result": None}

        local_queue.put((task_id, user_input))

        queue_depth = local_queue.qsize()
        position    = queue_depth  # tasks ahead (excluding the one possibly running)
        if position == 0:
            print(f"[Client] Task {task_id} accepted — executing now.\n")
        else:
            print(f"[Client] Task {task_id} accepted — position {position} in queue.\n")


if __name__ == "__main__":
    main()