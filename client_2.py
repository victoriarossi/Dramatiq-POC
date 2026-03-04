import uuid
import threading
import time
from backend import enqueue_task
from dramatiq.results.errors import ResultTimeout

# ── Shared state ────────────────────────────────────────────────────────────
# { task_id: {"input": str, "status": "PENDING"|"DONE"|"ERROR", "result": dict|None} }
tasks: dict = {}
tasks_lock = threading.Lock()

POLL_INTERVAL = 5    # seconds between each Redis poll attempt
SAP_TIMEOUT   = 180  # 3 minutes — report timeout beyond this

# ── Background poller ────────────────────────────────────────────────────────

def poll_task(task_id: str, message) -> None:
    """
    Runs in a daemon thread. Polls Redis every POLL_INTERVAL seconds.
    - Prints "PENDING" once after 30 seconds if not yet done.
    - Prints "TIMEOUT" if SAP takes longer than 3 minutes.
    - Prints "DONE" as soon as the result arrives.
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
            continue  # not ready yet, loop back

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
                print(f"  [{status}] {tid} | {label!r} | duration={dur}s")
            elif status == "ERROR":
                err = info["result"].get("error", "?")
                print(f"  [{status}] {tid} | {label!r} | {err}")
            else:
                print(f"  [{status}] {tid} | {label!r}")
        print()


# ── Main loop ────────────────────────────────────────────────────────────────

def main() -> None:
    print("SAP Integration Client 2")
    print("  • Type any text and press Enter to enqueue a task.")
    print("  • Type 'status' to see all tasks.")
    print("  • Type 'quit' to exit.\n")

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

        # Enqueue task
        task_id = str(uuid.uuid4())[:8]   # short readable ID
        message = enqueue_task(task_id, user_input)

        with tasks_lock:
            tasks[task_id] = {"input": user_input, "status": "PENDING", "result": None}

        print(f"[Client] Task {task_id} enqueued. Continuing to accept input...\n")

        # Spawn a background polling thread for this task
        t = threading.Thread(
            target=poll_task,
            args=(task_id, message),
            daemon=True,   # won't block process exit
            name=f"poller-{task_id}",
        )
        t.start()


if __name__ == "__main__":
    main()