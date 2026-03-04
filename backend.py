from actors import process_integration_task

def enqueue_task(task_id: str, user_input: str) -> object:
    """
    Dispatch a task to the RabbitMQ queue without blocking.
    Returns the Dramatiq message object so the caller can poll for results.
    """
    print(f"[Backend] Enqueueing task {task_id}: {user_input}")
    message = process_integration_task.send(task_id, user_input)
    print(f"[Backend] Task {task_id} queued with message ID: {message.message_id}")
    return message