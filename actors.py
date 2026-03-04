import dramatiq
from dramatiq.brokers.rabbitmq import RabbitmqBroker
from dramatiq.results import Results
from dramatiq.results.backends import RedisBackend
from mcp_server import sync_to_sap

# Setup result backend (Redis) and broker (RabbitMQ)
result_backend = RedisBackend(host="localhost")
broker = RabbitmqBroker(url="amqp://guest:guest@localhost:5672")
broker.add_middleware(Results(backend=result_backend))
dramatiq.set_broker(broker)

@dramatiq.actor(store_results=True, max_retries=3)
def process_integration_task(task_id: str, input_data: str):
    """
    Pick up a task from the queue, call MCP → SAP, store result in Redis.
    task_id is used by the client to poll for this specific result.
    """
    print(f"[Worker] Picked up task {task_id}: {input_data}")

    try:
        result = sync_to_sap(input_data)
        result["task_id"] = task_id
        print(f"[Worker] Task {task_id} complete: {result}")
        return result
    except Exception as e:
        print(f"[Worker] ERROR on task {task_id}: {str(e)}. Dramatiq will retry.")
        raise