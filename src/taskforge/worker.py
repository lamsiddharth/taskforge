from taskforge.queue import TaskQueue
from taskforge.models import TaskStatus
from taskforge.database import sessionLocal, update_task_status
from taskforge.worker_registry import WorkerRegistry
import uuid
import threading
import time
from dotenv import load_dotenv
import os

load_dotenv()
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 5))  # Default to 5 seconds if not set


def heartbeat(registry: WorkerRegistry, worker_id: str) -> None:
    while True:
        registry.touch(worker_id)
        time.sleep(HEARTBEAT_INTERVAL)


def main() -> None:
    worker_id = str(uuid.uuid4())
    task_queue = TaskQueue()
    registry = WorkerRegistry()
    db = sessionLocal()

    registry.register_worker(worker_id)
    print(f"Worker {worker_id} started. Waiting for tasks...")

    t = threading.Thread(
        target=heartbeat,
        args=(registry, worker_id),
        daemon=True,
    )
    t.start()

    while True:
        task = task_queue.pull()
        if not task:
            continue

        with registry.worker_context(worker_id):
            print(f"Worker {worker_id} processing task {task.id}")
            time.sleep(HEARTBEAT_INTERVAL)
            update_task_status(db, task.id, TaskStatus.COMPLETED)
            print(f"Task {task.id} status updated to COMPLETED")


if __name__ == "__main__":
    main()
