import threading
import time
import uuid

from taskforge.config import settings
from taskforge.database import claim_task, session_scope, update_task_status
from taskforge.models import TaskStatus
from taskforge.queue import TaskQueue
from taskforge.worker_registry import WorkerRegistry


def heartbeat(registry: WorkerRegistry, worker_id: str) -> None:
    """Runs on its own thread.

    Must not live in the main loop: that loop blocks in brpop while idle, so an
    inline heartbeat would stop beating exactly when the worker is healthy and
    waiting for work.
    """
    while True:
        registry.touch(worker_id)
        time.sleep(settings.heartbeat_interval)


def execute(task) -> None:
    """Placeholder for real work.

    The sleep is deliberate: it creates a window wide enough to kill the worker
    mid-task while verifying crash recovery.
    """
    print(f"  executing {task.task_type} ...")
    time.sleep(2)


def main() -> None:
    worker_id = str(uuid.uuid4())
    task_queue = TaskQueue()
    registry = WorkerRegistry()

    registry.register_worker(worker_id)
    print(f"Worker {worker_id} started. Waiting for tasks...")

    threading.Thread(
        target=heartbeat, args=(registry, worker_id), daemon=True
    ).start()

    while True:
        # Blocks here with no DB session held -- an idle worker holds zero
        # Postgres connections.
        task = task_queue.pull()
        if not task:
            continue

        with registry.worker_context(worker_id):
            # Claim before doing anything. A crash after this point leaves an
            # IN_PROGRESS row naming this worker, which is what makes the task
            # recoverable instead of silently lost.
            with session_scope() as db:
                claimed = claim_task(db, task.id, worker_id)

            # No row means Redis and Postgres disagree -- an enqueue whose DB
            # write rolled back, a queue that outlived a restore, or a manual
            # push. Drop it rather than crashing the worker on an orphan.
            if claimed is None :
                print(f"[{worker_id[:8]}] SKIP {task.id}: no such task in DB")
                continue

            elif claimed.task_status != TaskStatus.IN_PROGRESS or claimed.task_status is TaskStatus.FAILED or claimed.task_status is TaskStatus.COMPLETED:
                print(
                    f"[{worker_id[:8]}] SKIP {task.id}: "
                    f"status {claimed.task_status} != {claimed.task_status}"
                )
                continue
            elif claimed.worker_id != worker_id:
                print(
                    f"[{worker_id[:8]}] SKIP {task.id}: "   
                    f"claimed by {claimed.worker_id[:8]} != {worker_id[:8]}"
                )
                continue
            
            print(f"[{worker_id[:8]}] claimed {task.id}")

            try:
                execute(task)
                final = TaskStatus.COMPLETED
            except Exception as exc:
                print(f"[{worker_id[:8]}] task {task.id} failed: {exc}")
                final = TaskStatus.FAILED

            with session_scope() as db:
                row = update_task_status(db, task.id, final)
                # Print the row the DB returned, not the in-memory Task -- the
                # dataclass was never updated and would report a stale status.
                status = row.task_status if row else "row vanished"
                print(f"[{worker_id[:8]}] {task.id} -> {status}")


if __name__ == "__main__":
    main()
