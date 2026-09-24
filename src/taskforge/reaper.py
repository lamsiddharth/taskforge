"""Reaper: find tasks whose owning worker died, and make them runnable again."""

import time

from taskforge.config import settings
from taskforge.database import get_stale_in_progress_tasks, session_scope, get_task, get_pending_tasks, reset_abandoned_task, row_to_task, update_task_status
from taskforge.worker_registry import WorkerRegistry
from taskforge.models import TaskStatus
from taskforge.queue import TaskQueue

def find_abandoned_tasks(registry: WorkerRegistry) -> list[tuple[str, str]]:
    """Tasks sitting IN_PROGRESS whose claiming worker is no longer alive.

    Postgres says who claimed what (durable). Redis says who is alive right now
    (ephemeral, TTL-expired). An IN_PROGRESS row naming a worker with no live
    lease is an abandoned task.
    """
    abandoned = []
    with session_scope() as db:
        for row in get_stale_in_progress_tasks(db):
            if row.worker_id is None:
                continue
            if not registry.is_alive(row.worker_id):
                abandoned.append((str(row.id), row.worker_id))
    return abandoned

def find_pending_tasks_not_pushed_to_redis(registry: WorkerRegistry) -> list[tuple[str, str]]:
    """Tasks sitting PENDING whose claiming worker is no longer alive.

    Postgres says who claimed what (durable). Redis says who is alive right now
    (ephemeral, TTL-expired). A PENDING row naming a worker with no live
    lease is an abandoned task.
    """
    pending_tasks_not_pushed_to_redis = []
    with session_scope() as db:
        for row in get_pending_tasks(db):
            if row.worker_id is None or row.claimed_at is None:
                continue
            pending_tasks_not_pushed_to_redis.append((str(row.id), row.worker_id, row))
    return pending_tasks_not_pushed_to_redis

def main() -> None:
    registry = WorkerRegistry()
    print("Reaper started.")
    task_queue = TaskQueue()

    #intial checking for pending tasks that are abandoned
    for task_id, worker_id, task in find_pending_tasks_not_pushed_to_redis(registry):
        print(f"ABANDONED task {task_id} (dead worker {worker_id[:8]})")
        task_queue.push(row_to_task(task))
        
    while True:
        for task_id, worker_id in find_abandoned_tasks(registry):
            print(f"ABANDONED task {task_id} (dead worker {worker_id[:8]})")
            with session_scope() as db:
                task = get_task(db, task_id)

                if task.task_status == TaskStatus.IN_PROGRESS.value:
                    if task.retry_count < settings.max_retries:
                        if reset_abandoned_task(db, task_id, TaskStatus.PENDING):
                            fresh = get_task(db, task_id)
                            task_queue.push(row_to_task(fresh))
                            print(f"  reset {task_id} to PENDING, re-enqueued")
                        else:
                            print(f"  SKIP {task_id}: another reaper already claimed it")
                    else:
                        update_task_status(db, task_id, TaskStatus.FAILED)
                        print(f"  {task_id} -> FAILED (max retries {task.retry_count} reached)")
                else:
                    print(f"  SKIP {task_id}: status is {task.task_status}")

        time.sleep(settings.reaper_interval)


if __name__ == "__main__":
    main()
