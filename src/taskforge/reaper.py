"""Reaper: find tasks whose owning worker died, and make them runnable again.

Detection is implemented below. Recovery is deliberately NOT -- that is step 2.2,
and the decisions it forces (see the questions at the bottom) are the point.
"""

import time

from taskforge.config import settings
from taskforge.database import get_stale_in_progress_tasks, session_scope, get_task, update_task_status, get_pending_tasks, reset_abandoned_task
from taskforge.worker_registry import WorkerRegistry
from taskforge.models import TaskStatus, row_to_task
from taskforge.queue import Task, TaskQueue

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
            pending_tasks_not_pushed_to_redis.append((str(row.id), row.worker_id))
    return pending_tasks_not_pushed_to_redis

def main() -> None:
    registry = WorkerRegistry()
    print("Reaper started.")
    task_queue = TaskQueue()

    #intial checking for pending tasks that are abandoned
    for task_id, worker_id, task in find_pending_tasks_not_pushed_to_redis(registry):
        print(f"ABANDONED task {task_id} (dead worker {worker_id[:8]})")
        task_queue.push(row_to_task(get_task(task_id)))
        
    while True:
        for task_id, worker_id in find_abandoned_tasks(registry):
            print(f"ABANDONED task {task_id} (dead worker {worker_id[:8]})")
            # TODO 2.2 -- recovery. Before writing it, answer:
            #   1. Reset to PENDING and re-enqueue, or re-enqueue directly?
            #   2. If the "dead" worker was only stalled and later finishes,
            #      the task has now run twice. What stops the second copy from
            #      doing damage? (Hint: not a shorter timeout.)
            #   3. What happens if the reaper itself crashes between the DB
            #      write and the Redis push?
            #   4. Two reaper instances see the same abandoned task. Both
            #      requeue it. How is that prevented?

            with session_scope() as db:
                task = get_task(db, task_id)

                if task.task_status == TaskStatus.IN_PROGRESS:
                    updated_row = reset_abandoned_task(db, task_id, "PENDING")
                    if updated_row is not None:
                        task_queue.push(row_to_task(updated_row))
                    print(f"  reset {task_id} to PENDING")

                elif task.task_status is TaskStatus.COMPLETED or task.task_status is TaskStatus.FAILED or task.task_status is TaskStatus.PENDING:
                    print(f"  SKIP {task_id}: status is {task.task_status}, skipping reset")

                # else:
                #     updated_row = reset_abandoned_task(db, task_id, "PENDING")
                #     if updated_row is not None:
                #         task_queue.push(task)
                #     print(f"  reset {task_id} to PENDING")
                    

        time.sleep(settings.reaper_interval)


if __name__ == "__main__":
    main()
