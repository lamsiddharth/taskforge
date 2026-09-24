"""End-to-end test for Phase 2.2 -- reaper crash recovery.

What this verifies:
  1. A task submitted via the API reaches IN_PROGRESS with a worker_id.
  2. Killing the worker mid-task leaves the task stranded IN_PROGRESS.
  3. The reaper detects the abandoned task and resets it to PENDING.
  4. A second worker picks it up and completes it.

Run with:
    uv run python scripts/test_recovery.py

Requires the API to be running: uv run uvicorn taskforge.api:app --reload
Requires Postgres and Redis to be up.
"""

import subprocess
import sys
import time

import requests

API = "http://localhost:8000"
WORKER_CMD = [sys.executable, "-m", "taskforge.worker"]
REAPER_CMD = [sys.executable, "-m", "taskforge.reaper"]


def submit_task() -> dict:
    resp = requests.post(
        f"{API}/tasks",
        json={"task_type": "test", "task_priority": "high", "payload": {"x": 1}},
    )
    resp.raise_for_status()
    return resp.json()


def get_task(task_id: str) -> dict:
    resp = requests.get(f"{API}/tasks/{task_id}")
    resp.raise_for_status()
    return resp.json()


def wait_for_status(task_id: str, status: str, timeout: int = 30) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        task = get_task(task_id)
        if task["task_status"] == status:
            return task
        time.sleep(0.5)
    task = get_task(task_id)
    raise TimeoutError(
        f"Timed out waiting for {status}. Current status: {task['task_status']}"
    )


def check(condition: bool, msg: str) -> None:
    if condition:
        print(f"  PASS  {msg}")
    else:
        print(f"  FAIL  {msg}")
        sys.exit(1)


def main():
    print("\n=== Phase 2.2 Recovery Test ===\n")

    # ------------------------------------------------------------------ #
    # Step 1: submit a task and confirm it lands in Postgres as PENDING
    # ------------------------------------------------------------------ #
    print("1. Submitting task...")
    task = submit_task()
    task_id = task["id"]
    check(task["task_status"] == "pending", f"task {task_id[:8]} created as PENDING")

    # ------------------------------------------------------------------ #
    # Step 2: start a worker and wait for it to claim the task
    # execute() sleeps 2s, so we have a window to kill it
    # ------------------------------------------------------------------ #
    print("\n2. Starting worker...")
    worker = subprocess.Popen(WORKER_CMD)

    task = wait_for_status(task_id, "in_progress", timeout=15)
    worker_id = task["worker_id"]
    check(task["task_status"] == "in_progress", f"task claimed by worker {worker_id[:8] if worker_id else 'None'}")

    # ------------------------------------------------------------------ #
    # Step 3: kill the worker while it's mid-task
    # ------------------------------------------------------------------ #
    print("\n3. Killing worker mid-task...")
    worker.kill()
    worker.wait()
    check(True, "worker killed")

    # ------------------------------------------------------------------ #
    # Step 4: wait for the worker TTL to expire so the reaper sees it dead
    # Default: heartbeat_interval=5, worker_ttl=15 -- wait a bit longer
    # ------------------------------------------------------------------ #
    ttl_wait = 20
    print(f"\n4. Waiting {ttl_wait}s for worker TTL to expire...")
    time.sleep(ttl_wait)
    task = get_task(task_id)
    check(
        task["task_status"] == "in_progress",
        f"task still IN_PROGRESS after TTL (worker gone, task stranded)",
    )

    # ------------------------------------------------------------------ #
    # Step 5: run the reaper once and confirm it resets the task to PENDING
    # ------------------------------------------------------------------ #
    print("\n5. Running reaper...")
    reaper = subprocess.Popen(REAPER_CMD)
    time.sleep(8)  # enough for at least one full reaper tick (interval=5s + buffer)
    reaper.kill()
    reaper.wait()

    task = get_task(task_id)
    check(task["task_status"] == "pending", "reaper reset task to PENDING")
    check(task["worker_id"] == worker_id, "worker_id preserved (forensics intact)")

    # ------------------------------------------------------------------ #
    # Step 6: start a fresh worker and confirm it completes the task
    # ------------------------------------------------------------------ #
    print("\n6. Starting second worker...")
    worker2 = subprocess.Popen(WORKER_CMD)

    task = wait_for_status(task_id, "completed", timeout=20)
    worker2.kill()
    worker2.wait()

    check(task["task_status"] == "completed", "task completed by second worker")
    check(
        task["worker_id"] != worker_id,
        f"new worker_id assigned (was {worker_id[:8] if worker_id else 'None'}, now {task['worker_id'][:8] if task['worker_id'] else 'None'})",
    )

    print("\n=== All checks passed ===\n")


if __name__ == "__main__":
    main()
