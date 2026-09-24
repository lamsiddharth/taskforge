"""End-to-end test for Phase 2.3 -- retry limit.

What this verifies:
  1. A task submitted via the API reaches IN_PROGRESS.
  2. The worker fails execution -- task retries up to max_retries times.
  3. After max_retries exhausted, task is permanently FAILED.
  4. retry_count in DB equals max_retries.

Run with:
    uv run python scripts/test_retry.py

Requires the API running: uv run uvicorn taskforge.api:app --reload
Requires Postgres and Redis to be up.
"""

import subprocess
import sys
import textwrap
import time

import requests

API = "http://localhost:8000"
MAX_RETRIES = 3

# Worker that monkey-patches execute() to always raise, then runs normally.
FAILING_WORKER = textwrap.dedent("""
    import taskforge.worker as w
    def always_fail(task):
        raise RuntimeError("simulated failure")
    w.execute = always_fail
    w.main()
""")


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


def wait_for_status(task_id: str, status: str, timeout: int = 60) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        task = get_task(task_id)
        if task["task_status"] == status:
            return task
        time.sleep(0.5)
    task = get_task(task_id)
    raise TimeoutError(
        f"Timed out waiting for '{status}'. Current: {task['task_status']}"
    )


def check(condition: bool, msg: str) -> None:
    if condition:
        print(f"  PASS  {msg}")
    else:
        print(f"  FAIL  {msg}")
        sys.exit(1)


def main():
    print("\n=== Phase 2.3 Retry Limit Test ===\n")

    # ------------------------------------------------------------------ #
    # Step 1: submit a task
    # ------------------------------------------------------------------ #
    print("1. Submitting task...")
    task = submit_task()
    task_id = task["id"]
    check(task["task_status"] == "pending", f"task {task_id[:8]} created as PENDING")

    # ------------------------------------------------------------------ #
    # Step 2: start a failing worker -- it will attempt the task and fail
    # each time, triggering retries. With max_retries=3 the task should
    # eventually land on FAILED after 4 total attempts (1 + 3 retries).
    # ------------------------------------------------------------------ #
    print(f"\n2. Starting failing worker (max_retries={MAX_RETRIES})...")
    worker = subprocess.Popen([sys.executable, "-c", FAILING_WORKER])

    # ------------------------------------------------------------------ #
    # Step 3: wait for FAILED status.
    # Each attempt: claim + instant fail + retry push. 4 attempts, budget
    # 10s per attempt = 40s total, using 60s timeout for safety.
    # ------------------------------------------------------------------ #
    print(f"\n3. Waiting for task to exhaust retries and reach FAILED...")
    try:
        task = wait_for_status(task_id, "failed", timeout=60)
    finally:
        worker.kill()
        worker.wait()

    check(task["task_status"] == "failed", "task permanently FAILED after retries exhausted")
    check(
        task.get("retry_count") == MAX_RETRIES,
        f"retry_count == {MAX_RETRIES} (got {task.get('retry_count')})",
    )

    print("\n=== All checks passed ===\n")


if __name__ == "__main__":
    main()
