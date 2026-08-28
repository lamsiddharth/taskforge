import time

from taskforge.worker_registry import (
    HEARTBEAT_INTERVAL,
    WorkerRegistry,
)


def main() -> None:
    registry = WorkerRegistry()
    print("Reaper started.")

    while True:
        for worker_id, status in registry.get_dead_workers():
            # status == "busy" means it crashed mid-task. Phase 7 requeues here.
            print(f"DEAD worker {worker_id} (last status: {status})")

        for worker_id in registry.reap_dead_workers():
            print(f"REAPED worker {worker_id}")

        time.sleep(HEARTBEAT_INTERVAL)


if __name__ == "__main__":
    main()
