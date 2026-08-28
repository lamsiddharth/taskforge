import os
import time
from contextlib import contextmanager

import redis
from dotenv import load_dotenv

load_dotenv()

HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 5))

# Invariant: REAP_AFTER > DEAD_AFTER > HEARTBEAT_INTERVAL * 2
# DEAD_AFTER tolerates 3 consecutive missed heartbeats before accusing a worker,
# so a GC pause or network blip doesn't get a healthy worker declared dead.
# REAP_AFTER leaves a window where a dead worker's record is still readable,
# so a recovery pass can see its last known `status` before the key disappears.
DEAD_AFTER = HEARTBEAT_INTERVAL * 3
REAP_AFTER = HEARTBEAT_INTERVAL * 7


class WorkerRegistry:
    """Redis-backed view of which workers exist and what they're doing.

    Two fields per worker, both written only by the worker itself:
      status          -- "idle" | "busy"
      last_heartbeat  -- unix timestamp

    Liveness is deliberately NOT stored. It is derived from last_heartbeat on
    read, so it cannot go stale and no second writer can corrupt it.
    """

    def __init__(self):
        self.redis_client = redis.StrictRedis(
            host="localhost", port=6379, db=0, socket_timeout=None
        )

    def _key(self, worker_id: str) -> str:
        return f"worker:{worker_id}"

    def register_worker(self, worker_id: str) -> None:
        self.redis_client.hset(
            self._key(worker_id),
            mapping={"status": "idle", "last_heartbeat": time.time()},
        )

    def update_worker_status(self, worker_id: str, status: str) -> None:
        self.redis_client.hset(
            self._key(worker_id),
            mapping={"status": status, "last_heartbeat": time.time()},
        )

    def touch(self, worker_id: str) -> None:
        """Refresh only the heartbeat timestamp, leaving status untouched."""
        self.redis_client.hset(
            self._key(worker_id), mapping={"last_heartbeat": time.time()}
        )

    def get_worker_status(self, worker_id: str):
        return self.redis_client.hgetall(self._key(worker_id))

    def is_alive(self, worker_id: str) -> bool:
        """Derive liveness from the last heartbeat. Never reads a stored flag."""
        raw = self.redis_client.hget(self._key(worker_id), "last_heartbeat")
        if raw is None:
            return False
        try:
            return (time.time() - float(raw)) < DEAD_AFTER
        except ValueError:
            return False

    def _scan_workers(self):
        """Yield (worker_id, status, age_seconds) for every registered worker.

        Skips keys that vanish or are malformed mid-scan -- a key can be deleted
        between SCAN yielding it and the read below, so absence is expected here,
        not exceptional.
        """
        now = time.time()
        for key in self.redis_client.scan_iter("worker:*"):
            status, heartbeat = self.redis_client.hmget(
                key, "status", "last_heartbeat"
            )
            if heartbeat is None:
                continue
            try:
                age = now - float(heartbeat)
            except ValueError:
                continue
            worker_id = key.decode().split(":", 1)[1]
            yield worker_id, (status.decode() if status else None), age

    def get_dead_workers(self) -> list[tuple[str, str | None]]:
        """Workers that have missed enough heartbeats to be considered dead.

        Returns (worker_id, last_known_status). A dead worker whose status is
        still "busy" crashed mid-task -- that is the signal a recovery pass
        needs, which is why nothing overwrites status.
        """
        return [
            (worker_id, status)
            for worker_id, status, age in self._scan_workers()
            if age > DEAD_AFTER
        ]

    def reap_dead_workers(self) -> list[str]:
        """Delete records of workers dead long enough to be beyond recovery."""
        reaped = []
        for worker_id, _status, age in self._scan_workers():
            if age > REAP_AFTER:
                self.redis_client.delete(self._key(worker_id))
                reaped.append(worker_id)
        return reaped

    @contextmanager
    def worker_context(self, worker_id: str):
        """Mark busy for the duration of the block, idle on the way out.

        The finally clause matters: if the task raises, status still returns to
        idle instead of leaving the worker permanently marked busy.
        """
        self.update_worker_status(worker_id, "busy")
        try:
            yield
        finally:
            self.update_worker_status(worker_id, "idle")
