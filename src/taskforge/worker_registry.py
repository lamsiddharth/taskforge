import time
from contextlib import contextmanager

import redis

from taskforge.config import settings


class WorkerRegistry:
    """Redis-backed liveness for workers.

    Liveness is TTL-as-liveness: a worker refreshes its key on an interval, and
    the key existing IS the worker being alive. A dead worker's record never
    needs deleting -- expiry does it. Same mechanism as Consul/etcd session
    leases.

    This works because the crash forensics live in Postgres, not here. The
    tasks table records which worker claimed which task, so letting the Redis
    key vanish loses nothing recoverable.

    TTL is heartbeat_interval * 3 (see config): one dropped packet must not be
    enough to declare a healthy worker dead.
    """

    def __init__(self):
        self.redis_client = redis.StrictRedis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            socket_timeout=None,
        )

    def _alive_key(self, worker_id: str) -> str:
        return f"worker:{worker_id}:alive"

    def _meta_key(self, worker_id: str) -> str:
        return f"worker:{worker_id}"

    def register_worker(self, worker_id: str) -> None:
        self.touch(worker_id)
        self.redis_client.hset(
            self._meta_key(worker_id),
            mapping={"status": "idle", "registered_at": time.time()},
        )
        self.redis_client.expire(self._meta_key(worker_id), settings.worker_ttl)

    def touch(self, worker_id: str) -> None:
        """Refresh the liveness lease. Must be called more often than the TTL."""
        self.redis_client.set(
            self._alive_key(worker_id), "1", ex=settings.worker_ttl
        )
        # Keep metadata alive alongside the lease so the two expire together.
        self.redis_client.expire(self._meta_key(worker_id), settings.worker_ttl)

    def is_alive(self, worker_id: str) -> bool:
        """Liveness is key existence -- nothing stored, nothing to go stale."""
        return bool(self.redis_client.exists(self._alive_key(worker_id)))

    def update_worker_status(self, worker_id: str, status: str) -> None:
        self.redis_client.hset(self._meta_key(worker_id), "status", status)
        self.redis_client.expire(self._meta_key(worker_id), settings.worker_ttl)

    def get_worker_status(self, worker_id: str) -> dict:
        return self.redis_client.hgetall(self._meta_key(worker_id))

    def list_live_workers(self) -> list[str]:
        """Every worker whose lease is unexpired. Self-cleaning by construction."""
        return [
            key.decode().split(":")[1]
            for key in self.redis_client.scan_iter("worker:*:alive")
        ]

    @contextmanager
    def worker_context(self, worker_id: str):
        """Mark busy for the duration of the block, idle on the way out.

        The finally matters: if the task raises, status still returns to idle
        rather than leaving the worker permanently marked busy.
        """
        self.update_worker_status(worker_id, "busy")
        try:
            yield
        finally:
            self.update_worker_status(worker_id, "idle")
