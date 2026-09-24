import json
from dataclasses import asdict

import redis

from taskforge.config import settings
from taskforge.models import Task, handle_serialization, task_decoder

QUEUE_KEY = "tasks"


class TaskQueue:
    def __init__(self):
        self.redis_client = redis.StrictRedis(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            # brpop blocks indefinitely by design; a socket timeout would abort
            # the wait and raise while the worker is correctly idle.
            socket_timeout=None,
        )

    def push(self, task: Task) -> None:
        self.redis_client.lpush(
            QUEUE_KEY, json.dumps(asdict(task), default=handle_serialization)
        )

    def pull(self) -> Task | None:
        result = self.redis_client.brpop(QUEUE_KEY, timeout=0)
        if not result:
            return None
        try:
            return json.loads(result[1], object_hook=task_decoder)
        except Exception as e:
            print(f"[queue] bad item, discarding: {e} | raw={result[1]!r:.80}")
            return None
