from taskforge.models import Task, handle_serialization, task_decoder
import redis
import json
from dataclasses import asdict


class TaskQueue:
    def __init__(self):
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0,socket_timeout=None)

    def push(self, task: Task):
        self.redis_client.lpush("tasks", json.dumps(asdict(task), default=handle_serialization))

    def pull(self):
        result = self.redis_client.brpop("tasks", timeout=0)
        return json.loads(result[1], object_hook=task_decoder) if result else None