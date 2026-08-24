from taskforge.models import Task, handle_timestamp, task_decoder
import redis
import json
from dataclasses import asdict


class TaskQueue:
    def __init__(self):
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

    def push(self, task: Task):
        self.redis_client.lpush("tasks", json.dumps(asdict(task), default=handle_timestamp))

    def pull(self):
        result = self.redis_client.brpop("tasks")
        return json.loads(result[1], object_hook=task_decoder) if result else None