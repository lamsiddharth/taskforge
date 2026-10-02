import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


def utcnow() -> datetime:
    """Timezone-aware UTC now.

    Never use datetime.now() -- it returns a naive datetime in local time.
    The scheduler compares "is this due yet?" across processes that may sit in
    different timezones, and naive local times make that comparison meaningless.
    DST also shifts local time twice a year while UTC keeps ticking.

    Rule: store UTC always, convert to local only for display.
    """
    return datetime.now(timezone.utc)


def handle_serialization(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.value
    raise TypeError(f"Type {type(obj)} not serializable")


def task_decoder(dct):
    """json.loads object_hook -- rebuilds a Task from its JSON form.

    Called for every JSON object in the document, including nested payloads,
    so it must only claim dicts that actually look like a Task.
    """
    if not {"task_status", "task_priority", "task_type", "payload"} <= dct.keys():
        return dct

    if isinstance(dct.get("created_at"), str):
        dct["created_at"] = datetime.fromisoformat(dct["created_at"])
    if isinstance(dct.get("task_status"), str):
        dct["task_status"] = TaskStatus(dct["task_status"])

    return Task(**dct)


class TaskStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"



@dataclass
class Task:
    task_priority: str
    task_type: str
    payload: dict
    task_status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=utcnow)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_name: Optional[str] = None
    task_description: Optional[str] = None
    # Set when a worker claims the task. Together with IN_PROGRESS this is how
    # a crashed worker is detected: without it, a crash is indistinguishable
    # from a slow task.
    worker_id: Optional[str] = None
    retry_count: int = 0
    retried_at: Optional[datetime] = None

class TriggerType(Enum):
    CRON = "cron"
    INTERVAL = "interval"
    ONCE = "once"

class CatchupPolicy(Enum):
    """Catchup policy for missed task executions.

    CRON jobs can be missed if the scheduler is down or if the task takes longer
    than the interval. This policy determines what happens when the scheduler
    comes back up.
    """
    SKIP = "skip"  # skip missed executions
    FIREONCE = "fireonce"  # fire missed executions immediately
    CATCHUPALL = "catchupall"  # fire all missed executions in order

@dataclass
class Schedule:
    name: str
    task_type: str
    payload: dict
    priority: str
    trigger_type: TriggerType
    catchup_policy: CatchupPolicy = CatchupPolicy.SKIP
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    enabled: bool = True
    run_at: Optional[datetime] = None
    cron_expression: Optional[str] = None
    interval_seconds: Optional[int] = None
    timezone: Optional[str] = "UTC"
    queue_name: Optional[str] = None
    description: Optional[str] = None
    next_run_at: Optional[datetime] = None
    last_run_at: Optional[datetime] = None