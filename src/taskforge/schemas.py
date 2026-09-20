from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict

from taskforge.models import TaskStatus


class TaskCreate(BaseModel):
    """What a client is allowed to send.

    Deliberately excludes id, task_status, created_at and worker_id. Accepting
    the full internal model would let a client POST
    {"task_status": "completed", "id": "<existing uuid>"} and have it persisted
    verbatim -- a mass assignment vulnerability, and a way to overwrite another
    task's row.

    The API contract and the internal model change for different reasons and
    carry different levels of trust, so they are separate types.
    """

    task_type: str
    task_priority: str
    payload: dict = {}
    task_name: Optional[str] = None
    task_description: Optional[str] = None


class TaskRead(BaseModel):
    """What the server returns. Server-owned fields included, nothing hidden."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    task_type: str
    task_priority: str
    task_status: TaskStatus
    payload: dict
    created_at: datetime
    task_name: Optional[str] = None
    task_description: Optional[str] = None
    worker_id: Optional[str] = None
