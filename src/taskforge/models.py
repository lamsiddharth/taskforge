from dataclasses import dataclass, field
from enum import Enum

from typing import Optional
import uuid
from datetime import datetime

def handle_serialization(obj):
    if(isinstance(obj, datetime)):
        return obj.isoformat()
    if(isinstance(obj, Enum)):
            return obj.value
    raise TypeError(f"Type {type(obj)} not serializable")

def task_decoder(dct):
    if 'task_status' in dct and 'task_priority' in dct and 'task_type' in dct and 'payload' in dct:
        if 'created_at' in dct:
                dct['created_at'] = datetime.fromisoformat(dct['created_at'])
                return Task(**dct)
    return dct

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
    task_status: TaskStatus = field(default=TaskStatus.PENDING)
    created_at: datetime = field(default_factory=lambda: datetime.now())
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_name: Optional[str] = None
    task_description: Optional[str] = None

