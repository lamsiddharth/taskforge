from dataclasses import dataclass, field

from typing import Optional
import uuid
import time
from datetime import datetime

@dataclass
class Task:
    task_status: str
    task_priority: str
    task_type: str
    payload: dict
    created_at: datetime.datetime = field(default_factory=lambda: datetime.now())
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_name: Optional[str] = None
    task_description: Optional[str] = None