from dataclasses import dataclass, field

from typing import Optional
import uuid
from datetime import datetime

def handle_timestamp(obj):
    if(isinstance(obj, datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def task_decoder(dct):
    if 'task_status' in dct and 'task_priority' in dct and 'task_type' in dct and 'payload' in dct:
        if 'created_at' in dct:
                dct['created_at'] = datetime.fromisoformat(dct['created_at'])
                return Task(**dct)
    return dct

@dataclass
class Task:
    task_status: str
    task_priority: str
    task_type: str
    payload: dict
    created_at: datetime = field(default_factory=lambda: datetime.now())
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_name: Optional[str] = None
    task_description: Optional[str] = None