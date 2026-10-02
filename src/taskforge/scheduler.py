from datetime import timedelta

import croniter
from sqlalchemy.sql.functions import now
from taskforge.config import settings
from taskforge.database import session_scope, get_task, update_task_status, get_schedules
from taskforge.models import TaskStatus
from taskforge.queue import TaskQueue

corn = croniter.croniter("0 0 * * *") #every 10 seconds

class Scheduler:
    def __init__(self):
        self.tasks = []
        self.taskque = TaskQueue()
        self.db = session_scope()

    def add_task(self, task, cron_expression):
        self.taskque.push((task, cron_expression))
        
    def next_dire_time(self, cron_expression, start_time):
        c = croniter.croniter(cron_expression, start_time)
        return c.get_next()
    
    def run(self):
        tasks = []
        with session_scope() as db:
            tasks = get_schedules(db, enabled=True)
            for task in tasks:
                if task.trigger_type == "cron":
                    cron = croniter.croniter(task.cron_expression, task.next_run_at)
                    next_run = cron.get_next()
                    if next_run <= now():
                        self.taskque.push((task, task.cron_expression))
                        update_task_status(db, task.id, TaskStatus.PENDING)
                        task.next_run_at = next_run
                        db.commit()
                elif task.trigger_type == "interval":
                    if task.next_run_at is None:
                        task.next_run_at = task.run_at
                    if task.next_run_at <= settings.scheduler_interval:
                        self.taskque.push((task, None))
                        update_task_status(db, task.id, TaskStatus.PENDING)
                        task.next_run_at += timedelta(seconds=task.interval_seconds)
                        db.commit()
        