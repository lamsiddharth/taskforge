from fastapi import FastAPI
from taskforge.queue import TaskQueue
from taskforge.models import Task
from taskforge.database import save_task, get_db
from fastapi import Depends
import asyncio

app = FastAPI()
task_queue = TaskQueue()

@app.post("/tasks")
async def push_task(task: Task, db=Depends(get_db)):
    """
    Endpoint to push a new task to the task queue.
    """

    await asyncio.to_thread(save_task, db, task)

    task_queue.push(task)    
    return {"message": "Task received", "task": task}