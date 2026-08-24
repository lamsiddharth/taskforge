from fastapi import FastAPI
from taskforge.queue import TaskQueue
from taskforge.models import Task
app = FastAPI()
task_queue = TaskQueue()

@app.post("/tasks")
async def push_task(task: Task):
    """
    Endpoint to push a new task to the task queue.
    """
    task_queue.push(task)    
    return {"message": "Task received", "task": task}