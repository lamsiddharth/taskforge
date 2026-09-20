import asyncio
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query

from taskforge.database import get_db, get_task, init_db, list_tasks, save_task
from taskforge.models import Task
from taskforge.queue import TaskQueue
from taskforge.schemas import TaskCreate, TaskRead


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Runs once at startup. Replaces the deprecated @app.on_event("startup").
    init_db()
    yield


app = FastAPI(lifespan=lifespan)
task_queue = TaskQueue()


@app.post("/tasks", response_model=TaskRead, status_code=201)
async def push_task(payload: TaskCreate, db=Depends(get_db)):
    """Accept a client task request, persist it, then enqueue it.

    Postgres first: if the enqueue fails we still have a durable record of an
    accepted task. Enqueueing first risks a worker picking up a task that no
    row exists for.
    """
    task = Task(**payload.model_dump())

    await asyncio.to_thread(save_task, db, task)
    await asyncio.to_thread(task_queue.push, task)

    return task


@app.get("/tasks/{task_id}", response_model=TaskRead)
async def read_task(task_id: str, db=Depends(get_db)):
    row = await asyncio.to_thread(get_task, db, task_id)
    if row is None:
        raise HTTPException(status_code=404, detail="task not found")
    return row


@app.get("/tasks", response_model=list[TaskRead])
async def read_tasks(
    status: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db=Depends(get_db),
):
    """Paginated by default and capped at 200 -- an unbounded list endpoint is
    a table scan that works at 10 rows and fails at 10 million.
    """
    return await asyncio.to_thread(list_tasks, db, status, limit, offset)
