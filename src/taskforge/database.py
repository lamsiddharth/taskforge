from contextlib import contextmanager

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from taskforge.config import settings
from taskforge.models import Task, TaskStatus, utcnow

engine = create_engine(settings.database_url)


class Base(DeclarativeBase):
    pass


class TaskModel(Base):
    __tablename__ = "tasks"

    id = Column(UUID(as_uuid=False), primary_key=True, index=True)
    task_name = Column(String, nullable=True)
    task_description = Column(String, nullable=True)
    task_status = Column(String, nullable=False, index=True)
    task_priority = Column(String, nullable=False)
    task_type = Column(String, nullable=False)
    payload = Column(JSONB, nullable=False)
    # timezone=True -> Postgres TIMESTAMPTZ. Without it the tz is silently
    # dropped on write and comparisons across machines become meaningless.
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    # Which worker claimed this task, and when. The pair is what makes crash
    # detection possible: an IN_PROGRESS row whose worker is no longer alive
    # is an abandoned task.
    worker_id = Column(String, nullable=True, index=True)
    claimed_at = Column(TIMESTAMP(timezone=True), nullable=True)
    retried_at = Column(TIMESTAMP(timezone=True), nullable=True)
    retry_count = Column(Integer, nullable=False, default=0)


def init_db():
    """Create missing tables.

    Placeholder only: create_all does NOT alter existing tables. Add a column
    later and this silently does nothing, then queries fail on the missing
    column. Alembic is the real answer -- expect to need it by Phase 2/3.
    """
    Base.metadata.create_all(engine)


sessionLocal = sessionmaker(bind=engine)


def get_db():
    """FastAPI dependency. Only valid with Depends() -- FastAPI drives the
    generator to completion, which is what runs the finally.

    Non-FastAPI callers must use session_scope() instead: next(get_db()) never
    advances past the yield, so the cleanup contract is not honoured.
    """
    db = sessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope():
    """Session bound to one unit of work, for workers and scripts.

    Commits on success, rolls back on exception, always closes. Resource
    lifetime should match the unit of work -- never hold a session open across
    a blocking call like brpop.
    """
    db = sessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def save_task(db, task: Task) -> TaskModel:
    db_task = TaskModel(
        id=task.id,
        task_name=task.task_name,
        task_description=task.task_description,
        task_status=task.task_status.value,
        task_priority=task.task_priority,
        task_type=task.task_type,
        payload=task.payload,
        created_at=task.created_at,
        worker_id=task.worker_id,
    )
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task


def claim_task(db, task_id: str, worker_id: str) -> TaskModel | None:
    """Mark a task IN_PROGRESS and record its owner.

    Called before any work, so that a crash leaves a row saying "worker X was
    running this" rather than a row indistinguishable from one not yet started.
    """
    row = db.query(TaskModel).filter(TaskModel.id == task_id).first()
    if row is None:
        return None
    row.task_status = TaskStatus.IN_PROGRESS.value
    row.worker_id = worker_id
    row.claimed_at = utcnow()
    db.commit()
    db.refresh(row)
    return row


def update_task_status(db, task_id: str, new_status: TaskStatus) -> TaskModel | None:
    """Set status and return the updated row.

    Returns the row rather than mutating the caller's Task: the dataclass and
    the DB row are two representations of one entity, and silently letting them
    disagree is how stale reads happen. Callers should use the return value.
    """
    row = db.query(TaskModel).filter(TaskModel.id == task_id).first()
    if row is None:
        return None
    row.task_status = new_status.value
    db.commit()
    db.refresh(row)
    return row


def get_task(db, task_id: str) -> TaskModel | None:
    return db.query(TaskModel).filter(TaskModel.id == task_id).first()


def list_tasks(db, status: str | None = None, limit: int = 50, offset: int = 0):
    """Always paginated. An unbounded list endpoint is a full table scan that
    is fine at 10 rows and takes down the API at 10 million.
    """
    q = db.query(TaskModel)
    if status is not None:
        q = q.filter(TaskModel.task_status == status)
    return (
        q.order_by(TaskModel.created_at.desc()).limit(limit).offset(offset).all()
    )


def get_stale_in_progress_tasks(db):
    """Every task currently claimed. The reaper checks each one's worker_id
    against Redis liveness to decide which are abandoned.
    """
    return (
        db.query(TaskModel)
        .filter(TaskModel.task_status == TaskStatus.IN_PROGRESS.value)
        .all()
    )

def get_pending_tasks(db):
    """Every PENDING task that hasn't hit the retry ceiling.

    Used by the reaper startup scan to re-enqueue tasks that were reset to
    PENDING but never reached Redis (reaper crashed mid-recovery).
    """
    return (
        db.query(TaskModel)
        .filter(TaskModel.task_status == TaskStatus.PENDING.value, TaskModel.retry_count < settings.max_retries)
        .all()
    )

def reset_abandoned_task(db, task_id: str, new_status: TaskStatus) -> bool:
    """Set status and return the updated row.

    Returns the row rather than mutating the caller's Task: the dataclass and
    the DB row are two representations of one entity, and silently letting them
    disagree is how stale reads happen. Callers should use the return value.
    """
    row = db.query(TaskModel).filter(
        TaskModel.id == task_id,
        TaskModel.task_status == TaskStatus.IN_PROGRESS.value
    ).update({"task_status": TaskStatus.PENDING.value, "retry_count": TaskModel.retry_count + 1, "retried_at": utcnow()})
    db.commit()
    return row > 0

def row_to_task(row: TaskModel) -> Task:
    """Convert a SQLAlchemy row to a Task dataclass.

    The row is the DB's representation of a task, and the dataclass is the
    internal representation. They are two representations of one entity, and
    silently letting them disagree is how stale reads happen. Callers should
    use the return value.
    """
    return Task(
        id=str(row.id),
        task_name=row.task_name,
        task_description=row.task_description,
        task_status=TaskStatus(row.task_status),
        task_priority=row.task_priority,
        task_type=row.task_type,
        payload=row.payload,
        created_at=row.created_at,
        worker_id=row.worker_id,
        retry_count=row.retry_count,
        retried_at=row.retried_at
    )
def retry_task(db, task_id: str) -> TaskModel | None:
    """Increment the retry count and set the retried_at timestamp for a task.

    Returns the updated row or None if the task does not exist.
    """
    row = db.query(TaskModel).filter(TaskModel.id == task_id).first()
    if row is None:
        return None
    row.retry_count = (row.retry_count or 0) + 1
    row.retried_at = utcnow()
    row.task_status = TaskStatus.PENDING.value  # Reset status to PENDING for retry
    db.commit()
    db.refresh(row)
    return row