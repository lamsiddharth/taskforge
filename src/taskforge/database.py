from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy import Column, String
from sqlalchemy.dialects.postgresql import UUID, JSONB, TIMESTAMP

engine = create_engine("postgresql://taskforge:taskforge@localhost/taskforge")

class Base(DeclarativeBase):
    pass

class TaskModel(Base):
    __tablename__ = "tasks"
    id = Column(UUID, primary_key=True, index=True)
    task_name= Column(String, nullable=True)
    task_description= Column(String, nullable=True)
    task_status= Column(String, nullable=False)
    task_priority= Column(String, nullable=False)
    task_type= Column(String, nullable=False)
    payload= Column(JSONB, nullable=False)
    created_at= Column(TIMESTAMP, nullable=False)


def init_db():
    Base.metadata.create_all(engine)

sessionLocal = sessionmaker(bind=engine)

def get_db():
    db = sessionLocal()
    try:
        yield db
    finally:
        db.close()

def save_task(db, task):
    db_task = TaskModel(
        id=task.id,
        task_name=task.task_name,
        task_description=task.task_description,
        task_status=task.task_status.value,
        task_priority=task.task_priority,       
        task_type=task.task_type,
        payload=task.payload,
        created_at=task.created_at
    )
    db.add(db_task)
    db.commit()
    db.refresh(db_task)
    return db_task

def update_task_status(db, task_id, newStatus):
    task = db.query(TaskModel).filter(TaskModel.id == task_id).first()
    if task:
        task.task_status = newStatus.value
        db.commit()
        db.refresh(task)
        return task
    return None