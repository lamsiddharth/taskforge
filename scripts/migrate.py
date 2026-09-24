import sqlalchemy as sa
from taskforge.database import engine, Base

Base.metadata.create_all(engine)

with engine.connect() as conn:
    conn.execute(sa.text(
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0"
    ))
    conn.execute(sa.text(
        "ALTER TABLE tasks ADD COLUMN IF NOT EXISTS retried_at TIMESTAMPTZ"
    ))
    conn.commit()

print("Migration complete")
