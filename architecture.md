# TaskForge Architecture

## Current state (Phase 1 — execution layer)

```
  Client
    │
    │ HTTP POST /tasks
    ▼
┌─────────────────────────────────────────────────────┐
│  api.py  (FastAPI / uvicorn)                        │
│                                                     │
│  POST /tasks    ──► save_task() ──► Postgres        │
│                 ──► queue.push() ──► Redis          │
│  GET  /tasks/:id ──► get_task()  ──► Postgres       │
│  GET  /tasks    ──► list_tasks() ──► Postgres       │
└─────────────────────────────────────────────────────┘
           │                         │
           │ LPUSH tasks             │ INSERT / SELECT
           ▼                         ▼
    ┌─────────────┐         ┌──────────────────────────────┐
    │    Redis    │         │         PostgreSQL            │
    │             │         │                              │
    │  tasks list │         │  tasks table                 │
    │  (FIFO queue│         │  ─ id (UUID PK)              │
    │   via lpush/│         │  ─ task_status (index)       │
    │   brpop)    │         │  ─ task_priority             │
    │             │         │  ─ task_type                 │
    │  worker:    │         │  ─ payload (JSONB)           │
    │  {id}:alive │         │  ─ created_at (TIMESTAMPTZ)  │
    │  (TTL key)  │         │  ─ worker_id (index)         │
    │             │         │  ─ claimed_at (TIMESTAMPTZ)  │
    │  worker:{id}│         └──────────────────────────────┘
    │  (hash meta)│                    ▲
    └─────────────┘                    │ claim_task()
           │                           │ update_task_status()
           │ BRPOP tasks               │
           ▼                           │
┌──────────────────────────────────────┴──────────────┐
│  worker.py                                          │
│                                                     │
│  main thread                                        │
│    brpop (blocks, holds zero DB connections)        │
│    claim_task()  ──► Postgres (IN_PROGRESS)         │
│    execute()                                        │
│    update_task_status() ──► Postgres (COMPLETED /   │
│                                       FAILED)       │
│                                                     │
│  heartbeat thread (daemon)                          │
│    touch() every heartbeat_interval seconds         │
│    ──► SET worker:{id}:alive 1 EX worker_ttl        │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  reaper.py  (separate process)                      │
│                                                     │
│  every reaper_interval seconds:                     │
│    get_stale_in_progress_tasks() ──► Postgres       │
│    for each row: is_alive(worker_id) ──► Redis      │
│    if key missing → task is abandoned               │
│    TODO 2.2: reset to PENDING + re-enqueue          │
└─────────────────────────────────────────────────────┘
```

## Data flow for a single task

```
  Client POST /tasks
       │
       ▼
  Task created (PENDING) ──────────────────────► Postgres row written
       │
       ▼
  Redis LPUSH tasks
       │
       ▼  (brpop on worker side)
  Worker pulls task
       │
       ▼
  claim_task() ── worker_id + claimed_at written, status → IN_PROGRESS ──► Postgres
       │
       ▼
  execute() (placeholder: time.sleep)
       │
       ├── success ──► status → COMPLETED ──► Postgres
       └── exception ──► status → FAILED  ──► Postgres
```

## Crash detection path

```
  Worker crashes mid-task
       │
       ▼
  Heartbeat thread dies with it
       │
       ▼  (after worker_ttl seconds = heartbeat_interval × 3)
  worker:{id}:alive key expires in Redis
       │
       ▼
  Reaper's next tick:
    get_stale_in_progress_tasks() returns row with this worker_id
    is_alive(worker_id) → False (key gone)
    → task flagged as ABANDONED
       │
       ▼
  TODO 2.2: recovery
```

## Module dependency map

```
  config.py          ← no dependencies (settings singleton)
      ▲
      │ imported by
  ┌───┴──────────┬────────────┬──────────────┬──────────────┐
  │              │            │              │              │
models.py   database.py  queue.py    worker_registry.py  schemas.py
  ▲               ▲            ▲              ▲
  │               │            │              │
  └───────────────┴────────────┴──────────────┘
                  ▲
                  │ imported by
          ┌───────┴──────────────┐
          │                      │
        api.py               worker.py
                                  ▲
                                  │ also imports
                             worker_registry.py
                                  ▲
                                  │
                             reaper.py
```

## Phases (from ROADMAP.md)

| Phase | Focus | Status |
|-------|-------|--------|
| 1 | Fix execution layer — correctness | Done |
| 2 | Harden execution layer — heartbeat, reaper, retries | In progress (2.2 next) |
| 3 | Scheduler process — cron + interval triggers | Not started |
| 4 | DAG engine — task dependencies + cycle detection | Not started |
| 5 | Fault-tolerant scheduler — leader election | Not started |
| 6 | Observability — lineage, metrics | Not started |

## Key design decisions

**TTL-as-liveness** — `worker:{id}:alive` is a Redis key with an expiry. Alive = key exists.
No field to update, no status to go stale, no resurrection bug. The crash forensics (which
task, which worker) live in Postgres where they are durable.

**Postgres-first on submit** — `save_task()` before `queue.push()`. A Redis crash after write
leaves a recoverable PENDING row. The reverse (enqueue first) leaves a worker holding a task
with no DB row to claim.

**Session lifetime = unit of work** — Workers open a session only after pulling a task and
close it before the next `brpop`. An idle worker holds zero Postgres connections.

**`TaskCreate` / `TaskRead` split** — Clients cannot send `task_status`, `worker_id`, `id`, or
`created_at`. The API generates those. Without this, a client could forge `task_status=completed`
and skip execution entirely.
