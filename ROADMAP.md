# TaskForge — Roadmap

Building a **distributed task scheduler** (not just a task queue): trigger engine, DAG
engine, fault-tolerant scheduler, hardened execution layer, observability.

```
Client
  ↓ POST /schedules or /dags
FastAPI ──────────────► Postgres (schedules, dags, tasks, task_runs)
                              ↑ polls
                        Scheduler Process (leader-elected, 2 instances)
                              ↓ enqueues
                        Redis (ready lists + delayed ZSET, named queues)
                              ↑ BRPOP
                        Workers (N instances, heartbeating)
                              ↓ status
                        Postgres
                              ↑ scrapes
                        Prometheus → Grafana
```

Every arrow is a distributed systems problem to solve deliberately.

---

## Ground rules

- **One step per session.** Hit the verify condition before moving on.
- Phase N assumes Phase N−1 is solid. Failures here are silent; debugging a DAG race on
  top of a broken retry path is miserable.
- Keep `DECISIONS.md` — one short entry per non-obvious choice: what was picked, what was
  rejected, why. Especially 3.3, 4.4, 5.3.
- **Out of scope** (plausible-sounding detours, low learning per hour): auth, web UI,
  multi-tenancy, Kubernetes, custom wire protocol, autoscaling.

---

## Known bug in current code: the worker DB session

`db = next(get_db())` in `worker.py` is wrong, but subtly.

`get_db()` is a generator. `next(...)` pulls the session out, but nothing advances the
generator to its `finally: db.close()`. The generator has no remaining reference after
that line — so in CPython it's refcount-collected immediately, which throws
`GeneratorExit` into it and *does* run the `finally`.

**So: leak, or session closed out from under you?** Add a print inside `get_db`'s
`finally` and run the worker. That experiment is worth more than being told.

Either way the pattern is wrong: `get_db` is a *FastAPI dependency*, designed for FastAPI
to drive its lifecycle. A worker needs a plain context-managed session.

---

# Phase 1 — Make It Correct

**Time:** 3–4 sessions
**Exit:** `docker compose up` runs everything; a POSTed task goes
`PENDING → IN_PROGRESS → COMPLETED`; it can be queried back.

### 1.3 — Timezone-aware timestamps ⚠️ DO THIS FIRST

| | |
|---|---|
| **Files** | `models.py` (`created_at` default), `database.py` (`TIMESTAMP` column) |
| **Change** | `datetime.now()` is **naive** — no timezone. Column is `TIMESTAMP` without timezone. Switch to UTC-aware datetimes and `TIMESTAMP(timezone=True)`. |
| **Why now** | Phase 3 is a scheduler. It compares "is this due yet?" across processes on possibly different machines. Naive local timestamps make that comparison meaningless, and DST hands you a bug twice a year. Fixing this after there's data is a migration; now it's one line. |
| **Rule** | Store UTC always. Convert to local only at display time. |

### 1.1 — Session lifecycle in the worker

| | |
|---|---|
| **Files** | `worker.py`, `database.py` |
| **Change** | (a) Reverse the order — `pull()` first, open a session only once there's a task. (b) Stop using `get_db()` here; add a separate context-manager session helper for non-FastAPI callers (`contextlib.contextmanager`). |
| **Concept** | Resource lifetime should match the *unit of work*, not the loop iteration. A blocking call should never sit inside a held resource. |
| **Verify** | Postgres up, no tasks queued: `SELECT count(*) FROM pg_stat_activity WHERE datname='taskforge';` → idle worker holds 0 connections. |
| **Trap** | `Session.close()` returns the connection to the pool but leaves the Session reusable — it silently grabs a new connection on next use. "It works" is not evidence of correctness. |

### 1.2 — Stale status print

| | |
|---|---|
| **File** | `worker.py` (the print after `update_task_status`) |
| **Change** | `update_task_status` mutates the DB row, not the in-memory `Task`. Decide deliberately: does the function return new state, or does the caller re-read? Pick one, be consistent. |
| **Concept** | The identity gap. The `Task` dataclass and the `TaskModel` row are two objects representing one logical entity. Every ORM has this problem. |
| **Question** | There are now *three* representations of a task: `Task` dataclass, `TaskModel` row, JSON blob in Redis. Is that right, or is one redundant? |

### 1.4 — `init_db()` never runs

| | |
|---|---|
| **File** | `api.py` |
| **Change** | Call it from FastAPI's `lifespan` context manager (modern replacement for `@app.on_event("startup")`). |
| **Trap** | `create_all` only creates *missing* tables. It will not alter an existing one. The moment a column is added in Phase 2, it silently does nothing → "missing column" error. |
| **Note** | The real answer is Alembic. Not yet — but know this is a placeholder, and expect to reach for it around Phase 2/3. |

### 1.5 — Configuration

| | |
|---|---|
| **New** | `src/taskforge/config.py`, `.env`, committed `.env.example` |
| **Change** | Pydantic is already present (FastAPI dep) → add `pydantic-settings`, define one `Settings` class: `database_url`, `redis_host`, `redis_port`. Import in `database.py` and `queue.py`. |
| **Concept** | Twelve-Factor config. Hardcoded `localhost` in `queue.py` means the worker *must* share a box with Redis — which makes the word "distributed" false. This step makes it true. |
| **Trap** | `.env` → `.gitignore`; commit `.env.example`. The DB password is in that string. |
| **Verify** | Set `redis_host` to a bogus value → worker must fail to connect, proving config is read. |

### 1.6 — Client can forge task state ⚠️ design bug

| | |
|---|---|
| **File** | `api.py` |
| **Problem** | The endpoint takes `task: Task` — the full internal model. A client can POST `{"task_status": "completed", "id": "<any uuid>", "created_at": "1999-01-01"}` and it's persisted verbatim. They can also overwrite an existing task's ID. |
| **Change** | Add `schemas.py`: `TaskCreate` with *only* client fields (`task_type`, `task_priority`, `payload`, `task_name`, `task_description`), and `TaskRead` for responses. Server owns `id`, `task_status`, `created_at`. |
| **Concept** | Mass assignment vulnerability. Separate API contract from internal model — they change for different reasons and carry different trust levels. |

### 1.7 — `IN_PROGRESS` transition

| | |
|---|---|
| **Files** | `worker.py`, `database.py` |
| **Change** | Immediately after pulling and *before* any work, set `IN_PROGRESS`. Add a `worker_id` column recording which worker claimed it. |
| **Why** | Load-bearing for Phase 2. A task stuck in `IN_PROGRESS` with a dead owner is how a crashed worker is detected. Without this, a crash is indistinguishable from a slow task. |
| **Also** | Add a real work step — even `time.sleep(2)` — so there's a window to kill the worker in during Phase 2. |

### 1.8 — Read endpoints

| | |
|---|---|
| **File** | `api.py` |
| **Add** | `GET /tasks/{id}` (404 if absent), `GET /tasks?status=&limit=&offset=`, `response_model=TaskRead`. |
| **Concept** | Paginate from day one. `GET /tasks` unbounded is a table scan returning all history — fine at 10 rows, takes down the API at 10 million. |

### 1.9 — Docker Compose

| | |
|---|---|
| **New** | `docker-compose.yml`, `Dockerfile`, `.dockerignore` |
| **Services** | `postgres`, `redis`, `api`, `worker` |
| **Change** | Wire the worker into `[project.scripts]` in `pyproject.toml` — currently `taskforge` points at the stub `main` in `__init__.py`, and `worker.py:main` has no entry point. |
| **Trap** | `depends_on` waits for the container to *start*, not for Postgres to accept connections → connection-refused on first boot. Use `healthcheck` + `depends_on: condition: service_healthy`, or app-level connection retry. Both legitimate; know why you chose one. |
| **Verify** | `docker compose up --scale worker=3`, push 10 tasks, confirm they distribute rather than all landing on one worker. |

---

# Phase 2 — Make It Survive Failure

**Time:** 4–5 sessions
**Exit:** `docker kill` a worker mid-task → the task completes anyway, on another worker,
with no human action.

### 2.1 — Worker heartbeat

| | |
|---|---|
| **New** | `src/taskforge/heartbeat.py` |
| **Design** | Each worker owns a stable `worker_id` (uuid at startup). Writes Redis key `worker:{id}:alive` with a TTL, refreshed on an interval by a background thread. Liveness = key exists. |
| **Concept** | TTL-as-liveness. A dead worker's record never needs deleting — expiry does it. Same trick as Consul/etcd session leases. |
| **Design question** | Heartbeat interval 5s → what TTL? Not 5s. Think about one dropped packet. Convention is 2–3× the interval — reason out why before accepting it. |
| **Trap** | The heartbeat must be on a separate thread. The main loop blocks in `brpop`; a heartbeat inside it stops beating whenever the worker is idle — exactly backwards. |

### 2.2 — Reaper: detect dead workers, recover tasks

| | |
|---|---|
| **New** | `src/taskforge/reaper.py` (own process, or a thread in the scheduler once Phase 3 exists) |
| **Logic** | Periodically: find `IN_PROGRESS` tasks; for each check whether `worker:{worker_id}:alive` still exists; if not, reset to `PENDING` and re-enqueue. |
| **Concept — the big one** | **At-least-once delivery.** A worker can be alive but stalled (long GC pause, CPU starvation, network partition). Its heartbeat lapses, the reaper requeues, then the worker wakes and finishes. **The task ran twice.** |
| **Consequence** | Timeouts cannot prevent this — only shrink the window. The actual fix is making execution **idempotent**. This is why SQS documents at-least-once and why Stripe's API takes an idempotency key. Most important idea in the project. |
| **Compare** | Look up SQS "visibility timeout" — this is a reinvention of it. Converging on the same design as AWS is the point. |
| **Verify** | Give a task a 30s sleep. `docker kill` the worker at 15s. Task should complete on another worker. |

### 2.3 — Retries with backoff ⚠️ needs a new Redis structure

| | |
|---|---|
| **Files** | `models.py`, `database.py`, `queue.py` |
| **New columns** | `retry_count`, `max_retries`, `last_error`, `next_retry_at` |
| **The catch** | A Redis **list** can't hold a delayed item. `brpop` gives the next item, not the next *due* item. Delayed retries need a **sorted set** (`ZADD`, score = due unix ts) plus a promoter loop that `ZRANGEBYSCORE`s due items and `LPUSH`es them to the ready list. |
| **Concept** | A genuine architectural addition — two-structure queue: ZSET for scheduled/delayed, list for ready-now. Sidekiq, BullMQ and Celery all have this split. |
| **Backoff** | `base * 2^retry_count`, capped — then **add jitter**. |
| **Why jitter** | 500 tasks fail at once because a downstream API died. Without jitter all 500 retry at the same instant, re-kill it, and back off in lockstep forever. See "thundering herd" and AWS's "exponential backoff and jitter". |
| **Promoter trap** | Two promoters doing `ZRANGEBYSCORE` then `LPUSH` both promote the same item. Needs `ZPOPMIN`, a Lua script, or `WATCH`/`MULTI`. Decide which and why. |

### 2.4 — Named queues and priority

| | |
|---|---|
| **File** | `queue.py` |
| **Change** | Parameterize the queue name instead of hardcoding `"tasks"`. A worker subscribes to a list of queues. |
| **Free win** | `BRPOP` takes multiple keys and drains them in argument order → `BRPOP high default low 0` gives strict priority with no extra code. Understand the starvation risk that creates. |
| **Concept** | Task routing. Slow tasks shouldn't block fast ones — hence per-queue worker pools in real systems. |

### 2.5 — Tests

| | |
|---|---|
| **New** | `tests/`, `pytest` in a dev dependency group |
| **Cover** | FIFO ordering; `PENDING→IN_PROGRESS→COMPLETED`; retry increments and eventually reaches `FAILED`; reaper requeues a dead worker's task; `POST /tasks` rejects client-supplied `task_status`; round-trip through `handle_serialization`/`task_decoder`. |
| **How** | `fakeredis` for the queue; `testcontainers` or a per-test transaction rolled back in a fixture for Postgres. |
| **Priority** | `task_decoder` in `models.py` is fragile — it rebuilds a `Task` only if four specific keys are present, else silently returns a plain dict. Adding `retry_count` in 2.3 will change its behavior. Test it *before* touching it. |

---

# Phase 3 — The Scheduler

**Time:** 4–5 sessions
**Exit:** register a cron schedule → tasks fire on time, forever, with nobody touching the API.

### 3.1 — Schedule store

New table `schedules`:

| Column | Notes |
|---|---|
| `id` | uuid pk |
| `name` | unique, human-readable |
| `trigger_type` | `cron` \| `interval` \| `once` |
| `cron_expression` | nullable |
| `interval_seconds` | nullable |
| `run_at` | nullable, one-off |
| `timezone` | e.g. `Asia/Kolkata` — cron is tz-dependent, storage is UTC |
| `task_type`, `payload`, `queue_name`, `priority` | template for created tasks |
| `enabled` | bool — soft disable beats deletion |
| `last_run_at`, `next_run_at` | index `next_run_at`; it's the hot query |
| `catchup_policy` | see 3.4 |

**Constraint to think about:** exactly one of `cron_expression` / `interval_seconds` /
`run_at` should be set. Expressible as a DB `CHECK`, or does it belong in Pydantic
validation? Argue both sides.

### 3.2 — Trigger computation

Add `croniter`. Build one function whose entire job is: given a schedule and a "now",
return the next fire time. Pure — no I/O, no DB.

**Why pure:** highest-risk logic in the phase, and trivially unit-testable if it touches
nothing. Test DST boundaries, Feb 29, and `*/7` rollover across a month end.

### 3.3 — Scheduler process

**New:** `src/taskforge/scheduler.py`, its own Compose service.

Loop: sleep ~1s → `SELECT ... WHERE enabled AND next_run_at <= now()` → create task,
enqueue → update `last_run_at`, compute new `next_run_at`.

| Concern | Think about |
|---|---|
| Tick interval | 1s → up to 1s scheduling jitter. What's the accuracy requirement? |
| Ordering | Enqueue then update `next_run_at`, or reverse? One risks duplicate fire, the other risks missed fire. There is no third option — **pick your failure mode deliberately.** Crux of the phase. |
| Transaction boundary | Should enqueue + `next_run_at` update be atomic? They can't be — Redis and Postgres share no transaction. Look up **dual-write problem**. |

### 3.4 — Misfire / catchup policy

Scheduler down 09:00–11:00, hourly schedule missed two fires. On restart:

| Policy | Behavior |
|---|---|
| `skip` | ignore missed fires, resume from now |
| `fire_once` | run once immediately, then resume |
| `catchup_all` | run every missed interval — Airflow's default, and a notorious foot-gun; a week of downtime on a 5-minute schedule spawns 2,016 tasks at once |

Implement `skip` first. Understand why Airflow's choice causes so much pain.

### 3.5 — Exactly-once firing

| | |
|---|---|
| **New table** | `task_runs` — links `schedule_id` → spawned `task_id` with the logical `scheduled_for` timestamp |
| **Mechanism** | `UNIQUE (schedule_id, scheduled_for)`. To fire, insert that row first; if it raises a unique violation, another scheduler already fired this slot → skip. |
| **Concept** | The correct answer, and worth understanding why: the uniqueness guarantee is pushed down into the database — the only component here that can enforce it atomically. Locks and check-then-act both have races. A unique constraint does not. |
| **Bonus** | This table is also the lineage record for Phase 6, and it's what makes Phase 5's leader election a safety net rather than a correctness requirement. |

---

# Phase 4 — DAG Engine

**Time:** 5–6 sessions
**Exit:** a diamond DAG (A→B, A→C, B+D→E) runs in correct order; a mid-DAG failure marks
downstream tasks skipped rather than hanging.

### 4.1 — Model

| Table | Purpose |
|---|---|
| `dags` | definition: id, name, schedule_id (nullable), enabled |
| `dag_nodes` | one row per task in the definition: node_key, task_type, payload, retry config, `trigger_rule` |
| `dag_edges` | `(dag_id, upstream_node_key, downstream_node_key)` — adjacency as rows |
| `dag_runs` | one execution: id, dag_id, `scheduled_for`, status, started/finished |
| `node_instances` | one row per node per run: status, task_id, attempt. **`UNIQUE (dag_run_id, node_key)`** |

That last constraint is load-bearing — see 4.4.

**Design question:** edges as a table, or a `depends_on` array column on `dag_nodes`?
Both work. Which makes "find all downstream nodes of X" cheaper?

### 4.2 — Cycle detection

Reject cycles *at definition time*, not run time. Kahn's algorithm (repeatedly remove
in-degree-0 nodes; leftovers = cycle) or DFS with three-color marking.

Also validate: every edge references a real node; at least one root; no self-edges.

**Why definition-time:** a cycle found at runtime means half the DAG already executed side
effects. Validation belongs at the boundary.

### 4.3 — Dependency resolver

On every node completion: find downstream neighbours, evaluate whether each is runnable.

**Trigger rules** — Airflow's vocabulary, worth adopting:

| Rule | Fires when |
|---|---|
| `all_success` | every upstream succeeded (default) |
| `all_done` | every upstream finished, success or not — cleanup tasks |
| `one_failed` | any upstream failed — alerting tasks |
| `all_failed` | every upstream failed |

Failure propagation: when a node fails past its retries, downstream `all_success` nodes
become `UPSTREAM_FAILED` — a *distinct* status from `FAILED`. Don't collapse them;
"didn't run because a dependency broke" and "ran and broke" are different facts you'll
want to separate in Phase 6.

Also define precisely: when does a `dag_run` itself become `SUCCESS` or `FAILED`?

### 4.4 — The race you will hit

Node E depends on B and D. B and D finish on two different workers *at the same instant*.
Both resolvers ask "are all of E's upstreams done?" Both see yes. **Both enqueue E.**

Three real fixes, ascending quality:

1. `SELECT ... FOR UPDATE` on the `dag_run` row — serializes resolution per run. Simple, some contention.
2. The `UNIQUE (dag_run_id, node_key)` constraint from 4.1 — the second insert fails, so the second enqueue never happens. Cheap and correct.
3. Single-consumer resolution: completions go to a queue, one resolver processes serially. Scales worst, reasons best.

**Do #2.** But work out *why* #1 alone is weaker — and notice this is the same shape of
solution as 3.5. That repetition is the lesson: push invariants into the layer that can
enforce them atomically.

### 4.5 — DAG API

`POST /dags` (validate + store), `POST /dags/{id}/runs` (manual trigger),
`GET /dags/{id}/runs/{run_id}` returning every node's status, timing, attempt count.

That last response *is* what Airflow's graph view renders. If it's complete, the UI could
be drawn. It does not need to be drawn.

---

# Phase 5 — Leader Election

**Time:** 2–3 sessions
**Exit:** two schedulers running; kill the leader; schedules keep firing with no
duplicates and no gap longer than the lock TTL.

### 5.1 — Establish the problem first

Before writing any lock: run two scheduler containers. Watch both fire the same schedule.

Then notice — if 3.5 was done correctly, `UNIQUE (schedule_id, scheduled_for)` already
stopped the duplicate *task* from being created. So, honestly: **is leader election a
correctness fix, or an efficiency fix?**

The answer is nuanced and it's the most interesting question in the project. Work it out
before continuing.

### 5.2 — Distributed lock

`SET taskforge:scheduler:leader <unique-token> NX PX <ttl>`

| Element | Why |
|---|---|
| `NX` | set only if absent — the atomic acquire |
| `PX <ttl>` | holder dies → lock expires on its own |
| unique token | so you only ever release *your* lock |

Release must be compare-and-delete: check token matches, then delete — **atomically**.
Two separate commands is a race (you check, your lock expires, someone else acquires, you
delete theirs). Redis's answer is a small `EVAL` Lua script. Canonical example of why
Redis has Lua at all.

### 5.3 — Renewal and failover

Leader refreshes the TTL on an interval well under the TTL. Followers retry acquisition on
a loop. On acquiring, a follower must not assume it knows anything about in-flight state —
it re-reads from Postgres.

**The TTL tradeoff, plainly:** short TTL → fast failover, but a GC pause makes the leader
lose leadership it still thinks it holds. Long TTL → stable, but a crash leaves you idle
for that whole window. There is no correct value, only a choice about which failure hurts
less.

### 5.4 — Then read the critique

Read Martin Kleppmann's "How to do distributed locking", and Redis's Redlock page which
responds to it.

Core claim: **no lease-based lock is safe by itself** — a process can be paused (GC, VM
suspend) past its lease expiry and resume believing it still holds the lock. Mitigation is
a **fencing token**: a monotonically increasing number the downstream resource checks and
rejects if stale.

Then connect it back: the Phase 3.5 unique constraint is effectively acting as the fence.
This is why 5.1's question matters, and why real systems put the invariant at the data
layer rather than trusting the lock.

Being able to explain this section aloud is understanding distributed systems better than
most people who list them on a résumé.

### 5.5 — Verify

Scale to 2 schedulers. Confirm exactly one leader in the logs. `docker kill` it. Time the
gap before failover — should be roughly the TTL. Confirm `task_runs` shows no duplicate
`(schedule_id, scheduled_for)` pair across the whole test.

---

# Phase 6 — Observability

**Time:** 2–3 sessions
**Exit:** point at a dashboard and explain every number, including which ones would page
you at 3am.

### 6.1 — Lineage

Add to tasks: `enqueued_at`, `started_at`, `finished_at`, `worker_id`, `attempt`,
`triggered_by_schedule_id`, `dag_run_id`.

Three derived numbers matter, and they're different things:

- **queue latency** = `started_at − enqueued_at` → not enough workers
- **execution time** = `finished_at − started_at` → slow tasks
- **end-to-end** = `finished_at − scheduled_for` → what a user actually experiences

Conflating these is the most common mistake in queue monitoring.

### 6.2 — Metrics

Add `prometheus-client`; expose `/metrics` on the API and on workers.

| Metric | Type | Why that type |
|---|---|---|
| `tasks_enqueued_total{queue,task_type}` | Counter | monotonic; you `rate()` it |
| `tasks_completed_total{status}` | Counter | failure ratio derives from this |
| `queue_depth{queue}` | Gauge | point-in-time level |
| `task_duration_seconds` | Histogram | percentiles, not a mean |
| `workers_alive` | Gauge | from heartbeat keys |
| `scheduler_is_leader` | Gauge 0/1 | sum across instances should always equal 1 |

**Counter vs Gauge vs Histogram is the thing to actually learn** — picking wrong makes a
metric unqueryable. And never average a percentile across instances; that number is
meaningless.

**Gotcha:** multiple uvicorn workers each keep their own registry, so scrapes return
whichever process answered. `prometheus_client` has a multiprocess mode. Know it exists
before it burns an hour.

### 6.3 — Grafana

Add `prometheus` and `grafana` to Compose. One dashboard: queue depth over time,
throughput, p50/p95/p99 duration, failure rate, worker count, current leader.

Don't build panels from scratch — import a community queue dashboard and edit it. The
learning is in the PromQL, not the UI.

### 6.4 — Alerts (stretch)

Define rules and, more importantly, argue each threshold: queue depth growing
monotonically for 5 minutes; failure rate above 5%; `sum(scheduler_is_leader) != 1`; zero
workers alive.

That last one catches both split-brain and total scheduler death in one expression.
Understanding why is a nice capstone.

---

## Where the real learning is concentrated

If only four things are deeply understood from this project, make it these:

| Step | Idea |
|---|---|
| **2.2** | At-least-once delivery, and why idempotency — not better timeouts — is the answer |
| **3.5** | Pushing an invariant into the database as the only atomic enforcement point |
| **4.4** | Concurrent state machines racing, and the three-tier quality ladder of fixes |
| **5.4** | Why lease-based locks aren't safe alone, and what a fencing token does about it |

Everything else is scaffolding around those four ideas.
