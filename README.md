# Durable Agent Jobs

Long-running AI research jobs that **survive crashes, restarts and deploys**. Submit a topic,
and an agent plans sub-questions, waits for you to approve the plan, researches each question
with web search, and writes a cited report. That can take minutes. Kill a worker halfway
through, and another worker picks the job up **at the next unfinished step**, never from
scratch.

Built with **LangGraph** (Postgres checkpointer, `interrupt()`, `RetryPolicy`) and **LangChain**
(`ChatAnthropic` with Claude's server-side web search), a **PostgreSQL** job queue with separate
worker processes, a **FastAPI** API and a live **Streamlit** dashboard.

![Demo](docs/demo.gif)

<sub>Demo uses scripted model replies; the API, two worker processes, PostgreSQL, the crash and the recovery are real.</sub>

| Plan approval | Worker crash → recovery | Finished report |
|---|---|---|
| ![Approval](docs/screenshots/approval.png) | ![Recovery](docs/screenshots/crash-recovery.png) | ![Report](docs/screenshots/report.png) |

## Features

- **Crash and resume:**
  - LangGraph checkpoints the job state in Postgres after every step.
  - Workers hold a **lease** and heartbeat it. If a worker dies (`kill -9`, OOM, a deploy), its
    lease expires and another worker reclaims the job.
  - The new worker resumes from the last checkpoint, so completed steps never re-run.
- **Retries at two levels:**
  - *In the node:* LangGraph `RetryPolicy` retries transient API errors (rate limits, 5xx,
    connection drops) with backoff.
  - *Per job:* any other failure requeues the job with **exponential backoff and jitter**. It then
    resumes at the failed step, and fails permanently after `max_attempts`.
- **Human approval:** after planning, the job **pauses with `interrupt()`** until someone approves
  the plan, optionally editing the questions. It can wait for days, and no worker is held while
  it waits.
- **Pause, resume and cancel:** these take effect at the next checkpoint boundary, so the job
  stops cleanly between steps and never in the middle of one.
- **Many workers, one queue:** jobs are claimed with `SELECT … FOR UPDATE SKIP LOCKED`, so every
  job runs on exactly one worker. Start as many `python main.py worker` processes as you like, on
  one machine or several.
- **Graceful shutdown:** Ctrl+C makes a worker finish its current step and hand the job back.
- **Durable event log:** every transition is stored in `job_events`: claimed, step started or
  done, crashed, recovered, retry scheduled, approval given. The UI and the SSE stream read it,
  so they work across processes and restarts.
- **Real research:** each question is answered by Claude using Anthropic's **web search tool**, with
  cited sources. The writer step then turns the findings into one report with numbered citations.
- **Chaos testing:** with `ALLOW_CHAOS=true`, a job can ask its worker to "crash" after N steps,
  so you can watch recovery happen.
- **One database:** PostgreSQL holds the job queue, the event log and the LangGraph checkpoints. A job's state and its queue row commit to the same place.

## Project structure

```
durable-agent-jobs/
├── README.md
├── requirements.txt
├── .env.example            # copy to .env (never commit .env)
├── .gitignore
├── Dockerfile
├── docker-compose.yml      # Postgres + API + 2 workers + UI
├── main.py                 # FastAPI app (`main:app`) + `python main.py worker`
├── ui/
│   └── app.py              # Streamlit dashboard: jobs, steps, approval, workers, event log
├── docs/                   # demo.gif + screenshots
├── src/
│   ├── agent/
│   │   ├── agent.py        # LangGraph graph: plan -> approve (interrupt) -> research xN -> write
│   │   ├── state.py        # graph state (checkpointed after every node)
│   │   └── memory.py       # PostgresSaver (psycopg connection pool)
│   ├── jobs/
│   │   ├── db.py           # SQLAlchemy schema: jobs, job_events, workers
│   │   ├── store.py        # claim (SKIP LOCKED), leases, retries, controls, events
│   │   └── worker.py       # worker loop: heartbeat, resume, stop at boundaries, backoff
│   ├── models/
│   │   └── llm_client.py   # ChatAnthropic: plan (JSON schema), research (web search), write
│   ├── prompts/            # planning, research and writing prompts
│   ├── utils/              # config (env/.env), logging
│   └── api/
│       ├── routes.py       # FastAPI: jobs, approve/pause/resume/cancel, SSE, workers
│       └── schemas.py      # Pydantic models
├── tests/
│   ├── test_store.py       # concurrent claiming, lease reclaim, backoff, controls
│   ├── test_worker.py      # crash/resume, retries, approval, pause/cancel, graceful shutdown
│   └── test_api.py         # HTTP lifecycle with an embedded worker
└── logs/
```

## Setup

```bash
cd durable-agent-jobs
python -m venv .venv
# Windows: .venv\Scripts\activate   |   macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env              # set ANTHROPIC_API_KEY, DATABASE_URL and TEST_DATABASE_URL
```

Create the two databases once (in psql or pgAdmin):

```sql
CREATE DATABASE durable_jobs;
CREATE DATABASE durable_jobs_test;   -- used only by pytest
```

Tables and checkpoint storage are created automatically on first start.

## Run it

```bash
uvicorn main:app --reload         # terminal 1: API          -> http://localhost:8000/docs
python main.py worker             # terminal 2: a worker
python main.py worker             # terminal 3: another worker (optional)
streamlit run ui/app.py           # terminal 4: dashboard    -> http://localhost:8501
```

Or `docker compose up --build` for Postgres, the API, two workers and the UI in one go.

**Watch it survive a crash:** set `ALLOW_CHAOS=true`, submit a job with
"kill the worker after 3 steps", and approve the plan. The worker dies mid-job; within the
lease time the other worker logs `recovered` → `resumed from checkpoint` and finishes the job.
You can also simply close a worker's terminal mid-job.

## API

| Method | Path                    | Description                                                     |
|--------|-------------------------|-----------------------------------------------------------------|
| POST   | `/jobs`                 | Submit `{"topic": "...", "require_approval": true}`             |
| GET    | `/jobs`                 | Recent jobs with status and progress                            |
| GET    | `/jobs/{id}`            | Job + live graph state (plan, findings, completed steps)        |
| POST   | `/jobs/{id}/approve`    | `{"approved": true, "questions": [...]}` or `{"approved": false}` |
| POST   | `/jobs/{id}/pause`      | Pause (at the next step boundary if running)                    |
| POST   | `/jobs/{id}/resume`     | Resume a paused job, or retry a failed one                      |
| POST   | `/jobs/{id}/cancel`     | Cancel                                                          |
| GET    | `/jobs/{id}/events`     | **Server-Sent Events** from the persisted event log             |
| GET    | `/workers`              | Workers, heartbeats and current jobs                            |

Job states: `queued → running → waiting_approval → queued → running → completed`, plus
`paused`, `failed` (after `max_attempts`) and `cancelled`.

## Tests

```bash
pytest -q          # runs against TEST_DATABASE_URL (a dedicated PostgreSQL test database)
```

The worker tests kill a worker mid-job (a `BaseException` nothing catches, like a real crash),
let the lease expire, and assert that a second worker finishes the job **without re-running
any completed step**.

## Configuration

| Variable                   | Default                  | Meaning                                         |
|----------------------------|--------------------------|-------------------------------------------------|
| `DATABASE_URL`             | *(required)*             | `postgresql://user:pass@host:5432/durable_jobs` |
| `TEST_DATABASE_URL`        | *(for tests)*            | A separate database; truncated between tests    |
| `AGENT_MODEL`              | `claude-opus-5-5`        | Claude model                                    |
| `WEB_SEARCH_MAX_USES`      | `4`                      | Web searches per research question              |
| `WORKER_LEASE_SECONDS`     | `30`                     | Silence after which a job is reclaimed          |
| `WORKER_HEARTBEAT_SECONDS` | `5`                      | Lease renewal interval                          |
| `JOB_MAX_ATTEMPTS`         | `3`                      | Attempts before a job fails permanently         |
| `RETRY_BASE_SECONDS`       | `5`                      | Backoff base (doubles per attempt, ±20% jitter) |
| `EMBEDDED_WORKERS`         | `0`                      | Run workers inside the API process              |
| `ALLOW_CHAOS`              | `false`                  | Enable the simulated-crash demo option          |

## How it works

```mermaid
graph LR
    API[FastAPI] -- insert job --> DB[(PostgreSQL<br/>jobs · events · checkpoints)]
    W1[worker 1] -- "claim (SKIP LOCKED)<br/>heartbeat lease" --> DB
    W2[worker 2] -- "reclaim stale leases" --> DB
    subgraph "LangGraph job (checkpointed per node)"
      plan --> approve{{"interrupt()"}} --> research1 --> research2 --> research3 --> write
    end
    W1 -. runs .-> plan
```

1. The **API** inserts a `queued` row and returns. It never blocks on the agent.
2. A **worker** claims it, starts heartbeating, and runs the LangGraph graph with
   `thread_id = job_id`. After every node, the checkpointer writes the state to Postgres.
3. At `approve`, `interrupt()` stops the graph. The worker marks the job `waiting_approval` and
   moves on. Approving stores the decision and requeues the job, and the next worker resumes it
   with `Command(resume=decision)`.
4. If the worker dies, its heartbeat stops. Any worker's reaper requeues the job after the
   lease, and the next claim calls `graph.stream(None, config)`, which continues from the last
   checkpoint.
5. On an exception, the job is requeued with backoff. The retry also resumes from the last
   checkpoint, so only the failed step runs again.

## Ideas to extend

- Scheduled jobs (cron) and job priorities.
- Fan out research questions to several workers in parallel, then join.
- Postgres `LISTEN/NOTIFY` instead of polling for instant pickup.
