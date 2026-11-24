"""PostgreSQL schema (SQLAlchemy Core): jobs, their event log, and workers."""

from __future__ import annotations

from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, MetaData, String, Table, Text, create_engine
from sqlalchemy.dialects.postgresql import JSONB as JSON
from sqlalchemy.engine import Engine

metadata = MetaData()

jobs = Table(
    "jobs", metadata,
    Column("id", String(32), primary_key=True),
    Column("kind", String(40), nullable=False),
    Column("input", JSON, nullable=False),
    Column("status", String(20), nullable=False, index=True),
    # queued | running | waiting_approval | paused | completed | failed | cancelled
    Column("attempts", Integer, nullable=False, default=0),
    Column("max_attempts", Integer, nullable=False),
    Column("next_run_at", DateTime(timezone=True), nullable=False),
    Column("locked_by", String(80)),
    Column("heartbeat_at", DateTime(timezone=True)),
    Column("pause_requested", Integer, nullable=False, default=0),
    Column("cancel_requested", Integer, nullable=False, default=0),
    Column("resume_value", JSON),     # approval decision waiting to be fed to Command(resume=...)
    Column("approval", JSON),         # what the job is waiting for a human to approve
    Column("progress", JSON),         # {"step": 2, "total": 6, "label": "..."}
    Column("result", JSON),
    Column("error", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
)
Index("ix_jobs_claim", jobs.c.status, jobs.c.next_run_at)

job_events = Table(
    "job_events", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("job_id", String(32), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True),
    Column("at", DateTime(timezone=True), nullable=False),
    Column("type", String(40), nullable=False),
    Column("data", JSON, nullable=False),
    Column("worker", String(80)),
)

workers = Table(
    "workers", metadata,
    Column("id", String(80), primary_key=True),
    Column("host", String(120), nullable=False),
    Column("pid", Integer, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("heartbeat_at", DateTime(timezone=True), nullable=False),
    Column("current_job", String(32)),
    Column("processed", Integer, nullable=False, default=0),
    Column("status", String(20), nullable=False),  # idle | busy | stopped
    Column("uptime", Float),
)


def sqlalchemy_url(url: str) -> str:
    """Use the psycopg 3 driver for PostgreSQL URLs."""
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix):]
    return url


def make_engine(url: str) -> Engine:
    return create_engine(sqlalchemy_url(url), pool_pre_ping=True, pool_size=5, max_overflow=5)


def init_db(engine: Engine) -> None:
    metadata.create_all(engine)
