"""Durable job store: create, claim, heartbeat, retry, control and audit jobs.

All state lives in the database, so any number of API servers and worker processes
can share it, and nothing is lost when a process dies.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, insert, or_, select, update
from sqlalchemy.engine import Engine

from .db import job_events, jobs, workers

TERMINAL = {"completed", "failed", "cancelled"}
ACTIVE = {"queued", "running"}


def now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value):
    return value.isoformat() if isinstance(value, datetime) else value


def _row(row) -> dict | None:
    return {k: _iso(v) for k, v in row._mapping.items()} if row is not None else None


class InvalidTransition(Exception):
    pass


class JobStore:
    def __init__(self, engine: Engine):
        self.engine = engine

    # ------------------------------------------------------------------ create / read
    def create(self, kind: str, payload: dict, max_attempts: int) -> dict:
        job_id = uuid.uuid4().hex[:12]
        t = now()
        with self.engine.begin() as c:
            c.execute(insert(jobs).values(
                id=job_id, kind=kind, input=payload, status="queued", attempts=0, max_attempts=max_attempts,
                next_run_at=t, pause_requested=0, cancel_requested=0, created_at=t, updated_at=t,
                progress={"step": 0, "total": None, "label": "queued"},
            ))
            self._event(c, job_id, "created", {"input": payload})
        return self.get(job_id)

    def get(self, job_id: str) -> dict | None:
        with self.engine.connect() as c:
            return _row(c.execute(select(jobs).where(jobs.c.id == job_id)).first())

    def list(self, limit: int = 50) -> list[dict]:
        with self.engine.connect() as c:
            rows = c.execute(select(jobs).order_by(jobs.c.created_at.desc()).limit(limit)).all()
        return [_row(r) for r in rows]

    # ------------------------------------------------------------------ claiming
    def claim(self, worker_id: str) -> dict | None:
        """Atomically take the oldest due job. Safe with many concurrent workers."""
        t = now()
        with self.engine.begin() as c:
            query = (select(jobs.c.id)
                     .where(jobs.c.status == "queued", jobs.c.next_run_at <= t)
                     .order_by(jobs.c.next_run_at, jobs.c.created_at).limit(1)
                     .with_for_update(skip_locked=True))  # concurrent workers skip each other's locked rows
            job_id = c.execute(query).scalar()
            if job_id is None:
                return None
            claimed = c.execute(
                update(jobs).where(jobs.c.id == job_id, jobs.c.status == "queued")
                .values(status="running", locked_by=worker_id, heartbeat_at=t, updated_at=t,
                        attempts=jobs.c.attempts + 1)
            ).rowcount
            if not claimed:
                return None
            self._event(c, job_id, "claimed", {"worker": worker_id}, worker_id)
        return self.get(job_id)

    def heartbeat(self, job_id: str, worker_id: str) -> bool:
        """Extend the lease. Returns False if the job is no longer ours (e.g. it was reclaimed)."""
        with self.engine.begin() as c:
            return bool(c.execute(
                update(jobs).where(jobs.c.id == job_id, jobs.c.locked_by == worker_id, jobs.c.status == "running")
                .values(heartbeat_at=now())
            ).rowcount)

    def reclaim_stale(self, lease_seconds: float) -> list[str]:
        """Requeue running jobs whose worker stopped heartbeating (crashed or was killed)."""
        cutoff = now() - timedelta(seconds=lease_seconds)
        with self.engine.begin() as c:
            stale = c.execute(select(jobs.c.id, jobs.c.locked_by).where(
                jobs.c.status == "running", or_(jobs.c.heartbeat_at < cutoff, jobs.c.heartbeat_at.is_(None))
            )).all()
            for job_id, worker in stale:
                done = c.execute(update(jobs).where(jobs.c.id == job_id, jobs.c.status == "running").values(
                    status="queued", locked_by=None, next_run_at=now(), updated_at=now(),
                    attempts=jobs.c.attempts - 1,  # a crash isn't the job's fault; don't burn an attempt
                )).rowcount
                if done:
                    self._event(c, job_id, "recovered", {"lost_worker": worker, "lease_seconds": lease_seconds})
        return [s[0] for s in stale]

    # ------------------------------------------------------------------ worker-side transitions
    def update_progress(self, job_id: str, progress: dict) -> None:
        with self.engine.begin() as c:
            c.execute(update(jobs).where(jobs.c.id == job_id).values(progress=progress, updated_at=now()))

    def flags(self, job_id: str) -> dict:
        with self.engine.connect() as c:
            row = c.execute(select(jobs.c.pause_requested, jobs.c.cancel_requested, jobs.c.locked_by)
                            .where(jobs.c.id == job_id)).first()
        return {"pause": bool(row.pause_requested), "cancel": bool(row.cancel_requested), "locked_by": row.locked_by}

    def finish(self, job_id: str, worker_id: str, status: str, *, result: Any = None, error: str | None = None,
               approval: dict | None = None) -> None:
        values = {"status": status, "locked_by": None, "updated_at": now(), "error": error,
                  "pause_requested": 0, "cancel_requested": 0 if status != "cancelled" else 1}
        if status in TERMINAL:
            values["finished_at"] = now()
        if result is not None:
            values["result"] = result
        if status == "waiting_approval":
            values["approval"] = approval
        with self.engine.begin() as c:
            c.execute(update(jobs).where(jobs.c.id == job_id).values(**values))
            self._event(c, job_id, status, {k: v for k, v in
                                            {"error": error, "approval": approval}.items() if v}, worker_id)

    def schedule_retry(self, job_id: str, worker_id: str, error: str, base: float, cap: float) -> dict:
        """Back off exponentially (with jitter) or fail permanently when attempts run out."""
        job = self.get(job_id)
        if job["attempts"] >= job["max_attempts"]:
            self.finish(job_id, worker_id, "failed", error=error)
            return {"status": "failed"}
        delay = min(cap, base * 2 ** (job["attempts"] - 1)) * random.uniform(0.8, 1.2)
        run_at = now() + timedelta(seconds=delay)
        with self.engine.begin() as c:
            c.execute(update(jobs).where(jobs.c.id == job_id).values(
                status="queued", locked_by=None, next_run_at=run_at, error=error, updated_at=now()))
            self._event(c, job_id, "retry_scheduled", {
                "error": error, "attempt": job["attempts"], "max_attempts": job["max_attempts"],
                "delay_seconds": round(delay, 1)}, worker_id)
        return {"status": "queued", "delay": delay}

    def take_resume_value(self, job_id: str) -> Any:
        with self.engine.begin() as c:
            value = c.execute(select(jobs.c.resume_value).where(jobs.c.id == job_id)).scalar()
            if value is not None:
                c.execute(update(jobs).where(jobs.c.id == job_id).values(resume_value=None))
        return value

    # ------------------------------------------------------------------ user-side controls
    def approve(self, job_id: str, decision: dict) -> dict:
        with self.engine.begin() as c:
            ok = c.execute(update(jobs).where(jobs.c.id == job_id, jobs.c.status == "waiting_approval").values(
                status="queued", resume_value=decision, approval=None, next_run_at=now(), updated_at=now(),
                attempts=0,  # waiting for a human is not a failed attempt
            )).rowcount
            if not ok:
                raise InvalidTransition("This job is not waiting for approval.")
            self._event(c, job_id, "approval_given", decision)
        return self.get(job_id)

    def pause(self, job_id: str) -> dict:
        return self._control(job_id, "pause")

    def cancel(self, job_id: str) -> dict:
        return self._control(job_id, "cancel")

    def resume(self, job_id: str) -> dict:
        with self.engine.begin() as c:
            ok = c.execute(update(jobs).where(jobs.c.id == job_id, jobs.c.status.in_(["paused", "failed"])).values(
                status="queued", next_run_at=now(), pause_requested=0, updated_at=now(), error=None,
                attempts=0,  # a manual resume/retry gets a fresh set of attempts
            )).rowcount
            if not ok:
                raise InvalidTransition("Only paused or failed jobs can be resumed.")
            self._event(c, job_id, "resumed_by_user", {})
        return self.get(job_id)

    def _control(self, job_id: str, action: str) -> dict:
        job = self.get(job_id)
        if job is None:
            raise KeyError(job_id)
        with self.engine.begin() as c:
            if job["status"] in TERMINAL:
                raise InvalidTransition(f"Job is already {job['status']}.")
            if action == "pause" and job["status"] in ("paused", "waiting_approval"):
                raise InvalidTransition(f"Job is {job['status']}.")
            if job["status"] in ("queued", "paused", "waiting_approval"):
                # Not running: apply immediately.
                status = "cancelled" if action == "cancel" else "paused"
                c.execute(update(jobs).where(jobs.c.id == job_id).values(
                    status=status, updated_at=now(), finished_at=now() if status == "cancelled" else None))
                self._event(c, job_id, status, {"by": "user"})
            else:
                # Running: the worker stops cleanly at the next checkpoint boundary.
                flag = "cancel_requested" if action == "cancel" else "pause_requested"
                c.execute(update(jobs).where(jobs.c.id == job_id).values(**{flag: 1, "updated_at": now()}))
                self._event(c, job_id, f"{action}_requested", {"by": "user"})
        return self.get(job_id)

    # ------------------------------------------------------------------ events
    def add_event(self, job_id: str, type_: str, data: dict, worker: str | None = None) -> None:
        with self.engine.begin() as c:
            self._event(c, job_id, type_, data, worker)

    @staticmethod
    def _event(c, job_id: str, type_: str, data: dict, worker: str | None = None) -> None:
        c.execute(insert(job_events).values(job_id=job_id, at=now(), type=type_, data=data, worker=worker))

    def events(self, job_id: str, after_id: int = 0, limit: int = 500) -> list[dict]:
        with self.engine.connect() as c:
            rows = c.execute(select(job_events).where(and_(job_events.c.job_id == job_id, job_events.c.id > after_id))
                             .order_by(job_events.c.id).limit(limit)).all()
        return [_row(r) for r in rows]

    # ------------------------------------------------------------------ workers
    def register_worker(self, worker_id: str, host: str, pid: int) -> None:
        t = now()
        with self.engine.begin() as c:
            c.execute(workers.delete().where(workers.c.id == worker_id))
            c.execute(insert(workers).values(id=worker_id, host=host, pid=pid, started_at=t, heartbeat_at=t,
                                             processed=0, status="idle"))

    def worker_heartbeat(self, worker_id: str, status: str, current_job: str | None, processed: int) -> None:
        with self.engine.begin() as c:
            c.execute(update(workers).where(workers.c.id == worker_id).values(
                heartbeat_at=now(), status=status, current_job=current_job, processed=processed))

    def list_workers(self, alive_within: float = 60.0) -> list[dict]:
        with self.engine.connect() as c:
            rows = c.execute(select(workers).order_by(workers.c.started_at)).all()
        out = []
        for r in rows:
            item = _row(r)
            age = (now() - r.heartbeat_at.replace(tzinfo=r.heartbeat_at.tzinfo or timezone.utc)).total_seconds()
            item["seconds_since_heartbeat"] = round(age, 1)
            item["alive"] = item["status"] != "stopped" and age < alive_within
            out.append(item)
        return out
