"""Worker process: claims jobs from the database and runs them durably.

Run as many as you like (`python main.py worker`), on one machine or several. Each:
  * claims one due job at a time (FOR UPDATE SKIP LOCKED on Postgres),
  * heartbeats a lease while working; if it dies, another worker reclaims the job,
  * runs the LangGraph job from its last checkpoint, never from scratch,
  * checks pause/cancel requests at every checkpoint boundary,
  * retries failures with exponential backoff, then fails permanently.
"""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from typing import Callable

from langgraph.types import Command

from ..agent import ResearchJob, total_steps
from ..utils.config import Settings
from ..utils.logger import get_logger
from .store import JobStore

log = get_logger("worker")


def completed_steps(values: dict) -> int:
    done = 1 if values.get("questions") else 0
    if values.get("require_approval") and values.get("approved"):
        done += 1
    done += len(values.get("findings") or [])
    return done + (1 if values.get("report") else 0)


class Heartbeat(threading.Thread):
    def __init__(self, store: JobStore, job_id: str, worker_id: str, interval: float,
                 on_beat: Callable[[], None] | None = None):
        super().__init__(daemon=True, name=f"heartbeat-{job_id}")
        self.store, self.job_id, self.worker_id, self.interval = store, job_id, worker_id, interval
        self.on_beat = on_beat or (lambda: None)
        self._stop_event = threading.Event()
        self.lost = False

    def run(self) -> None:
        while not self._stop_event.wait(self.interval):
            if not self.store.heartbeat(self.job_id, self.worker_id):
                self.lost = True  # someone reclaimed the job; stop at the next boundary
                return
            self.on_beat()  # also keep the worker row fresh while busy

    def stop(self) -> None:
        self._stop_event.set()


class Worker:
    def __init__(self, settings: Settings, store: JobStore, job_factory: Callable[[], ResearchJob],
                 worker_id: str | None = None, crash: Callable[[], None] | None = None):
        self.settings = settings
        self.store = store
        self.job_factory = job_factory
        self.id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:4]}"
        self.crash = crash or (lambda: os._exit(137))  # chaos testing: die like a killed process
        self.stop_event = threading.Event()
        self.processed = 0
        self.current: str | None = None
        self._last_reap = 0.0

    # ------------------------------------------------------------------ loop
    def run_forever(self) -> None:
        self.store.register_worker(self.id, socket.gethostname(), os.getpid())
        log.info("worker %s started", self.id)
        try:
            while not self.stop_event.is_set():
                self.store.worker_heartbeat(self.id, "idle", None, self.processed)
                if not self.run_once():
                    self.stop_event.wait(self.settings.poll_seconds)
        finally:
            self.store.worker_heartbeat(self.id, "stopped", None, self.processed)
            log.info("worker %s stopped", self.id)

    def run_once(self) -> bool:
        """Reclaim stale jobs, then claim and process one job. Returns True if a job ran."""
        if self.stop_event.is_set():
            return False  # shutting down: don't take new work
        if time.monotonic() - self._last_reap > self.settings.lease_seconds / 3:
            for job_id in self.store.reclaim_stale(self.settings.lease_seconds):
                log.warning("reclaimed job %s from a dead worker", job_id)
            self._last_reap = time.monotonic()
        job = self.store.claim(self.id)
        if job is None:
            return False
        self.process(job)
        return True

    def process(self, job: dict) -> None:
        self.current = job["id"]
        self.store.worker_heartbeat(self.id, "busy", job["id"], self.processed)
        beat = Heartbeat(self.store, job["id"], self.id, self.settings.heartbeat_seconds,
                         on_beat=lambda: self.store.worker_heartbeat(self.id, "busy", job["id"], self.processed))
        beat.start()
        try:
            self._execute(job, beat)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"[:1000]
            log.warning("job %s failed: %s", job["id"], error)
            outcome = self.store.schedule_retry(job["id"], self.id, error, self.settings.retry_base_seconds,
                                                self.settings.retry_max_seconds)
            log.info("job %s -> %s", job["id"], outcome["status"])
        finally:
            beat.stop()
            self.processed += 1
            self.current = None

    # ------------------------------------------------------------------ one job
    def _execute(self, job: dict, beat: Heartbeat) -> None:
        store, job_id = self.store, job["id"]
        runner = self.job_factory()
        config = {"configurable": {"thread_id": job_id}}
        snapshot = runner.graph.get_state(config)

        resume = store.take_resume_value(job_id)
        if resume is not None:
            graph_input = Command(resume=resume)
            store.add_event(job_id, "resumed", {"from": "approval"}, self.id)
        elif snapshot.values:
            graph_input = None  # continue from the last checkpoint
            store.add_event(job_id, "resumed", {"from": "checkpoint", "completed_steps": completed_steps(snapshot.values),
                                                "next": list(snapshot.next)}, self.id)
        else:
            payload = job["input"]
            graph_input = {"topic": payload["topic"], "require_approval": payload.get("require_approval", True),
                           "findings": []}

        steps_this_run = 0
        chaos_at = job["input"].get("chaos_crash_after_step") if self.settings.allow_chaos else None
        for mode, chunk in runner.graph.stream(graph_input, config, stream_mode=["custom", "updates"]):
            if mode == "custom":
                kind = chunk["type"]
                store.add_event(job_id, kind, {k: v for k, v in chunk.items() if k != "type"}, self.id)
                values = runner.graph.get_state(config).values
                done = completed_steps(values)
                step = done + 1 if kind == "step_started" else done
                store.update_progress(job_id, {"step": step, "total": total_steps(values), "label": chunk.get("label")})
                if kind == "step_done":
                    steps_this_run += 1
                    if chaos_at and steps_this_run == chaos_at and not self._crashed_before(job_id):
                        store.add_event(job_id, "chaos_crash", {"after_step": chunk.get("step")}, self.id)
                        self.crash()  # simulated kill -9: no cleanup, the lease just expires
                continue
            if "__interrupt__" in chunk:
                continue
            # A node finished and its checkpoint is saved: a safe point to stop.
            stop = self._should_stop(job_id, beat)
            if stop:
                return

        snapshot = runner.graph.get_state(config)
        values = snapshot.values
        if snapshot.interrupts:
            store.finish(job_id, self.id, "waiting_approval", approval=snapshot.interrupts[0].value)
        elif values.get("approved") is False:
            store.finish(job_id, self.id, "cancelled", error=values.get("rejected_reason", "Plan rejected."))
        elif not snapshot.next and values.get("report"):
            store.update_progress(job_id, {"step": total_steps(values), "total": total_steps(values), "label": "done"})
            store.finish(job_id, self.id, "completed", result={
                "report": values["report"], "questions": values["questions"],
                "findings": values["findings"], "usage": values.get("usage") or {},
            })
        else:
            raise RuntimeError(f"Graph stopped unexpectedly before finishing (next={list(snapshot.next)}).")

    def _should_stop(self, job_id: str, beat: Heartbeat) -> bool:
        flags = self.store.flags(job_id)
        if beat.lost or flags["locked_by"] != self.id:
            log.warning("lost the lease on %s; another worker owns it now", job_id)
            return True
        if flags["cancel"]:
            self.store.finish(job_id, self.id, "cancelled", error="Cancelled by user.")
            return True
        if flags["pause"]:
            self.store.finish(job_id, self.id, "paused")
            return True
        if self.stop_event.is_set():
            self.store.finish(job_id, self.id, "queued")  # graceful shutdown: hand the job back
            return True
        return False

    def _crashed_before(self, job_id: str) -> bool:
        return any(e["type"] == "chaos_crash" for e in self.store.events(job_id))
