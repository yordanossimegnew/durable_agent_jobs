"""FastAPI app: submit, inspect, control and stream durable research jobs.

The API never runs jobs itself (unless EMBEDDED_WORKERS > 0). It writes to the database;
worker processes pick the work up. Event streams read the persisted event log, so they
work across processes and survive restarts.
"""

from __future__ import annotations

import json
import threading
import time
from contextlib import asynccontextmanager
from typing import Callable

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

from ..jobs import TERMINAL, InvalidTransition, JobStore, Worker, completed_steps, make_job_factory, make_store
from ..agent import total_steps
from ..utils.config import Settings, load_settings
from ..utils.logger import get_logger
from .schemas import ApprovalRequest, HealthResponse, Job, JobDetail, JobRequest

log = get_logger("api")
STREAM_UNTIL = TERMINAL | {"waiting_approval", "paused"}


def _redact_db(url: str) -> str:
    if "@" in url:
        scheme, rest = url.split("://", 1)
        return f"{scheme}://***@{rest.split('@', 1)[1]}"
    return url


def create_app(settings: Settings | None = None, store: JobStore | None = None,
               job_factory: Callable | None = None) -> FastAPI:
    settings = settings or load_settings()
    store = store or make_store(settings)
    job_factory = job_factory or make_job_factory(settings)
    embedded: list[tuple[Worker, threading.Thread]] = []

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        for i in range(settings.embedded_workers):
            worker = Worker(settings, store, job_factory)
            thread = threading.Thread(target=worker.run_forever, daemon=True, name=f"embedded-worker-{i}")
            thread.start()
            embedded.append((worker, thread))
        yield
        for worker, thread in embedded:
            worker.stop_event.set()
        for worker, thread in embedded:
            thread.join(timeout=10)

    app = FastAPI(title="Durable Agent Jobs", version="1.0.0", lifespan=lifespan)

    def get_job(job_id: str) -> dict:
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job

    def control(fn, job_id: str) -> dict:
        get_job(job_id)
        try:
            return fn(job_id)
        except InvalidTransition as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        alive = sum(w["alive"] for w in store.list_workers(alive_within=settings.lease_seconds))
        return HealthResponse(model=settings.model, database=_redact_db(settings.database_url), workers_alive=alive,
                              embedded_workers=settings.embedded_workers, chaos_enabled=settings.allow_chaos)

    @app.post("/jobs", response_model=Job, status_code=201)
    def create_job(request: JobRequest) -> dict:
        payload = {"topic": request.topic, "require_approval": request.require_approval}
        if request.chaos_crash_after_step:
            if not settings.allow_chaos:
                raise HTTPException(status_code=400, detail="Chaos testing is disabled (set ALLOW_CHAOS=true).")
            payload["chaos_crash_after_step"] = request.chaos_crash_after_step
        return store.create("research", payload, request.max_attempts or settings.default_max_attempts)

    @app.get("/jobs", response_model=list[Job])
    def list_jobs(limit: int = Query(50, ge=1, le=200)) -> list[dict]:
        return store.list(limit)

    @app.get("/jobs/{job_id}", response_model=JobDetail)
    def job_detail(job_id: str) -> dict:
        job = get_job(job_id)
        values = job_factory().graph.get_state({"configurable": {"thread_id": job_id}}).values or {}
        state = {"questions": values.get("questions"), "rationale": values.get("rationale"),
                 "findings": values.get("findings") or [], "completed_steps": completed_steps(values),
                 "total_steps": total_steps(values) if values else None, "usage": values.get("usage")}
        return {**job, "state": state}

    @app.post("/jobs/{job_id}/approve", response_model=Job)
    def approve(job_id: str, request: ApprovalRequest) -> dict:
        decision = request.model_dump(exclude_none=True)
        return control(lambda j: store.approve(j, decision), job_id)

    @app.post("/jobs/{job_id}/pause", response_model=Job)
    def pause(job_id: str) -> dict:
        return control(store.pause, job_id)

    @app.post("/jobs/{job_id}/resume", response_model=Job)
    def resume(job_id: str) -> dict:
        return control(store.resume, job_id)

    @app.post("/jobs/{job_id}/cancel", response_model=Job)
    def cancel(job_id: str) -> dict:
        return control(store.cancel, job_id)

    @app.get("/jobs/{job_id}/events")
    def events(job_id: str, after: int = Query(0, ge=0), follow: bool = True,
               poll: float = Query(0.5, ge=0.05, le=5)) -> StreamingResponse:
        """Server-Sent Events from the persisted event log. With follow=true, streams until the job
        stops (finished, paused or waiting for approval)."""
        get_job(job_id)

        def sse():
            last, idle = after, 0.0
            while True:
                batch = store.events(job_id, after_id=last)
                for e in batch:
                    last = e["id"]
                    yield f"data: {json.dumps(e)}\n\n"
                status = store.get(job_id)["status"]
                if not follow or (status in STREAM_UNTIL and not batch):
                    yield f"event: end\ndata: {json.dumps({'type': 'end', 'status': status})}\n\n"
                    return
                idle = 0.0 if batch else idle + poll
                if idle >= 15:
                    yield ": keepalive\n\n"
                    idle = 0.0
                time.sleep(poll)

        return StreamingResponse(sse(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/workers")
    def workers() -> list[dict]:
        return store.list_workers(alive_within=settings.lease_seconds)

    return app
