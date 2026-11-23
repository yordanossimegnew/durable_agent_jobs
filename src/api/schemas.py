"""Request/response models for the HTTP API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class JobRequest(BaseModel):
    topic: str = Field(..., min_length=3, max_length=500, description="What to research.")
    require_approval: bool = Field(True, description="Pause after planning until a human approves the plan.")
    max_attempts: int | None = Field(None, ge=1, le=10, description="Attempts before the job fails permanently.")
    chaos_crash_after_step: int | None = Field(
        None, ge=1, le=20, description="Demo only (needs ALLOW_CHAOS=true): kill the worker after N steps.")


class ApprovalRequest(BaseModel):
    approved: bool
    questions: list[str] | None = Field(None, description="Optionally edit the plan before approving.")
    reason: str | None = Field(None, max_length=500, description="Why the plan was rejected.")


class Job(BaseModel):
    id: str
    kind: str
    input: dict
    status: str
    attempts: int
    max_attempts: int
    next_run_at: str | None
    locked_by: str | None
    heartbeat_at: str | None
    pause_requested: int
    cancel_requested: int
    approval: dict | None
    progress: dict | None
    result: dict | None
    error: str | None
    created_at: str
    updated_at: str
    finished_at: str | None


class JobDetail(Job):
    state: dict = Field(default_factory=dict, description="Live graph state from the checkpointer.")


class HealthResponse(BaseModel):
    status: str = "ok"
    model: str
    database: str
    workers_alive: int
    embedded_workers: int
    chaos_enabled: bool
