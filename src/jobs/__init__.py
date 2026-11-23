"""Durable job infrastructure: database schema, job store and workers."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from ..agent import ResearchJob, open_checkpointer
from ..models import AnthropicResearchModels, ResearchModels
from ..utils.config import Settings
from .db import init_db, make_engine
from .store import ACTIVE, TERMINAL, InvalidTransition, JobStore
from .worker import Worker, completed_steps


def make_store(settings: Settings) -> JobStore:
    engine = make_engine(settings.database_url)
    init_db(engine)
    return JobStore(engine)


def make_job_factory(settings: Settings, models: ResearchModels | None = None, checkpointer: Any = None,
                     retry: Any = None):
    """Returns a zero-arg callable building a ResearchJob that shares one checkpointer."""
    saver = checkpointer or open_checkpointer(settings.database_url)

    @lru_cache(maxsize=1)
    def default_models():
        return AnthropicResearchModels(settings)

    def factory() -> ResearchJob:
        return ResearchJob(models or default_models(), saver, **({"retry": retry} if retry else {}))

    factory.checkpointer = saver  # type: ignore[attr-defined]
    return factory


__all__ = [
    "ACTIVE", "InvalidTransition", "JobStore", "TERMINAL", "Worker", "completed_steps", "make_job_factory",
    "make_store",
]
