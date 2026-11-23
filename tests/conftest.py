"""Fixtures. Tests run against a real PostgreSQL test database.

Set TEST_DATABASE_URL (in the environment or .env), pointing at a database used only for tests:

    TEST_DATABASE_URL=postgresql://postgres:<password>@localhost:5432/durable_jobs_test
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from langgraph.types import RetryPolicy
from sqlalchemy import text

from src.agent import NODE_RETRY
from src.jobs import Worker, make_job_factory, make_store
from src.utils.config import Settings
from tests.fakes import FakeModels, WorkerKilled

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
FAST_RETRY = RetryPolicy(max_attempts=3, initial_interval=0.01, backoff_factor=1.0, jitter=False,
                         retry_on=NODE_RETRY.retry_on)
TABLES = ("job_events", "jobs", "workers", "checkpoint_writes", "checkpoint_blobs", "checkpoints")


def pytest_collection_modifyitems(config, items):
    if not TEST_DATABASE_URL:
        skip = pytest.mark.skip(reason="TEST_DATABASE_URL is not set (see tests/conftest.py)")
        for item in items:
            item.add_marker(skip)


@pytest.fixture
def settings(tmp_path) -> Settings:
    s = Settings(database_url=TEST_DATABASE_URL, lease_seconds=0.6, heartbeat_seconds=0.1, poll_seconds=0.05,
                 retry_base_seconds=0.01, retry_max_seconds=0.05, allow_chaos=True,
                 knowledge_base_dir=tmp_path / "kb", log_dir=tmp_path / "logs")
    make_job_factory(s).checkpointer.conn.close()  # creates the checkpoint tables, then closes its pool
    store = make_store(s)
    with store.engine.begin() as c:  # clean slate for every test
        c.execute(text(f"TRUNCATE {', '.join(TABLES)}"))
    store.engine.dispose()
    return s


@pytest.fixture
def store(settings):
    store = make_store(settings)
    yield store
    store.engine.dispose()


@pytest.fixture
def models():
    return FakeModels()


@pytest.fixture
def factory(settings, models):
    factory = make_job_factory(settings, models, retry=FAST_RETRY)
    yield factory
    factory.checkpointer.conn.close()


@pytest.fixture
def make_worker(settings, store, factory):
    def build(worker_id="w1", **kw):
        def crash():
            raise WorkerKilled()
        return Worker(settings, store, factory, worker_id, crash=crash, **kw)
    return build
