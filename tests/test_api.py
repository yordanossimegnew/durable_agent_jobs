"""FastAPI endpoints with an embedded worker."""

from __future__ import annotations

import json
import time
from dataclasses import replace

from fastapi.testclient import TestClient

from src.api import create_app


def app_client(settings, store, factory, workers=1):
    return TestClient(create_app(replace(settings, embedded_workers=workers), store=store, job_factory=factory))


def wait_status(client, job_id, statuses, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = client.get(f"/jobs/{job_id}").json()
        if job["status"] in statuses:
            return job
        time.sleep(0.05)
    raise AssertionError(f"job stuck in {job['status']}")


def test_full_job_lifecycle_over_http(settings, store, factory):
    with app_client(settings, store, factory) as c:
        assert c.get("/health").json()["status"] == "ok"
        job = c.post("/jobs", json={"topic": "Durable agents"}).json()
        assert job["status"] == "queued"

        waiting = wait_status(c, job["id"], {"waiting_approval"})
        assert waiting["approval"]["questions"] == ["Q1?", "Q2?", "Q3?"]
        assert waiting["state"]["completed_steps"] == 1 and waiting["state"]["total_steps"] == 6

        with c.stream("GET", f"/jobs/{job['id']}/events") as r:
            events = [json.loads(l[6:]) for l in r.iter_lines() if l.startswith("data: ")]
        assert [e["type"] for e in events][:3] == ["created", "claimed", "step_started"]

        assert c.post(f"/jobs/{job['id']}/approve", json={"approved": True}).status_code == 200
        done = wait_status(c, job["id"], {"completed"})
        assert done["result"]["report"].startswith("# Durable agents")
        assert len(done["state"]["findings"]) == 3

        workers = c.get("/workers").json()
        assert workers and workers[0]["processed"] >= 2
        assert c.post(f"/jobs/{job['id']}/pause").status_code == 409  # already finished


def test_validation_and_errors(settings, store, factory):
    with app_client(settings, store, factory, workers=0) as c:
        assert c.post("/jobs", json={"topic": "x"}).status_code == 422
        assert c.get("/jobs/missing").status_code == 404
        job = c.post("/jobs", json={"topic": "Queued only"}).json()
        assert c.post(f"/jobs/{job['id']}/approve", json={"approved": True}).status_code == 409
        assert c.post(f"/jobs/{job['id']}/cancel").json()["status"] == "cancelled"

        no_chaos = TestClient(create_app(replace(settings, allow_chaos=False), store=store, job_factory=factory))
        assert no_chaos.post("/jobs", json={"topic": "crash me", "chaos_crash_after_step": 1}).status_code == 400
