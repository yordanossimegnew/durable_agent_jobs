"""The durable job store: claiming, leases, retries, controls."""

from __future__ import annotations

import threading
import time

import pytest

from src.jobs import InvalidTransition


def new_job(store, **payload):
    return store.create("research", {"topic": "t", "require_approval": False, **payload}, max_attempts=3)


def test_claim_is_exclusive_under_concurrency(store):
    ids = {new_job(store)["id"] for _ in range(12)}
    claimed, lock = [], threading.Lock()

    def worker(n):
        while (job := store.claim(f"w{n}")) is not None:
            with lock:
                claimed.append(job["id"])

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(claimed) == sorted(ids)  # every job claimed exactly once
    assert all(store.get(i)["status"] == "running" for i in ids)


def test_stale_running_job_is_reclaimed(store):
    job = new_job(store)
    store.claim("dead-worker")
    assert store.reclaim_stale(lease_seconds=5) == []  # heartbeat is fresh
    time.sleep(0.3)
    assert store.reclaim_stale(lease_seconds=0.2) == [job["id"]]
    j = store.get(job["id"])
    assert j["status"] == "queued" and j["locked_by"] is None and j["attempts"] == 0
    assert store.events(job["id"])[-1]["type"] == "recovered"


def test_heartbeat_only_extends_own_lease(store):
    job = new_job(store)
    store.claim("w1")
    assert store.heartbeat(job["id"], "w1") is True
    assert store.heartbeat(job["id"], "w2") is False


def test_retry_backoff_then_permanent_failure(store):
    job = new_job(store)
    delays = []
    for attempt in range(1, 4):
        store.get(job["id"])  # wait until due
        claimed = None
        deadline = time.time() + 5
        while claimed is None and time.time() < deadline:
            claimed = store.claim("w1")
            time.sleep(0.01)
        assert claimed["attempts"] == attempt
        out = store.schedule_retry(job["id"], "w1", "boom", base=0.02, cap=1)
        delays.append(out.get("delay"))
    assert out["status"] == "failed" and store.get(job["id"])["status"] == "failed"
    assert delays[0] < delays[1]  # exponential backoff
    types = [e["type"] for e in store.events(job["id"])]
    assert types.count("retry_scheduled") == 2 and types[-1] == "failed"


def test_controls(store):
    queued = new_job(store)
    assert store.pause(queued["id"])["status"] == "paused"  # not running: applies immediately
    assert store.resume(queued["id"])["status"] == "queued"
    store.claim("w1")
    running = store.pause(queued["id"])
    assert running["status"] == "running" and running["pause_requested"] == 1  # worker stops at next boundary
    assert store.cancel(queued["id"])["cancel_requested"] == 1

    done = new_job(store)
    store.cancel(done["id"])
    with pytest.raises(InvalidTransition):
        store.resume(done["id"])
    with pytest.raises(InvalidTransition):
        store.approve(done["id"], {"approved": True})
