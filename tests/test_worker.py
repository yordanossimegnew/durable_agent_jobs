"""Workers running LangGraph jobs durably: approval, crash recovery, retries, pause/cancel."""

from __future__ import annotations

import threading
import time

import anthropic
import pytest

from tests.fakes import WorkerKilled, api_error


def create(store, **payload):
    return store.create("research", {"topic": "Durable agents", "require_approval": False, **payload}, 3)


def drain(worker, store, job_id, until=("completed", "failed", "cancelled", "waiting_approval", "paused"), timeout=10):
    deadline = time.time() + timeout
    while store.get(job_id)["status"] not in until and time.time() < deadline:
        if not worker.run_once():
            time.sleep(0.02)
    return store.get(job_id)


def types(store, job_id):
    return [e["type"] for e in store.events(job_id)]


def test_job_runs_to_completion(store, make_worker, models):
    job = create(store)
    done = drain(make_worker(), store, job["id"])
    assert done["status"] == "completed"
    assert done["result"]["report"].startswith("# Durable agents")
    assert [f["question"] for f in done["result"]["findings"]] == ["Q1?", "Q2?", "Q3?"]
    assert done["progress"] == {"step": 5, "total": 5, "label": "done"}
    assert done["result"]["usage"] == {"input_tokens": 50, "output_tokens": 25}


def test_human_approval_with_edited_plan(store, make_worker, models):
    job = create(store, require_approval=True)
    w = make_worker()
    waiting = drain(w, store, job["id"])
    assert waiting["status"] == "waiting_approval"
    assert waiting["approval"]["questions"] == ["Q1?", "Q2?", "Q3?"]
    assert models.calls == ["plan"]  # nothing researched before approval

    store.approve(job["id"], {"approved": True, "questions": ["Q1?", "Q3?"]})
    done = drain(w, store, job["id"])
    assert done["status"] == "completed"
    assert [f["question"] for f in done["result"]["findings"]] == ["Q1?", "Q3?"]


def test_rejected_plan_cancels_the_job(store, make_worker):
    job = create(store, require_approval=True)
    w = make_worker()
    drain(w, store, job["id"])
    store.approve(job["id"], {"approved": False, "reason": "Too broad"})
    done = drain(w, store, job["id"])
    assert done["status"] == "cancelled" and done["error"] == "Too broad"


def test_crashed_worker_job_resumes_from_checkpoint(store, make_worker, models, settings):
    job = create(store, chaos_crash_after_step=2)  # dies right after plan + research 1
    with pytest.raises(WorkerKilled):
        drain(make_worker("w1"), store, job["id"])
    assert store.get(job["id"])["status"] == "running"  # nobody updated it: the process "died"

    time.sleep(settings.lease_seconds + 0.2)  # lease expires
    done = drain(make_worker("w2"), store, job["id"])
    assert done["status"] == "completed"
    assert models.calls.count("plan") == 1 and models.calls.count("research:Q1?") == 1  # no step re-ran
    t = types(store, job["id"])
    assert t.index("chaos_crash") < t.index("recovered") < len(t) - 1
    resumed = [e for e in store.events(job["id"]) if e["type"] == "resumed"][-1]
    assert resumed["data"]["from"] == "checkpoint" and resumed["data"]["completed_steps"] == 2
    assert resumed["worker"] == "w2"


def test_transient_errors_retry_inside_the_node(store, make_worker, models):
    models.fail["research:2"] = [api_error(anthropic.InternalServerError), api_error(anthropic.APIConnectionError)]
    job = create(store)
    done = drain(make_worker(), store, job["id"])
    assert done["status"] == "completed" and done["attempts"] == 1  # RetryPolicy absorbed both errors
    assert models.calls.count("research:Q2?") == 3


def test_job_level_retry_with_backoff_resumes_mid_job(store, make_worker, models):
    models.fail["write"] = [RuntimeError("disk full")]  # not transient: fails the attempt
    job = create(store)
    done = drain(make_worker(), store, job["id"], until=("completed", "failed"))
    assert done["status"] == "completed" and done["attempts"] == 2
    assert models.calls.count("research:Q1?") == 1  # retry resumed at `write`, not from scratch
    retry = next(e for e in store.events(job["id"]) if e["type"] == "retry_scheduled")
    assert retry["data"]["attempt"] == 1 and "disk full" in retry["data"]["error"]


def test_job_fails_after_max_attempts(store, make_worker, models):
    models.fail["research:1"] = RuntimeError("always broken")
    job = create(store)
    done = drain(make_worker(), store, job["id"], until=("completed", "failed"))
    assert done["status"] == "failed" and done["attempts"] == 3 and "always broken" in done["error"]


def test_pause_and_resume_at_checkpoint_boundary(store, make_worker, models):
    models.delay = 0.3
    job = create(store)
    w = make_worker()
    t = threading.Thread(target=lambda: drain(w, store, job["id"]))
    t.start()
    while not any(c.startswith("research") for c in models.calls):
        time.sleep(0.02)
    store.pause(job["id"])
    t.join(timeout=10)
    paused = store.get(job["id"])
    assert paused["status"] == "paused"
    done_before = len([c for c in models.calls if c.startswith("research")])
    assert done_before < 3

    store.resume(job["id"])
    models.delay = 0
    done = drain(w, store, job["id"])
    assert done["status"] == "completed"
    assert len([c for c in models.calls if c.startswith("research")]) == 3  # each question exactly once


def test_cancel_running_job(store, make_worker, models):
    models.delay = 0.3
    job = create(store)
    w = make_worker()
    t = threading.Thread(target=lambda: drain(w, store, job["id"]))
    t.start()
    while not models.calls:
        time.sleep(0.02)
    store.cancel(job["id"])
    t.join(timeout=10)
    assert store.get(job["id"])["status"] == "cancelled"
    assert "write" not in models.calls


def test_graceful_shutdown_hands_job_back(store, make_worker, models):
    models.delay = 0.2
    job = create(store)
    w = make_worker()
    t = threading.Thread(target=w.run_once)  # claims the job and starts working on it
    t.start()
    while not models.calls:
        time.sleep(0.02)
    w.stop_event.set()  # like Ctrl+C: finish the current step, then hand the job back
    t.join(timeout=10)
    assert store.get(job["id"])["status"] == "queued"
    assert w.run_once() is False  # a stopping worker takes no new work
    models.delay = 0
    assert drain(make_worker("w2"), store, job["id"])["status"] == "completed"
