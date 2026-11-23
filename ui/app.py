"""Streamlit dashboard for Durable Agent Jobs.

    uvicorn main:app             # terminal 1: API
    python main.py worker        # terminal 2 (and 3, 4...): workers
    streamlit run ui/app.py      # terminal 5: this UI
"""

from __future__ import annotations

import os
from datetime import datetime

import requests
import streamlit as st

API_URL = os.getenv("AGENT_API_URL", "http://localhost:8000")
STATUS = {"queued": ("⏳", "#64748b"), "running": ("⚙️", "#2563eb"), "waiting_approval": ("✋", "#d97706"),
          "paused": ("⏸️", "#7c3aed"), "completed": ("✅", "#16a34a"), "failed": ("❌", "#dc2626"),
          "cancelled": ("🚫", "#6b7280")}
EVENT_STYLE = {"chaos_crash": ("💥", "#fee2e2"), "recovered": ("🛟", "#dcfce7"), "resumed": ("▶️", "#e0f2fe"),
               "retry_scheduled": ("🔁", "#fef3c7"), "waiting_approval": ("✋", "#fef3c7"), "approval_given": ("👍", "#dcfce7"),
               "completed": ("✅", "#dcfce7"), "failed": ("❌", "#fee2e2"), "paused": ("⏸️", "#ede9fe"),
               "cancelled": ("🚫", "#f1f5f9"), "claimed": ("🤝", "#f8fafc"), "created": ("🆕", "#f8fafc")}

st.set_page_config(page_title="Durable Agent Jobs", page_icon="🗂️", layout="wide")
st.markdown("""<style>
.badge {display:inline-block;padding:2px 10px;border-radius:10px;color:#fff;font-size:0.8rem;font-weight:600}
.step {display:flex;align-items:center;gap:8px;padding:5px 8px;border-radius:6px;margin:2px 0;font-size:0.9rem}
.step.done {background:#f0fdf4} .step.active {background:#eff6ff;font-weight:600} .step.todo {color:#94a3b8}
.ev {font:12.5px/1.5 Consolas,monospace;padding:3px 8px;border-radius:4px;margin:1px 0}
.ev .t {color:#64748b} .ev .w {color:#7c3aed}
.wk {padding:6px 10px;border-radius:8px;margin:4px 0;font-size:0.85rem;border:1px solid #e5e7eb}
</style>""", unsafe_allow_html=True)


def api(method, path, **kw):
    try:
        r = requests.request(method, f"{API_URL}{path}", timeout=10, **kw)
        return r.json() if r.ok else {"_error": r.json().get("detail", r.text)}
    except requests.RequestException as exc:
        return {"_error": str(exc)}


def badge(status):
    icon, color = STATUS.get(status, ("•", "#64748b"))
    return f'<span class="badge" style="background:{color}">{icon} {status.replace("_", " ")}</span>'


def hhmmss(iso):
    try:
        return datetime.fromisoformat(iso).astimezone().strftime("%H:%M:%S")
    except (TypeError, ValueError):
        return ""


health = api("GET", "/health")
ok = "_error" not in health

# ----------------------------------------------------------------------------- sidebar
with st.sidebar:
    st.title("🗂️ Durable Agent Jobs")
    st.caption("LangGraph research jobs that survive crashes")
    if not ok:
        st.error(f"API not reachable at {API_URL}. Start it with `uvicorn main:app`.", icon="🚫")
    else:
        st.markdown(f"**Model:** `{health['model']}`  \n**PostgreSQL:** `{health['database'].split('/')[-1]}`")
    st.markdown("### New job")
    with st.form("new-job", clear_on_submit=True):
        topic = st.text_area("Research topic", height=80,
                             placeholder="How do LangGraph checkpointers make agents durable?")
        approval = st.checkbox("Require plan approval", value=True)
        crash = 0
        if ok and health.get("chaos_enabled"):
            crash = st.number_input("💥 Chaos: kill the worker after N steps (0 = off)", 0, 10, 0)
        if st.form_submit_button("Submit job", type="primary", disabled=not ok) and topic.strip():
            payload = {"topic": topic.strip(), "require_approval": approval}
            if crash:
                payload["chaos_crash_after_step"] = int(crash)
            created = api("POST", "/jobs", json=payload)
            if "_error" in created:
                st.error(created["_error"])
            else:
                st.session_state.job_id = created["id"]


@st.fragment(run_every=1.0)
def workers_panel():
    st.markdown("### Workers")
    workers = api("GET", "/workers")
    if not isinstance(workers, list) or not workers:
        st.caption("No workers yet. Start one with `python main.py worker`.")
        return
    for w in workers:
        alive = w["alive"]
        dot = "🟢" if alive and w["status"] == "busy" else "🟡" if alive else "🔴"
        what = f"job <code>{w['current_job']}</code>" if w["current_job"] and alive else ("idle" if alive else "no heartbeat")
        st.markdown(f'<div class="wk">{dot} <b>{w["id"].split("-")[-1]}</b> · pid {w["pid"]} · {what}<br>'
                    f'<span style="color:#64748b">heartbeat {w["seconds_since_heartbeat"]}s ago · {w["processed"]} jobs</span></div>',
                    unsafe_allow_html=True)


with st.sidebar:
    workers_panel()


# ----------------------------------------------------------------------------- jobs
@st.fragment(run_every=1.0)
def jobs_panel():
    jobs = api("GET", "/jobs")
    if not isinstance(jobs, list) or not jobs:
        st.info("No jobs yet. Submit one from the sidebar.")
        return
    st.markdown("### Jobs")
    for job in jobs[:8]:
        p = job.get("progress") or {}
        frac = (p.get("step") or 0) / p["total"] if p.get("total") else 0
        c1, c2, c3, c4 = st.columns([5, 2, 3, 1])
        c1.markdown(f"**{job['input']['topic'][:70]}**  \n<span style='color:#94a3b8;font-size:0.8rem'>{job['id']} · "
                    f"attempt {job['attempts']}/{job['max_attempts']}</span>", unsafe_allow_html=True)
        c2.markdown(badge(job["status"]), unsafe_allow_html=True)
        c3.progress(min(frac, 1.0), text=p.get("label") or "")
        if c4.button("Open", key=f"open-{job['id']}"):
            st.session_state.job_id = job["id"]
            st.rerun()


jobs_panel()


@st.fragment(run_every=1.0)
def job_detail(job_id):
    job = api("GET", f"/jobs/{job_id}")
    if "_error" in job:
        st.error(job["_error"])
        return
    state = job["state"]
    st.divider()
    left, right = st.columns([3, 2])
    with left:
        st.markdown(f"## {job['input']['topic']}")
        st.markdown(f"{badge(job['status'])} &nbsp; <span style='color:#64748b'>job {job['id']} · attempt "
                    f"{job['attempts']}/{job['max_attempts']}" + (f" · worker <b>{job['locked_by'].split('-')[-1]}</b>"
                    if job["locked_by"] else "") + "</span>", unsafe_allow_html=True)
        if job["error"] and job["status"] != "completed":
            st.warning(job["error"])

        # Controls
        b1, b2, b3, _ = st.columns([1.4, 1.5, 1.4, 1.7])
        if b1.button("⏸️ Pause", disabled=job["status"] not in ("queued", "running")):
            api("POST", f"/jobs/{job_id}/pause")
        if b2.button("▶️ Resume", disabled=job["status"] not in ("paused", "failed")):
            api("POST", f"/jobs/{job_id}/resume")
        if b3.button("🚫 Cancel", disabled=job["status"] in ("completed", "failed", "cancelled")):
            api("POST", f"/jobs/{job_id}/cancel")

        # Approval
        if job["status"] == "waiting_approval":
            with st.container(border=True):
                st.markdown("#### ✋ Approve the research plan")
                st.caption(job["approval"].get("rationale", ""))
                edited = st.text_area("Questions (one per line; edit before approving)",
                                      "\n".join(job["approval"]["questions"]), height=150, key=f"plan-{job_id}")
                a1, a2 = st.columns(2)
                if a1.button("👍 Approve plan", type="primary", key=f"approve-{job_id}"):
                    qs = [q for q in edited.splitlines() if q.strip()]
                    api("POST", f"/jobs/{job_id}/approve", json={"approved": True, "questions": qs})
                if a2.button("👎 Reject", key=f"reject-{job_id}"):
                    api("POST", f"/jobs/{job_id}/approve", json={"approved": False, "reason": "Rejected in the UI"})

        # Step timeline
        questions = state.get("questions") or []
        steps = [("plan", "Plan the research")]
        if job["input"].get("require_approval"):
            steps.append(("approve", "Human approval"))
        steps += [(f"research_{i}", f"Research: {q}") for i, q in enumerate(questions, 1)] or [("research", "Research")]
        steps.append(("write", "Write the report"))
        done = state.get("completed_steps") or 0
        st.markdown("#### Steps")
        for i, (_, label) in enumerate(steps):
            if i < done:
                css, icon = "done", "✅"
            elif i == done and job["status"] in ("running", "queued"):
                css, icon = "active", "⚙️" if job["status"] == "running" else "⏳"
            elif i == done and job["status"] == "waiting_approval":
                css, icon = "active", "✋"
            else:
                css, icon = "todo", "○"
            st.markdown(f'<div class="step {css}">{icon} {label}</div>', unsafe_allow_html=True)

        if job["status"] == "completed":
            st.markdown("#### 📄 Report")
            with st.container(border=True):
                st.markdown(job["result"]["report"])
            for i, f in enumerate(job["result"]["findings"], 1):
                with st.expander(f"Finding {i}: {f['question']}"):
                    st.markdown(f["answer"])
                    for s in f["sources"]:
                        st.markdown(f"- [{s['title']}]({s['url']})")

    with right:
        st.markdown("#### Event log (from the database)")
        rows = _events(job_id)
        html = []
        for e in rows[-40:]:
            icon, bg = EVENT_STYLE.get(e["type"], ("•", "#ffffff"))
            data = e["data"]
            detail = data.get("label") or data.get("error") or data.get("question") or ""
            if e["type"] == "resumed":
                detail = f"from {data.get('from')}" + (f", {data['completed_steps']} steps already done"
                                                       if data.get("completed_steps") is not None else "")
            if e["type"] == "recovered":
                detail = "lease expired; job requeued"
            if e["type"] == "retry_scheduled":
                detail = f"attempt {data['attempt']} failed; retry in {data['delay_seconds']}s"
            worker = f' <span class="w">{e["worker"].split("-")[-1]}</span>' if e.get("worker") else ""
            html.append(f'<div class="ev" style="background:{bg}"><span class="t">{hhmmss(e["at"])}</span> '
                        f'{icon} <b>{e["type"]}</b>{worker} {detail}</div>')
        st.markdown("".join(html), unsafe_allow_html=True)


def _events(job_id):
    """Read the whole persisted event log (non-following SSE)."""
    import json as _json

    out = []
    try:
        with requests.get(f"{API_URL}/jobs/{job_id}/events", params={"follow": "false"}, stream=True, timeout=10) as r:
            for line in r.iter_lines(decode_unicode=True):
                if line and line.startswith("data: "):
                    e = _json.loads(line[6:])
                    if e.get("type") != "end":
                        out.append(e)
    except requests.RequestException:
        pass
    return out


if st.session_state.get("job_id"):
    job_detail(st.session_state.job_id)
