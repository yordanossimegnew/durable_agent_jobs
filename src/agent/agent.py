"""A long-running research job as a LangGraph graph.

    START -> plan -> approve -> research -> research -> ... -> write -> END
                       |          (one checkpointed step per question)
                 interrupt() until a human approves the plan

Durability comes from two layers:
  * the checkpointer saves state after every node, so a crashed or paused job resumes
    at the next unfinished step instead of starting over;
  * node RetryPolicy retries transient API errors in place; anything that still fails
    is retried by the worker with exponential backoff (see jobs/worker.py).
"""

from __future__ import annotations

from typing import Any

import anthropic
from langgraph.config import get_stream_writer
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy, interrupt

from ..models import ResearchModels
from .state import ResearchState

TRANSIENT = (anthropic.APIConnectionError, anthropic.RateLimitError, anthropic.InternalServerError,
             anthropic.APITimeoutError)
NODE_RETRY = RetryPolicy(max_attempts=3, initial_interval=2.0, backoff_factor=2.0, retry_on=TRANSIENT)


def total_steps(state: dict) -> int:
    """plan + (approval) + one per question + write."""
    return 1 + (1 if state.get("require_approval") else 0) + len(state.get("questions") or []) + 1


class ResearchJob:
    def __init__(self, models: ResearchModels, checkpointer: Any, retry: RetryPolicy = NODE_RETRY):
        self.models = models
        self.retry = retry
        self.graph = self._build(checkpointer)

    # ------------------------------------------------------------------ nodes
    def _plan(self, state: ResearchState) -> dict:
        writer = get_stream_writer()
        writer({"type": "step_started", "step": "plan", "label": "Planning the research"})
        plan = self.models.plan(state["topic"])
        questions = [q.strip() for q in plan.questions if q.strip()][:6]
        writer({"type": "step_done", "step": "plan", "label": f"Planned {len(questions)} questions",
                "questions": questions, "rationale": plan.rationale})
        return {"questions": questions, "rationale": plan.rationale, "findings": []}

    def _approve(self, state: ResearchState) -> dict:
        if not state.get("require_approval"):
            return {"approved": True}
        # Pauses the job (for minutes or days) until someone answers via the API.
        decision = interrupt({"kind": "plan_approval", "topic": state["topic"],
                              "questions": state["questions"], "rationale": state.get("rationale", "")})
        if not decision.get("approved"):
            return {"approved": False, "rejected_reason": decision.get("reason") or "Plan rejected."}
        edited = [q.strip() for q in decision.get("questions") or [] if q.strip()]
        get_stream_writer()({"type": "step_done", "step": "approve", "label": "Plan approved",
                             "edited": bool(edited) and edited != state["questions"]})
        return {"approved": True, "questions": edited or state["questions"]}

    def _research(self, state: ResearchState) -> dict:
        index = len(state.get("findings") or [])
        question = state["questions"][index]
        writer = get_stream_writer()
        label = f"Researching {index + 1}/{len(state['questions'])}"
        writer({"type": "step_started", "step": f"research_{index + 1}", "label": label, "question": question})
        result = self.models.research(state["topic"], question)
        finding = {"question": question, "answer": result["answer"], "sources": result["sources"]}
        writer({"type": "step_done", "step": f"research_{index + 1}", "label": label,
                "question": question, "sources": len(result["sources"])})
        return {"findings": [*state.get("findings", []), finding], "usage": result.get("usage")}

    def _write(self, state: ResearchState) -> dict:
        writer = get_stream_writer()
        writer({"type": "step_started", "step": "write", "label": "Writing the report"})
        result = self.models.write(state["topic"], state["findings"])
        writer({"type": "step_done", "step": "write", "label": "Report written"})
        return {"report": result["report"], "usage": result.get("usage")}

    # ------------------------------------------------------------------ routing
    @staticmethod
    def _after_approve(state: ResearchState) -> str:
        return "research" if state.get("approved") and state.get("questions") else END

    @staticmethod
    def _after_research(state: ResearchState) -> str:
        return "research" if len(state["findings"]) < len(state["questions"]) else "write"

    def _build(self, checkpointer):
        g = StateGraph(ResearchState)
        g.add_node("plan", self._plan, retry_policy=self.retry)
        g.add_node("approve", self._approve)
        g.add_node("research", self._research, retry_policy=self.retry)
        g.add_node("write", self._write, retry_policy=self.retry)
        g.add_edge(START, "plan")
        g.add_edge("plan", "approve")
        g.add_conditional_edges("approve", self._after_approve, ["research", END])
        g.add_conditional_edges("research", self._after_research, ["research", "write"])
        g.add_edge("write", END)
        return g.compile(checkpointer=checkpointer)
