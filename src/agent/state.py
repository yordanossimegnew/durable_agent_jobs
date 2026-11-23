"""Graph state for a research job (checkpointed after every node)."""

from __future__ import annotations

from typing import Annotated, TypedDict


def add_usage(a: dict | None, b: dict | None) -> dict:
    a, b = a or {}, b or {}
    return {k: a.get(k, 0) + b.get(k, 0) for k in ("input_tokens", "output_tokens")}


class ResearchState(TypedDict, total=False):
    topic: str
    require_approval: bool
    rationale: str
    questions: list[str]
    approved: bool
    rejected_reason: str
    findings: list[dict]  # one per completed question: {"question", "answer", "sources"}
    report: str
    usage: Annotated[dict, add_usage]
