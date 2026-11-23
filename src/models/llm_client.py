"""Claude via LangChain for the three kinds of work a research job does.

    plan      structured output (native JSON schema) -> list of sub-questions
    research  Anthropic's server-side web search tool -> answer + cited sources
    write     plain generation -> the final markdown report

The graph only depends on the ResearchModels protocol, so tests use scripted fakes.
"""

from __future__ import annotations

from typing import Protocol

from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from ..prompts import PLAN_PROMPT, RESEARCH_PROMPT, WRITE_PROMPT, findings_block
from ..utils.config import Settings

FALLBACK_BETA = "server-side-fallback-2026-07-01"
MAX_CONTINUATIONS = 3  # server tools may pause a long turn (stop_reason "pause_turn")


class Plan(BaseModel):
    rationale: str = Field(description="One or two sentences on how the questions cover the topic.")
    questions: list[str] = Field(description="3 to 5 focused research questions, in a sensible order.")


class ResearchModels(Protocol):
    def plan(self, topic: str) -> Plan: ...
    def research(self, topic: str, question: str) -> dict: ...  # {"answer", "sources", "usage"}
    def write(self, topic: str, findings: list[dict]) -> dict: ...  # {"report", "usage"}


def _usage(msg) -> dict:
    meta = getattr(msg, "usage_metadata", None) or {}
    return {"input_tokens": meta.get("input_tokens", 0), "output_tokens": meta.get("output_tokens", 0)}


def _text(msg: AIMessage) -> str:
    if isinstance(msg.content, str):
        return msg.content
    return "".join(b.get("text", "") for b in msg.content if isinstance(b, dict) and b.get("type") == "text")


def _sources(msg: AIMessage) -> list[dict]:
    """Cited URLs first (from text citations), then other search results, de-duplicated."""
    cited, seen, other = [], set(), []
    if isinstance(msg.content, str):
        return []
    for block in msg.content:
        if not isinstance(block, dict):
            continue
        for c in block.get("citations") or []:
            url = c.get("url")
            if url and url not in seen:
                seen.add(url)
                cited.append({"url": url, "title": c.get("title") or url})
        if block.get("type") == "web_search_tool_result" and isinstance(block.get("content"), list):
            for r in block["content"]:
                if r.get("url"):
                    other.append({"url": r["url"], "title": r.get("title") or r["url"]})
    for s in other:
        if s["url"] not in seen and len(cited) < 8:
            seen.add(s["url"])
            cited.append(s)
    return cited


class AnthropicResearchModels:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.chat = ChatAnthropic(
            model=settings.model,
            max_tokens=settings.max_tokens,
            output_config={"effort": settings.effort},
            betas=[FALLBACK_BETA],
            model_kwargs={"fallbacks": "default"},
            max_retries=3,
        )

    def plan(self, topic: str) -> Plan:
        model = self.chat.with_structured_output(Plan, method="json_schema")
        return model.invoke([SystemMessage(PLAN_PROMPT), HumanMessage(f"<topic>{topic}</topic>")])

    def research(self, topic: str, question: str) -> dict:
        tool = {"type": "web_search_20260209", "name": "web_search", "max_uses": self.settings.web_search_max_uses}
        model = self.chat.bind_tools([tool])
        messages = [SystemMessage(RESEARCH_PROMPT),
                    HumanMessage(f"<topic>{topic}</topic>\n<question>{question}</question>")]
        usage = {"input_tokens": 0, "output_tokens": 0}
        response = model.invoke(messages)
        for _ in range(MAX_CONTINUATIONS):
            for k, v in _usage(response).items():
                usage[k] += v
            if (response.response_metadata or {}).get("stop_reason") != "pause_turn":
                break
            messages.append(response)  # resend the paused turn so the server can continue it
            response = model.invoke(messages)
        return {"answer": _text(response).strip(), "sources": _sources(response), "usage": usage}

    def write(self, topic: str, findings: list[dict]) -> dict:
        msg = self.chat.invoke([SystemMessage(WRITE_PROMPT),
                                HumanMessage(f"<topic>{topic}</topic>\n{findings_block(findings)}")])
        return {"report": _text(msg).strip(), "usage": _usage(msg)}
