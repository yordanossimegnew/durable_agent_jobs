"""Scripted research models so tests run offline, plus failure injection."""

from __future__ import annotations

import time

import anthropic
import httpx

from src.models import Plan


class WorkerKilled(BaseException):
    """Simulates the worker process dying (not an Exception, so nothing catches it)."""


def api_error(cls=anthropic.InternalServerError, message="overloaded"):
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    if cls is anthropic.APIConnectionError:
        return cls(request=request)
    return cls(message, response=httpx.Response(500 if cls is anthropic.InternalServerError else 429, request=request),
               body=None)


class FakeModels:
    def __init__(self, questions=("Q1?", "Q2?", "Q3?"), delay: float = 0.0, fail: dict | None = None):
        self.questions = list(questions)
        self.delay = delay
        self.fail = dict(fail or {})  # {"research:2": [exc, exc], "write": RuntimeError(...)} -> raised in order
        self.calls: list[str] = []

    def _maybe_fail(self, key: str):
        errors = self.fail.get(key)
        if isinstance(errors, list) and errors:
            raise errors.pop(0)
        if isinstance(errors, BaseException):
            raise errors

    def plan(self, topic):
        self.calls.append("plan")
        self._maybe_fail("plan")
        return Plan(rationale=f"Covers {topic}.", questions=self.questions)

    def research(self, topic, question):
        n = self.questions.index(question) + 1 if question in self.questions else 0
        self.calls.append(f"research:{question}")
        if self.delay:
            time.sleep(self.delay)
        self._maybe_fail(f"research:{n}")
        return {"answer": f"Answer to {question}", "sources": [{"url": f"https://example.com/{n}", "title": f"Source {n}"}],
                "usage": {"input_tokens": 10, "output_tokens": 5}}

    def write(self, topic, findings):
        self.calls.append("write")
        self._maybe_fail("write")
        return {"report": f"# {topic}\n\n" + "\n".join(f"- {f['answer']}" for f in findings),
                "usage": {"input_tokens": 20, "output_tokens": 10}}
