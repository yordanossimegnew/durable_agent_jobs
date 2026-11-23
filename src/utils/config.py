"""Configuration loaded from environment variables (and an optional .env file)."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # PostgreSQL holds the job queue, the event log and the LangGraph checkpoints.
    database_url: str = "postgresql://postgres:postgres@localhost:5432/durable_jobs"
    model: str = "claude-opus-5-5"
    effort: str = "high"
    max_tokens: int = 16000
    web_search_max_uses: int = 4
    # Worker behaviour
    lease_seconds: float = 30.0       # a running job whose heartbeat is older than this is reclaimed
    heartbeat_seconds: float = 5.0
    poll_seconds: float = 1.0
    retry_base_seconds: float = 5.0   # job-level backoff: base * 2^(attempt-1), capped
    retry_max_seconds: float = 300.0
    default_max_attempts: int = 3
    embedded_workers: int = 0         # >0 runs workers inside the API process (handy for local dev)
    allow_chaos: bool = False         # lets jobs request a simulated worker crash (demos only)
    log_level: str = "INFO"
    log_dir: Path = PROJECT_ROOT / "logs"
    knowledge_base_dir: Path = PROJECT_ROOT / "data" / "knowledge_base"

    def __post_init__(self) -> None:
        if not self.database_url.startswith(("postgres://", "postgresql://", "postgresql+psycopg://")):
            raise ValueError("DATABASE_URL must be a PostgreSQL URL, e.g. postgresql://user:pass@localhost:5432/durable_jobs")


def load_settings(**overrides) -> Settings:
    load_dotenv(PROJECT_ROOT / ".env")
    settings = Settings(
        database_url=os.getenv("DATABASE_URL", Settings.database_url),
        model=os.getenv("AGENT_MODEL", Settings.model),
        effort=os.getenv("AGENT_EFFORT", Settings.effort),
        max_tokens=int(os.getenv("AGENT_MAX_TOKENS", Settings.max_tokens)),
        web_search_max_uses=int(os.getenv("WEB_SEARCH_MAX_USES", Settings.web_search_max_uses)),
        lease_seconds=float(os.getenv("WORKER_LEASE_SECONDS", Settings.lease_seconds)),
        heartbeat_seconds=float(os.getenv("WORKER_HEARTBEAT_SECONDS", Settings.heartbeat_seconds)),
        poll_seconds=float(os.getenv("WORKER_POLL_SECONDS", Settings.poll_seconds)),
        retry_base_seconds=float(os.getenv("RETRY_BASE_SECONDS", Settings.retry_base_seconds)),
        retry_max_seconds=float(os.getenv("RETRY_MAX_SECONDS", Settings.retry_max_seconds)),
        default_max_attempts=int(os.getenv("JOB_MAX_ATTEMPTS", Settings.default_max_attempts)),
        embedded_workers=int(os.getenv("EMBEDDED_WORKERS", Settings.embedded_workers)),
        allow_chaos=_env_bool("ALLOW_CHAOS", Settings.allow_chaos),
        log_level=os.getenv("LOG_LEVEL", Settings.log_level),
    )
    return replace(settings, **{k: v for k, v in overrides.items() if v is not None})
