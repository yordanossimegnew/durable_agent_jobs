"""LangGraph checkpointer backed by PostgreSQL (the same database as the job queue)."""

from __future__ import annotations

from langgraph.checkpoint.postgres import PostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


def open_checkpointer(database_url: str) -> PostgresSaver:
    conninfo = database_url.replace("postgresql+psycopg://", "postgresql://")
    pool = ConnectionPool(conninfo, min_size=1, max_size=10, open=True,
                          kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row})
    saver = PostgresSaver(pool)
    saver.setup()  # creates the checkpoint tables on first run
    return saver
