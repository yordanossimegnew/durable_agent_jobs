"""Entry point.

    uvicorn main:app --reload        # API server (docs at /docs)
    python main.py worker            # a worker process; start as many as you like
    python main.py                   # API on port 8000
"""

from __future__ import annotations

import argparse
import signal

from src.api import create_app
from src.utils import load_settings, setup_logging

settings = load_settings()
setup_logging(settings.log_level, settings.log_dir)
app = create_app(settings)


def run_worker() -> None:
    from src.jobs import Worker, make_job_factory, make_store

    worker = Worker(settings, make_store(settings), make_job_factory(settings))

    def shutdown(*_):
        print("\nStopping after the current step (Ctrl+C again to force)…")
        worker.stop_event.set()
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    signal.signal(signal.SIGINT, shutdown)
    print(f"worker {worker.id} · db {settings.database_url.split('@')[-1]} · lease {settings.lease_seconds}s")
    worker.run_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Durable Agent Jobs")
    sub = parser.add_subparsers(dest="command")
    serve = sub.add_parser("serve", help="Run the FastAPI server (default).")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8000)
    sub.add_parser("worker", help="Run a worker that processes jobs from the database.")
    args = parser.parse_args()
    if args.command == "worker":
        run_worker()
    else:
        import uvicorn

        uvicorn.run(app, host=getattr(args, "host", "0.0.0.0"), port=getattr(args, "port", 8000))


if __name__ == "__main__":
    main()
