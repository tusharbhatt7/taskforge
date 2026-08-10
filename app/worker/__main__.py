"""Worker process entrypoint: `python -m app.worker [--queues a,b] [--concurrency N]`.

Run as many of these as you want, anywhere that can reach the database — job claiming
is coordinated entirely through Postgres row locks, so workers need no knowledge of
each other.
"""

import argparse
import asyncio

from app.core.logging import setup_logging
from app.worker.runner import WorkerRunner


def main() -> None:
    parser = argparse.ArgumentParser(description="Taskforge worker")
    parser.add_argument("--queues", type=str, default=None,
                        help="comma-separated queue names (default: all queues)")
    parser.add_argument("--concurrency", type=int, default=None,
                        help="max jobs executed concurrently (default: from settings)")
    args = parser.parse_args()

    setup_logging()
    queues = [q.strip() for q in args.queues.split(",") if q.strip()] if args.queues else None
    asyncio.run(WorkerRunner(queues=queues, concurrency=args.concurrency).run())


if __name__ == "__main__":
    main()
