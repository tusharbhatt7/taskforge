"""Job handler registry.

A handler is `async def fn(payload: dict, ctx: JobContext) -> dict`. The returned dict
is stored as the job's result; raising any exception fails the attempt (the engine
decides between retry and dead-letter). Register with @handler("name").
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class JobContext:
    job_id: UUID
    attempt: int
    max_attempts: int


Handler = Callable[[dict, JobContext], Awaitable[dict]]

HANDLERS: dict[str, Handler] = {}


def handler(name: str) -> Callable[[Handler], Handler]:
    def register(fn: Handler) -> Handler:
        HANDLERS[name] = fn
        return fn

    return register


def public_types() -> list[str]:
    """Job types a client may submit. `ai_triage` is engine-internal: it is enqueued by
    the dead-letter transition and takes a job id the submitter shouldn't be choosing."""
    return sorted(t for t in HANDLERS if t != "ai_triage")


# Importing the modules registers their handlers.
from app.worker.handlers import demo, http_fetch, llm, thumbnail, triage  # noqa: E402, F401
