"""Demo handlers that make platform behavior visible: latency, retries, dead-letters."""

import asyncio
import random

from app.worker.handlers import JobContext, handler

MAX_SLEEP_SECONDS = 60.0


@handler("sleep")
async def sleep(payload: dict, ctx: JobContext) -> dict:
    # Clamped so one payload can't occupy a worker slot indefinitely.
    seconds = min(float(payload.get("seconds", 2)), MAX_SLEEP_SECONDS)
    await asyncio.sleep(seconds)
    return {"slept_seconds": seconds}


@handler("email_sim")
async def email_sim(payload: dict, ctx: JobContext) -> dict:
    """Simulates an email provider call: variable latency, no external side effects."""
    to = payload.get("to", "someone@example.com")
    await asyncio.sleep(random.uniform(0.3, 1.5))
    return {"delivered_to": to, "subject": payload.get("subject", "(no subject)"), "provider": "sim"}


@handler("flaky")
async def flaky(payload: dict, ctx: JobContext) -> dict:
    """Fails deterministically for the first `fail_times` attempts, then succeeds.
    Uses the attempt counter as its only state, so behavior survives worker crashes.
    fail_times >= max_attempts demonstrates the dead-letter path."""
    fail_times = int(payload.get("fail_times", 2))
    await asyncio.sleep(random.uniform(0.2, 0.8))
    if ctx.attempt <= fail_times:
        raise RuntimeError(
            f"flaky: simulated failure on attempt {ctx.attempt}/{ctx.max_attempts} "
            f"(will fail {fail_times} time(s) total)"
        )
    return {"succeeded_on_attempt": ctx.attempt}
