"""The `ai_triage` handler — the platform analysing its own dead-letter queue.

Triage is an ordinary job, so it inherits claiming, leasing, retries and dead-lettering.
The loop guard lives in `states.request_triage`, which refuses to enqueue triage for a
triage job; without it a failing triage handler would dead-letter, enqueue triage for
itself, and repeat forever at real cost.
"""

import uuid

from app.ai.triage import triage_job
from app.db.session import SessionLocal
from app.engine.errors import PermanentError
from app.worker.handlers import JobContext, handler


@handler("ai_triage")
async def ai_triage(payload: dict, ctx: JobContext) -> dict:
    raw_id = payload.get("target_job_id")
    try:
        target_job_id = uuid.UUID(str(raw_id))
    except (TypeError, ValueError):
        raise PermanentError(f"target_job_id must be a job UUID, got {raw_id!r}") from None

    # Its own session: the analysis and its event must commit together, independently of
    # whatever transaction dead-lettered the target job.
    async with SessionLocal() as session:
        result = await triage_job(session, target_job_id)
        await session.commit()
    return result
