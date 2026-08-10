"""Job lifecycle state machine.

    pending ──(all parents succeed)──> queued ──(claim)──> running ──> succeeded
       │                                  ^                   │
       │                                  └──(retry+backoff)──┤ failed attempt
       │                                                      └──(attempts exhausted)──> dead
       └──(any parent dead/canceled)──> canceled

All transitions here run inside the caller's transaction: the state change, the attempt
record, dependent releases, the webhook enqueue and the pg_notify event commit atomically
or not at all.
"""

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai.client import is_configured
from app.core.config import get_settings
from app.db.models import AttemptOutcome, Job, JobAttempt, JobState, Queue, WebhookDelivery
from app.engine import events
from app.engine.retry import backoff_seconds


def _now() -> datetime:
    return datetime.now(UTC)


async def complete_job(session: AsyncSession, job: Job, result: dict | None,
                       worker_id: uuid.UUID | None = None) -> None:
    job.state = JobState.SUCCEEDED.value
    job.result = result
    job.error = None
    job.finished_at = _now()
    job.leased_by = None
    job.lease_expires_at = None
    await _close_attempt(session, job, AttemptOutcome.SUCCEEDED, None)
    await _release_dependents(session, job)
    await _enqueue_webhook(session, job, "job.succeeded")
    await events.emit(session, "job.succeeded", job.user_id, job_id=job.id, queue=job.queue,
                      job_type=job.type, worker_id=worker_id)


async def fail_job(session: AsyncSession, job: Job, error: str,
                   worker_id: uuid.UUID | None = None,
                   attempt_outcome: AttemptOutcome = AttemptOutcome.FAILED,
                   retry_delay_override: float | None = None,
                   permanent: bool = False) -> None:
    """A single attempt failed. Either schedule a retry or dead-letter the job.

    `permanent` short-circuits the retry budget for failures that cannot succeed on a
    retry (see app/engine/errors.py); `retry_delay_override` replaces our backoff with a
    delay the downstream service asked for.
    """
    settings = get_settings()
    await _close_attempt(session, job, attempt_outcome, error)
    job.leased_by = None
    job.lease_expires_at = None

    if permanent or job.attempts >= job.max_attempts:
        job.state = JobState.DEAD.value
        job.error = error
        job.finished_at = _now()
        await _cascade_cancel_dependents(session, job, f"parent job {job.id} dead-lettered")
        await _enqueue_webhook(session, job, "job.dead")
        await request_triage(session, job)
        await events.emit(session, "job.dead", job.user_id, job_id=job.id, queue=job.queue,
                          job_type=job.type, error=error[:300], worker_id=worker_id,
                          permanent=permanent)
    else:
        delay = retry_delay_override if retry_delay_override is not None else backoff_seconds(
            job.attempts, settings.retry_base_seconds, settings.retry_cap_seconds)
        job.state = JobState.QUEUED.value
        job.error = error
        job.run_at = _now() + timedelta(seconds=delay)
        await events.emit(session, "job.retrying", job.user_id, job_id=job.id, queue=job.queue,
                          job_type=job.type, attempt=job.attempts, next_run_in_s=round(delay, 1),
                          error=error[:300], worker_id=worker_id)


async def cancel_job(session: AsyncSession, job: Job, reason: str) -> None:
    job.state = JobState.CANCELED.value
    job.error = reason
    job.finished_at = _now()
    await _cascade_cancel_dependents(session, job, f"parent job {job.id} canceled")
    await _enqueue_webhook(session, job, "job.canceled")
    await events.emit(session, "job.canceled", job.user_id, job_id=job.id, queue=job.queue, job_type=job.type)


async def _close_attempt(session: AsyncSession, job: Job, outcome: AttemptOutcome, error: str | None) -> None:
    await session.execute(
        update(JobAttempt)
        .where(JobAttempt.job_id == job.id,
               JobAttempt.attempt_no == job.attempts,
               JobAttempt.outcome == AttemptOutcome.RUNNING.value)
        .values(outcome=outcome.value, error=error, finished_at=_now(),
                duration_ms=text("EXTRACT(EPOCH FROM (now() - started_at)) * 1000"))
    )


async def _release_dependents(session: AsyncSession, parent: Job) -> None:
    """Move pending children to queued once *all* their parents have succeeded."""
    result = await session.execute(
        text("""
            UPDATE jobs SET state = 'queued', run_at = now()
            WHERE state = 'pending'
              AND id IN (SELECT job_id FROM job_deps WHERE parent_id = :pid)
              AND NOT EXISTS (
                  SELECT 1 FROM job_deps d JOIN jobs p ON p.id = d.parent_id
                  WHERE d.job_id = jobs.id AND p.state != 'succeeded'
              )
            RETURNING id, user_id, queue, type
        """),
        {"pid": parent.id},
    )
    for row in result:
        await events.emit(session, "job.queued", row.user_id, job_id=row.id, queue=row.queue,
                          job_type=row.type, released_by=parent.id)


async def _cascade_cancel_dependents(session: AsyncSession, parent: Job, reason: str) -> None:
    """A failed/canceled parent can never satisfy its children: cancel the whole subtree."""
    frontier = [parent.id]
    while frontier:
        result = await session.execute(
            text("""
                UPDATE jobs SET state = 'canceled', error = :reason, finished_at = now()
                WHERE state = 'pending'
                  AND id IN (SELECT job_id FROM job_deps WHERE parent_id = ANY(:pids))
                RETURNING id, user_id, queue, type, callback_url
            """).bindparams(reason=reason, pids=frontier),
        )
        frontier = []
        for row in result:
            frontier.append(row.id)
            if row.callback_url:
                child = await session.get(Job, row.id)
                await _enqueue_webhook(session, child, "job.canceled")
            await events.emit(session, "job.canceled", row.user_id, job_id=row.id,
                              queue=row.queue, job_type=row.type, reason=reason)


async def _enqueue_webhook(session: AsyncSession, job: Job, event: str) -> None:
    if not job.callback_url:
        return
    session.add(WebhookDelivery(
        user_id=job.user_id,
        job_id=job.id,
        url=job.callback_url,
        event=event,
        payload={
            "event": event,
            "job": {
                "id": str(job.id), "type": job.type, "queue": job.queue, "state": job.state,
                "attempts": job.attempts, "result": job.result, "error": job.error,
                "created_at": job.created_at.isoformat() if job.created_at else None,
                "finished_at": job.finished_at.isoformat() if job.finished_at else None,
            },
        },
    ))


TRIAGE_JOB_TYPE = "ai_triage"
TRIAGE_QUEUE = "triage"


async def request_triage(session: AsyncSession, dead_job: Job) -> None:
    """Enqueue AI triage for a job that just dead-lettered.

    Triage is itself an ordinary job on this platform — it gets the same claiming,
    leasing, retry and dead-letter treatment as any other work. Three guards matter:

    1. Never triage a triage job. Without this, a failing triage handler dead-letters,
       which enqueues triage for *it*, which dead-letters — an unbounded loop that would
       spend real money.
    2. Don't enqueue when no API key is configured. Otherwise every dead-lettered job
       spawns a triage job that also dead-letters, doubling the noise in the queue an
       operator is trying to read.
    3. Run on a dedicated queue, so pausing a business queue never stops ops tooling
       (and a triage backlog never starves paying work).
    """
    settings = get_settings()
    if (not settings.ai_triage_enabled
            or not is_configured()
            or dead_job.type == TRIAGE_JOB_TYPE):
        return

    await session.execute(
        pg_insert(Queue).values(user_id=dead_job.user_id, name=TRIAGE_QUEUE)
        .on_conflict_do_nothing(constraint="uq_queue_user_name")
    )
    session.add(Job(
        user_id=dead_job.user_id,
        queue=TRIAGE_QUEUE,
        type=TRIAGE_JOB_TYPE,
        payload={"target_job_id": str(dead_job.id)},
        state=JobState.QUEUED.value,
        priority=-10,          # never ahead of the user's own work
        max_attempts=2,
        # One triage per dead job, even if this transition somehow runs twice.
        idempotency_key=f"triage:{dead_job.id}",
    ))


async def resolve_initial_state(session: AsyncSession, depends_on: list[uuid.UUID]) -> str:
    """State for a newly submitted job given its parents (pending / queued / canceled)."""
    if not depends_on:
        return JobState.QUEUED.value
    parents = (await session.execute(select(Job.state).where(Job.id.in_(depends_on)))).scalars().all()
    if any(s in (JobState.DEAD.value, JobState.CANCELED.value) for s in parents):
        return JobState.CANCELED.value
    if all(s == JobState.SUCCEEDED.value for s in parents):
        return JobState.QUEUED.value
    return JobState.PENDING.value
