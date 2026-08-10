"""Atomic job claiming.

`FOR UPDATE SKIP LOCKED` is what makes the platform horizontally scalable: any number
of workers can run this query concurrently and Postgres guarantees no two of them ever
lock the same row — contended rows are skipped, not waited on. No coordinator, no
distributed lock service; the database is the arbiter.
"""

import uuid

from sqlalchemy import bindparam, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import AttemptOutcome, Job, JobAttempt
from app.engine import events

_CLAIM_SQL = """
WITH candidates AS (
    SELECT j.id
    FROM jobs j
    LEFT JOIN queues q ON q.user_id = j.user_id AND q.name = j.queue
    WHERE j.state = 'queued'
      AND j.run_at <= now()
      AND COALESCE(q.paused, false) = false
      {queue_filter}
    ORDER BY j.priority DESC, j.run_at ASC
    LIMIT :batch
    FOR UPDATE OF j SKIP LOCKED
)
UPDATE jobs j
SET state = 'running',
    leased_by = :worker_id,
    lease_expires_at = now() + make_interval(secs => :lease),
    attempts = j.attempts + 1,
    started_at = COALESCE(j.started_at, now())
FROM candidates c
WHERE j.id = c.id
RETURNING j.id
"""


async def claim_jobs(session: AsyncSession, worker_id: uuid.UUID, batch: int, lease_seconds: int,
                     queues: list[str] | None = None) -> list[Job]:
    """Claim up to `batch` due jobs for this worker. Commits the claim before returning,
    so the lease is durable even if the worker dies immediately after."""
    if queues:
        stmt = text(_CLAIM_SQL.format(queue_filter="AND j.queue IN :queues")).bindparams(
            bindparam("queues", expanding=True))
        params = {"worker_id": worker_id, "batch": batch, "lease": lease_seconds, "queues": queues}
    else:
        stmt = text(_CLAIM_SQL.format(queue_filter=""))
        params = {"worker_id": worker_id, "batch": batch, "lease": lease_seconds}

    ids = [row.id for row in await session.execute(stmt, params)]
    if not ids:
        await session.commit()
        return []

    jobs = list((await session.execute(select(Job).where(Job.id.in_(ids)))).scalars())
    for job in jobs:
        session.add(JobAttempt(job_id=job.id, attempt_no=job.attempts, worker_id=worker_id,
                               outcome=AttemptOutcome.RUNNING.value))
        await events.emit(session, "job.started", job.user_id, job_id=job.id, queue=job.queue,
                          job_type=job.type, attempt=job.attempts, worker_id=worker_id)
    await session.commit()
    return jobs


async def renew_leases(session: AsyncSession, worker_id: uuid.UUID, job_ids: list[uuid.UUID],
                       lease_seconds: int) -> None:
    """Extend leases on in-flight jobs. Only rows still leased by this worker are touched:
    if the reaper already reclaimed a job (e.g. after a long GC pause), we must not
    resurrect someone else's lease. The caller owns the transaction."""
    if not job_ids:
        return
    await session.execute(
        text("""
            UPDATE jobs SET lease_expires_at = now() + make_interval(secs => :lease)
            WHERE id = ANY(:ids) AND leased_by = :worker_id AND state = 'running'
        """).bindparams(lease=lease_seconds, ids=job_ids, worker_id=worker_id)
    )
