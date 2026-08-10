"""Failure recovery.

Every claim carries a lease. A healthy worker renews its leases while executing; a
crashed worker cannot, so its leases expire and the reaper returns those jobs to the
queue (or dead-letters them if attempts are exhausted). Workers whose heartbeats go
silent are marked dead. Both scans use SKIP LOCKED so multiple API instances can run
reapers concurrently without stepping on each other.
"""

import logging

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models import AttemptOutcome, Job, WorkerState
from app.engine import events, states

logger = logging.getLogger("taskforge.reaper")


async def reap_expired_leases(session: AsyncSession) -> int:
    """Reclaim running jobs whose lease expired (their worker died mid-execution)."""
    rows = (await session.execute(
        select(Job)
        .where(Job.state == "running", Job.lease_expires_at < text("now()"))
        .with_for_update(skip_locked=True)
    )).scalars().all()

    for job in rows:
        lost_worker = job.leased_by
        logger.warning("lease expired, reclaiming job", extra={"job_id": str(job.id)})
        # Immediate requeue (no backoff): the job didn't fail, its worker did.
        await states.fail_job(session, job, error=f"worker {lost_worker} lost (lease expired)",
                              worker_id=lost_worker, attempt_outcome=AttemptOutcome.LOST,
                              retry_delay_override=0)
    await session.commit()
    return len(rows)


async def mark_dead_workers(session: AsyncSession) -> int:
    settings = get_settings()
    result = await session.execute(
        text("""
            UPDATE workers SET state = :dead
            WHERE state = :online AND last_heartbeat_at < now() - make_interval(secs => :cutoff)
            RETURNING id, hostname, pid
        """),
        {"dead": WorkerState.DEAD.value, "online": WorkerState.ONLINE.value,
         "cutoff": settings.worker_dead_after_seconds},
    )
    count = 0
    for row in result:
        count += 1
        logger.warning("worker went silent, marking dead", extra={"worker_id": str(row.id)})
        await events.emit(session, "worker.dead", None, worker_id=row.id, hostname=row.hostname, pid=row.pid)
    await session.commit()
    return count
