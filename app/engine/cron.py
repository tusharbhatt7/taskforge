"""Recurring jobs: cron schedules materialize into ordinary queued jobs.

Drift-safe advancement: next_run_at is computed from max(previous next_run_at, now),
so a schedule that was un-runnable for hours (instance asleep, DB down) fires once on
recovery instead of replaying a backlog storm. SKIP LOCKED keeps this safe to run from
multiple API instances.
"""

import logging
from datetime import UTC, datetime

from croniter import croniter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Job, JobState, Schedule
from app.engine import events

logger = logging.getLogger("taskforge.cron")


def next_fire(cron_expr: str, after: datetime) -> datetime:
    return croniter(cron_expr, after).get_next(datetime)


async def run_due_schedules(session: AsyncSession) -> int:
    now = datetime.now(UTC)
    due = (await session.execute(
        select(Schedule)
        .where(Schedule.enabled, Schedule.next_run_at <= now)
        .with_for_update(skip_locked=True)
    )).scalars().all()

    for sched in due:
        job = Job(
            user_id=sched.user_id, queue=sched.queue, type=sched.job_type,
            payload=sched.payload, priority=sched.priority, max_attempts=sched.max_attempts,
            state=JobState.QUEUED.value, run_at=now,
        )
        session.add(job)
        await session.flush()
        sched.last_run_at = now
        sched.next_run_at = next_fire(sched.cron, max(sched.next_run_at, now))
        await events.emit(session, "job.created", sched.user_id, job_id=job.id, queue=job.queue,
                          job_type=job.type, schedule=sched.name)
        logger.info("schedule fired", extra={"event": sched.name, "job_id": str(job.id)})
    await session.commit()
    return len(due)
