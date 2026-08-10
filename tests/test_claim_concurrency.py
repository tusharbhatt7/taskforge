"""The core correctness guarantee: a job is never executed by two workers at once.

These tests hit real Postgres because the guarantee *is* a Postgres behaviour
(FOR UPDATE SKIP LOCKED). Mocking the database would test nothing.
"""

import asyncio
import uuid

from sqlalchemy import func, select

from app.db.models import Job, JobAttempt, JobState
from app.engine.claim import claim_jobs
from tests.conftest import TestSession


async def _seed_jobs(db, user, count: int, **overrides) -> list[Job]:
    jobs = [Job(user_id=user.id, type="sleep", payload={"seconds": 0},
                state=JobState.QUEUED.value, **overrides) for _ in range(count)]
    db.add_all(jobs)
    await db.commit()
    return jobs


async def test_concurrent_workers_never_double_claim(db, user):
    """20 jobs, 5 workers claiming simultaneously: every job claimed exactly once."""
    await _seed_jobs(db, user, 20)

    async def worker_claim(worker_id: uuid.UUID) -> list[uuid.UUID]:
        # Each worker uses its own session/connection — real concurrency, not serialized.
        async with TestSession() as session:
            jobs = await claim_jobs(session, worker_id, batch=10, lease_seconds=30)
            return [j.id for j in jobs]

    results = await asyncio.gather(*(worker_claim(uuid.uuid4()) for _ in range(5)))

    claimed = [jid for batch in results for jid in batch]
    assert len(claimed) == 20, "every queued job should be claimed"
    assert len(set(claimed)) == 20, "no job may be claimed by two workers"

    # And exactly one attempt row per job, all at attempt_no 1.
    async with TestSession() as session:
        rows = (await session.execute(
            select(JobAttempt.job_id, func.count()).group_by(JobAttempt.job_id)
        )).all()
    assert len(rows) == 20
    assert all(count == 1 for _, count in rows)


async def test_claim_respects_priority_then_fifo(db, user):
    low = (await _seed_jobs(db, user, 1, priority=0))[0]
    high = (await _seed_jobs(db, user, 1, priority=50))[0]

    async with TestSession() as session:
        claimed = await claim_jobs(session, uuid.uuid4(), batch=1, lease_seconds=30)
    assert [j.id for j in claimed] == [high.id], "higher priority must be claimed first"
    assert low.id not in [j.id for j in claimed]


async def test_claim_skips_future_run_at(db, user):
    """Delayed jobs are invisible until their run_at arrives."""
    from datetime import UTC, datetime, timedelta

    await _seed_jobs(db, user, 1, run_at=datetime.now(UTC) + timedelta(hours=1))
    async with TestSession() as session:
        assert await claim_jobs(session, uuid.uuid4(), batch=10, lease_seconds=30) == []


async def test_claim_skips_paused_queue(db, user):
    from sqlalchemy import update

    from app.db.models import Queue

    await _seed_jobs(db, user, 3, queue="default")
    await db.execute(update(Queue).where(Queue.user_id == user.id).values(paused=True))
    await db.commit()

    async with TestSession() as session:
        assert await claim_jobs(session, uuid.uuid4(), batch=10, lease_seconds=30) == []


async def test_claim_filters_by_queue(db, user):
    await _seed_jobs(db, user, 2, queue="default")
    await _seed_jobs(db, user, 3, queue="images")

    async with TestSession() as session:
        claimed = await claim_jobs(session, uuid.uuid4(), batch=10, lease_seconds=30,
                                   queues=["images"])
    assert len(claimed) == 3
    assert {j.queue for j in claimed} == {"images"}


async def test_claim_sets_lease_and_increments_attempts(db, user):
    job = (await _seed_jobs(db, user, 1))[0]
    worker_id = uuid.uuid4()

    async with TestSession() as session:
        claimed = await claim_jobs(session, worker_id, batch=1, lease_seconds=30)
    assert len(claimed) == 1

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.state == JobState.RUNNING.value
        assert fresh.leased_by == worker_id
        assert fresh.attempts == 1
        assert fresh.lease_expires_at is not None
        assert fresh.started_at is not None
