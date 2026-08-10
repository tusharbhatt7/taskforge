"""Crash recovery: an expired lease must return the job to the queue.

This is the platform's headline guarantee — a worker can die at any moment without
losing work — so it is tested by actually expiring a lease in the database.
"""

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select

from app.db.models import AttemptOutcome, Job, JobAttempt, JobState, Worker, WorkerState
from app.engine.claim import claim_jobs, renew_leases
from app.engine.reaper import mark_dead_workers, reap_expired_leases
from tests.conftest import TestSession


async def _queued_job(db, user, **kw) -> Job:
    job = Job(user_id=user.id, type="sleep", payload={}, state=JobState.QUEUED.value, **kw)
    db.add(job)
    await db.commit()
    return job


async def _expire_lease(job_id: uuid.UUID) -> None:
    """Simulate a worker that stopped renewing (i.e. crashed)."""
    async with TestSession() as session:
        job = await session.get(Job, job_id)
        job.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        await session.commit()


async def test_expired_lease_requeues_job_for_another_worker(db, user):
    job = await _queued_job(db, user, max_attempts=3)
    dead_worker = uuid.uuid4()

    async with TestSession() as session:
        await claim_jobs(session, dead_worker, batch=1, lease_seconds=30)
    await _expire_lease(job.id)

    async with TestSession() as session:
        reclaimed = await reap_expired_leases(session)
    assert reclaimed == 1

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.state == JobState.QUEUED.value, "job must return to the queue"
        assert fresh.leased_by is None, "the dead worker's lease must be released"
        assert "lease expired" in fresh.error

        # The lost attempt is recorded distinctly from a handler failure — this is what
        # lets you tell 'the code is broken' apart from 'the machine died'.
        attempt = (await session.execute(
            select(JobAttempt).where(JobAttempt.job_id == job.id, JobAttempt.attempt_no == 1)
        )).scalar_one()
        assert attempt.outcome == AttemptOutcome.LOST.value

    # A surviving worker picks it straight back up (no backoff: the job didn't fail).
    survivor = uuid.uuid4()
    async with TestSession() as session:
        claimed = await claim_jobs(session, survivor, batch=1, lease_seconds=30)
    assert [j.id for j in claimed] == [job.id]

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.attempts == 2
        assert fresh.leased_by == survivor


async def test_expired_lease_dead_letters_when_attempts_exhausted(db, user):
    job = await _queued_job(db, user, max_attempts=1)

    async with TestSession() as session:
        await claim_jobs(session, uuid.uuid4(), batch=1, lease_seconds=30)
    await _expire_lease(job.id)

    async with TestSession() as session:
        await reap_expired_leases(session)

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.state == JobState.DEAD.value
        assert fresh.finished_at is not None


async def test_healthy_worker_keeps_its_job(db, user):
    """A live worker renewing its lease must never have work stolen from it."""
    job = await _queued_job(db, user)
    worker_id = uuid.uuid4()

    async with TestSession() as session:
        await claim_jobs(session, worker_id, batch=1, lease_seconds=30)
    await _expire_lease(job.id)

    # Renewal arrives before the reaper runs.
    async with TestSession() as session:
        await renew_leases(session, worker_id, [job.id], lease_seconds=30)
        await session.commit()

    async with TestSession() as session:
        assert await reap_expired_leases(session) == 0

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.state == JobState.RUNNING.value
        assert fresh.leased_by == worker_id
        assert fresh.attempts == 1


async def test_renew_cannot_steal_back_a_reclaimed_job(db, user):
    """After the reaper reclaims a job, a late renewal from the old worker is a no-op.
    Without the `leased_by` guard, a worker waking from a long stall could resurrect a
    lease on a job another worker already owns — and then two workers would run it."""
    job = await _queued_job(db, user)
    stalled_worker = uuid.uuid4()

    async with TestSession() as session:
        await claim_jobs(session, stalled_worker, batch=1, lease_seconds=30)
    await _expire_lease(job.id)
    async with TestSession() as session:
        await reap_expired_leases(session)

    new_owner = uuid.uuid4()
    async with TestSession() as session:
        await claim_jobs(session, new_owner, batch=1, lease_seconds=30)

    # The stalled worker finally sends its renewal.
    async with TestSession() as session:
        await renew_leases(session, stalled_worker, [job.id], lease_seconds=999)
        await session.commit()

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.leased_by == new_owner, "the late renewal must not change ownership"


async def test_silent_workers_are_marked_dead(db):
    stale = Worker(hostname="host-a", pid=1, queues=[], state=WorkerState.ONLINE.value,
                   last_heartbeat_at=datetime.now(UTC) - timedelta(minutes=5))
    healthy = Worker(hostname="host-b", pid=2, queues=[], state=WorkerState.ONLINE.value,
                     last_heartbeat_at=datetime.now(UTC))
    db.add_all([stale, healthy])
    await db.commit()

    async with TestSession() as session:
        assert await mark_dead_workers(session) == 1

    async with TestSession() as session:
        assert (await session.get(Worker, stale.id)).state == WorkerState.DEAD.value
        assert (await session.get(Worker, healthy.id)).state == WorkerState.ONLINE.value
