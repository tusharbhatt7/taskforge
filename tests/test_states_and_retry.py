"""State machine, retry backoff, dependency graph and webhook enqueueing."""

import uuid

import pytest
from sqlalchemy import select

from app.db.models import Job, JobDep, JobState, WebhookDelivery
from app.engine import states
from app.engine.retry import backoff_seconds
from tests.conftest import TestSession


def test_backoff_grows_exponentially_and_is_capped():
    plain = [backoff_seconds(a, base=5, cap=300, jitter=False) for a in range(1, 8)]
    assert plain[:5] == [5, 10, 20, 40, 80]
    assert plain[-1] == 300, "must saturate at the cap"
    assert all(b <= 300 for b in plain)


def test_backoff_jitter_stays_within_half_to_1_5x():
    """Full jitter spreads retries so a fleet failing together doesn't retry in lockstep."""
    base_delay = 40  # attempt 4 with base=5
    samples = [backoff_seconds(4, base=5, cap=300) for _ in range(300)]
    assert all(base_delay * 0.5 <= s <= base_delay * 1.5 for s in samples)
    assert len(set(samples)) > 100, "jitter should actually vary"


def test_backoff_floors_at_attempt_one():
    assert backoff_seconds(0, base=5, jitter=False) == backoff_seconds(1, base=5, jitter=False)


async def _job(db, user, **kw) -> Job:
    job = Job(user_id=user.id, type="sleep", payload={},
              state=kw.pop("state", JobState.RUNNING.value), **kw)
    db.add(job)
    await db.commit()
    return job


async def test_failure_below_max_attempts_schedules_retry(db, user):
    job = await _job(db, user, attempts=1, max_attempts=3)
    await states.fail_job(db, job, "boom")
    await db.commit()

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.state == JobState.QUEUED.value
        assert fresh.error == "boom"
        assert fresh.leased_by is None
        assert fresh.finished_at is None, "a retrying job is not finished"


async def test_failure_at_max_attempts_dead_letters(db, user):
    job = await _job(db, user, attempts=3, max_attempts=3)
    await states.fail_job(db, job, "final boom")
    await db.commit()

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.state == JobState.DEAD.value
        assert fresh.finished_at is not None


async def test_success_stores_result_and_clears_lease(db, user):
    job = await _job(db, user, attempts=1, leased_by=uuid.uuid4())
    await states.complete_job(db, job, {"ok": True})
    await db.commit()

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.state == JobState.SUCCEEDED.value
        assert fresh.result == {"ok": True}
        assert fresh.error is None
        assert fresh.leased_by is None


async def test_dependent_job_is_released_only_when_all_parents_succeed(db, user):
    parent_a = await _job(db, user, state=JobState.RUNNING.value, attempts=1)
    parent_b = await _job(db, user, state=JobState.RUNNING.value, attempts=1)
    child = await _job(db, user, state=JobState.PENDING.value)
    db.add_all([JobDep(job_id=child.id, parent_id=parent_a.id),
                JobDep(job_id=child.id, parent_id=parent_b.id)])
    await db.commit()

    await states.complete_job(db, parent_a, {})
    await db.commit()
    async with TestSession() as session:
        assert (await session.get(Job, child.id)).state == JobState.PENDING.value, \
            "one parent done is not enough"

    await states.complete_job(db, parent_b, {})
    await db.commit()
    async with TestSession() as session:
        assert (await session.get(Job, child.id)).state == JobState.QUEUED.value


async def test_dead_parent_cascades_cancel_through_the_whole_subtree(db, user):
    parent = await _job(db, user, attempts=1, max_attempts=1)
    child = await _job(db, user, state=JobState.PENDING.value)
    grandchild = await _job(db, user, state=JobState.PENDING.value)
    db.add_all([JobDep(job_id=child.id, parent_id=parent.id),
                JobDep(job_id=grandchild.id, parent_id=child.id)])
    await db.commit()

    await states.fail_job(db, parent, "parent exploded")
    await db.commit()

    async with TestSession() as session:
        assert (await session.get(Job, parent.id)).state == JobState.DEAD.value
        assert (await session.get(Job, child.id)).state == JobState.CANCELED.value
        assert (await session.get(Job, grandchild.id)).state == JobState.CANCELED.value, \
            "cancellation must reach transitive dependents"


async def test_resolve_initial_state(db, user):
    succeeded = await _job(db, user, state=JobState.SUCCEEDED.value)
    running = await _job(db, user, state=JobState.RUNNING.value)
    dead = await _job(db, user, state=JobState.DEAD.value)

    assert await states.resolve_initial_state(db, []) == JobState.QUEUED.value
    assert await states.resolve_initial_state(db, [succeeded.id]) == JobState.QUEUED.value
    assert await states.resolve_initial_state(db, [running.id]) == JobState.PENDING.value
    assert await states.resolve_initial_state(db, [succeeded.id, running.id]) == JobState.PENDING.value
    assert await states.resolve_initial_state(db, [dead.id]) == JobState.CANCELED.value


@pytest.mark.parametrize("terminal,expected_event", [
    ("succeed", "job.succeeded"),
    ("fail", "job.dead"),
])
async def test_terminal_state_enqueues_signed_webhook(db, user, terminal, expected_event):
    job = await _job(db, user, attempts=1, max_attempts=1,
                     callback_url="https://example.com/hook")
    if terminal == "succeed":
        await states.complete_job(db, job, {"ok": 1})
    else:
        await states.fail_job(db, job, "nope")
    await db.commit()

    async with TestSession() as session:
        delivery = (await session.execute(
            select(WebhookDelivery).where(WebhookDelivery.job_id == job.id)
        )).scalar_one()
        assert delivery.event == expected_event
        assert delivery.state == "pending"
        assert delivery.payload["job"]["id"] == str(job.id)


async def test_no_callback_url_means_no_webhook(db, user):
    job = await _job(db, user, attempts=1)
    await states.complete_job(db, job, {})
    await db.commit()

    async with TestSession() as session:
        assert (await session.execute(select(WebhookDelivery))).scalars().all() == []
