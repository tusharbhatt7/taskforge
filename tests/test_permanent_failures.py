"""Permanent vs retryable failures, and auto-triage on dead-letter."""

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.db.models import Job, JobState, Queue
from app.engine import states
from app.engine.errors import PermanentError, RetryAfterError
from tests.conftest import TestSession


async def _running_job(db, user, **kw) -> Job:
    job = Job(user_id=user.id, type=kw.pop("type", "sleep"), payload={},
              state=JobState.RUNNING.value, **kw)
    db.add(job)
    await db.commit()
    return job


async def test_permanent_failure_dead_letters_on_the_first_attempt(db, user):
    """A malformed payload shouldn't consume its whole retry budget to reach the same
    conclusion three times."""
    job = await _running_job(db, user, attempts=1, max_attempts=5)
    await states.fail_job(db, job, "PermanentError: payload missing 'text'", permanent=True)
    await db.commit()

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.state == JobState.DEAD.value
        assert fresh.attempts == 1, "the remaining retry budget must go unused"
        assert fresh.finished_at is not None


async def test_the_same_failure_without_the_flag_still_retries(db, user):
    """Control: the flag is what changes behaviour, not the error text."""
    job = await _running_job(db, user, attempts=1, max_attempts=5)
    await states.fail_job(db, job, "PermanentError: payload missing 'text'", permanent=False)
    await db.commit()

    async with TestSession() as session:
        assert (await session.get(Job, job.id)).state == JobState.QUEUED.value


async def test_retry_after_override_replaces_our_backoff(db, user):
    """A 429 carrying Retry-After: 90 must schedule at ~90s, not at our own base delay."""
    from datetime import UTC, datetime

    job = await _running_job(db, user, attempts=1, max_attempts=3)
    await states.fail_job(db, job, "RetryAfterError: rate limited",
                          retry_delay_override=90.0)
    await db.commit()

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        assert fresh.state == JobState.QUEUED.value
        delay = (fresh.run_at - datetime.now(UTC)).total_seconds()
        assert 80 <= delay <= 95, f"expected ~90s, got {delay:.0f}s"


async def test_dead_letter_enqueues_a_triage_job(db, user):
    job = await _running_job(db, user, type="http_fetch", attempts=3, max_attempts=3)
    await states.fail_job(db, job, "ConnectError: refused")
    await db.commit()

    async with TestSession() as session:
        triage = (await session.execute(
            select(Job).where(Job.type == states.TRIAGE_JOB_TYPE)
        )).scalar_one()
        assert triage.payload == {"target_job_id": str(job.id)}
        assert triage.queue == states.TRIAGE_QUEUE
        assert triage.state == JobState.QUEUED.value
        assert triage.priority < 0, "ops tooling must not outrank the user's own work"

        # The dedicated queue must exist so it shows up in the dashboard and can be paused.
        queue = (await session.execute(
            select(Queue).where(Queue.user_id == user.id, Queue.name == states.TRIAGE_QUEUE)
        )).scalar_one_or_none()
        assert queue is not None


async def test_a_failing_triage_job_does_not_trigger_triage_of_itself(db, user):
    """The loop guard. Without it, a broken triage handler dead-letters, enqueues triage
    for itself, dead-letters again — an unbounded loop that spends real money."""
    job = await _running_job(db, user, type=states.TRIAGE_JOB_TYPE, attempts=2, max_attempts=2)
    await states.fail_job(db, job, "PermanentError: target job no longer exists")
    await db.commit()

    async with TestSession() as session:
        triage_jobs = (await session.execute(
            select(Job).where(Job.type == states.TRIAGE_JOB_TYPE)
        )).scalars().all()
        assert len(triage_jobs) == 1, "only the original triage job should exist"


async def test_triage_is_not_enqueued_without_an_api_key(db, user, monkeypatch):
    """Otherwise every dead-lettered job spawns a triage job that also dead-letters,
    doubling the noise in the queue an operator is trying to read."""
    monkeypatch.setattr("app.engine.states.is_configured", lambda: False)

    job = await _running_job(db, user, attempts=1, max_attempts=1)
    await states.fail_job(db, job, "boom")
    await db.commit()

    async with TestSession() as session:
        assert (await session.execute(
            select(Job).where(Job.type == states.TRIAGE_JOB_TYPE)
        )).scalars().all() == []


async def test_triage_is_not_enqueued_when_disabled(db, user, monkeypatch):
    from app.core.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "ai_triage_enabled", False)

    job = await _running_job(db, user, attempts=1, max_attempts=1)
    await states.fail_job(db, job, "boom")
    await db.commit()

    async with TestSession() as session:
        assert (await session.execute(
            select(Job).where(Job.type == states.TRIAGE_JOB_TYPE)
        )).scalars().all() == []


async def test_retrying_a_dead_job_twice_does_not_duplicate_triage(db, user):
    """Triage carries an idempotency key, so a job that dead-letters, gets requeued, and
    dead-letters again produces one triage job, not two."""
    job = await _running_job(db, user, attempts=1, max_attempts=1)
    await states.fail_job(db, job, "boom")
    await db.commit()

    async with TestSession() as session:
        fresh = await session.get(Job, job.id)
        fresh.state = JobState.RUNNING.value
        fresh.attempts = 1
        await session.commit()
        await states.fail_job(session, fresh, "boom again")
        # The unique partial index on (user_id, idempotency_key) rejects the duplicate.
        with pytest.raises(IntegrityError):
            await session.commit()


# ---------------- handler-level ----------------
async def test_llm_handlers_reject_bad_payloads_permanently():
    from app.worker.handlers import HANDLERS, JobContext

    ctx = JobContext(job_id=uuid.uuid4(), attempt=1, max_attempts=3)
    for job_type, payload in [
        ("llm_summarize", {}),
        ("llm_summarize", {"text": "   "}),
        ("llm_classify", {"text": "hi"}),                       # no labels
        ("llm_classify", {"text": "hi", "labels": []}),
        ("llm_classify", {"text": "hi", "labels": [""]}),
        ("llm_extract", {"text": "hi"}),                        # no fields
    ]:
        with pytest.raises(PermanentError):
            await HANDLERS[job_type](payload, ctx)


async def test_oversized_input_is_rejected_rather_than_truncated():
    """Silently truncating would produce a confidently wrong summary of partial input."""
    from app.core.config import get_settings
    from app.worker.handlers import HANDLERS, JobContext

    limit = get_settings().ai_max_input_chars
    ctx = JobContext(job_id=uuid.uuid4(), attempt=1, max_attempts=3)
    with pytest.raises(PermanentError, match="over the"):
        await HANDLERS["llm_summarize"]({"text": "x" * (limit + 1)}, ctx)


async def test_ai_triage_handler_rejects_a_non_uuid_target():
    from app.worker.handlers import HANDLERS, JobContext

    ctx = JobContext(job_id=uuid.uuid4(), attempt=1, max_attempts=2)
    with pytest.raises(PermanentError, match="target_job_id"):
        await HANDLERS["ai_triage"]({"target_job_id": "not-a-uuid"}, ctx)


async def test_ai_triage_is_not_publicly_submittable(auth_client):
    """It takes a job id the submitter has no business choosing, so it must not appear in
    the public type list or be accepted by the submit endpoint."""
    from app.worker.handlers import public_types

    assert "ai_triage" not in public_types()
    res = await auth_client.post("/api/v1/jobs", json={
        "type": "ai_triage", "payload": {"target_job_id": str(uuid.uuid4())},
    })
    assert res.status_code == 422


async def test_error_classes_carry_their_data():
    exc = RetryAfterError("rate limited", retry_after=30.5)
    assert exc.retry_after == 30.5
    assert RetryAfterError("x", retry_after=-5).retry_after == 0.0
