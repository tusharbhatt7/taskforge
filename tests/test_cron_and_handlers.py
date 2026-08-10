"""Cron scheduling, handler behaviour, webhook signing."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from app.core.security import sign_webhook, verify_password
from app.db.models import Job, Schedule
from app.engine.cron import next_fire, run_due_schedules
from app.worker.handlers import HANDLERS, JobContext
from tests.conftest import TestSession


def test_next_fire_advances():
    base = datetime(2026, 1, 1, 10, 30, 0, tzinfo=UTC)
    assert next_fire("*/5 * * * *", base) == datetime(2026, 1, 1, 10, 35, tzinfo=UTC)
    assert next_fire("0 * * * *", base) == datetime(2026, 1, 1, 11, 0, tzinfo=UTC)


async def test_due_schedule_materializes_a_job(db, user):
    sched = Schedule(user_id=user.id, name="every-minute", cron="* * * * *", job_type="sleep",
                     payload={"seconds": 1}, next_run_at=datetime.now(UTC) - timedelta(seconds=5))
    db.add(sched)
    await db.commit()

    async with TestSession() as session:
        assert await run_due_schedules(session) == 1

    async with TestSession() as session:
        job = (await session.execute(select(Job))).scalar_one()
        assert job.type == "sleep"
        assert job.payload == {"seconds": 1}
        assert job.state == "queued"

        fresh = await session.get(Schedule, sched.id)
        assert fresh.last_run_at is not None
        assert fresh.next_run_at > datetime.now(UTC), "next_run_at must move into the future"


async def test_schedule_fires_once_not_once_per_missed_interval(db, user):
    """A schedule un-runnable for hours (instance asleep) must not replay a backlog."""
    sched = Schedule(user_id=user.id, name="stale", cron="* * * * *", job_type="sleep",
                     payload={}, next_run_at=datetime.now(UTC) - timedelta(hours=6))
    db.add(sched)
    await db.commit()

    async with TestSession() as session:
        await run_due_schedules(session)
    async with TestSession() as session:
        await run_due_schedules(session)

    async with TestSession() as session:
        jobs = (await session.execute(select(Job))).scalars().all()
        assert len(jobs) == 1, "6 hours of missed minutes must not become 360 jobs"


async def test_disabled_schedule_does_not_fire(db, user):
    db.add(Schedule(user_id=user.id, name="off", cron="* * * * *", job_type="sleep", payload={},
                    enabled=False, next_run_at=datetime.now(UTC) - timedelta(minutes=1)))
    await db.commit()

    async with TestSession() as session:
        assert await run_due_schedules(session) == 0


async def test_invalid_cron_is_rejected_by_the_api(auth_client):
    res = await auth_client.post("/api/v1/schedules", json={
        "name": "bad", "cron": "not a cron", "job_type": "sleep",
    })
    assert res.status_code == 422


async def test_schedule_crud(auth_client):
    created = (await auth_client.post("/api/v1/schedules", json={
        "name": "nightly", "cron": "0 3 * * *", "job_type": "thumbnail",
        "payload": {"width": 64, "height": 64},
    })).json()
    assert created["next_run_at"]

    updated = (await auth_client.put(f"/api/v1/schedules/{created['id']}", json={
        "name": "nightly", "cron": "0 4 * * *", "job_type": "thumbnail", "payload": {},
    })).json()
    assert updated["cron"] == "0 4 * * *"

    assert (await auth_client.delete(f"/api/v1/schedules/{created['id']}")).status_code == 204
    assert (await auth_client.get("/api/v1/schedules")).json() == []


# ---------------- handlers ----------------
def _ctx(attempt: int = 1, max_attempts: int = 3) -> JobContext:
    return JobContext(job_id=uuid.uuid4(), attempt=attempt, max_attempts=max_attempts)


async def test_all_advertised_handlers_are_registered():
    assert set(HANDLERS) == {"sleep", "flaky", "http_fetch", "thumbnail", "email_sim"}


async def test_sleep_handler_caps_duration(monkeypatch):
    """An unbounded sleep would pin a worker slot forever. Patch the sleep itself so the
    test asserts the clamp without actually waiting a minute."""
    import app.worker.handlers.demo as demo

    slept = []
    monkeypatch.setattr(demo.asyncio, "sleep", lambda s: slept.append(s) or _noop())

    result = await HANDLERS["sleep"]({"seconds": 999}, _ctx())
    assert slept == [demo.MAX_SLEEP_SECONDS]
    assert result["slept_seconds"] == demo.MAX_SLEEP_SECONDS


async def _noop():
    return None


async def test_flaky_fails_then_succeeds_based_on_attempt():
    with pytest.raises(RuntimeError, match="attempt 1/3"):
        await HANDLERS["flaky"]({"fail_times": 2}, _ctx(attempt=1))
    with pytest.raises(RuntimeError):
        await HANDLERS["flaky"]({"fail_times": 2}, _ctx(attempt=2))

    result = await HANDLERS["flaky"]({"fail_times": 2}, _ctx(attempt=3))
    assert result["succeeded_on_attempt"] == 3


async def test_thumbnail_generates_from_synthetic_source():
    result = await HANDLERS["thumbnail"]({"width": 64, "height": 64}, _ctx())
    assert result["width"] <= 64 and result["height"] <= 64
    assert result["thumbnail_bytes"] > 0


async def test_email_sim_reports_recipient():
    result = await HANDLERS["email_sim"]({"to": "x@y.com", "subject": "Hi"}, _ctx())
    assert result["delivered_to"] == "x@y.com"


@pytest.mark.parametrize("url", [
    "http://127.0.0.1:8000/admin",
    "http://localhost/secret",
    "http://169.254.169.254/latest/meta-data/",  # cloud metadata endpoint
])
async def test_http_fetch_refuses_internal_addresses(url):
    """Job payloads are user input: SSRF into the worker's own network must be blocked."""
    with pytest.raises(ValueError, match="non-public|unsupported"):
        await HANDLERS["http_fetch"]({"url": url}, _ctx())


async def test_http_fetch_rejects_non_http_schemes():
    with pytest.raises(ValueError, match="unsupported"):
        await HANDLERS["http_fetch"]({"url": "file:///etc/passwd"}, _ctx())


async def test_http_fetch_rejects_write_methods():
    with pytest.raises(ValueError, match="GET and HEAD"):
        await HANDLERS["http_fetch"]({"url": "https://example.com", "method": "POST"}, _ctx())


# ---------------- security primitives ----------------
def test_webhook_signature_is_stable_and_key_dependent():
    body = b'{"event":"job.succeeded"}'
    assert sign_webhook("whsec_abc", body) == sign_webhook("whsec_abc", body)
    assert sign_webhook("whsec_abc", body) != sign_webhook("whsec_xyz", body)
    assert sign_webhook("whsec_abc", body).startswith("sha256=")


def test_password_hashing_is_salted():
    from app.core.security import hash_password

    h1, h2 = hash_password("password123"), hash_password("password123")
    assert h1 != h2, "identical passwords must not produce identical hashes"
    assert verify_password("password123", h1)
    assert not verify_password("password124", h1)
