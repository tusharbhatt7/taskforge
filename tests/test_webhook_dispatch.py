"""Webhook dispatcher state machine.

Regression coverage: an earlier version reused the same bind parameter for both a varchar
column and a text comparison, so the bookkeeping UPDATE aborted its transaction. The POST
had already been sent, the row stayed 'pending', and the delivery repeated forever. These
tests assert the row actually advances after each outcome.
"""

import httpx
import pytest
from sqlalchemy import select

from app.db.models import Job, JobState, WebhookDelivery
from app.engine.webhooks import MAX_WEBHOOK_ATTEMPTS, dispatch_due_webhooks
from tests.conftest import TestSession


class _StubResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code


@pytest.fixture
def http_stub(monkeypatch):
    """Capture outbound webhook POSTs without touching the network."""
    sent: list[dict] = []
    script: list = []

    async def fake_post(self, url, content=None, headers=None, **kwargs):
        sent.append({"url": url, "content": content, "headers": headers})
        outcome = script.pop(0) if script else _StubResponse(200)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)
    return {"sent": sent, "script": script}


async def _delivery(db, user, url="https://example.com/hook") -> WebhookDelivery:
    job = Job(user_id=user.id, type="sleep", payload={}, state=JobState.SUCCEEDED.value,
              callback_url=url)
    db.add(job)
    await db.flush()
    delivery = WebhookDelivery(user_id=user.id, job_id=job.id, url=url, event="job.succeeded",
                               payload={"event": "job.succeeded", "job": {"id": str(job.id)}})
    db.add(delivery)
    await db.commit()
    return delivery


async def test_successful_delivery_is_marked_delivered(db, user, http_stub):
    delivery = await _delivery(db, user)

    async with TestSession() as session:
        assert await dispatch_due_webhooks(session) == 1

    async with TestSession() as session:
        fresh = await session.get(WebhookDelivery, delivery.id)
        assert fresh.state == "delivered"
        assert fresh.attempts == 1
        assert fresh.response_status == 200
        assert fresh.delivered_at is not None, "delivered_at must be stamped"
        assert fresh.last_error is None


async def test_delivered_webhook_is_not_sent_again(db, user, http_stub):
    """The bug this suite exists for: a delivered row must leave the pending set."""
    await _delivery(db, user)

    async with TestSession() as session:
        await dispatch_due_webhooks(session)
    async with TestSession() as session:
        assert await dispatch_due_webhooks(session) == 0, "no pending deliveries should remain"

    assert len(http_stub["sent"]) == 1, "exactly one POST, not an infinite redelivery loop"


async def test_signature_header_is_present_and_verifiable(db, user, http_stub):
    import hashlib
    import hmac

    await _delivery(db, user)
    async with TestSession() as session:
        await dispatch_due_webhooks(session)

    request = http_stub["sent"][0]
    expected = "sha256=" + hmac.new(user.webhook_secret.encode(),
                                    request["content"], hashlib.sha256).hexdigest()
    assert hmac.compare_digest(request["headers"]["X-Taskforge-Signature"], expected)
    assert request["headers"]["X-Taskforge-Event"] == "job.succeeded"


async def test_server_error_schedules_a_retry(db, user, http_stub):
    delivery = await _delivery(db, user)
    http_stub["script"].append(_StubResponse(500))

    async with TestSession() as session:
        await dispatch_due_webhooks(session)

    async with TestSession() as session:
        fresh = await session.get(WebhookDelivery, delivery.id)
        assert fresh.state == "pending", "still pending so it will be retried"
        assert fresh.attempts == 1
        assert fresh.last_error == "HTTP 500"
        assert fresh.next_attempt_at > fresh.created_at, "retry must be pushed into the future"


async def test_retries_stop_after_max_attempts(db, user, http_stub):
    delivery = await _delivery(db, user)
    http_stub["script"].extend(_StubResponse(500) for _ in range(MAX_WEBHOOK_ATTEMPTS))

    for _ in range(MAX_WEBHOOK_ATTEMPTS):
        async with TestSession() as session:
            # Make the retry immediately due so the loop doesn't have to wait out backoff.
            from sqlalchemy import text
            await session.execute(
                text("UPDATE webhook_deliveries SET next_attempt_at = now() WHERE id = :id"),
                {"id": delivery.id})
            await session.commit()
            await dispatch_due_webhooks(session)

    async with TestSession() as session:
        fresh = await session.get(WebhookDelivery, delivery.id)
        assert fresh.state == "failed"
        assert fresh.attempts == MAX_WEBHOOK_ATTEMPTS
        assert fresh.delivered_at is None


async def test_network_error_is_recorded_not_raised(db, user, http_stub):
    delivery = await _delivery(db, user)
    http_stub["script"].append(httpx.ConnectError("connection refused"))

    async with TestSession() as session:
        await dispatch_due_webhooks(session)  # must not propagate

    async with TestSession() as session:
        fresh = await session.get(WebhookDelivery, delivery.id)
        assert fresh.state == "pending"
        assert "ConnectError" in fresh.last_error
        assert fresh.response_status is None


async def test_only_due_deliveries_are_dispatched(db, user, http_stub):
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import text

    delivery = await _delivery(db, user)
    async with TestSession() as session:
        await session.execute(
            text("UPDATE webhook_deliveries SET next_attempt_at = :future WHERE id = :id"),
            {"future": datetime.now(UTC) + timedelta(hours=1), "id": delivery.id})
        await session.commit()

    async with TestSession() as session:
        assert await dispatch_due_webhooks(session) == 0
    assert http_stub["sent"] == []


async def test_dispatcher_handles_an_empty_queue(db, user, http_stub):
    async with TestSession() as session:
        assert await dispatch_due_webhooks(session) == 0

    async with TestSession() as session:
        assert (await session.execute(select(WebhookDelivery))).scalars().all() == []
