"""AI integration: error translation, triage, fingerprint dedupe, retry semantics.

No test here touches the network. What matters isn't that the model produces good prose —
it's that an API failure lands in the right place in the platform's retry machinery, and
that a burst of identical failures doesn't turn into a burst of identical API calls.
"""

import asyncio
import uuid

import anthropic
import httpx
import pytest
from sqlalchemy import select

from app.ai import triage as triage_mod
from app.ai.client import PRICING, AIClient, Usage, _retry_after_of
from app.ai.prompts import TRIAGE_SCHEMA, classify_schema, extract_schema
from app.ai.triage import build_prompt, fingerprint, triage_job
from app.db.models import AttemptOutcome, Job, JobAttempt, JobState, JobTriage
from app.engine.errors import PermanentError, RetryAfterError
from tests.conftest import TestSession


# ---------------- error translation ----------------
def _status_error(status_code: int, headers: dict | None = None) -> anthropic.APIStatusError:
    """Build a real SDK exception so the tests exercise the actual class hierarchy."""
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    response = httpx.Response(status_code, headers=headers or {}, json={
        "type": "error", "error": {"type": "x", "message": "boom"},
    }, request=request)
    cls = {
        400: anthropic.BadRequestError,
        401: anthropic.AuthenticationError,
        403: anthropic.PermissionDeniedError,
        404: anthropic.NotFoundError,
        429: anthropic.RateLimitError,
    }.get(status_code, anthropic.InternalServerError)
    return cls("boom", response=response, body=None)


def test_rate_limit_becomes_retry_after_honouring_the_header():
    """A 429 must not be retried on our own backoff — the server named a delay."""
    translated = AIClient._translate(_status_error(429, {"retry-after": "42"}))
    assert isinstance(translated, RetryAfterError)
    assert translated.retry_after == 42.0


def test_rate_limit_without_header_falls_back_to_a_sane_delay():
    translated = AIClient._translate(_status_error(429))
    assert isinstance(translated, RetryAfterError)
    assert translated.retry_after == 60.0


def test_garbage_retry_after_header_does_not_crash():
    assert _retry_after_of(_status_error(429, {"retry-after": "soon"}), default=7.0) == 7.0


@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
def test_client_faults_are_permanent(status_code):
    """Retrying a bad request or a revoked key just burns the retry budget."""
    assert isinstance(AIClient._translate(_status_error(status_code)), PermanentError)


@pytest.mark.parametrize("status_code", [500, 502, 529])
def test_server_and_overloaded_errors_are_retryable(status_code):
    translated = AIClient._translate(_status_error(status_code))
    assert isinstance(translated, RetryAfterError)
    assert not isinstance(translated, PermanentError)


def test_teapot_status_is_permanent_not_retried_forever():
    """An unmapped 4xx must not be treated as transient."""
    assert isinstance(AIClient._translate(_status_error(418)), PermanentError)


def test_connection_and_timeout_errors_stay_retryable():
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    for exc in (anthropic.APITimeoutError(request=request),
                anthropic.APIConnectionError(request=request)):
        translated = AIClient._translate(exc)
        assert not isinstance(translated, PermanentError)


def test_unknown_exception_passes_through_unchanged():
    original = ValueError("something else entirely")
    assert AIClient._translate(original) is original


def test_missing_api_key_is_a_permanent_error(monkeypatch):
    """A deployment without a key should dead-letter AI jobs immediately with a clear
    message, not retry three times first."""
    from app.core.config import get_settings

    get_settings.cache_clear()
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    try:
        with pytest.raises(PermanentError, match="ANTHROPIC_API_KEY"):
            AIClient()
    finally:
        get_settings.cache_clear()


# ---------------- cost accounting ----------------
def test_cost_uses_per_model_rates():
    usage = Usage(input_tokens=1_000_000, output_tokens=1_000_000, model="claude-opus-5")
    rate_in, rate_out = PRICING["claude-opus-5"]
    assert usage.cost_usd == pytest.approx(rate_in + rate_out)


def test_cached_reads_are_cheaper_than_fresh_input():
    fresh = Usage(input_tokens=100_000, model="claude-opus-5").cost_usd
    cached = Usage(cache_read_tokens=100_000, model="claude-opus-5").cost_usd
    assert cached == pytest.approx(fresh * 0.1)


def test_cache_writes_cost_a_premium():
    fresh = Usage(input_tokens=100_000, model="claude-opus-5").cost_usd
    written = Usage(cache_write_tokens=100_000, model="claude-opus-5").cost_usd
    assert written == pytest.approx(fresh * 1.25)


def test_unknown_model_costs_zero_rather_than_crashing():
    assert Usage(input_tokens=999, model="some-future-model").cost_usd == 0.0


# ---------------- fingerprinting ----------------
def test_same_failure_with_different_ids_shares_a_fingerprint():
    """This is the dedupe guarantee: 500 identical failures cost one API call."""
    a = fingerprint("http_fetch", "ConnectError: failed to reach host "
                                  "id=8f2c1a9b-0000-4000-8000-000000000001 at 2026-01-01T10:00:00Z")
    b = fingerprint("http_fetch", "ConnectError: failed to reach host "
                                  "id=1111aaaa-0000-4000-8000-000000000002 at 2026-03-09T22:14:07Z")
    assert a == b


def test_different_failures_do_not_collide():
    assert fingerprint("http_fetch", "ConnectError: refused") != \
           fingerprint("http_fetch", "TimeoutError: deadline exceeded")


def test_same_error_from_different_job_types_is_a_different_class():
    assert fingerprint("http_fetch", "boom") != fingerprint("thumbnail", "boom")


def test_fingerprint_is_stable_and_bounded():
    assert fingerprint("t", "e") == fingerprint("t", "e")
    assert len(fingerprint("t", "e")) == 16
    assert fingerprint("t", "") == fingerprint("t", None or "")


# ---------------- schemas ----------------
def test_schemas_satisfy_structured_output_constraints():
    """Structured outputs require every property listed in `required` and
    additionalProperties:false; numeric bounds are silently unsupported."""
    for schema in (TRIAGE_SCHEMA, classify_schema(["a", "b"]),
                   extract_schema(["name", "email"])):
        assert schema["additionalProperties"] is False
        assert set(schema["required"]) == set(schema["properties"])
        for prop in schema["properties"].values():
            assert "minimum" not in prop and "maximum" not in prop


def test_classify_schema_constrains_the_label_to_caller_labels():
    schema = classify_schema(["spam", "ham"])
    assert schema["properties"]["label"]["enum"] == ["spam", "ham"]


def test_extract_schema_requires_every_requested_field():
    schema = extract_schema(["invoice_no", "total"])
    assert set(schema["properties"]["fields"]["required"]) == {"invoice_no", "total"}
    assert schema["properties"]["fields"]["additionalProperties"] is False


# ---------------- triage ----------------
class FakeAIClient:
    """Stand-in for AIClient. Records calls so tests can assert the API was *not* hit."""

    calls: list[dict] = []
    response: dict = {
        "category": "network",
        "is_transient": True,
        "root_cause": "The host refused the connection.",
        "suggested_action": "Requeue once the host is reachable.",
        "confidence": 0.9,
    }

    def __init__(self) -> None:
        pass

    async def complete_json(self, *, system, user, schema, max_tokens=None):
        from app.ai.client import Completion

        FakeAIClient.calls.append({"system": system, "user": user})
        # A real API call takes seconds. Suspending here is what makes the
        # read-then-write window in triage_job observable — without it the fake returns
        # without yielding and the concurrency test can silently pass on a broken lock.
        await asyncio.sleep(0.05)
        return Completion(
            data=dict(FakeAIClient.response),
            usage=Usage(input_tokens=1200, output_tokens=95, model="claude-opus-5"),
        )


@pytest.fixture
def fake_ai(monkeypatch):
    FakeAIClient.calls = []
    FakeAIClient.response = {
        "category": "network",
        "is_transient": True,
        "root_cause": "The host refused the connection.",
        "suggested_action": "Requeue once the host is reachable.",
        "confidence": 0.9,
    }
    monkeypatch.setattr(triage_mod, "AIClient", FakeAIClient)
    return FakeAIClient


async def _dead_job(db, user, *, job_type="http_fetch", error="ConnectError: refused") -> Job:
    job = Job(user_id=user.id, type=job_type, payload={"url": "https://example.com"},
              state=JobState.DEAD.value, attempts=3, max_attempts=3, error=error)
    db.add(job)
    await db.flush()
    db.add(JobAttempt(job_id=job.id, attempt_no=1, outcome=AttemptOutcome.FAILED.value,
                      error=error, duration_ms=120.0))
    await db.commit()
    return job


async def test_triage_writes_an_analysis(db, user, fake_ai):
    job = await _dead_job(db, user)

    async with TestSession() as session:
        result = await triage_job(session, job.id)
        await session.commit()

    assert result["category"] == "network"
    assert result["is_transient"] is True
    assert result["reused_prior_analysis"] is False

    async with TestSession() as session:
        row = (await session.execute(
            select(JobTriage).where(JobTriage.job_id == job.id)
        )).scalar_one()
        assert row.root_cause
        assert row.model == "claude-opus-5"
        assert row.cost_usd > 0
        assert row.reused_from_id is None


async def test_identical_failures_reuse_one_analysis(db, user, fake_ai):
    """The cost-control guarantee: the second job with the same error signature must not
    trigger a second API call."""
    first = await _dead_job(db, user, error="ConnectError: refused by 10.0.0.1")
    second = await _dead_job(db, user, error="ConnectError: refused by 10.0.0.9")

    async with TestSession() as session:
        await triage_job(session, first.id)
        await session.commit()
    async with TestSession() as session:
        result = await triage_job(session, second.id)
        await session.commit()

    assert len(fake_ai.calls) == 1, "the second job must not hit the API"
    assert result["reused_prior_analysis"] is True

    async with TestSession() as session:
        row = (await session.execute(
            select(JobTriage).where(JobTriage.job_id == second.id)
        )).scalar_one()
        assert row.reused_from_id is not None
        assert row.cost_usd == 0.0
        assert row.root_cause  # the analysis itself was copied, not left blank


async def test_concurrent_triage_of_the_same_failure_calls_the_api_once(db, user, fake_ai):
    """Regression: the sequential test above passed while the live system still paid twice.

    A dependency outage dead-letters many jobs at once and their triage jobs get claimed
    in the same batch. Without the advisory lock, every one reads "no prior analysis"
    before any of them commits, and every one pays for the same answer.
    """
    jobs = [await _dead_job(db, user, error=f"ConnectError: refused by 10.0.0.{i}")
            for i in range(5)]

    async def run(job_id):
        # A session each — real concurrency, not a serialized loop.
        async with TestSession() as session:
            result = await triage_job(session, job_id)
            await session.commit()
            return result

    results = await asyncio.gather(*(run(job.id) for job in jobs))

    assert len(fake_ai.calls) == 1, (
        f"5 concurrent triages of one error signature made {len(fake_ai.calls)} API calls"
    )
    assert sum(1 for r in results if r["reused_prior_analysis"]) == 4

    async with TestSession() as session:
        rows = (await session.execute(select(JobTriage))).scalars().all()
        assert len(rows) == 5, "every job still gets its own record"
        assert sum(1 for r in rows if r.reused_from_id is None) == 1
        assert all(r.root_cause for r in rows), "reused rows must carry the analysis"


async def test_distinct_failures_each_get_their_own_analysis(db, user, fake_ai):
    a = await _dead_job(db, user, error="ConnectError: refused")
    b = await _dead_job(db, user, error="ValueError: bad payload field 'url'")

    for job in (a, b):
        async with TestSession() as session:
            await triage_job(session, job.id)
            await session.commit()

    assert len(fake_ai.calls) == 2


async def test_triage_is_idempotent(db, user, fake_ai):
    job = await _dead_job(db, user)
    async with TestSession() as session:
        await triage_job(session, job.id)
        await session.commit()
    async with TestSession() as session:
        result = await triage_job(session, job.id)
        await session.commit()

    assert result["status"] == "already_triaged"
    assert len(fake_ai.calls) == 1


async def test_confidence_is_clamped(db, user, fake_ai):
    """The JSON schema can't express 0.0-1.0, so the value is clamped in Python."""
    fake_ai.response = {**fake_ai.response, "confidence": 4.2}
    job = await _dead_job(db, user)

    async with TestSession() as session:
        await triage_job(session, job.id)
        await session.commit()

    async with TestSession() as session:
        row = (await session.execute(
            select(JobTriage).where(JobTriage.job_id == job.id)
        )).scalar_one()
        assert row.confidence == 1.0


async def test_triage_of_a_deleted_job_is_permanent(db, user, fake_ai):
    async with TestSession() as session:
        with pytest.raises(PermanentError):
            await triage_job(session, uuid.uuid4())


def test_prompt_includes_the_attempt_history_and_bounds_the_payload():
    job = Job(user_id=uuid.uuid4(), type="http_fetch", queue="default",
              payload={"blob": "x" * 50_000}, attempts=3, max_attempts=3, error="boom")
    attempts = [
        JobAttempt(job_id=uuid.uuid4(), attempt_no=1, outcome="failed",
                   error="ConnectError: refused", duration_ms=100.0),
        JobAttempt(job_id=uuid.uuid4(), attempt_no=2, outcome="lost", error=None,
                   duration_ms=None),
    ]
    prompt = build_prompt(job, attempts)

    assert "http_fetch" in prompt
    assert "attempt 1" in prompt and "ConnectError: refused" in prompt
    assert "attempt 2" in prompt and "lost" in prompt
    assert len(prompt) < 6000, "an unbounded payload would cost tokens without adding signal"


def test_prompt_handles_a_job_with_no_attempt_rows():
    job = Job(user_id=uuid.uuid4(), type="sleep", queue="default", payload={},
              attempts=1, max_attempts=1, error="reaped")
    assert "reaped" in build_prompt(job, [])
