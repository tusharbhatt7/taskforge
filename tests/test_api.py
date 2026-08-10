"""API surface: auth boundaries, tenancy isolation, idempotency, validation."""

import asyncio
import uuid


async def test_register_login_and_me(client):
    email = f"flow{uuid.uuid4().hex[:6]}@test.dev"
    res = await client.post("/api/v1/auth/register", json={"email": email, "password": "password123"})
    assert res.status_code == 201

    res = await client.post("/api/v1/auth/login", json={"email": email, "password": "password123"})
    assert res.status_code == 200
    token = res.json()["access_token"]

    res = await client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert res.status_code == 200
    assert res.json()["email"] == email


async def test_duplicate_email_is_rejected(client):
    email = f"dupe{uuid.uuid4().hex[:6]}@test.dev"
    body = {"email": email, "password": "password123"}
    assert (await client.post("/api/v1/auth/register", json=body)).status_code == 201
    assert (await client.post("/api/v1/auth/register", json=body)).status_code == 409


async def test_wrong_password_is_rejected(client):
    email = f"wrong{uuid.uuid4().hex[:6]}@test.dev"
    await client.post("/api/v1/auth/register", json={"email": email, "password": "password123"})
    res = await client.post("/api/v1/auth/login", json={"email": email, "password": "nope-nope-nope"})
    assert res.status_code == 401


async def test_short_password_is_rejected(client):
    res = await client.post("/api/v1/auth/register",
                            json={"email": "short@test.dev", "password": "abc"})
    assert res.status_code == 422


async def test_endpoints_require_authentication(client):
    for method, path in [("get", "/api/v1/jobs"), ("post", "/api/v1/jobs"),
                         ("get", "/api/v1/queues"), ("get", "/api/v1/schedules"),
                         ("get", "/api/v1/metrics/overview"), ("get", "/api/v1/api-keys")]:
        res = await getattr(client, method)(path, **({"json": {}} if method == "post" else {}))
        assert res.status_code == 401, f"{method} {path} should require auth"


async def test_garbage_token_is_rejected(client):
    res = await client.get("/api/v1/jobs", headers={"Authorization": "Bearer not-a-real-jwt"})
    assert res.status_code == 401


async def test_submit_job_and_fetch_detail(auth_client):
    res = await auth_client.post("/api/v1/jobs", json={
        "type": "sleep", "payload": {"seconds": 1}, "priority": 5, "max_attempts": 4,
    })
    assert res.status_code == 201, res.text
    job = res.json()
    assert job["state"] == "queued"
    assert job["priority"] == 5

    detail = (await auth_client.get(f"/api/v1/jobs/{job['id']}")).json()
    assert detail["id"] == job["id"]
    assert detail["job_attempts"] == []


async def test_unknown_job_type_is_rejected_with_helpful_message(auth_client):
    res = await auth_client.post("/api/v1/jobs", json={"type": "does_not_exist"})
    assert res.status_code == 422
    assert "Unknown job type" in res.json()["detail"]


async def test_idempotency_key_returns_the_same_job(auth_client):
    body = {"type": "sleep", "payload": {"seconds": 1}, "idempotency_key": "charge-order-42"}
    first = await auth_client.post("/api/v1/jobs", json=body)
    second = await auth_client.post("/api/v1/jobs", json=body)

    assert first.status_code == 201
    assert second.status_code == 200, "a repeat submission is not a new job"
    assert first.json()["id"] == second.json()["id"]

    listing = (await auth_client.get("/api/v1/jobs")).json()
    assert listing["total"] == 1


async def test_concurrent_identical_submissions_create_one_job(auth_client):
    """Two racing clients with the same idempotency key: the unique partial index
    rejects the loser, which returns the winner's row instead of erroring."""
    body = {"type": "sleep", "payload": {}, "idempotency_key": "race-me"}
    results = await asyncio.gather(*(auth_client.post("/api/v1/jobs", json=body) for _ in range(5)))

    assert all(r.status_code in (200, 201) for r in results), [r.status_code for r in results]
    assert len({r.json()["id"] for r in results}) == 1

    listing = (await auth_client.get("/api/v1/jobs")).json()
    assert listing["total"] == 1


async def test_delay_seconds_schedules_into_the_future(auth_client):
    from datetime import UTC, datetime

    job = (await auth_client.post("/api/v1/jobs",
                                  json={"type": "sleep", "delay_seconds": 3600})).json()
    run_at = datetime.fromisoformat(job["run_at"].replace("Z", "+00:00"))
    assert (run_at - datetime.now(UTC)).total_seconds() > 3000


async def test_job_with_pending_dependency_starts_pending(auth_client):
    parent = (await auth_client.post("/api/v1/jobs", json={"type": "sleep"})).json()
    child = (await auth_client.post("/api/v1/jobs",
                                    json={"type": "sleep", "depends_on": [parent["id"]]})).json()
    assert child["state"] == "pending"

    detail = (await auth_client.get(f"/api/v1/jobs/{child['id']}")).json()
    assert detail["depends_on"] == [parent["id"]]


async def test_depends_on_unknown_job_is_rejected(auth_client):
    res = await auth_client.post("/api/v1/jobs",
                                 json={"type": "sleep", "depends_on": [str(uuid.uuid4())]})
    assert res.status_code == 422


async def test_cancel_then_retry_lifecycle(auth_client):
    job = (await auth_client.post("/api/v1/jobs", json={"type": "sleep"})).json()

    canceled = (await auth_client.post(f"/api/v1/jobs/{job['id']}/cancel")).json()
    assert canceled["state"] == "canceled"

    # Canceling twice is a conflict, not a silent success.
    assert (await auth_client.post(f"/api/v1/jobs/{job['id']}/cancel")).status_code == 409

    requeued = (await auth_client.post(f"/api/v1/jobs/{job['id']}/retry")).json()
    assert requeued["state"] == "queued"
    assert requeued["attempts"] == 0, "retry resets the attempt budget"


async def test_cannot_retry_a_queued_job(auth_client):
    job = (await auth_client.post("/api/v1/jobs", json={"type": "sleep"})).json()
    assert (await auth_client.post(f"/api/v1/jobs/{job['id']}/retry")).status_code == 409


async def test_job_filters_and_pagination(auth_client):
    for _ in range(3):
        await auth_client.post("/api/v1/jobs", json={"type": "sleep", "queue": "alpha"})
    await auth_client.post("/api/v1/jobs", json={"type": "email_sim", "queue": "beta"})

    assert (await auth_client.get("/api/v1/jobs?queue=alpha")).json()["total"] == 3
    assert (await auth_client.get("/api/v1/jobs?type=email_sim")).json()["total"] == 1
    assert (await auth_client.get("/api/v1/jobs?state=queued")).json()["total"] == 4

    page = (await auth_client.get("/api/v1/jobs?limit=2&offset=0")).json()
    assert len(page["items"]) == 2 and page["total"] == 4


async def test_invalid_state_filter_is_rejected(auth_client):
    assert (await auth_client.get("/api/v1/jobs?state=bogus")).status_code == 422


async def test_users_cannot_see_or_touch_each_others_jobs(client):
    async def register(tag: str) -> str:
        res = await client.post("/api/v1/auth/register",
                                json={"email": f"{tag}{uuid.uuid4().hex[:6]}@test.dev",
                                      "password": "password123"})
        return res.json()["access_token"]

    token_a, token_b = await register("alice"), await register("bob")
    head_a = {"Authorization": f"Bearer {token_a}"}
    head_b = {"Authorization": f"Bearer {token_b}"}

    job = (await client.post("/api/v1/jobs", json={"type": "sleep"}, headers=head_a)).json()

    assert (await client.get("/api/v1/jobs", headers=head_b)).json()["total"] == 0
    assert (await client.get(f"/api/v1/jobs/{job['id']}", headers=head_b)).status_code == 404
    assert (await client.post(f"/api/v1/jobs/{job['id']}/cancel", headers=head_b)).status_code == 404


async def test_api_key_can_submit_jobs_and_is_shown_once(auth_client):
    created = (await auth_client.post("/api/v1/api-keys", json={"name": "ci"})).json()
    assert created["key"].startswith("tf_live_")

    listing = (await auth_client.get("/api/v1/api-keys")).json()
    assert "key" not in listing[0], "the plaintext key must never be returned again"
    assert listing[0]["prefix"] == created["key"][:12]

    # The key authenticates on its own, without the JWT.
    res = await auth_client.post("/api/v1/jobs", json={"type": "sleep"},
                                 headers={"Authorization": "", "X-API-Key": created["key"]})
    assert res.status_code == 201


async def test_revoked_api_key_stops_working(auth_client):
    created = (await auth_client.post("/api/v1/api-keys", json={"name": "temp"})).json()
    await auth_client.delete(f"/api/v1/api-keys/{created['id']}")

    res = await auth_client.get("/api/v1/jobs",
                                headers={"Authorization": "", "X-API-Key": created["key"]})
    assert res.status_code == 401


async def test_queue_pause_and_resume(auth_client):
    await auth_client.post("/api/v1/jobs", json={"type": "sleep", "queue": "images"})

    queues = {q["name"]: q for q in (await auth_client.get("/api/v1/queues")).json()}
    assert "images" in queues, "submitting to a new queue should create it"
    assert queues["images"]["counts"]["queued"] == 1

    assert (await auth_client.post("/api/v1/queues/images/pause")).json()["paused"] is True
    assert (await auth_client.post("/api/v1/queues/images/resume")).json()["paused"] is False
    assert (await auth_client.post("/api/v1/queues/nope/pause")).status_code == 404


async def test_metrics_overview_shape(auth_client):
    await auth_client.post("/api/v1/jobs", json={"type": "sleep"})
    data = (await auth_client.get("/api/v1/metrics/overview")).json()
    assert data["job_states"]["queued"] == 1
    for key in ("throughput_per_minute", "exec_duration_ms", "success_rate_1h", "workers"):
        assert key in data


async def test_rate_limit_blocks_a_burst_of_logins(client):
    """Credential endpoints are the ones worth protecting from brute force."""
    from app.api.deps import auth_limiter

    body = {"email": "nobody@test.dev", "password": "whatever123"}
    statuses = [(await client.post("/api/v1/auth/login", json=body)).status_code
                for _ in range(auth_limiter.limit + 5)]

    assert 429 in statuses, "the limiter must eventually reject"
    assert statuses.count(429) == 5
    assert statuses[0] == 401, "early attempts still get a normal auth failure"


async def test_healthz(client):
    assert (await client.get("/healthz")).json() == {"status": "ok"}
