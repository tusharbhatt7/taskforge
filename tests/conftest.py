import os
import uuid

# Point every import at the test database before app modules read settings.
os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://taskforge:taskforge@localhost:5432/taskforge_test"
)
os.environ.setdefault("SECRET_KEY", "test-secret-long-enough-for-hs256-signing")

import pytest  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402

from app.core.config import get_settings  # noqa: E402
from app.core.security import generate_webhook_secret, hash_password  # noqa: E402
from app.db.models import Base, Queue, User  # noqa: E402
from app.db.session import get_db  # noqa: E402
from app.main import create_app  # noqa: E402

settings = get_settings()
test_engine = create_async_engine(settings.database_url)
TestSession = async_sessionmaker(test_engine, expire_on_commit=False)


@pytest.fixture(scope="session", autouse=True)
async def _schema():
    async with test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield
    await test_engine.dispose()


@pytest.fixture(autouse=True)
def _reset_rate_limits():
    """Every test hits the API from the same client host, so the shared sliding windows
    would leak across tests and trip the limiter. Rate limiting itself is covered by
    test_rate_limit_blocks_a_burst_of_logins."""
    from app.api.deps import api_limiter, auth_limiter

    auth_limiter._hits.clear()
    api_limiter._hits.clear()
    yield


@pytest.fixture(autouse=True)
async def _clean_tables(_schema):
    """Truncate between tests so each one starts from a known state."""
    async with TestSession() as session:
        from sqlalchemy import text
        await session.execute(text(
            "TRUNCATE users, jobs, job_deps, job_attempts, workers, schedules, "
            "webhook_deliveries, queues, api_keys RESTART IDENTITY CASCADE"
        ))
        await session.commit()
    yield


@pytest.fixture
async def db():
    async with TestSession() as session:
        yield session


@pytest.fixture
async def user(db):
    row = User(email=f"u{uuid.uuid4().hex[:8]}@test.dev",
               password_hash=hash_password("password123"),
               webhook_secret=generate_webhook_secret())
    db.add(row)
    await db.flush()
    db.add(Queue(user_id=row.id, name="default"))
    await db.commit()
    return row


@pytest.fixture
async def client():
    """ASGI client that shares the test session factory (no real network, no lifespan:
    background reaper/cron loops stay off so tests control timing explicitly)."""
    app = create_app()

    async def override_get_db():
        async with TestSession() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
async def auth_client(client):
    """Registered-and-authenticated client, plus the user id it belongs to."""
    email = f"api{uuid.uuid4().hex[:8]}@test.dev"
    res = await client.post("/api/v1/auth/register", json={"email": email, "password": "password123"})
    assert res.status_code == 201, res.text
    token = res.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {token}"
    return client
