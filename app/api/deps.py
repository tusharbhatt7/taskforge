import uuid
from datetime import UTC, datetime

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.rate_limit import SlidingWindowRateLimiter, enforce_rate_limit
from app.core.security import decode_access_token, hash_api_key
from app.db.models import ApiKey, User
from app.db.session import get_db

auth_limiter = SlidingWindowRateLimiter(limit=20)  # register/login attempts per IP per minute
api_limiter = SlidingWindowRateLimiter(limit=get_settings().rate_limit_per_minute)

_credentials_error = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Not authenticated. Send 'Authorization: Bearer <jwt>' or 'X-API-Key: tf_live_...'.",
    headers={"WWW-Authenticate": "Bearer"},
)


async def get_current_user(request: Request, db: AsyncSession = Depends(get_db)) -> User:
    """Accepts either a dashboard JWT (Authorization: Bearer) or a programmatic API key
    (X-API-Key). SSE clients can pass ?token= since EventSource cannot set headers."""
    authorization = request.headers.get("Authorization", "")
    if authorization.startswith("Bearer "):
        user = await _user_from_jwt(db, authorization.removeprefix("Bearer "))
        if user:
            return _limited(request, user)

    if api_key := request.headers.get("X-API-Key"):
        user = await _user_from_api_key(db, api_key)
        if user:
            return _limited(request, user)

    if token := request.query_params.get("token"):
        user = await _user_from_jwt(db, token)
        if user:
            return _limited(request, user)

    raise _credentials_error


def _limited(request: Request, user: User) -> User:
    enforce_rate_limit(api_limiter, request, key=str(user.id))
    return user


async def _user_from_jwt(db: AsyncSession, token: str) -> User | None:
    user_id = decode_access_token(token)
    if not user_id:
        return None
    try:
        return await db.get(User, uuid.UUID(user_id))
    except ValueError:
        return None


async def _user_from_api_key(db: AsyncSession, key: str) -> User | None:
    row = (await db.execute(select(ApiKey).where(ApiKey.key_hash == hash_api_key(key)))).scalar_one_or_none()
    if not row:
        return None
    row.last_used_at = datetime.now(UTC)
    await db.commit()
    return await db.get(User, row.user_id)
