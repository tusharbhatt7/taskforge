import time
from collections import defaultdict, deque

from fastapi import HTTPException, Request, status


class SlidingWindowRateLimiter:
    """In-memory sliding-window limiter, keyed per caller.

    Per-process by design: the free tier runs a single API instance. The interface is
    the seam — swapping in a Redis-backed window requires no changes at call sites.
    """

    def __init__(self, limit: int, window_seconds: int = 60):
        self.limit = limit
        self.window = window_seconds
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def check(self, key: str) -> bool:
        now = time.monotonic()
        hits = self._hits[key]
        while hits and hits[0] <= now - self.window:
            hits.popleft()
        if len(hits) >= self.limit:
            return False
        hits.append(now)
        return True


def enforce_rate_limit(limiter: SlidingWindowRateLimiter, request: Request, key: str | None = None) -> None:
    caller = key or (request.client.host if request.client else "unknown")
    if not limiter.check(caller):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Slow down.",
            headers={"Retry-After": "60"},
        )
