import time

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.models import User
from app.db.session import get_db

router = APIRouter(prefix="/metrics", tags=["metrics"])

_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL_S = 5.0


@router.get("/overview")
async def metrics_overview(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    cache_key = str(user.id)
    if cached := _cache.get(cache_key):
        ts, data = cached
        if time.monotonic() - ts < CACHE_TTL_S:
            return data

    states = dict((await db.execute(
        text("SELECT state, count(*) FROM jobs WHERE user_id = :uid GROUP BY state"), {"uid": user.id}
    )).all())

    # Completions per minute over the last 30 minutes (terminal states only).
    throughput = [
        {"minute": row.minute.isoformat(), "count": row.count, "succeeded": row.succeeded}
        for row in (await db.execute(text("""
            SELECT date_trunc('minute', finished_at) AS minute,
                   count(*) AS count,
                   count(*) FILTER (WHERE state = 'succeeded') AS succeeded
            FROM jobs
            WHERE user_id = :uid AND finished_at > now() - interval '30 minutes'
            GROUP BY 1 ORDER BY 1
        """), {"uid": user.id})).all()
    ]

    latency = (await db.execute(text("""
        SELECT percentile_cont(0.5)  WITHIN GROUP (ORDER BY a.duration_ms) AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms) AS p95,
               percentile_cont(0.99) WITHIN GROUP (ORDER BY a.duration_ms) AS p99,
               count(*) AS samples
        FROM job_attempts a
        JOIN jobs j ON j.id = a.job_id
        WHERE j.user_id = :uid AND a.duration_ms IS NOT NULL
          AND a.started_at > now() - interval '1 hour'
    """), {"uid": user.id})).one()

    success = (await db.execute(text("""
        SELECT count(*) FILTER (WHERE state = 'succeeded') AS ok, count(*) AS total
        FROM jobs
        WHERE user_id = :uid AND finished_at > now() - interval '1 hour'
          AND state IN ('succeeded', 'dead', 'canceled')
    """), {"uid": user.id})).one()

    workers = dict((await db.execute(text("SELECT state, count(*) FROM workers GROUP BY state"))).all())

    data = {
        "job_states": states,
        "throughput_per_minute": throughput,
        "exec_duration_ms": {"p50": latency.p50, "p95": latency.p95, "p99": latency.p99,
                             "samples": latency.samples},
        "success_rate_1h": (success.ok / success.total) if success.total else None,
        "workers": workers,
    }
    _cache[cache_key] = (time.monotonic(), data)
    return data
