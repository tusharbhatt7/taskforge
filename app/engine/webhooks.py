"""Completion webhooks: signed, retried, fully logged.

Deliveries are rows, not fire-and-forget tasks — enqueued transactionally with the job's
terminal state, then drained by this dispatcher with per-delivery retry state. Consumers
verify authenticity via the X-Taskforge-Signature HMAC computed with their account's
webhook secret.
"""

import json
import logging
from datetime import UTC, datetime, timedelta

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import sign_webhook
from app.engine.retry import backoff_seconds

logger = logging.getLogger("taskforge.webhooks")

MAX_WEBHOOK_ATTEMPTS = 3


async def dispatch_due_webhooks(session: AsyncSession, batch: int = 20) -> int:
    rows = (await session.execute(
        text("""
            SELECT wd.id, wd.url, wd.event, wd.payload, wd.attempts, u.webhook_secret
            FROM webhook_deliveries wd
            JOIN users u ON u.id = wd.user_id
            WHERE wd.state = 'pending' AND wd.next_attempt_at <= now()
            ORDER BY wd.next_attempt_at
            LIMIT :batch
            FOR UPDATE OF wd SKIP LOCKED
        """),
        {"batch": batch},
    )).all()

    async with httpx.AsyncClient(timeout=10, follow_redirects=False) as client:
        for row in rows:
            body = json.dumps(row.payload, default=str).encode()
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Taskforge-Webhooks/1.0",
                "X-Taskforge-Event": row.event,
                "X-Taskforge-Delivery": str(row.id),
                "X-Taskforge-Signature": sign_webhook(row.webhook_secret, body),
            }
            status_code, error = None, None
            try:
                resp = await client.post(row.url, content=body, headers=headers)
                status_code = resp.status_code
                if 200 <= status_code < 300:
                    await _mark(session, row.id, "delivered", row.attempts + 1, status_code, None)
                    continue
                error = f"HTTP {status_code}"
            except httpx.HTTPError as exc:
                error = f"{type(exc).__name__}: {exc}"

            attempts = row.attempts + 1
            if attempts >= MAX_WEBHOOK_ATTEMPTS:
                await _mark(session, row.id, "failed", attempts, status_code, error)
                logger.warning("webhook permanently failed: %s (%s)", row.url, error)
            else:
                await _retry(session, row.id, attempts, status_code, error)
    await session.commit()
    return len(rows)


async def _mark(session: AsyncSession, delivery_id, state: str, attempts: int,
                status_code: int | None, error: str | None) -> None:
    # delivered_at is computed here rather than with a CASE over :state — binding the same
    # parameter to both a varchar column and a text comparison makes asyncpg fail to
    # deduce its type, which previously aborted the transaction and left the delivery
    # 'pending' forever (an infinite redelivery loop).
    await session.execute(
        text("""
            UPDATE webhook_deliveries
            SET state = :state, attempts = :attempts, response_status = :status,
                last_error = :error, delivered_at = :delivered_at
            WHERE id = :id
        """),
        {"id": delivery_id, "state": state, "attempts": attempts, "status": status_code,
         "error": error, "delivered_at": datetime.now(UTC) if state == "delivered" else None},
    )


async def _retry(session: AsyncSession, delivery_id, attempts: int,
                 status_code: int | None, error: str | None) -> None:
    next_at = datetime.now(UTC) + timedelta(seconds=backoff_seconds(attempts, base=10, cap=600))
    await session.execute(
        text("""
            UPDATE webhook_deliveries
            SET attempts = :attempts, response_status = :status, last_error = :error,
                next_attempt_at = :next_at
            WHERE id = :id
        """),
        {"id": delivery_id, "attempts": attempts, "status": status_code, "error": error, "next_at": next_at},
    )
