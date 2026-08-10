"""Realtime event fan-out.

Workers and the API server are separate OS processes, so in-process pub/sub can't
carry job events to dashboard SSE connections. Postgres LISTEN/NOTIFY bridges them:
every state change calls pg_notify() inside the same transaction that made the change,
and the API server holds one LISTEN connection that fans events out to SSE subscribers.
"""

import asyncio
import contextlib
import json
import logging
import uuid
from typing import Any

import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings

logger = logging.getLogger("taskforge.events")

CHANNEL = "taskforge_events"


async def emit(session: AsyncSession, event_type: str, user_id: uuid.UUID | None = None,
               **data: Any) -> None:
    """Publish an event via pg_notify, transactionally with the change that caused it."""
    payload = json.dumps(
        {"type": event_type, "user_id": str(user_id) if user_id else None, **data}, default=str)
    await session.execute(text("SELECT pg_notify(:ch, :payload)"), {"ch": CHANNEL, "payload": payload})


class EventHub:
    """Fans NOTIFY payloads out to per-connection asyncio queues (one per SSE client)."""

    def __init__(self) -> None:
        self._subscribers: dict[asyncio.Queue, str | None] = {}
        self._listen_task: asyncio.Task | None = None

    def subscribe(self, user_id: str | None) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._subscribers[q] = user_id
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.pop(q, None)

    def publish(self, raw: str) -> None:
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            return
        for q, user_id in list(self._subscribers.items()):
            # Tenancy filter: user-scoped events go only to their owner; platform-level
            # events (workers coming online/dying) go to everyone.
            if event.get("user_id") is None or event["user_id"] == user_id:
                with contextlib.suppress(asyncio.QueueFull):
                    q.put_nowait(event)

    async def start(self) -> None:
        self._listen_task = asyncio.create_task(self._listen_forever())

    async def stop(self) -> None:
        if self._listen_task:
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task

    async def _listen_forever(self) -> None:
        """Hold a dedicated LISTEN connection; reconnect with backoff if it drops."""
        delay = 1.0
        while True:
            try:
                conn = await asyncpg.connect(get_settings().asyncpg_dsn)
                await conn.add_listener(CHANNEL, lambda *args: self.publish(args[3]))
                logger.info("event hub listening on %s", CHANNEL)
                delay = 1.0
                try:
                    while True:
                        await asyncio.sleep(5)
                        await conn.execute("SELECT 1")  # detect dead connections
                finally:
                    await conn.close()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("event listener disconnected (%s); retrying in %.0fs", exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)


hub = EventHub()
