import asyncio
import json

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from app.api.deps import get_current_user
from app.db.models import User
from app.engine.events import hub

router = APIRouter(tags=["stream"])


@router.get("/stream")
async def stream_events(user: User = Depends(get_current_user)):
    """Server-Sent Events feed of live job/worker events for the dashboard.
    EventSource can't set headers, so browsers authenticate with ?token=<jwt>."""
    queue = hub.subscribe(str(user.id))

    async def generate():
        try:
            yield ": connected\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"data: {json.dumps(event, default=str)}\n\n"
                except TimeoutError:
                    yield ": keepalive\n\n"  # comment frame keeps proxies from closing us
        finally:
            hub.unsubscribe(queue)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
