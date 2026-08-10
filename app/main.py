import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.core.config import get_settings
from app.core.logging import request_id_var, setup_logging
from app.db.session import SessionLocal, engine
from app.engine.cron import run_due_schedules
from app.engine.events import hub
from app.engine.reaper import mark_dead_workers, reap_expired_leases
from app.engine.webhooks import dispatch_due_webhooks

logger = logging.getLogger("taskforge.api")
STATIC_DIR = Path(__file__).parent / "static"


def _loop(name: str, interval: float, fn: Callable[[], Awaitable]) -> asyncio.Task:
    """Run a maintenance coroutine forever, surviving individual failures."""

    async def runner():
        while True:
            try:
                await fn()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("%s loop iteration failed", name)
            await asyncio.sleep(interval)

    return asyncio.create_task(runner(), name=name)


async def _reap() -> None:
    async with SessionLocal() as session:
        await reap_expired_leases(session)
        await mark_dead_workers(session)


async def _cron() -> None:
    async with SessionLocal() as session:
        await run_due_schedules(session)


async def _webhooks() -> None:
    async with SessionLocal() as session:
        await dispatch_due_webhooks(session)


async def _keep_alive() -> None:
    # Render's free tier spins instances down after 15 idle minutes, which would pause
    # all background processing. A self-ping through the public URL counts as traffic.
    url = get_settings().render_external_url
    if url:
        async with httpx.AsyncClient(timeout=30) as client:
            await client.get(url.rstrip("/") + "/healthz")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    setup_logging()
    await hub.start()
    tasks = [
        _loop("reaper", settings.reaper_interval_seconds, _reap),
        _loop("cron", settings.cron_interval_seconds, _cron),
        _loop("webhooks", settings.webhook_interval_seconds, _webhooks),
        _loop("keep-alive", settings.keep_alive_minutes * 60, _keep_alive),
    ]
    logger.info("api server up (env=%s)", settings.env)
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*tasks)
        await hub.stop()
        await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Taskforge",
        version="1.0.0",
        description="Distributed job execution platform: Postgres-backed queues, worker leases, "
                    "retries with backoff, dead-letter queues, cron schedules, signed webhooks.",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
        request_id_var.set(request_id)
        start = time.perf_counter()
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        # The dashboard ships with the API, so a redeploy changes both at once. Without
        # this, browsers heuristically cache the old JS (StaticFiles sends ETag but no
        # Cache-Control) and run it against the new API. `no-cache` forces revalidation,
        # which the ETag then answers with a cheap 304.
        if request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-cache"
        if request.url.path.startswith("/api/") and request.url.path != "/api/v1/stream":
            logger.info("%s %s -> %s (%.1fms)", request.method, request.url.path,
                        response.status_code, (time.perf_counter() - start) * 1000)
        return response

    from app.api import api_keys, auth, jobs, metrics, queues, schedules, stream, workers

    for router in (auth.router, api_keys.router, jobs.router, queues.router,
                   schedules.router, workers.router, metrics.router, stream.router):
        app.include_router(router, prefix="/api/v1")

    @app.get("/healthz", include_in_schema=False)
    async def healthz():
        return {"status": "ok"}

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/app", include_in_schema=False)
    async def dashboard():
        return FileResponse(STATIC_DIR / "app.html")

    return app


app = create_app()
