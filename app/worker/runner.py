import asyncio
import contextlib
import logging
import os
import random
import signal
import socket
import uuid
from datetime import UTC, datetime

from sqlalchemy import text

from app.core.config import get_settings
from app.db.models import Job, Worker, WorkerState
from app.db.session import SessionLocal, engine
from app.engine import events, states
from app.engine.claim import claim_jobs, renew_leases
from app.engine.errors import PermanentError, RetryAfterError
from app.worker.handlers import HANDLERS, JobContext

logger = logging.getLogger("taskforge.worker")

JOB_TIMEOUT_SECONDS = 120
IDLE_POLL_SECONDS = 1.0


class WorkerRunner:
    def __init__(self, queues: list[str] | None = None, concurrency: int | None = None):
        self.settings = get_settings()
        self.id = uuid.uuid4()
        self.queues = queues
        self.concurrency = concurrency or self.settings.worker_concurrency
        self.in_flight: dict[uuid.UUID, asyncio.Task] = {}
        self.stopping = asyncio.Event()

    async def run(self) -> None:
        await self._register()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stopping.set)

        heartbeat = asyncio.create_task(self._heartbeat_forever(), name="heartbeat")
        logger.info("worker online: id=%s queues=%s concurrency=%d",
                    self.id, self.queues or "all", self.concurrency)
        try:
            await self._claim_forever()
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat
            await self._shutdown()

    async def _claim_forever(self) -> None:
        while not self.stopping.is_set():
            free_slots = self.concurrency - len(self.in_flight)
            claimed = []
            if free_slots > 0:
                async with SessionLocal() as session:
                    claimed = await claim_jobs(
                        session, self.id,
                        batch=min(free_slots, self.settings.claim_batch_size),
                        lease_seconds=self.settings.lease_seconds,
                        queues=self.queues,
                    )
                for job in claimed:
                    task = asyncio.create_task(self._execute(job.id, job.type, job.payload,
                                                             job.attempts, job.max_attempts))
                    self.in_flight[job.id] = task
                    task.add_done_callback(lambda _t, jid=job.id: self.in_flight.pop(jid, None))
            if not claimed:
                # Idle poll with jitter so a fleet of workers doesn't hammer in lockstep.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.stopping.wait(),
                                           timeout=IDLE_POLL_SECONDS * random.uniform(0.7, 1.3))

    async def _execute(self, job_id: uuid.UUID, job_type: str, payload: dict,
                       attempt: int, max_attempts: int) -> None:
        ctx = JobContext(job_id=job_id, attempt=attempt, max_attempts=max_attempts)
        error: str | None = None
        result: dict | None = None
        permanent = False
        retry_after: float | None = None
        try:
            handler = HANDLERS.get(job_type)
            if handler is None:
                # An unregistered type can never succeed, however many times we try.
                raise PermanentError(f"no handler registered for job type '{job_type}'")
            result = await asyncio.wait_for(handler(payload, ctx), timeout=JOB_TIMEOUT_SECONDS)
        except TimeoutError:
            error = f"job timed out after {JOB_TIMEOUT_SECONDS}s"
        except PermanentError as exc:
            error = f"{type(exc).__name__}: {exc}"
            permanent = True
        except RetryAfterError as exc:
            error = f"{type(exc).__name__}: {exc}"
            retry_after = exc.retry_after
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"

        async with SessionLocal() as session:
            job = await session.get(Job, job_id, with_for_update=True)
            if job is None or job.state != "running" or job.leased_by != self.id:
                # The reaper (or a cancel) took this job away — e.g. our lease expired
                # during a stall. The other side owns the state now; don't fight it.
                logger.warning("job %s no longer ours, dropping result", job_id)
                await session.rollback()
                return
            if error is None:
                await states.complete_job(session, job, result, worker_id=self.id)
                logger.info("job succeeded", extra={"job_id": str(job_id)})
            else:
                await states.fail_job(session, job, error, worker_id=self.id,
                                      permanent=permanent, retry_delay_override=retry_after)
                logger.warning("job attempt failed%s: %s", " (permanent)" if permanent else "",
                               error, extra={"job_id": str(job_id)})
            await session.commit()

    async def _heartbeat_forever(self) -> None:
        while True:
            await asyncio.sleep(self.settings.heartbeat_seconds)
            try:
                async with SessionLocal() as session:
                    row = (await session.execute(
                        text("""
                            UPDATE workers SET last_heartbeat_at = now(), state = :online
                            WHERE id = :id
                            RETURNING kill_requested
                        """),
                        {"id": self.id, "online": WorkerState.ONLINE.value},
                    )).one_or_none()
                    await renew_leases(session, self.id, list(self.in_flight),
                                       self.settings.lease_seconds)
                    await session.commit()
                    if row and row.kill_requested:
                        logger.warning("chaos kill requested — dying WITHOUT cleanup, pid=%d",
                                       os.getpid())
                        # Simulate a hard crash: no lease release, no state updates.
                        # The reaper must recover our in-flight jobs. That's the demo.
                        os._exit(137)
            except Exception:
                logger.exception("heartbeat failed (will retry)")

    async def _register(self) -> None:
        async with SessionLocal() as session:
            session.add(Worker(
                id=self.id, hostname=socket.gethostname(), pid=os.getpid(),
                queues=self.queues or [], concurrency=self.concurrency,
                state=WorkerState.ONLINE.value,
            ))
            await events.emit(session, "worker.online", None, worker_id=self.id,
                              hostname=socket.gethostname(), pid=os.getpid())
            await session.commit()

    async def _shutdown(self) -> None:
        """Graceful drain: no new claims, finish in-flight jobs, then deregister."""
        if self.in_flight:
            logger.info("draining %d in-flight job(s)...", len(self.in_flight))
            done, pending = await asyncio.wait(list(self.in_flight.values()), timeout=30)
            for task in pending:
                task.cancel()
        async with SessionLocal() as session:
            await session.execute(
                text("UPDATE workers SET state = :s, last_heartbeat_at = :t WHERE id = :id"),
                {"s": WorkerState.STOPPED.value, "t": datetime.now(UTC), "id": self.id},
            )
            await events.emit(session, "worker.stopped", None, worker_id=self.id)
            await session.commit()
        await engine.dispose()
        logger.info("worker stopped cleanly")
