import uuid
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.models import Job, JobAttempt, JobDep, JobState, Queue, User, WebhookDelivery
from app.db.session import get_db
from app.engine import events, states
from app.schemas.schemas import (
    JobDetailOut,
    JobListOut,
    JobOut,
    JobSubmitIn,
    WebhookDeliveryOut,
)
from app.worker.handlers import HANDLERS

router = APIRouter(tags=["jobs"])

VALID_STATES = {s.value for s in JobState}
# Starlette renamed its 422 constant; the literal avoids a deprecation warning on new
# versions and an AttributeError on old ones.
UNPROCESSABLE = 422


@router.post("/jobs", response_model=JobOut, status_code=status.HTTP_201_CREATED)
async def submit_job(body: JobSubmitIn, response: Response, user: User = Depends(get_current_user),
                     db: AsyncSession = Depends(get_db)):
    if body.type not in HANDLERS:
        raise HTTPException(
            UNPROCESSABLE,
            f"Unknown job type '{body.type}'. Available: {sorted(HANDLERS)}",
        )

    # Idempotent submission: same (user, idempotency_key) always returns the same job.
    if body.idempotency_key:
        if existing := await _find_by_idempotency_key(db, user.id, body.idempotency_key):
            response.status_code = status.HTTP_200_OK
            return existing

    now = datetime.now(UTC)
    run_at = now
    if body.delay_seconds:
        run_at = now + timedelta(seconds=body.delay_seconds)
    elif body.run_at:
        aware = body.run_at if body.run_at.tzinfo else body.run_at.replace(tzinfo=UTC)
        run_at = max(aware, now)

    depends_on = list(dict.fromkeys(body.depends_on))
    if depends_on:
        owned = (await db.execute(
            select(func.count()).select_from(Job).where(Job.id.in_(depends_on), Job.user_id == user.id)
        )).scalar_one()
        if owned != len(depends_on):
            raise HTTPException(UNPROCESSABLE,
                                "One or more depends_on jobs do not exist.")

    state = await states.resolve_initial_state(db, depends_on)
    job = Job(
        user_id=user.id, queue=body.queue, type=body.type, payload=body.payload,
        priority=body.priority, run_at=run_at, max_attempts=body.max_attempts,
        idempotency_key=body.idempotency_key, state=state,
        callback_url=str(body.callback_url) if body.callback_url else None,
    )
    if state == JobState.CANCELED.value:
        job.error = "canceled at submission: a depends_on parent already failed"
        job.finished_at = now
    db.add(job)

    try:
        await db.flush()
        for parent_id in depends_on:
            db.add(JobDep(job_id=job.id, parent_id=parent_id))
        await db.execute(
            pg_insert(Queue).values(user_id=user.id, name=body.queue)
            .on_conflict_do_nothing(constraint="uq_queue_user_name")
        )
        await events.emit(db, "job.created", user.id, job_id=job.id, queue=job.queue,
                          job_type=job.type, state=job.state)
        await db.commit()
    except IntegrityError:
        # Two racing submissions with the same idempotency key: the unique partial index
        # rejects the loser, which then returns the winner's row.
        await db.rollback()
        if body.idempotency_key:
            if existing := await _find_by_idempotency_key(db, user.id, body.idempotency_key):
                response.status_code = status.HTTP_200_OK
                return existing
        raise

    await db.refresh(job)
    return job


@router.get("/jobs", response_model=JobListOut)
async def list_jobs(
    state: str | None = Query(default=None),
    queue: str | None = Query(default=None),
    type: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    if state is not None and state not in VALID_STATES:
        raise HTTPException(UNPROCESSABLE,
                            f"Invalid state. Valid: {sorted(VALID_STATES)}")
    filters = [Job.user_id == user.id]
    if state:
        filters.append(Job.state == state)
    if queue:
        filters.append(Job.queue == queue)
    if type:
        filters.append(Job.type == type)

    total = (await db.execute(select(func.count()).select_from(Job).where(*filters))).scalar_one()
    items = (await db.execute(
        select(Job).where(*filters).order_by(Job.created_at.desc()).limit(limit).offset(offset)
    )).scalars().all()
    return JobListOut(items=items, total=total, limit=limit, offset=offset)


@router.get("/jobs/{job_id}", response_model=JobDetailOut)
async def get_job(job_id: uuid.UUID, user: User = Depends(get_current_user),
                  db: AsyncSession = Depends(get_db)):
    job = await _owned_job(db, user, job_id)
    attempts = (await db.execute(
        select(JobAttempt).where(JobAttempt.job_id == job.id).order_by(JobAttempt.attempt_no)
    )).scalars().all()
    parents = (await db.execute(
        select(JobDep.parent_id).where(JobDep.job_id == job.id)
    )).scalars().all()
    detail = JobDetailOut.model_validate(job)
    detail.job_attempts = attempts
    detail.depends_on = list(parents)
    return detail


@router.post("/jobs/{job_id}/cancel", response_model=JobOut)
async def cancel_job(job_id: uuid.UUID, user: User = Depends(get_current_user),
                     db: AsyncSession = Depends(get_db)):
    # Lock the row so we can't race a worker claiming it out from under us.
    job = (await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == user.id).with_for_update()
    )).scalar_one_or_none()
    if not job:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Job not found.")
    if job.state not in (JobState.PENDING.value, JobState.QUEUED.value):
        raise HTTPException(status.HTTP_409_CONFLICT,
                            f"Only pending/queued jobs can be canceled (job is '{job.state}').")
    await states.cancel_job(db, job, "canceled by user")
    await db.commit()
    await db.refresh(job)
    return job


@router.post("/jobs/{job_id}/retry", response_model=JobOut)
async def retry_job(job_id: uuid.UUID, user: User = Depends(get_current_user),
                    db: AsyncSession = Depends(get_db)):
    """Requeue a dead-lettered or canceled job with a fresh retry budget."""
    job = (await db.execute(
        select(Job).where(Job.id == job_id, Job.user_id == user.id).with_for_update()
    )).scalar_one_or_none()
    if not job:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Job not found.")
    if job.state not in (JobState.DEAD.value, JobState.CANCELED.value):
        raise HTTPException(status.HTTP_409_CONFLICT,
                            f"Only dead/canceled jobs can be retried (job is '{job.state}').")
    job.state = JobState.QUEUED.value
    job.attempts = 0
    job.error = None
    job.result = None
    job.run_at = datetime.now(UTC)
    job.finished_at = None
    await events.emit(db, "job.queued", user.id, job_id=job.id, queue=job.queue,
                      job_type=job.type, requeued=True)
    await db.commit()
    await db.refresh(job)
    return job


@router.get("/webhook-deliveries", response_model=list[WebhookDeliveryOut])
async def list_webhook_deliveries(limit: int = Query(default=50, ge=1, le=200),
                                  user: User = Depends(get_current_user),
                                  db: AsyncSession = Depends(get_db)):
    rows = await db.execute(
        select(WebhookDelivery).where(WebhookDelivery.user_id == user.id)
        .order_by(WebhookDelivery.created_at.desc()).limit(limit)
    )
    return list(rows.scalars())


async def _owned_job(db: AsyncSession, user: User, job_id: uuid.UUID) -> Job:
    job = await db.get(Job, job_id)
    if not job or job.user_id != user.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Job not found.")
    return job


async def _find_by_idempotency_key(db: AsyncSession, user_id: uuid.UUID, key: str) -> Job | None:
    return (await db.execute(
        select(Job).where(Job.user_id == user_id, Job.idempotency_key == key)
    )).scalar_one_or_none()
