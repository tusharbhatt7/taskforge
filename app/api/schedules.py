import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.models import Schedule, User
from app.db.session import get_db
from app.engine.cron import next_fire
from app.schemas.schemas import ScheduleIn, ScheduleOut
from app.worker.handlers import public_types

router = APIRouter(prefix="/schedules", tags=["schedules"])

UNPROCESSABLE = 422


@router.post("", response_model=ScheduleOut, status_code=status.HTTP_201_CREATED)
async def create_schedule(body: ScheduleIn, user: User = Depends(get_current_user),
                          db: AsyncSession = Depends(get_db)):
    _validate_type(body.job_type)
    sched = Schedule(
        user_id=user.id, name=body.name, cron=body.cron, job_type=body.job_type,
        payload=body.payload, queue=body.queue, priority=body.priority,
        max_attempts=body.max_attempts, enabled=body.enabled,
        next_run_at=next_fire(body.cron, datetime.now(UTC)),
    )
    db.add(sched)
    await db.commit()
    await db.refresh(sched)
    return sched


@router.get("", response_model=list[ScheduleOut])
async def list_schedules(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    rows = await db.execute(
        select(Schedule).where(Schedule.user_id == user.id).order_by(Schedule.created_at)
    )
    return list(rows.scalars())


@router.put("/{schedule_id}", response_model=ScheduleOut)
async def update_schedule(schedule_id: uuid.UUID, body: ScheduleIn,
                          user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    _validate_type(body.job_type)
    sched = await _owned(db, user, schedule_id)
    cron_changed = sched.cron != body.cron
    for field in ("name", "cron", "job_type", "payload", "queue", "priority", "max_attempts", "enabled"):
        setattr(sched, field, getattr(body, field))
    if cron_changed:
        sched.next_run_at = next_fire(body.cron, datetime.now(UTC))
    await db.commit()
    await db.refresh(sched)
    return sched


@router.delete("/{schedule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_schedule(schedule_id: uuid.UUID, user: User = Depends(get_current_user),
                          db: AsyncSession = Depends(get_db)):
    sched = await _owned(db, user, schedule_id)
    await db.delete(sched)
    await db.commit()


def _validate_type(job_type: str) -> None:
    if job_type not in public_types():
        raise HTTPException(UNPROCESSABLE,
                            f"Unknown job type '{job_type}'. Available: {public_types()}")


async def _owned(db: AsyncSession, user: User, schedule_id: uuid.UUID) -> Schedule:
    sched = await db.get(Schedule, schedule_id)
    if not sched or sched.user_id != user.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Schedule not found.")
    return sched
