from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.models import Job, Queue, User
from app.db.session import get_db
from app.engine import events
from app.schemas.schemas import QueueOut

router = APIRouter(prefix="/queues", tags=["queues"])


@router.get("", response_model=list[QueueOut])
async def list_queues(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    queues = (await db.execute(
        select(Queue).where(Queue.user_id == user.id).order_by(Queue.name)
    )).scalars().all()
    counts = (await db.execute(
        select(Job.queue, Job.state, func.count())
        .where(Job.user_id == user.id)
        .group_by(Job.queue, Job.state)
    )).all()
    by_queue: dict[str, dict[str, int]] = {}
    for queue_name, state, count in counts:
        by_queue.setdefault(queue_name, {})[state] = count
    return [QueueOut(name=q.name, paused=q.paused, counts=by_queue.get(q.name, {})) for q in queues]


@router.post("/{name}/pause", response_model=QueueOut)
async def pause_queue(name: str, user: User = Depends(get_current_user),
                      db: AsyncSession = Depends(get_db)):
    return await _set_paused(db, user, name, True)


@router.post("/{name}/resume", response_model=QueueOut)
async def resume_queue(name: str, user: User = Depends(get_current_user),
                       db: AsyncSession = Depends(get_db)):
    return await _set_paused(db, user, name, False)


async def _set_paused(db: AsyncSession, user: User, name: str, paused: bool) -> QueueOut:
    queue = (await db.execute(
        select(Queue).where(Queue.user_id == user.id, Queue.name == name)
    )).scalar_one_or_none()
    if not queue:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Queue not found.")
    queue.paused = paused
    await events.emit(db, "queue.paused" if paused else "queue.resumed", user.id, queue=name)
    await db.commit()
    return QueueOut(name=queue.name, paused=queue.paused, counts={})
