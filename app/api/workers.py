import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.db.models import User, Worker, WorkerState
from app.db.session import get_db
from app.engine import events
from app.schemas.schemas import WorkerOut

router = APIRouter(prefix="/workers", tags=["workers"])


@router.get("", response_model=list[WorkerOut])
async def list_workers(db: AsyncSession = Depends(get_db), _: User = Depends(get_current_user)):
    rows = await db.execute(select(Worker).order_by(Worker.started_at.desc()).limit(50))
    return list(rows.scalars())


@router.post("/chaos/kill", response_model=WorkerOut)
async def chaos_kill_worker(worker_id: uuid.UUID | None = None,
                            user: User = Depends(get_current_user),
                            db: AsyncSession = Depends(get_db)):
    """Chaos engineering, demo-sized: flag a worker for a hard crash (os._exit, no
    cleanup). Its in-flight jobs keep their leases until the reaper reclaims them —
    which is exactly the recovery path this platform exists to demonstrate."""
    if worker_id:
        worker = await db.get(Worker, worker_id)
        if not worker or worker.state != WorkerState.ONLINE.value:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "No such online worker.")
    else:
        worker = (await db.execute(
            select(Worker).where(Worker.state == WorkerState.ONLINE.value).order_by(func.random()).limit(1)
        )).scalar_one_or_none()
        if not worker:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "No online workers to kill.")
    worker.kill_requested = True
    await events.emit(db, "worker.kill_requested", None, worker_id=worker.id,
                      hostname=worker.hostname, pid=worker.pid, by=user.email)
    await db.commit()
    await db.refresh(worker)
    return worker
