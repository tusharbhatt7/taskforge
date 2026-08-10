import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.security import generate_api_key
from app.db.models import ApiKey, User
from app.db.session import get_db
from app.schemas.schemas import ApiKeyCreatedOut, ApiKeyCreateIn, ApiKeyOut

router = APIRouter(prefix="/api-keys", tags=["api-keys"])


@router.post("", response_model=ApiKeyCreatedOut, status_code=status.HTTP_201_CREATED)
async def create_api_key(body: ApiKeyCreateIn, user: User = Depends(get_current_user),
                         db: AsyncSession = Depends(get_db)):
    plaintext, key_hash, prefix = generate_api_key()
    row = ApiKey(user_id=user.id, name=body.name, prefix=prefix, key_hash=key_hash)
    db.add(row)
    await db.commit()
    await db.refresh(row)
    # The plaintext key is returned exactly once; only its SHA-256 hash is stored.
    return ApiKeyCreatedOut(id=row.id, name=row.name, prefix=row.prefix, created_at=row.created_at,
                            last_used_at=None, key=plaintext)


@router.get("", response_model=list[ApiKeyOut])
async def list_api_keys(user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    rows = await db.execute(select(ApiKey).where(ApiKey.user_id == user.id).order_by(ApiKey.created_at))
    return list(rows.scalars())


@router.delete("/{key_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_api_key(key_id: uuid.UUID, user: User = Depends(get_current_user),
                         db: AsyncSession = Depends(get_db)):
    row = await db.get(ApiKey, key_id)
    if not row or row.user_id != user.id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "API key not found.")
    await db.delete(row)
    await db.commit()
