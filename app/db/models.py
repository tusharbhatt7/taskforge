import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


def _uuid_pk() -> Mapped[uuid.UUID]:
    return mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


def _tstz(**kw) -> Mapped[datetime]:
    return mapped_column(DateTime(timezone=True), **kw)


class JobState(enum.StrEnum):
    PENDING = "pending"  # waiting on depends_on parents
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD = "dead"  # retries exhausted -> dead-letter queue
    CANCELED = "canceled"


class AttemptOutcome(enum.StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    LOST = "lost"  # lease expired: worker died mid-execution


class WorkerState(enum.StrEnum):
    ONLINE = "online"
    STOPPED = "stopped"  # graceful shutdown
    DEAD = "dead"  # heartbeats went silent


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = _uuid_pk()
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    webhook_secret: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = _tstz(server_default=func.now())


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    prefix: Mapped[str] = mapped_column(String(12), nullable=False)
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    created_at: Mapped[datetime] = _tstz(server_default=func.now())
    last_used_at: Mapped[datetime | None] = _tstz(nullable=True)


class Queue(Base):
    __tablename__ = "queues"
    __table_args__ = (UniqueConstraint("user_id", "name", name="uq_queue_user_name"),)

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    paused: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    created_at: Mapped[datetime] = _tstz(server_default=func.now())


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        # Backbone of the claim query: only 'queued' rows are ever scanned, so a partial
        # index keeps it small and hot no matter how much terminal-state history accrues.
        Index(
            "ix_jobs_claim",
            "queue",
            text("priority DESC"),
            "run_at",
            postgresql_where=text("state = 'queued'"),
        ),
        # Idempotent submission: one live job per (user, idempotency_key).
        Index(
            "uq_jobs_idempotency",
            "user_id",
            "idempotency_key",
            unique=True,
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
        Index("ix_jobs_user_created", "user_id", text("created_at DESC")),
        Index("ix_jobs_lease", "lease_expires_at", postgresql_where=text("state = 'running'")),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    queue: Mapped[str] = mapped_column(String(100), nullable=False, server_default="default")
    type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    state: Mapped[str] = mapped_column(String(20), nullable=False, server_default=JobState.QUEUED.value)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    run_at: Mapped[datetime] = _tstz(nullable=False, server_default=func.now())
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("3"))
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    leased_by: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = _tstz(nullable=True)
    callback_url: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = _tstz(server_default=func.now())
    started_at: Mapped[datetime | None] = _tstz(nullable=True)
    finished_at: Mapped[datetime | None] = _tstz(nullable=True)


class JobDep(Base):
    __tablename__ = "job_deps"

    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True
    )
    parent_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), primary_key=True, index=True
    )


class JobAttempt(Base):
    __tablename__ = "job_attempts"
    __table_args__ = (Index("ix_attempts_job", "job_id", "attempt_no"),)

    id: Mapped[uuid.UUID] = _uuid_pk()
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    outcome: Mapped[str] = mapped_column(String(20), nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = _tstz(server_default=func.now())
    finished_at: Mapped[datetime | None] = _tstz(nullable=True)
    duration_ms: Mapped[float | None] = mapped_column(Float, nullable=True)


class Worker(Base):
    __tablename__ = "workers"

    id: Mapped[uuid.UUID] = _uuid_pk()
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)
    queues: Mapped[list] = mapped_column(JSONB, nullable=False, server_default=text("'[]'::jsonb"))
    concurrency: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("4"))
    state: Mapped[str] = mapped_column(String(20), nullable=False, server_default=WorkerState.ONLINE.value)
    kill_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    started_at: Mapped[datetime] = _tstz(server_default=func.now())
    last_heartbeat_at: Mapped[datetime] = _tstz(server_default=func.now())


class Schedule(Base):
    __tablename__ = "schedules"

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    cron: Mapped[str] = mapped_column(String(100), nullable=False)
    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    queue: Mapped[str] = mapped_column(String(100), nullable=False, server_default="default")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("3"))
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    next_run_at: Mapped[datetime] = _tstz(nullable=False)
    last_run_at: Mapped[datetime | None] = _tstz(nullable=True)
    created_at: Mapped[datetime] = _tstz(server_default=func.now())


class WebhookDelivery(Base):
    __tablename__ = "webhook_deliveries"
    __table_args__ = (
        Index("ix_webhooks_due", "next_attempt_at", postgresql_where=text("state = 'pending'")),
    )

    id: Mapped[uuid.UUID] = _uuid_pk()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    url: Mapped[str] = mapped_column(String(2000), nullable=False)
    event: Mapped[str] = mapped_column(String(50), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(String(20), nullable=False, server_default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    next_attempt_at: Mapped[datetime] = _tstz(nullable=False, server_default=func.now())
    response_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = _tstz(server_default=func.now())
    delivered_at: Mapped[datetime | None] = _tstz(nullable=True)
