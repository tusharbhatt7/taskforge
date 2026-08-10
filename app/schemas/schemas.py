import uuid
from datetime import datetime
from typing import Any

from croniter import croniter
from pydantic import BaseModel, EmailStr, Field, HttpUrl, field_validator


# ---- auth ----
class RegisterIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class TokenOut(BaseModel):
    access_token: str
    token_type: str = "bearer"


class UserOut(BaseModel):
    id: uuid.UUID
    email: str
    webhook_secret: str
    created_at: datetime

    model_config = {"from_attributes": True}


# ---- api keys ----
class ApiKeyCreateIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)


class ApiKeyOut(BaseModel):
    id: uuid.UUID
    name: str
    prefix: str
    created_at: datetime
    last_used_at: datetime | None

    model_config = {"from_attributes": True}


class ApiKeyCreatedOut(ApiKeyOut):
    key: str  # plaintext, shown exactly once


# ---- jobs ----
class JobSubmitIn(BaseModel):
    type: str = Field(min_length=1, max_length=100)
    queue: str = Field(default="default", min_length=1, max_length=100, pattern=r"^[a-z0-9_\-]+$")
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=-100, le=100)
    run_at: datetime | None = None
    delay_seconds: int | None = Field(default=None, ge=0, le=86400 * 30)
    max_attempts: int = Field(default=3, ge=1, le=10)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=255)
    depends_on: list[uuid.UUID] = Field(default_factory=list, max_length=20)
    callback_url: HttpUrl | None = None


class JobOut(BaseModel):
    id: uuid.UUID
    queue: str
    type: str
    payload: dict[str, Any]
    state: str
    priority: int
    run_at: datetime
    attempts: int
    max_attempts: int
    idempotency_key: str | None
    leased_by: uuid.UUID | None
    callback_url: str | None
    result: dict[str, Any] | None
    error: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    model_config = {"from_attributes": True}


class AttemptOut(BaseModel):
    attempt_no: int
    worker_id: uuid.UUID | None
    outcome: str
    error: str | None
    started_at: datetime
    finished_at: datetime | None
    duration_ms: float | None

    model_config = {"from_attributes": True}


class TriageOut(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    fingerprint: str
    category: str
    is_transient: bool
    root_cause: str
    suggested_action: str
    confidence: float
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    reused_from_id: uuid.UUID | None
    created_at: datetime

    model_config = {"from_attributes": True}


class JobDetailOut(JobOut):
    depends_on: list[uuid.UUID] = []
    job_attempts: list[AttemptOut] = []
    triage: TriageOut | None = None


class JobListOut(BaseModel):
    items: list[JobOut]
    total: int
    limit: int
    offset: int


# ---- queues ----
class QueueOut(BaseModel):
    name: str
    paused: bool
    counts: dict[str, int]


# ---- workers ----
class WorkerOut(BaseModel):
    id: uuid.UUID
    hostname: str
    pid: int
    queues: list[str]
    concurrency: int
    state: str
    started_at: datetime
    last_heartbeat_at: datetime

    model_config = {"from_attributes": True}


# ---- schedules ----
class ScheduleIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    cron: str
    job_type: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = Field(default="default", pattern=r"^[a-z0-9_\-]+$")
    priority: int = Field(default=0, ge=-100, le=100)
    max_attempts: int = Field(default=3, ge=1, le=10)
    enabled: bool = True

    @field_validator("cron")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        if not croniter.is_valid(v):
            raise ValueError("invalid cron expression (expected 5-field crontab syntax, e.g. '*/5 * * * *')")
        return v


class ScheduleOut(BaseModel):
    id: uuid.UUID
    name: str
    cron: str
    job_type: str
    payload: dict[str, Any]
    queue: str
    priority: int
    max_attempts: int
    enabled: bool
    next_run_at: datetime
    last_run_at: datetime | None
    created_at: datetime

    model_config = {"from_attributes": True}


# ---- webhook deliveries ----
class WebhookDeliveryOut(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    url: str
    event: str
    state: str
    attempts: int
    response_status: int | None
    last_error: str | None
    created_at: datetime
    delivered_at: datetime | None

    model_config = {"from_attributes": True}
