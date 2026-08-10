"""AI triage of dead-lettered jobs.

Cost control is the interesting part. A single broken dependency can dead-letter hundreds
of jobs that all failed the same way; triaging each one individually would be hundreds of
identical API calls for one piece of information. Errors are therefore fingerprinted —
volatile details (ids, numbers, timestamps, quoted strings) normalized out — and a job
whose fingerprint has already been explained reuses that analysis instead of paying for
it again.
"""

import hashlib
import json
import logging
import re
import uuid

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.ai.client import AIClient, Usage
from app.ai.prompts import TRIAGE_SCHEMA, TRIAGE_SYSTEM
from app.db.models import Job, JobAttempt, JobTriage
from app.engine import events
from app.engine.errors import PermanentError

logger = logging.getLogger("taskforge.ai.triage")

MAX_PAYLOAD_CHARS = 2000
MAX_ERROR_CHARS = 1200

# Substitutions that collapse "same failure, different instance" into one fingerprint.
_NORMALIZERS = [
    (re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I), "<uuid>"),
    (re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\S*"), "<ts>"),
    (re.compile(r"\b0x[0-9a-f]+\b", re.I), "<hex>"),
    (re.compile(r"'[^']*'"), "'<v>'"),
    (re.compile(r'"[^"]*"'), '"<v>"'),
    (re.compile(r"\b\d+(\.\d+)?\b"), "<n>"),
    (re.compile(r"\s+"), " "),
]


def fingerprint(job_type: str, error: str) -> str:
    """Stable id for a *class* of failure, ignoring per-instance detail."""
    normalized = error or ""
    for pattern, replacement in _NORMALIZERS:
        normalized = pattern.sub(replacement, normalized)
    digest = hashlib.sha256(f"{job_type}|{normalized.strip().lower()}".encode()).hexdigest()
    return digest[:16]


def build_prompt(job: Job, attempts: list[JobAttempt]) -> str:
    """Render the evidence. Payload and errors are truncated: an enormous payload adds
    cost without adding diagnostic signal, and blows the cache prefix."""
    payload = json.dumps(job.payload, default=str)[:MAX_PAYLOAD_CHARS]
    lines = [
        f"Job type: {job.type}",
        f"Queue: {job.queue}",
        f"Attempts made: {job.attempts} of {job.max_attempts} allowed",
        f"Payload: {payload}",
        "",
        "Attempt history (oldest first):",
    ]
    for attempt in attempts:
        duration = f"{attempt.duration_ms:.0f}ms" if attempt.duration_ms else "unknown duration"
        error = (attempt.error or "(no error recorded)")[:MAX_ERROR_CHARS]
        lines.append(f"  attempt {attempt.attempt_no} — outcome={attempt.outcome}, "
                     f"{duration}: {error}")
    if not attempts:
        lines.append(f"  (no attempt rows; final error: {(job.error or 'unknown')[:MAX_ERROR_CHARS]})")
    return "\n".join(lines)


async def triage_job(session: AsyncSession, target_job_id: uuid.UUID) -> dict:
    """Analyse one dead-lettered job. Returns the handler result for the triage job."""
    job = await session.get(Job, target_job_id)
    if job is None:
        # The job was deleted between dead-lettering and triage running. Nothing to
        # analyse and nothing a retry would recover.
        raise PermanentError(f"target job {target_job_id} no longer exists")

    existing = (await session.execute(
        select(JobTriage).where(JobTriage.job_id == job.id)
    )).scalar_one_or_none()
    if existing:
        return {"status": "already_triaged", "triage_id": str(existing.id)}

    attempts = (await session.execute(
        select(JobAttempt).where(JobAttempt.job_id == job.id).order_by(JobAttempt.attempt_no)
    )).scalars().all()

    print_id = fingerprint(job.type, job.error or "")

    # Serialize triage of one fingerprint across all workers.
    #
    # Without this the dedupe below is a read-then-write race: a dependency outage
    # dead-letters many jobs at once, their triage jobs get claimed in the same batch,
    # every one sees no prior analysis (none has committed yet), and every one pays for
    # the same answer — the exact cost the fingerprint exists to avoid.
    #
    # The lock is transaction-scoped, so it releases on commit and the waiters then find
    # the row the winner wrote. Note this is the opposite policy to job claiming, which
    # uses SKIP LOCKED to *avoid* waiting: there, contention means another worker has the
    # job and we should move on; here, contention means the answer is already being
    # computed and waiting is precisely what saves the call.
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtext(:uid), hashtext(:fp))"),
        {"uid": str(job.user_id), "fp": print_id},
    )

    prior = (await session.execute(
        select(JobTriage)
        .where(JobTriage.user_id == job.user_id,
               JobTriage.fingerprint == print_id,
               JobTriage.reused_from_id.is_(None))
        .order_by(JobTriage.created_at.desc())
        .limit(1)
    )).scalar_one_or_none()

    if prior is not None:
        logger.info("reusing triage for fingerprint %s", print_id, extra={"job_id": str(job.id)})
        record = JobTriage(
            job_id=job.id, user_id=job.user_id, fingerprint=print_id,
            category=prior.category, is_transient=prior.is_transient,
            root_cause=prior.root_cause, suggested_action=prior.suggested_action,
            confidence=prior.confidence, model=prior.model,
            input_tokens=0, output_tokens=0, cost_usd=0.0,
            reused_from_id=prior.id,
        )
        usage = Usage()
        reused = True
    else:
        client = AIClient()
        completion = await client.complete_json(
            system=TRIAGE_SYSTEM,
            user=build_prompt(job, attempts),
            schema=TRIAGE_SCHEMA,
        )
        data = completion.data
        usage = completion.usage
        record = JobTriage(
            job_id=job.id, user_id=job.user_id, fingerprint=print_id,
            category=data["category"],
            is_transient=bool(data["is_transient"]),
            root_cause=data["root_cause"],
            suggested_action=data["suggested_action"],
            # The schema can't express a numeric range, so clamp here.
            confidence=min(1.0, max(0.0, float(data["confidence"]))),
            model=usage.model,
            input_tokens=usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens,
            output_tokens=usage.output_tokens,
            cost_usd=usage.cost_usd,
            reused_from_id=None,
        )
        reused = False

    session.add(record)
    await session.flush()
    await events.emit(session, "job.triaged", job.user_id, job_id=job.id, job_type=job.type,
                      category=record.category, is_transient=record.is_transient,
                      confidence=record.confidence, reused=reused)

    return {
        "target_job_id": str(job.id),
        "fingerprint": print_id,
        "category": record.category,
        "is_transient": record.is_transient,
        "confidence": record.confidence,
        "reused_prior_analysis": reused,
        "usage": usage.as_dict(),
    }
