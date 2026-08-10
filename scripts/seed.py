"""Seed a demo account, an API key, a cron schedule and a starter workload.

    uv run python -m scripts.seed
"""

import asyncio
import sys
from datetime import UTC, datetime

from sqlalchemy import select

from app.core.config import get_settings
from app.core.logging import setup_logging
from app.core.security import generate_api_key, generate_webhook_secret, hash_password
from app.db.models import ApiKey, Job, JobDep, JobState, Queue, Schedule, User
from app.db.session import SessionLocal, engine
from app.engine.cron import next_fire

DEMO_EMAIL = "demo@taskforge.dev"
DEMO_PASSWORD = "demo1234"


async def main() -> None:
    setup_logging()
    async with SessionLocal() as db:
        user = (await db.execute(select(User).where(User.email == DEMO_EMAIL))).scalar_one_or_none()
        if user:
            print(f"demo user already exists: {DEMO_EMAIL}")
        else:
            user = User(email=DEMO_EMAIL, password_hash=hash_password(DEMO_PASSWORD),
                        webhook_secret=generate_webhook_secret())
            db.add(user)
            await db.flush()
            for name in ("default", "images", "emails"):
                db.add(Queue(user_id=user.id, name=name))
            plaintext, key_hash, prefix = generate_api_key()
            db.add(ApiKey(user_id=user.id, name="seed-key", prefix=prefix, key_hash=key_hash))
            print(f"created demo user {DEMO_EMAIL} / {DEMO_PASSWORD}")
            print(f"API key (shown once): {plaintext}")

        if not (await db.execute(select(Schedule).where(Schedule.user_id == user.id))).first():
            db.add(Schedule(
                user_id=user.id, name="heartbeat-fetch", cron="*/2 * * * *",
                job_type="http_fetch", payload={"url": "https://example.com"},
                queue="default", next_run_at=next_fire("*/2 * * * *", datetime.now(UTC)),
            ))
            print("created schedule 'heartbeat-fetch' (*/2 * * * *)")

        # A starter workload that exercises every visible behaviour at once.
        specs = [
            ("sleep", {"seconds": 3}, "default", 0, 3),
            ("sleep", {"seconds": 8}, "default", 0, 3),
            ("email_sim", {"to": "ana@example.com", "subject": "Welcome"}, "emails", 5, 3),
            ("email_sim", {"to": "bo@example.com", "subject": "Receipt"}, "emails", 0, 3),
            ("thumbnail", {"width": 128, "height": 128}, "images", 0, 3),
            ("thumbnail", {"width": 64, "height": 64}, "images", 0, 3),
            ("http_fetch", {"url": "https://example.com"}, "default", 0, 3),
            ("flaky", {"fail_times": 1}, "default", 0, 3),      # retries once, then succeeds
            ("flaky", {"fail_times": 9}, "default", 0, 2),      # exhausts retries -> dead letter
            # A dead-letter with a *distinct* error signature, so AI triage has two
            # fingerprints to work with rather than one (when a key is configured).
            ("http_fetch", {"url": "https://taskforge-nonexistent.invalid"},
             "default", 0, 1),
        ]
        jobs = []
        for job_type, payload, queue, priority, max_attempts in specs:
            job = Job(user_id=user.id, type=job_type, payload=payload, queue=queue,
                      priority=priority, max_attempts=max_attempts, state=JobState.QUEUED.value)
            db.add(job)
            jobs.append(job)
        await db.flush()

        # A 2-stage pipeline: the child stays 'pending' until the parent succeeds.
        parent = Job(user_id=user.id, type="sleep", payload={"seconds": 4},
                     queue="default", state=JobState.QUEUED.value)
        db.add(parent)
        await db.flush()
        child = Job(user_id=user.id, type="email_sim",
                    payload={"to": "ops@example.com", "subject": "Stage 1 finished"},
                    queue="emails", state=JobState.PENDING.value)
        db.add(child)
        await db.flush()
        db.add(JobDep(job_id=child.id, parent_id=parent.id))

        await db.commit()
        print(f"queued {len(jobs) + 2} demo jobs (including a dependent 2-stage pipeline)")
        if get_settings().anthropic_api_key:
            print("ANTHROPIC_API_KEY detected: llm_* job types are enabled and the two "
                  "seeded dead-letters will be triaged automatically.")
        else:
            print("No ANTHROPIC_API_KEY: llm_* job types and AI triage are disabled. "
                  "Everything else works.")

    await engine.dispose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
