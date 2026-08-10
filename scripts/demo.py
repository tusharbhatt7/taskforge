"""Continuous demo workload generator — keeps a shared dashboard alive and moving.

    uv run python -m scripts.demo --url http://localhost:8000 --rate 6
    uv run python -m scripts.demo --url https://your-app.onrender.com --once

Uses the public HTTP API with the demo account, exactly as a real client would.
"""

import argparse
import asyncio
import random
import sys

import httpx

MIX = [
    ("sleep", lambda: {"seconds": random.randint(1, 6)}, 3),
    ("email_sim", lambda: {"to": f"user{random.randint(1, 999)}@example.com",
                           "subject": random.choice(["Receipt", "Digest", "Welcome", "Alert"])}, 3),
    ("thumbnail", lambda: {"width": random.choice([64, 128, 256]),
                           "height": random.choice([64, 128, 256])}, 3),
    ("http_fetch", lambda: {"url": random.choice([
        "https://example.com", "https://www.wikipedia.org", "https://httpbin.org/status/200"])}, 3),
    ("flaky", lambda: {"fail_times": 1}, 3),
    ("flaky", lambda: {"fail_times": 2}, 3),
    ("flaky", lambda: {"fail_times": 9}, 2),  # always dead-letters
]
WEIGHTS = [5, 5, 3, 3, 2, 2, 1]


async def login(client: httpx.AsyncClient, email: str, password: str) -> str:
    res = await client.post("/api/v1/auth/login", json={"email": email, "password": password})
    if res.status_code != 200:
        res = await client.post("/api/v1/auth/register", json={"email": email, "password": password})
    res.raise_for_status()
    return res.json()["access_token"]


async def submit_one(client: httpx.AsyncClient, headers: dict) -> str:
    job_type, payload_fn, max_attempts = random.choices(MIX, weights=WEIGHTS, k=1)[0]
    body = {"type": job_type, "payload": payload_fn(), "max_attempts": max_attempts,
            "queue": random.choice(["default", "default", "images", "emails"]),
            "priority": random.choice([0, 0, 0, 10])}
    res = await client.post("/api/v1/jobs", json=body, headers=headers)
    res.raise_for_status()
    return f"{job_type} -> {res.json()['id'][:8]}"


async def main() -> None:
    parser = argparse.ArgumentParser(description="Taskforge demo workload generator")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--email", default="demo@taskforge.dev")
    parser.add_argument("--password", default="demo1234")
    parser.add_argument("--rate", type=float, default=6.0, help="jobs per minute")
    parser.add_argument("--once", action="store_true", help="submit one batch of 10 and exit")
    args = parser.parse_args()

    async with httpx.AsyncClient(base_url=args.url.rstrip("/"), timeout=30) as client:
        token = await login(client, args.email, args.password)
        headers = {"Authorization": f"Bearer {token}"}

        if args.once:
            results = await asyncio.gather(*(submit_one(client, headers) for _ in range(10)))
            for line in results:
                print(line)
            return

        interval = 60.0 / max(args.rate, 0.1)
        print(f"submitting ~{args.rate:.0f} jobs/min to {args.url} (ctrl-c to stop)")
        while True:
            try:
                print(await submit_one(client, headers))
            except httpx.HTTPError as exc:
                print(f"submit failed: {exc}", file=sys.stderr)
            await asyncio.sleep(interval * random.uniform(0.5, 1.5))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nstopped")
