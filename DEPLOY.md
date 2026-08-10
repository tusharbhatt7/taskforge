# Deploying Taskforge (free, ~10 minutes)

Two free accounts, both with GitHub sign-in: **Neon** for PostgreSQL (permanently free)
and **Render** for the app (free web service).

Why not Render's own Postgres? Render's free database expires after 30 days. Neon's free
tier does not, which matters for a link you'll be putting on a résumé.

---

## Step 1 — Database (Neon)

1. Go to **https://neon.tech** → **Sign up** → *Continue with GitHub*.
2. Create a project (any name, e.g. `taskforge`). Pick the region closest to you.
3. On the project dashboard, find **Connection string** and copy it. It looks like:
   ```
   postgresql://neondb_owner:npg_AbC123@ep-cool-name-a1b2c3.ap-southeast-1.aws.neon.tech/neondb?sslmode=require
   ```
4. Keep it somewhere for the next step. **Paste it exactly as Neon gives it** — the app
   normalizes the scheme and the `sslmode` parameter for you.

> Treat this string like a password: it grants full access to the database. Don't commit
> it or paste it into a public issue.

## Step 2 — App (Render)

1. Go to **https://render.com** → **Get Started** → *GitHub*, and authorize access to the
   `taskforge` repository.
2. Click **New +** → **Blueprint**.
3. Select the `taskforge` repo. Render reads `render.yaml` and proposes one Docker web
   service — no manual build settings needed.
4. It will ask for the environment variable marked `sync: false`:
   - **DATABASE_URL** → paste the Neon connection string from Step 1.

   (`SECRET_KEY` is generated automatically. `WORKER_COUNT` defaults to `2`.)
5. Click **Apply** / **Create**. The first Docker build takes roughly 3–5 minutes.

Watch the **Logs** tab. A healthy start looks like:
```
running migrations...
INFO  [alembic.runtime.migration] Running upgrade  -> 4ba72d8001ea
starting 2 worker(s)...
starting api on port 10000...
{"level": "INFO", "logger": "taskforge.worker", "msg": "worker online: ..."}
{"level": "INFO", "logger": "taskforge.api", "msg": "api server up (env=production)"}
{"level": "INFO", "logger": "taskforge.events", "msg": "event hub listening on taskforge_events"}
```

## Step 3 — Seed the demo data

In Render, open your service → **Shell** tab:
```bash
python -m scripts.seed
```
This creates the demo account (`demo@taskforge.dev` / `demo1234`), three queues, a cron
schedule, and a starter workload including a deliberate dead-letter and a dependent
two-stage pipeline.

## Step 4 — Check it

Open `https://<your-service>.onrender.com` and log in with the demo account. Then:

- **Overview** — live event stream should start moving within seconds
- **Workers** — two workers `online`; click **💥 Kill a random worker** and watch its job
  get reclaimed and finished by the survivor (~25 seconds)
- **Dead letter** — the seeded `flaky` job that exhausted its retries
- **/docs** — interactive Swagger reference

Keep the dashboard busy for a demo:
```bash
uv run python -m scripts.demo --url https://<your-service>.onrender.com --rate 8
```

---

## Things worth knowing

**Cold starts.** Render's free instances sleep after 15 minutes without traffic. The app
pings its own public URL every 10 minutes (using `RENDER_EXTERNAL_URL`, which Render
injects) to stay awake, but the very first request after a deploy can still take ~30
seconds. If you're sharing the link with a recruiter, open it once yourself first.

**Neon autosuspend.** Free Neon databases suspend when idle and take a second or two to
wake. The connection pool uses `pool_pre_ping`, so this recovers transparently.

**Redeploying.** Push to `main` and Render rebuilds automatically. Migrations run on every
boot via `start.sh`, so schema changes ship with the code.

**Scaling out (what to say in an interview).** The single container runs the API and both
workers only because the free tier is one machine. Workers coordinate purely through
Postgres row locks, so adding a second service with `WORKER_ONLY=1` on the same image
gives you workers on their own machine with no code change — and `FOR UPDATE SKIP LOCKED`
guarantees they still never double-claim a job.

## If something breaks

| Symptom | Cause | Fix |
|---|---|---|
| `TypeError: connect() got an unexpected keyword argument 'sslmode'` | An older build without URL normalization | Redeploy latest `main` |
| `password authentication failed` | Connection string truncated on copy | Re-copy the whole string from Neon |
| Build fails at `uv sync --frozen` | `uv.lock` out of sync with `pyproject.toml` | Run `uv lock` locally, commit, push |
| Dashboard loads but no live events | SSE blocked or DB `LISTEN` failed | Check logs for `event hub listening`; hard-refresh |
| Jobs stay `queued` forever | No worker running, or the queue is paused | Check **Workers** tab and `WORKER_COUNT`; resume the queue on **Overview** |

## Afterwards

Add the live URL to the top of `README.md` (replacing the `_(add your Render URL here)_`
placeholder) and to your résumé. A screenshot of the Overview tab mid-workload makes the
README much stronger — take one and drop it in as `docs/dashboard.png`.
