#!/bin/sh
# Container entrypoint.
#
# WORKER_ONLY=1  -> run only a worker (this is how you scale out: same image, more machines)
# otherwise      -> run migrations, the API, and WORKER_COUNT worker processes together
#
# Co-locating API and workers is a free-tier concession, not the architecture: workers
# coordinate purely through Postgres row locks, so moving them to their own instances
# requires no code change.

set -e

if [ "$WORKER_ONLY" = "1" ]; then
  echo "starting worker-only process"
  exec python -m app.worker
fi

echo "running migrations..."
alembic upgrade head

WORKER_COUNT="${WORKER_COUNT:-2}"
echo "starting $WORKER_COUNT worker(s)..."

# Respawn workers if they exit: a chaos-killed worker should come back, exactly as a
# real orchestrator (Kubernetes, ECS) would restart the container.
i=1
while [ "$i" -le "$WORKER_COUNT" ]; do
  (
    while true; do
      python -m app.worker || echo "worker exited ($?), restarting in 2s"
      sleep 2
    done
  ) &
  i=$((i + 1))
done

echo "starting api on port ${PORT:-8000}..."
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}" --workers 1
