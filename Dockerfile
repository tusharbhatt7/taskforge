FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

# Pinned to the uv minor that wrote uv.lock — an older uv cannot read a newer lock format.
COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /usr/local/bin/uv

WORKDIR /app

# Dependencies are their own layer so code edits don't trigger a reinstall.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY alembic.ini start.sh ./
COPY alembic ./alembic
COPY app ./app
COPY scripts ./scripts
COPY sdk ./sdk

RUN chmod +x start.sh \
    && useradd --create-home --uid 10001 taskforge \
    && chown -R taskforge:taskforge /app
USER taskforge

EXPOSE 8000

CMD ["./start.sh"]
