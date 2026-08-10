from functools import lru_cache
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic_settings import BaseSettings, SettingsConfigDict

# libpq query parameters that asyncpg does not accept as connect() kwargs. Managed
# Postgres providers (Neon, Supabase, Heroku) hand out URLs containing these, so they are
# translated rather than rejected — otherwise pasting the provider's URL verbatim fails
# at startup with an opaque TypeError.
_LIBPQ_ONLY_PARAMS = {"sslmode", "channel_binding", "target_session_attrs",
                      "connect_timeout", "application_name", "options"}
_SSL_REQUIRED_MODES = {"require", "verify-ca", "verify-full"}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    env: str = "dev"
    database_url: str = "postgresql+asyncpg://taskforge:taskforge@localhost:5432/taskforge"
    secret_key: str = "dev-secret-change-me"
    access_token_ttl_minutes: int = 60 * 24 * 7

    # Engine tuning. Lease must comfortably exceed heartbeat_seconds, or a worker that
    # merely paused (GC, slow query) loses jobs it is still healthily working on.
    lease_seconds: int = 20
    heartbeat_seconds: int = 4
    worker_dead_after_seconds: int = 20
    reaper_interval_seconds: int = 5
    cron_interval_seconds: int = 15
    webhook_interval_seconds: int = 5
    worker_concurrency: int = 4
    claim_batch_size: int = 4
    default_max_attempts: int = 3
    retry_base_seconds: float = 5.0
    retry_cap_seconds: float = 300.0

    # Rate limiting (per user/IP, per minute)
    rate_limit_per_minute: int = 120

    # ---- AI ----
    # Unset key = AI job types and triage are disabled; everything else works unchanged.
    anthropic_api_key: str | None = None
    ai_model: str = "claude-opus-5"
    ai_max_tokens: int = 8000
    ai_effort: str | None = None          # low | medium | high | xhigh | max (None = API default)
    ai_timeout_seconds: float = 120.0
    ai_triage_enabled: bool = True        # auto-triage jobs that dead-letter
    ai_server_side_fallbacks: bool = True  # recover safety-classifier refusals in-call
    ai_max_input_chars: int = 40_000      # cap payload size before it reaches the API

    # Free-tier keep-alive: Render injects RENDER_EXTERNAL_URL automatically
    render_external_url: str | None = None
    keep_alive_minutes: int = 10

    @property
    def sqlalchemy_url(self) -> str:
        """DATABASE_URL normalized for the SQLAlchemy asyncpg driver."""
        return _normalize(self.database_url)[0]

    @property
    def connect_args(self) -> dict:
        """asyncpg connect kwargs implied by the URL (e.g. TLS for a managed provider)."""
        return _normalize(self.database_url)[1]

    @property
    def asyncpg_dsn(self) -> str:
        """DSN for raw asyncpg connections (LISTEN/NOTIFY), without the driver prefix."""
        return self.sqlalchemy_url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _normalize(url: str) -> tuple[str, dict]:
    """Accept any common Postgres URL shape and return (sqlalchemy_url, connect_args).

    Handles the `postgres://` scheme, missing `+asyncpg` driver, and libpq-only query
    parameters, so a connection string copied straight from a provider dashboard works.
    """
    parts = urlsplit(url)
    scheme = parts.scheme
    if scheme in ("postgres", "postgresql"):
        scheme = "postgresql+asyncpg"

    connect_args: dict = {}
    kept: list[tuple[str, str]] = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        if key.lower() not in _LIBPQ_ONLY_PARAMS:
            kept.append((key, value))
            continue
        if key.lower() == "sslmode" and value.lower() in _SSL_REQUIRED_MODES:
            connect_args["ssl"] = True

    return urlunsplit((scheme, parts.netloc, parts.path, urlencode(kept), parts.fragment)), connect_args


@lru_cache
def get_settings() -> Settings:
    return Settings()
