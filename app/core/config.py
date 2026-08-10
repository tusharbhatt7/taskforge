from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


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

    # Free-tier keep-alive: Render injects RENDER_EXTERNAL_URL automatically
    render_external_url: str | None = None
    keep_alive_minutes: int = 10

    @property
    def asyncpg_dsn(self) -> str:
        """DSN for raw asyncpg connections (LISTEN/NOTIFY), without the SQLAlchemy driver prefix."""
        return self.database_url.replace("postgresql+asyncpg://", "postgresql://", 1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
