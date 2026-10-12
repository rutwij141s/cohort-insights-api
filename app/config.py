"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """All runtime configuration, sourced from env vars / .env file."""

    # ── MongoDB ──────────────────────────────────────────────
    mongodb_url: str = "mongodb://localhost:27017"
    mongodb_database: str = "cohort_insights"

    # ── Redis ─────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"

    # ── Pipeline timing (seconds) ─────────────────────────────
    pipeline_stage1_min_delay: float = 10.0
    pipeline_stage1_max_delay: float = 20.0
    pipeline_stage2_min_delay: float = 5.0
    pipeline_stage2_max_delay: float = 15.0
    pipeline_failure_rate: float = 0.1
    pipeline_max_retries: int = 2
    pipeline_retry_base_delay: float = 1.0

    # ── Rate limiting ─────────────────────────────────────────
    rate_limit_max_concurrent: int = 3

    # ── Content cache ────────────────────────────────────────
    cache_ttl: int = 3600

    # ── Misc ─────────────────────────────────────────────────
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


# Singleton — imported throughout the app
settings = Settings()
