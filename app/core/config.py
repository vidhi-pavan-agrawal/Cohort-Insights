"""Environment-driven configuration (12-factor). Every value can be set via env var or `.env`."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Application
    app_name: str = "cohort-insights-api"
    environment: Literal["local", "test", "staging", "production"] = "local"
    log_level: str = "INFO"
    log_json: bool = True

    # MongoDB
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "cohort_insights"
    mongo_server_selection_timeout_ms: int = Field(3000, ge=100)

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    redis_socket_timeout_seconds: float = Field(0.5, gt=0)

    # Rate limiting
    rate_limit_max_active: int = Field(3, ge=1)
    rate_limit_key_ttl_seconds: int = Field(86_400, ge=60)

    # Content cache
    cache_ttl_seconds: int = Field(7 * 86_400, ge=1)

    # Pipeline
    pipeline_enabled: bool = True
    pipeline_worker_concurrency: int = Field(4, ge=1)
    processing_min_seconds: float = Field(10.0, ge=0)
    processing_max_seconds: float = Field(20.0, ge=0)
    enriching_min_seconds: float = Field(5.0, ge=0)
    enriching_max_seconds: float = Field(15.0, ge=0)
    stage_failure_rate: float = Field(0.1, ge=0, le=1)
    stage_max_attempts: int = Field(3, ge=1)
    retry_backoff_base_seconds: float = Field(1.0, ge=0)
    retry_backoff_max_seconds: float = Field(8.0, ge=0)
    lease_seconds: float = Field(60.0, gt=0)
    recovery_interval_seconds: float = Field(15.0, gt=0)

    # Input limits
    max_title_length: int = 300
    max_content_length: int = 100_000
    max_ref_length: int = 200

    @model_validator(mode="after")
    def _check_ranges(self) -> "Settings":
        if self.processing_min_seconds > self.processing_max_seconds:
            raise ValueError("PROCESSING_MIN_SECONDS must be <= PROCESSING_MAX_SECONDS")
        if self.enriching_min_seconds > self.enriching_max_seconds:
            raise ValueError("ENRICHING_MIN_SECONDS must be <= ENRICHING_MAX_SECONDS")
        worst_attempt = max(self.processing_max_seconds, self.enriching_max_seconds)
        if self.lease_seconds <= worst_attempt + self.retry_backoff_max_seconds:
            raise ValueError("LEASE_SECONDS must exceed the longest stage attempt plus max backoff")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
