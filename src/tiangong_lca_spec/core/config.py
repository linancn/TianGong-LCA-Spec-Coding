"""Application configuration primitives."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from .models import SettingsProfile


class Settings(BaseSettings):
    """Central configuration for the spec coding workflow."""

    mcp_base_url: HttpUrl = "https://lcamcp.tiangong.earth/mcp"
    mcp_api_key: str | None = None
    tidas_base_url: HttpUrl = "http://192.168.1.140:9278/mcp"
    tidas_tool_name: str = "Tidas_Data_Validate_Tool"

    request_timeout: float = 30.0
    max_retries: int = 3
    retry_backoff: float = 0.5
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    workflow_profile: Literal["default", "batch", "debug"] = "default"
    max_concurrency: int = 4

    cache_dir: Path = Path("artifacts/cache")
    artifacts_dir: Path = Path("artifacts")

    model_config = SettingsConfigDict(
        env_prefix="LCA_",
        env_file=(".env", ".env.local"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def profile(self) -> SettingsProfile:
        """Expose derived profile information for orchestrator policies."""
        if self.workflow_profile == "batch":
            return SettingsProfile(
                concurrency=self.max_concurrency,
                retry_attempts=self.max_retries + 2,
                cache_results=True,
                profile_name="batch",
            )
        if self.workflow_profile == "debug":
            return SettingsProfile(
                concurrency=1,
                retry_attempts=1,
                cache_results=False,
                profile_name="debug",
            )
        return SettingsProfile(
            concurrency=min(self.max_concurrency, 4),
            retry_attempts=self.max_retries,
            cache_results=True,
            profile_name="default",
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance."""
    settings = Settings()
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    return settings
