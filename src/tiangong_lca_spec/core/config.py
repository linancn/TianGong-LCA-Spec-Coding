"""Application configuration primitives."""

from __future__ import annotations

import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from pydantic import HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from .models import SettingsProfile

DEFAULT_SECRETS_PATH = Path(".secrets/secrets.toml")


def _authorization_header(api_key: str | None) -> dict[str, str]:
    if not api_key:
        return {}
    return {"Authorization": f"Bearer {api_key}"}


class Settings(BaseSettings):
    """Central configuration for the spec coding workflow."""

    mcp_base_url: HttpUrl = "https://lcamcp.tiangong.earth/mcp"
    mcp_api_key: str | None = None
    mcp_transport: Literal["streamable_http"] = "streamable_http"
    flow_search_service_name: str = "tiangong_lca_remote"
    flow_search_tool_name: str = "Search_flows_Tool"
    flow_search_max_parallel: int = 1

    tidas_base_url: HttpUrl = "http://192.168.1.140:9278/mcp"
    tidas_api_key: str | None = None
    tidas_transport: Literal["streamable_http"] = "streamable_http"
    tidas_tool_name: str = "Tidas_Data_Validate_Tool"
    tidas_service_name: str = "Tidas_Data_Validate"

    request_timeout: float = 30.0
    flow_search_timeout: float | None = None
    tidas_timeout: float | None = None
    max_retries: int = 3
    retry_backoff: float = 0.5
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    workflow_profile: Literal["default", "batch", "debug"] = "default"
    max_concurrency: int = 4

    cache_dir: Path = Path("artifacts/cache")
    artifacts_dir: Path = Path("artifacts")

    model_config = SettingsConfigDict(env_prefix="LCA_", env_file=(), extra="ignore")

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

    def flow_search_mcp_config(self) -> dict[str, Any]:
        """Return the MCP configuration block for the flow search service."""
        config: dict[str, Any] = {
            "transport": self.mcp_transport,
            "url": str(self.mcp_base_url),
        }
        headers = _authorization_header(self.mcp_api_key)
        if headers:
            config["headers"] = headers
        timeout = self.flow_search_timeout or self.request_timeout
        if timeout and timeout > 0:
            config["timeout"] = float(timeout)
        return config

    def tidas_mcp_config(self) -> dict[str, Any]:
        """Return the MCP configuration block for the TIDAS validation service."""
        config: dict[str, Any] = {
            "transport": self.tidas_transport,
            "url": str(self.tidas_base_url),
        }
        headers = _authorization_header(self.tidas_api_key)
        if headers:
            config["headers"] = headers
        timeout = self.tidas_timeout or self.request_timeout
        if timeout and timeout > 0:
            config["timeout"] = float(timeout)
        return config

    def mcp_service_configs(self) -> dict[str, dict[str, Any]]:
        """Return a mapping of MCP service names to their configuration blocks."""
        flow_service_name = self.flow_search_service_name or "tiangong_lca_remote"
        tidas_service_name = self.tidas_service_name
        if not tidas_service_name:
            tidas_service_name = self.tidas_tool_name.removesuffix("_Tool")
        if not tidas_service_name:
            tidas_service_name = self.tidas_tool_name
        return {
            flow_service_name: self.flow_search_mcp_config(),
            tidas_service_name: self.tidas_mcp_config(),
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance."""
    overrides = _load_settings_overrides()
    settings = Settings(**overrides)
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    return settings


def get_mcp_service_configs() -> dict[str, dict[str, Any]]:
    """Convenience helper returning the configured MCP service blocks."""
    return get_settings().mcp_service_configs()


def _load_settings_overrides(secrets_path: Path = DEFAULT_SECRETS_PATH) -> dict[str, Any]:
    """Load configuration overrides from the secrets TOML file."""
    if not secrets_path.exists():
        return {}
    data = _read_toml(secrets_path)
    overrides: dict[str, Any] = {}

    flow_cfg = _extract_section(data, "tiangong_lca_remote", "mcp", "flow_search")
    if flow_cfg:
        overrides.update(
            {
                "mcp_base_url": flow_cfg.get("url", overrides.get("mcp_base_url")),
                "mcp_transport": flow_cfg.get("transport", overrides.get("mcp_transport")),
                "flow_search_service_name": flow_cfg.get(
                    "service_name", overrides.get("flow_search_service_name")
                ),
                "flow_search_tool_name": flow_cfg.get(
                    "tool_name", overrides.get("flow_search_tool_name")
                ),
            }
        )
        api_key = _sanitize_api_key(flow_cfg.get("api_key") or flow_cfg.get("authorization"))
        if api_key is not None:
            overrides["mcp_api_key"] = api_key
        timeout_value = _coerce_float(flow_cfg.get("timeout"))
        if timeout_value is not None:
            overrides["flow_search_timeout"] = timeout_value

    tidas_cfg = _extract_section(data, "tidas_data_validate", "tidas", "validation")
    if tidas_cfg:
        overrides.update(
            {
                "tidas_base_url": tidas_cfg.get("url", overrides.get("tidas_base_url")),
                "tidas_transport": tidas_cfg.get("transport", overrides.get("tidas_transport")),
                "tidas_service_name": tidas_cfg.get(
                    "service_name", overrides.get("tidas_service_name")
                ),
                "tidas_tool_name": tidas_cfg.get("tool_name", overrides.get("tidas_tool_name")),
            }
        )
        api_key = _sanitize_api_key(tidas_cfg.get("api_key") or tidas_cfg.get("authorization"))
        if api_key is not None:
            overrides["tidas_api_key"] = api_key
        timeout_value = _coerce_float(tidas_cfg.get("timeout"))
        if timeout_value is not None:
            overrides["tidas_timeout"] = timeout_value

    general_cfg = data.get("lca") or {}
    overrides.update(
        {key: value for key, value in general_cfg.items() if key in Settings.model_fields}
    )
    return {key: value for key, value in overrides.items() if value is not None}


def _read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def _extract_section(data: dict[str, Any], *candidates: str) -> dict[str, Any] | None:
    for key in candidates:
        section = data.get(key)
        if isinstance(section, dict):
            return section
    return None


def _sanitize_api_key(value: str | None) -> str | None:
    if not value:
        return None
    token = value.strip()
    if token.lower().startswith("bearer "):
        token = token[7:].strip()
    return token or None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
