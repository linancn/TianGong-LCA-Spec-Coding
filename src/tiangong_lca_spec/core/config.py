"""Application configuration primitives."""

from __future__ import annotations

import json
import os
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, HttpUrl
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
    flow_search_service_name: str = "TianGong_LCA_Remote"
    flow_search_tool_name: str = "Search_flows_Tool"

    tidas_base_url: HttpUrl = "http://192.168.1.140:9278/mcp"
    tidas_api_key: str | None = None
    tidas_transport: Literal["streamable_http"] = "streamable_http"
    tidas_tool_name: str = "Tidas_Data_Validate_Tool"
    tidas_service_name: str = "Tidas_Data_Validate"

    request_timeout: float = 30.0
    max_retries: int = 3
    retry_backoff: float = 0.5
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    workflow_profile: Literal["default", "batch", "debug"] = "default"
    max_concurrency: int = 4

    cache_dir: Path = Path("artifacts/cache")
    artifacts_dir: Path = Path("artifacts")

    langsmith_api_key: str | None = None
    langsmith_endpoint: HttpUrl | None = None
    langsmith_project: str | None = None
    langsmith_session: str | None = None
    langsmith_tracing_v2: bool = False
    langsmith_callbacks_background: bool | None = None
    langsmith_metadata: dict[str, Any] = Field(default_factory=dict)
    langsmith_tags: tuple[str, ...] = ()
    langsmith_environment: dict[str, str] = Field(default_factory=dict)

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
        return config

    def mcp_service_configs(self) -> dict[str, dict[str, Any]]:
        """Return a mapping of MCP service names to their configuration blocks."""
        flow_service_name = self.flow_search_service_name or "TianGong_LCA_Remote"
        tidas_service_name = self.tidas_service_name
        if not tidas_service_name:
            tidas_service_name = self.tidas_tool_name.removesuffix("_Tool")
        if not tidas_service_name:
            tidas_service_name = self.tidas_tool_name
        return {
            flow_service_name: self.flow_search_mcp_config(),
            tidas_service_name: self.tidas_mcp_config(),
        }

    def langsmith_env_vars(self) -> dict[str, str]:
        """Return the environment variables needed for LangSmith + LangGraph logging."""
        env: dict[str, str] = {
            key: _stringify_env_value(value)
            for key, value in self.langsmith_environment.items()
        }
        if self.langsmith_api_key:
            env["LANGSMITH_API_KEY"] = self.langsmith_api_key
            env["LANGCHAIN_API_KEY"] = self.langsmith_api_key
        if self.langsmith_endpoint:
            endpoint_value = str(self.langsmith_endpoint)
            env.setdefault("LANGSMITH_ENDPOINT", endpoint_value)
            env.setdefault("LANGCHAIN_ENDPOINT", endpoint_value)
        if self.langsmith_project:
            env["LANGCHAIN_PROJECT"] = self.langsmith_project
        if self.langsmith_session:
            env.setdefault("LANGCHAIN_SESSION", self.langsmith_session)
        tracing_enabled = self.langsmith_tracing_v2 or (
            "LANGCHAIN_TRACING_V2" in env
            and env["LANGCHAIN_TRACING_V2"].strip().lower() == "true"
        )
        if tracing_enabled or self.langsmith_api_key:
            env["LANGCHAIN_TRACING_V2"] = _bool_to_env(
                tracing_enabled
                or self.langsmith_tracing_v2
                or bool(self.langsmith_api_key)
            )
        if self.langsmith_callbacks_background is not None:
            env["LANGCHAIN_CALLBACKS_BACKGROUND"] = _bool_to_env(
                self.langsmith_callbacks_background
            )
        if self.langsmith_tags and "LANGCHAIN_TAGS" not in env:
            env["LANGCHAIN_TAGS"] = ",".join(self.langsmith_tags)
        if self.langsmith_metadata and "LANGCHAIN_METADATA" not in env:
            env["LANGCHAIN_METADATA"] = json.dumps(self.langsmith_metadata, ensure_ascii=False)
        return env

    def apply_langsmith_environment(self, *, force: bool = False) -> None:
        """Set LangSmith-related environment variables for downstream tooling."""
        for key, value in self.langsmith_env_vars().items():
            if force or os.environ.get(key) != value:
                os.environ[key] = value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings instance."""
    overrides = _load_settings_overrides()
    settings = Settings(**overrides)
    settings.artifacts_dir.mkdir(parents=True, exist_ok=True)
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    settings.apply_langsmith_environment()
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

    flow_cfg = _extract_section(data, "TianGong_LCA_Remote", "mcp", "flow_search")
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

    langsmith_cfg = _extract_section(data, "LANGSMITH", "langsmith")
    if langsmith_cfg:
        env_map: dict[str, str] = {}

        api_key = _sanitize_api_key(
            langsmith_cfg.get("API_KEY")
            or langsmith_cfg.get("api_key")
            or langsmith_cfg.get("token")
        )
        if api_key is not None:
            overrides["langsmith_api_key"] = api_key
            env_map["LANGSMITH_API_KEY"] = api_key
            env_map["LANGCHAIN_API_KEY"] = api_key

        endpoint = (
            langsmith_cfg.get("ENDPOINT")
            or langsmith_cfg.get("endpoint")
            or langsmith_cfg.get("api_url")
        )
        if endpoint:
            overrides["langsmith_endpoint"] = endpoint
            env_map["LANGSMITH_ENDPOINT"] = str(endpoint)
            env_map["LANGCHAIN_ENDPOINT"] = str(endpoint)

        project = langsmith_cfg.get("PROJECT") or langsmith_cfg.get("project")
        if project:
            overrides["langsmith_project"] = project
            env_map["LANGCHAIN_PROJECT"] = str(project)

        session = langsmith_cfg.get("SESSION") or langsmith_cfg.get("session")
        if session:
            overrides["langsmith_session"] = str(session)
            env_map.setdefault("LANGCHAIN_SESSION", str(session))

        tracing_value = (
            langsmith_cfg.get("TRACING_V2")
            or langsmith_cfg.get("tracing_v2")
            or langsmith_cfg.get("tracing")
        )
        if tracing_value is None and api_key:
            tracing_value = True
        if tracing_value is not None:
            tracing_bool = _coerce_bool(tracing_value)
            overrides["langsmith_tracing_v2"] = tracing_bool
            env_map["LANGCHAIN_TRACING_V2"] = _bool_to_env(tracing_bool)

        callbacks_value = (
            langsmith_cfg.get("CALLBACKS_BACKGROUND") or langsmith_cfg.get("callbacks_background")
        )
        if callbacks_value is None and (
            overrides.get("langsmith_tracing_v2") or env_map.get("LANGCHAIN_TRACING_V2") == "true"
        ):
            callbacks_value = True
        if callbacks_value is not None:
            callbacks_bool = _coerce_bool(callbacks_value)
            overrides["langsmith_callbacks_background"] = callbacks_bool
            env_map["LANGCHAIN_CALLBACKS_BACKGROUND"] = _bool_to_env(callbacks_bool)

        tags_value = langsmith_cfg.get("TAGS") or langsmith_cfg.get("tags")
        tags = _normalize_tags(tags_value)
        if tags:
            overrides["langsmith_tags"] = tags
            env_map.setdefault("LANGCHAIN_TAGS", ",".join(tags))

        metadata_value = (
            langsmith_cfg.get("METADATA") or langsmith_cfg.get("metadata")
        )
        if isinstance(metadata_value, dict):
            overrides["langsmith_metadata"] = metadata_value
            env_map.setdefault(
                "LANGCHAIN_METADATA",
                json.dumps(metadata_value, ensure_ascii=False),
            )

        known_keys = {
            "API_KEY",
            "api_key",
            "token",
            "ENDPOINT",
            "endpoint",
            "api_url",
            "PROJECT",
            "project",
            "SESSION",
            "session",
            "TRACING_V2",
            "tracing_v2",
            "tracing",
            "CALLBACKS_BACKGROUND",
            "callbacks_background",
            "TAGS",
            "tags",
            "METADATA",
            "metadata",
        }
        for raw_key, raw_value in langsmith_cfg.items():
            if raw_key in known_keys:
                continue
            env_key = f"LANGSMITH_{raw_key.upper()}"
            env_map[env_key] = _stringify_env_value(raw_value)

        if env_map:
            overrides["langsmith_environment"] = env_map

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


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    return bool(value)


def _bool_to_env(value: bool) -> str:
    return "true" if value else "false"


def _stringify_env_value(value: Any) -> str:
    if isinstance(value, bool):
        return _bool_to_env(value)
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _normalize_tags(value: Any) -> tuple[str, ...]:
    if not value:
        return ()
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    if isinstance(value, (list, tuple)):
        normalized: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                normalized.append(text)
        return tuple(normalized)
    return ()
