"""MCP client dedicated to calling the TIDAS validation tool."""

from __future__ import annotations

from typing import Any, Mapping

from tenacity import RetryError, Retrying, stop_after_attempt, wait_exponential

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import SpecCodingError, TidasValidationError
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.mcp_client import MCPToolClient

LOGGER = get_logger(__name__)


class TidasClient:
    def __init__(
        self,
        settings: Settings | None = None,
        *,
        mcp_client: MCPToolClient | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._server_name = self._settings.tidas_service_name
        self._tool_name = self._settings.tidas_tool_name
        self._mcp = mcp_client or MCPToolClient(self._settings)

    def validate(self, datasets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        arguments: Mapping[str, Any] = {"process_datasets": datasets}
        LOGGER.info("tidas_validation.request", process_count=len(datasets))
        try:
            raw = self._call_with_retry(arguments)
        except (RetryError, TidasValidationError):
            raise
        except Exception as exc:  # pylint: disable=broad-except
            raise TidasValidationError("TIDAS validation failed") from exc

        findings = self._normalize_results(raw)
        LOGGER.info("tidas_validation.response", finding_count=len(findings))
        return findings

    def _call_with_retry(self, arguments: Mapping[str, Any]) -> Any:
        retryer = Retrying(
            stop=stop_after_attempt(max(1, self._settings.max_retries)),
            wait=wait_exponential(
                multiplier=max(self._settings.retry_backoff, 0.1),
                min=0.5,
                max=8,
            ),
            reraise=True,
        )
        try:
            for attempt in retryer:
                with attempt:
                    return self._mcp.invoke_json_tool(
                        self._server_name,
                        self._tool_name,
                        arguments,
                    )
        except RetryError as exc:
            raise TidasValidationError("TIDAS validation failed after retries") from exc
        except SpecCodingError as exc:
            raise TidasValidationError("Unable to parse TIDAS response") from exc

    def _normalize_results(self, raw: Any) -> list[dict[str, Any]]:
        if raw is None:
            return []
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, dict)]
        if isinstance(raw, dict):
            findings = raw.get("findings") or raw.get("results") or raw.get("validation_findings")
            if isinstance(findings, list):
                return [item for item in findings if isinstance(item, dict)]
        LOGGER.warning("tidas_validation.unexpected_payload", payload_type=type(raw).__name__)
        return []

    def close(self) -> None:
        self._mcp.close()

    def __enter__(self) -> "TidasClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
