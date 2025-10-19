"""MCP client dedicated to calling the TIDAS validation tool."""

from __future__ import annotations

import json
from typing import Any, Mapping

from tenacity import RetryError, Retrying, stop_after_attempt, wait_exponential

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import SpecCodingError, TidasValidationError
from tiangong_lca_spec.core.json_utils import parse_json_response
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
        LOGGER.info("tidas_validation.request", dataset_count=len(datasets))
        findings: list[dict[str, Any]] = []
        for index, dataset in enumerate(datasets, start=1):
            arguments = self._build_arguments(dataset)
            LOGGER.debug(
                "tidas_validation.invoke_dataset",
                dataset_index=index,
                entity_type=arguments.get("entityType"),
            )
            try:
                raw = self._call_with_retry(arguments)
            except TidasValidationError as exc:
                converted = self._extract_findings_from_error(exc)
                if converted is not None:
                    findings.extend(converted)
                    continue
                raise
            except Exception as exc:  # pylint: disable=broad-except
                msg = f"TIDAS validation failed for dataset at index {index}"
                raise TidasValidationError(msg) from exc
            normalized = self._normalize_results(raw)
            findings.extend(normalized)
        LOGGER.info("tidas_validation.response", finding_count=len(findings))
        return findings

    def _build_arguments(self, dataset: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "entityType": "process",
            "data": {"processDataSet": dataset},
        }

    def _extract_findings_from_error(
        self,
        error: TidasValidationError,
    ) -> list[dict[str, Any]] | None:
        cause = error.__cause__
        if not cause:
            return None
        message = str(cause)
        marker = "Validation Errors:"
        if marker not in message:
            return None
        start = message.find("[", message.index(marker))
        end = message.rfind("]")
        if start == -1 or end == -1:
            return None
        try:
            payload = json.loads(message[start : end + 1])
        except json.JSONDecodeError:
            return None
        findings: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            findings.append(
                {
                    "severity": item.get("severity", "error"),
                    "message": item.get("message", "TIDAS validation error"),
                    "path": "/".join(str(part) for part in item.get("path", [])),
                    "code": item.get("code"),
                }
            )
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
                    text_content, _ = self._mcp.invoke_tool(
                        self._server_name,
                        self._tool_name,
                        arguments,
                    )
                    raw_text = _coerce_text(text_content)
                    if not raw_text:
                        return None
                    try:
                        return parse_json_response(raw_text)
                    except SpecCodingError as exc:
                        if "Validation passed" in raw_text:
                            return []
                        raise SpecCodingError("Unable to parse TIDAS response") from exc
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


def _coerce_text(content: str | list[str] | None) -> str:
    if content is None:
        return ""
    if isinstance(content, list):
        return "\n".join(piece for piece in content if piece)
    return str(content)
