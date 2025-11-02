"""MCP client wrapping the flow search tool."""

from __future__ import annotations

from typing import Any, Mapping

import httpx
from mcp import McpError
from tenacity import Retrying, stop_after_attempt, wait_exponential

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import FlowSearchError, SpecCodingError
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.mcp_client import MCPToolClient
from tiangong_lca_spec.core.models import FlowQuery

LOGGER = get_logger(__name__)

TIMEOUT_ERRORS = (httpx.TimeoutException, TimeoutError)
DEFAULT_CONTEXT_LIMIT = 800


class FlowSearchClient:
    """Thin wrapper around the MCP flow search tool."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        mcp_client: MCPToolClient | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._server_name = self._settings.flow_search_service_name
        self._tool_name = getattr(self._settings, "flow_search_tool_name", "Search_Flows_Tool")
        self._mcp = mcp_client or MCPToolClient(self._settings)
        self._timeout_seconds = self._resolve_timeout()
        self._max_attempts = max(1, self._settings.max_retries)
        self._context_char_limit = int(getattr(self._settings, "flow_search_context_chars", DEFAULT_CONTEXT_LIMIT))

    def _build_arguments(
        self,
        query: FlowQuery,
        *,
        include_context: bool = True,
    ) -> Mapping[str, Any]:
        parts: list[str] = []
        if query.exchange_name:
            parts.append(f"exchange: {query.exchange_name}")
        if query.process_name:
            parts.append(f"process: {query.process_name}")
        if query.description:
            parts.append(f"description: {query.description}")
        if include_context and query.paper_md:
            limit = max(self._context_char_limit, 0)
            if limit > 0:
                parts.append(f"context: {query.paper_md[:limit]}")
        joined = " \n".join(parts)
        return {"query": joined or query.exchange_name}

    def search(self, query: FlowQuery) -> list[dict[str, Any]]:
        """Execute the remote flow search and return parsed candidates."""
        include_context = True
        arguments = self._build_arguments(query, include_context=include_context)
        LOGGER.info("flow_search.request", arguments=arguments)
        try:
            raw = self._call_with_retry(arguments)
        except FlowSearchError as exc:
            if self._should_strip_context(exc, include_context):
                LOGGER.warning(
                    "flow_search.context_stripped",
                    exchange=query.exchange_name,
                    process=query.process_name,
                )
                include_context = False
                arguments = self._build_arguments(query, include_context=False)
                raw = self._call_with_retry(arguments)
            else:
                raise
        except Exception as exc:  # pylint: disable=broad-except
            raise FlowSearchError("Flow search invocation failed") from exc

        records = self._normalize_results(raw)
        LOGGER.info("flow_search.response", candidate_count=len(records))
        return records

    def _call_with_retry(self, arguments: Mapping[str, Any]) -> Any:
        retryer = Retrying(
            stop=stop_after_attempt(self._max_attempts),
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
        except SpecCodingError as exc:
            raise FlowSearchError("Flow search returned malformed payload") from exc
        except TIMEOUT_ERRORS as exc:  # type: ignore[misc]
            attempts = retryer.statistics.get("attempt_number") or self._max_attempts
            attempts_int = max(int(attempts), 1)
            LOGGER.error(
                "flow_search.timeout",
                attempts=attempts_int,
                timeout=self._timeout_seconds,
                server=self._server_name,
                tool=self._tool_name,
            )
            message = "Flow search request timed out"
            if attempts_int > 1:
                message += f" after {attempts_int} attempts"
            if self._timeout_seconds:
                message += f" (timeout={self._timeout_seconds:.0f}s)"
            raise FlowSearchError(message) from exc

    def _resolve_timeout(self) -> float | None:
        timeout = getattr(self._settings, "flow_search_timeout", None)
        if timeout is None or timeout <= 0:
            timeout = getattr(self._settings, "request_timeout", None)
        if timeout is None or timeout <= 0:
            return None
        return float(timeout)

    @staticmethod
    def _should_strip_context(exc: FlowSearchError, include_context: bool) -> bool:
        if not include_context:
            return False
        cause: Exception | None = exc.__cause__  # type: ignore[assignment]
        while cause:
            if isinstance(cause, httpx.HTTPStatusError):
                status = cause.response.status_code
                if status == 413 or status >= 500:
                    return True
            if isinstance(cause, McpError):
                message = str(cause)
                if "413" in message or "payload too large" in message.lower():
                    return True
            cause = cause.__cause__  # type: ignore[assignment]
        return False

    def close(self) -> None:
        self._mcp.close()

    def __enter__(self) -> "FlowSearchClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _normalize_results(self, raw: Any) -> list[dict[str, Any]]:
        if raw is None:
            return []
        if isinstance(raw, list):
            normalized = []
            for item in raw:
                if isinstance(item, dict):
                    flattened = self._flatten_flow_dataset(item)
                    if flattened:
                        normalized.append(flattened)
            return normalized
        if isinstance(raw, dict):
            candidates = raw.get("candidates") or raw.get("flows") or raw.get("results") or raw.get("data")
            if isinstance(candidates, list):
                normalized: list[dict[str, Any]] = []
                for item in candidates:
                    if isinstance(item, dict):
                        payload = item.get("json") if isinstance(item.get("json"), dict) else item
                        flattened = self._flatten_flow_dataset(payload)
                        if flattened:
                            normalized.append(flattened)
                return normalized
        LOGGER.warning("flow_search.unexpected_payload", payload_type=type(raw).__name__)
        return []

    @staticmethod
    def _flatten_flow_dataset(payload: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        flow = payload.get("flowDataSet") or payload
        if not isinstance(flow, dict):
            return None
        info = flow.get("flowInformation", {})
        data_info = info.get("dataSetInformation", {})
        name_block = data_info.get("name") or {}
        base_name = _preferred_language_text(name_block.get("baseName"))
        if not base_name:
            return None
        geography = _extract_geography(info.get("geography"))
        flow_properties = _preferred_language_text(name_block.get("flowProperties")) or _preferred_language_text(
            name_block.get("functionalUnitFlowProperties")
        )
        return {
            "uuid": data_info.get("common:UUID") or flow.get("@uuid"),
            "base_name": base_name,
            "treatment_standards_routes": _preferred_language_text(name_block.get("treatmentStandardsRoutes")),
            "mix_and_location_types": _preferred_language_text(name_block.get("mixAndLocationTypes")),
            "flow_properties": flow_properties,
            "version": flow.get("administrativeInformation", {}).get("publicationAndOwnership", {}).get("common:dataSetVersion"),
            "general_comment": _preferred_language_text(data_info.get("common:generalComment")),
            "geography": geography,
            "classification": data_info.get("classificationInformation", {}).get("common:classification", {}).get("common:class"),
        }


ENGLISH_LANG_KEYS = (
    "en",
    "en-us",
    "en-gb",
    "english",
)

CHINESE_LANG_KEYS = (
    "zh-hans",
    "zh-cn",
    "zh",
    "\u7b80\u4f53\u4e2d\u6587",
)


def _preferred_language_text(value: Any) -> str | None:
    english = _find_language_text(value, ENGLISH_LANG_KEYS)
    if english:
        return english
    chinese = _find_language_text(value, CHINESE_LANG_KEYS)
    if chinese:
        return chinese
    return _first_text(value)


def _find_language_text(value: Any, language_keys: tuple[str, ...]) -> str | None:
    normalized_targets = {_normalize_language_key(token) for token in language_keys}
    return _find_language_text_recursive(value, normalized_targets)


def _find_language_text_recursive(value: Any, language_targets: set[str]) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        for item in value:
            match = _find_language_text_recursive(item, language_targets)
            if match:
                return match
        return None
    if isinstance(value, dict):
        lang = value.get("@xml:lang") or value.get("xml:lang") or value.get("@lang") or value.get("lang")
        lang_normalized = _normalize_language_key(str(lang)) if lang else None
        if lang_normalized and lang_normalized in language_targets:
            text = value.get("#text") or value.get("text") or value.get("@value")
            if text:
                return str(text)
        for key, item in value.items():
            if isinstance(key, str) and _normalize_language_key(key) in language_targets:
                match = _first_text(item)
                if match:
                    return match
        for item in value.values():
            match = _find_language_text_recursive(item, language_targets)
            if match:
                return match
        return None
    if isinstance(value, str):
        return value
    return None


def _normalize_language_key(token: str) -> str:
    return token.lower().replace("_", "-").strip()


def _first_text(value: Any) -> str | None:
    if isinstance(value, dict):
        text = value.get("#text") or value.get("text")
        if text:
            return text
        for candidate in value.values():
            if isinstance(candidate, str):
                return candidate
            if isinstance(candidate, list):
                for item in candidate:
                    text = _first_text(item)
                    if text:
                        return text
            if isinstance(candidate, dict):
                text = _first_text(candidate)
                if text:
                    return text
    elif isinstance(value, list):
        for item in value:
            text = _first_text(item)
            if text:
                return text
    elif isinstance(value, str):
        return value
    return None


def _extract_geography(raw_geo: Any) -> dict[str, Any] | None:
    if not isinstance(raw_geo, dict):
        return None
    location = raw_geo.get("locationOfOperationSupplyOrProduction") or raw_geo.get("location")
    if isinstance(location, dict):
        code = location.get("@location") or location.get("code") or _first_text(location.get("name"))
        description = _first_text(location.get("descriptionOfRestrictions")) or _first_text(location.get("common:other"))
        return {"code": code, "description": description}
    return None
