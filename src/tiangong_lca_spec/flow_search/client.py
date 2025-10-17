"""MCP client wrapping the flow search tool."""

from __future__ import annotations

from typing import Any, Mapping

from tenacity import RetryError, Retrying, stop_after_attempt, wait_exponential

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import FlowSearchError, SpecCodingError
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.mcp_client import MCPToolClient
from tiangong_lca_spec.core.models import FlowQuery

LOGGER = get_logger(__name__)

FLOW_SEARCH_TOOL = "FlowDataSearch"


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
        self._mcp = mcp_client or MCPToolClient(self._settings)

    def _build_arguments(self, query: FlowQuery) -> Mapping[str, Any]:
        return {
            "exchange_name": query.exchange_name,
            "description": query.description,
            "process_name": query.process_name,
        }

    def search(self, query: FlowQuery) -> list[dict[str, Any]]:
        """Execute the remote flow search and return parsed candidates."""
        arguments = self._build_arguments(query)
        LOGGER.info("flow_search.request", arguments=arguments)
        try:
            raw = self._call_with_retry(arguments)
        except (RetryError, FlowSearchError):
            raise
        except Exception as exc:  # pylint: disable=broad-except
            raise FlowSearchError("Flow search invocation failed") from exc

        records = self._normalize_results(raw)
        LOGGER.info("flow_search.response", candidate_count=len(records))
        return records

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
                        FLOW_SEARCH_TOOL,
                        arguments,
                    )
        except RetryError as exc:
            raise FlowSearchError("Flow search failed after retries") from exc
        except SpecCodingError as exc:
            raise FlowSearchError("Flow search returned malformed payload") from exc

    def _normalize_results(self, raw: Any) -> list[dict[str, Any]]:
        if raw is None:
            return []
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, dict)]
        if isinstance(raw, dict):
            candidates = raw.get("candidates") or raw.get("flows") or raw.get("results")
            if isinstance(candidates, list):
                return [item for item in candidates if isinstance(item, dict)]
        LOGGER.warning("flow_search.unexpected_payload", payload_type=type(raw).__name__)
        return []

    def close(self) -> None:
        self._mcp.close()

    def __enter__(self) -> "FlowSearchClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
