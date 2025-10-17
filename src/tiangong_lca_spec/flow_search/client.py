"""HTTP client wrapping the MCP flow search endpoint."""

from __future__ import annotations

from typing import Any

import httpx
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import FlowSearchError
from tiangong_lca_spec.core.json_utils import parse_json_response
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.models import FlowQuery

LOGGER = get_logger(__name__)


class FlowSearchClient:
    """Thin wrapper around the MCP flow search API."""

    def __init__(
        self, settings: Settings | None = None, *, client: httpx.Client | None = None
    ) -> None:
        self._settings = settings or get_settings()
        self._client = client or httpx.Client(
            base_url=str(self._settings.mcp_base_url),
            timeout=self._settings.request_timeout,
            headers=self._build_headers(),
        )

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "tiangong-lca-spec/0.1",
        }
        if self._settings.mcp_api_key:
            headers["Authorization"] = f"Bearer {self._settings.mcp_api_key}"
        return headers

    def _build_payload(self, query: FlowQuery) -> dict[str, Any]:
        return {
            "tool": "FlowDataSearch",
            "input": {
                "exchange_name": query.exchange_name,
                "description": query.description,
                "process_name": query.process_name,
            },
        }

    @retry(
        stop=stop_after_attempt(get_settings().max_retries),
        wait=wait_exponential(multiplier=get_settings().retry_backoff, min=0.5, max=8),
        reraise=True,
    )
    def _post(self, payload: dict[str, Any]) -> httpx.Response:
        response = self._client.post("", json=payload)
        response.raise_for_status()
        return response

    def search(self, query: FlowQuery) -> list[dict[str, Any]]:
        """Execute the remote flow search and return parsed candidates."""
        payload = self._build_payload(query)
        LOGGER.info("flow_search.request", payload=payload)
        try:
            response = self._post(payload)
        except RetryError as exc:
            raise FlowSearchError("Flow search failed after retries") from exc

        parsed = self._parse_response(response.text)
        records: list[dict[str, Any]]
        if isinstance(parsed, dict):
            records = parsed.get("candidates") or parsed.get("flows") or []
        elif isinstance(parsed, list):
            records = parsed
        else:
            records = []
        LOGGER.info("flow_search.response", candidate_count=len(records))
        return records

    def _parse_response(self, content: str) -> Any:
        try:
            return parse_json_response(content)
        except Exception as exc:  # pylint: disable=broad-except
            raise FlowSearchError("Unable to parse MCP response") from exc

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "FlowSearchClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
