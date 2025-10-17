"""HTTP client dedicated to calling the TIDAS validation MCP tool."""

from __future__ import annotations

from typing import Any

import httpx
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import TidasValidationError
from tiangong_lca_spec.core.json_utils import parse_json_response
from tiangong_lca_spec.core.logging import get_logger

LOGGER = get_logger(__name__)


class TidasClient:
    def __init__(
        self, settings: Settings | None = None, *, client: httpx.Client | None = None
    ) -> None:
        self._settings = settings or get_settings()
        self._client = client or httpx.Client(
            base_url=str(self._settings.tidas_base_url),
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

    @retry(
        stop=stop_after_attempt(get_settings().max_retries),
        wait=wait_exponential(multiplier=get_settings().retry_backoff, min=0.5, max=8),
        reraise=True,
    )
    def _post(self, payload: dict[str, Any]) -> httpx.Response:
        response = self._client.post("", json=payload)
        response.raise_for_status()
        return response

    def validate(self, datasets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload = {
            "tool": self._settings.tidas_tool_name,
            "input": {"process_datasets": datasets},
        }
        LOGGER.info("tidas_validation.request", process_count=len(datasets))
        try:
            response = self._post(payload)
        except RetryError as exc:
            raise TidasValidationError("TIDAS validation failed after retries") from exc

        try:
            parsed = parse_json_response(response.text)
        except Exception as exc:  # pylint: disable=broad-except
            raise TidasValidationError("Unable to parse TIDAS response") from exc

        LOGGER.info("tidas_validation.response")
        if isinstance(parsed, dict):
            return parsed.get("findings") or parsed.get("results") or []
        if isinstance(parsed, list):
            return parsed
        return []

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "TidasClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
