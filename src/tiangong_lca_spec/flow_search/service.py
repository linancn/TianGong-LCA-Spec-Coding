"""Public service layer for flow search."""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.models import FlowCandidate, FlowQuery, UnmatchedFlow
from tiangong_lca_spec.publishing.crud import DatabaseCrudClient

from .client import FlowSearchClient
from .validators import hydrate_candidate

LOGGER = get_logger(__name__)


class FlowSearchService:
    """High-level facade responsible for flow lookup and validation."""

    def __init__(self, settings: Settings | None = None, *, client: FlowSearchClient | None = None) -> None:
        self._settings = settings or get_settings()
        self._client = client or FlowSearchClient(self._settings)
        self._state_code_filter = self._settings.flow_search_state_code
        self._crud = DatabaseCrudClient(self._settings) if self._state_code_filter is not None else None
        self._state_code_cache: dict[str, bool] = {}

    def lookup(self, query: FlowQuery) -> tuple[list[FlowCandidate], list[UnmatchedFlow]]:
        LOGGER.info("flow_search.lookup", exchange=query.exchange_name)
        primary_query = FlowQuery(
            exchange_name=query.exchange_name,
            description=query.description,
        )
        raw_candidates = self._client.search(primary_query)
        matches, filtered_out = self._normalize_candidates(query, raw_candidates)
        if not matches:
            if query.exchange_name:
                LOGGER.info(
                    "flow_search.retry_name_only",
                    exchange=query.exchange_name,
                )
                name_only_query = FlowQuery(exchange_name=query.exchange_name)
                fallback_raw = self._client.search(name_only_query)
                fallback_matches, fallback_filtered = self._normalize_candidates(query, fallback_raw)
                if fallback_matches:
                    matches.extend(fallback_matches)
                if fallback_filtered:
                    filtered_out.extend(fallback_filtered)
        if matches:
            return matches, filtered_out
        unmatched = UnmatchedFlow(
            base_name=query.exchange_name,
            general_comment=query.description,
        )
        return [], filtered_out + [unmatched]

    def _normalize_candidates(self, query: FlowQuery, payload: Iterable[dict]) -> tuple[list[FlowCandidate], list[UnmatchedFlow]]:
        candidates: list[FlowCandidate] = []
        for item in payload or []:
            flow_uuid = str(item.get("uuid") or item.get("flow_uuid") or "").strip()
            if flow_uuid and not self._passes_state_code(flow_uuid):
                continue
            candidates.append(hydrate_candidate(item))
        return candidates, []

    def close(self) -> None:
        self._client.close()
        if self._crud:
            self._crud.close()

    def _passes_state_code(self, flow_uuid: str) -> bool:
        if self._state_code_filter is None or not self._crud:
            return True
        cached = self._state_code_cache.get(flow_uuid)
        if cached is not None:
            return cached
        record = None
        try:
            record = self._crud.select_flow_record(flow_uuid)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning(
                "flow_search.state_code_lookup_failed",
                uuid=flow_uuid,
                error=str(exc),
            )
            self._state_code_cache[flow_uuid] = False
            return False
        state_code = record.get("state_code") if isinstance(record, dict) else None
        matches = state_code == self._state_code_filter
        self._state_code_cache[flow_uuid] = matches
        if not matches:
            LOGGER.info(
                "flow_search.state_code_filtered",
                uuid=flow_uuid,
                state_code=state_code,
                required=self._state_code_filter,
            )
        return matches


@lru_cache(maxsize=512)
def _cached_search(query: FlowQuery) -> tuple[tuple[FlowCandidate, ...], tuple[UnmatchedFlow, ...]]:
    service = FlowSearchService()
    try:
        matches, unmatched = service.lookup(query)
    finally:
        service.close()
    return tuple(matches), tuple(unmatched)


def search_flows(query: FlowQuery) -> tuple[list[FlowCandidate], list[UnmatchedFlow]]:
    """Cached flow search, ready for pipeline usage."""
    matches, unmatched = _cached_search(query)
    return list(matches), list(unmatched)
