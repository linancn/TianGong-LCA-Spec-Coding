"""Public service layer for flow search."""

from __future__ import annotations

from dataclasses import replace
from functools import lru_cache
from typing import Iterable

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.models import FlowCandidate, FlowQuery, UnmatchedFlow

from .client import FlowSearchClient
from .validators import hydrate_candidate, passes_similarity

LOGGER = get_logger(__name__)


class FlowSearchService:
    """High-level facade responsible for flow lookup and validation."""

    def __init__(self, settings: Settings | None = None, *, client: FlowSearchClient | None = None) -> None:
        self._settings = settings or get_settings()
        self._client = client or FlowSearchClient(self._settings)

    def lookup(self, query: FlowQuery) -> tuple[list[FlowCandidate], list[UnmatchedFlow]]:
        LOGGER.info("flow_search.lookup", exchange=query.exchange_name, process=query.process_name)
        raw_candidates = self._client.search(query)
        matches, filtered_out = self._normalize_candidates(query, raw_candidates)
        if not matches:
            if query.paper_md:
                fallback_query = replace(query, paper_md=None)
                LOGGER.info(
                    "flow_search.retry_without_context",
                    exchange=query.exchange_name,
                    process=query.process_name,
                )
                fallback_raw = self._client.search(fallback_query)
                fallback_matches, fallback_filtered = self._normalize_candidates(query, fallback_raw)
                if fallback_matches:
                    matches.extend(fallback_matches)
                if fallback_filtered:
                    filtered_out.extend(fallback_filtered)
            if not matches and (query.description or query.process_name):
                stripped_query = FlowQuery(
                    exchange_name=query.exchange_name,
                )
                LOGGER.info(
                    "flow_search.retry_minimal",
                    exchange=query.exchange_name,
                    process=query.process_name,
                )
                minimal_raw = self._client.search(stripped_query)
                minimal_matches, minimal_filtered = self._normalize_candidates(query, minimal_raw)
                if minimal_matches:
                    matches.extend(minimal_matches)
                if minimal_filtered:
                    filtered_out.extend(minimal_filtered)
        if matches:
            return matches, filtered_out
        unmatched = UnmatchedFlow(
            base_name=query.exchange_name,
            general_comment=query.description,
            process_name=query.process_name,
        )
        return [], filtered_out + [unmatched]

    def _normalize_candidates(self, query: FlowQuery, payload: Iterable[dict]) -> tuple[list[FlowCandidate], list[UnmatchedFlow]]:
        candidates: list[FlowCandidate] = []
        filtered: list[UnmatchedFlow] = []
        for item in payload or []:
            if not passes_similarity(query, item):
                LOGGER.info("flow_search.filtered_out", base_name=item.get("base_name"))
                filtered.append(
                    UnmatchedFlow(
                        base_name=item.get("base_name") or query.exchange_name,
                        general_comment=item.get("general_comment"),
                        process_name=query.process_name,
                    )
                )
                continue
            candidates.append(hydrate_candidate(item))
        return candidates, filtered

    def close(self) -> None:
        self._client.close()


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
