"""Exchange to flow alignment utilities."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import FlowAlignmentError, FlowSearchError
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.models import FlowCandidate, FlowQuery, UnmatchedFlow
from tiangong_lca_spec.flow_search import search_flows

LOGGER = get_logger(__name__)

FlowSearchCallable = Callable[[FlowQuery], tuple[list[FlowCandidate], list[UnmatchedFlow]]]


class FlowAlignmentService:
    """Aligns exchanges with flow search candidates."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        flow_search_fn: FlowSearchCallable | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._profile = self._settings.profile
        self._flow_search = flow_search_fn or search_flows

    def align_exchanges(
        self, process_dataset: dict[str, Any], paper_md: str | None = None
    ) -> dict[str, Any]:
        try:
            exchanges = list(self._iter_exchanges(process_dataset))
        except KeyError as exc:
            raise FlowAlignmentError("Process dataset missing exchanges") from exc

        process_name = self._extract_process_name(process_dataset)
        LOGGER.info("flow_alignment.start", process=process_name, exchange_count=len(exchanges))

        matched: list[FlowCandidate] = []
        unmatched: list[UnmatchedFlow] = []
        origin_exchanges: dict[str, list[dict[str, Any]]] = {}

        search_jobs: dict[Any, tuple[dict[str, Any], FlowQuery]] = {}
        for exchange in exchanges:
            query = self._build_query(exchange, process_name, paper_md)
            future = self._executor.submit(self._flow_search, query)
            search_jobs[future] = (exchange, query)

        for future in as_completed(search_jobs):
            exchange, query = search_jobs[future]
            exchange_name = self._safe_exchange_name(exchange)
            try:
                matches, misses = future.result()
                if matches:
                    matched.extend(matches)
                if misses:
                    unmatched.extend(misses)
                origin_exchanges.setdefault(exchange_name, []).append(exchange)
            except FlowSearchError as exc:
                LOGGER.warning(
                    "flow_alignment.retry_serial",
                    exchange=exchange_name,
                    process=process_name,
                    error=str(exc),
                )
                try:
                    matches, misses = self._flow_search(query)
                    if matches:
                        matched.extend(matches)
                    if misses:
                        unmatched.extend(misses)
                    origin_exchanges.setdefault(exchange_name, []).append(exchange)
                    continue
                except Exception as serial_exc:  # pylint: disable=broad-except
                    LOGGER.error(
                        "flow_alignment.exchange_failed",
                        exchange=exchange_name,
                        error=str(serial_exc),
                    )
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.error(
                    "flow_alignment.exchange_failed", exchange=exchange_name, error=str(exc)
                )
            unmatched.append(
                UnmatchedFlow(
                    base_name=exchange_name,
                    general_comment=exchange.get("generalComment1")
                    or exchange.get("generalComment")
                    or exchange.get("comment"),
                    process_name=process_name,
                )
            )

        return {
            "process_name": process_name,
            "matched_flows": matched,
            "unmatched_flows": unmatched,
            "origin_exchanges": origin_exchanges,
        }

    def _build_query(
        self, exchange: dict[str, Any], process_name: str | None, paper_md: str | None
    ) -> FlowQuery:
        return FlowQuery(
            exchange_name=self._safe_exchange_name(exchange),
            description=exchange.get("generalComment1")
            or exchange.get("generalComment")
            or exchange.get("comment"),
            process_name=process_name,
            paper_md=paper_md,
        )

    @property
    def _executor(self) -> ThreadPoolExecutor:
        if not hasattr(self, "__executor"):
            limit = max(1, self._settings.flow_search_max_parallel)
            max_workers = max(1, min(self._profile.concurrency, limit))
            self.__executor = ThreadPoolExecutor(max_workers=max_workers)
        return self.__executor

    def close(self) -> None:
        if hasattr(self, "__executor"):
            self.__executor.shutdown(wait=True)
            delattr(self, "__executor")

    def _iter_exchanges(self, process_dataset: dict[str, Any]) -> Iterable[dict[str, Any]]:
        exchanges_block = process_dataset.get("exchanges") or {}
        exchanges = exchanges_block.get("exchange") or []
        if isinstance(exchanges, list):
            return exchanges
        return [exchanges]

    @staticmethod
    def _extract_process_name(process_dataset: dict[str, Any]) -> str | None:
        process_info = (
            process_dataset.get("processInformation")
            or process_dataset.get("process_information")
            or {}
        )
        data_info = (
            process_info.get("dataSetInformation") or process_info.get("data_set_information") or {}
        )
        name_block = data_info.get("name")
        resolved = _resolve_base_name(name_block)
        if resolved:
            return resolved
        return process_dataset.get("process_name") or "unknown_process"

    @staticmethod
    def _safe_exchange_name(exchange: dict[str, Any]) -> str:
        return (
            exchange.get("exchangeName")
            or exchange.get("name")
            or exchange.get("flowName")
            or "unknown_exchange"
        )


def align_exchanges(process_dataset: dict[str, Any], paper_md: str | None = None) -> dict[str, Any]:
    """Functional wrapper around FlowAlignmentService."""
    service = FlowAlignmentService()
    try:
        return service.align_exchanges(process_dataset, paper_md)
    finally:
        service.close()


def _resolve_base_name(name_block: Any) -> str | None:
    if isinstance(name_block, dict):
        base = name_block.get("baseName")
        if isinstance(base, dict):
            text = base.get("#text") or base.get("text")
            if text:
                return text
            for value in base.values():
                if isinstance(value, str):
                    return value
        elif base:
            return str(base)
        text = name_block.get("#text") or name_block.get("text")
        if text:
            return str(text)
        for value in name_block.values():
            if isinstance(value, str):
                return value
    elif isinstance(name_block, list) and name_block:
        return _resolve_base_name(name_block[0])
    elif isinstance(name_block, str):
        return name_block
    return None
