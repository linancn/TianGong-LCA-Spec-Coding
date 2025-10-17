"""Exchange to flow alignment utilities."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import FlowAlignmentError
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

        search_jobs = {
            self._submit_exchange(exchange, process_name, paper_md): exchange
            for exchange in exchanges
        }

        for future in as_completed(search_jobs):
            exchange = search_jobs[future]
            exchange_name = self._safe_exchange_name(exchange)
            try:
                matches, misses = future.result()
                if matches:
                    matched.extend(matches)
                if misses:
                    unmatched.extend(misses)
                origin_exchanges.setdefault(exchange_name, []).append(exchange)
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

    def _submit_exchange(
        self, exchange: dict[str, Any], process_name: str | None, paper_md: str | None
    ):
        query = FlowQuery(
            exchange_name=self._safe_exchange_name(exchange),
            description=exchange.get("generalComment1")
            or exchange.get("generalComment")
            or exchange.get("comment"),
            process_name=process_name,
            paper_md=paper_md,
        )
        return self._executor.submit(self._flow_search, query)

    @property
    def _executor(self) -> ThreadPoolExecutor:
        if not hasattr(self, "__executor"):
            max_workers = max(1, self._profile.concurrency)
            self.__executor = ThreadPoolExecutor(max_workers=max_workers)
        return self.__executor

    def close(self) -> None:
        if hasattr(self, "__executor"):
            self.__executor.shutdown(wait=True)

    def _iter_exchanges(self, process_dataset: dict[str, Any]) -> Iterable[dict[str, Any]]:
        exchanges_block = process_dataset.get("exchanges") or {}
        exchanges = exchanges_block.get("exchange") or process_dataset.get("exchange_list") or []
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
        return data_info.get("name") or process_dataset.get("process_name")

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
