"""Merger utilities for combining extraction results with flow matches."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Iterable

from tiangong_lca_spec.core.models import FlowCandidate, ProcessDataset


def merge_results(
    process_blocks: list[dict[str, Any]],
    matched_lookup: dict[str, list[FlowCandidate]],
    origin_exchanges: dict[str, list[dict[str, Any]]],
) -> list[ProcessDataset]:
    datasets: list[ProcessDataset] = []
    for block in process_blocks:
        process_name = _extract_process_name(block)
        exchanges = origin_exchanges.get(process_name) or block.get("exchange_list") or []
        exchanges_list = _ensure_list(exchanges)
        merged_exchanges = _merge_exchange_candidates(
            exchanges_list,
            matched_lookup.get(process_name, []),
        )

        dataset = ProcessDataset(
            process_information=block.get("process_information", {}),
            modelling_and_validation=block.get("modelling_and_validation", {}),
            administrative_information=block.get("administrative_information", {}),
            exchanges=merged_exchanges,
        )
        datasets.append(dataset)
    return datasets


def _extract_process_name(block: dict[str, Any]) -> str:
    process_info = block.get("process_information", {})
    dataset_info = process_info.get("dataSetInformation", {})
    return dataset_info.get("name") or block.get("process_name", "unknown_process")


def _merge_exchange_candidates(
    exchanges: list[dict[str, Any]],
    candidates: list[FlowCandidate],
) -> list[dict[str, Any]]:
    candidate_map = {candidate.base_name.lower(): candidate for candidate in candidates}
    merged: list[dict[str, Any]] = []
    for exchange in exchanges:
        enriched = dict(exchange)
        base_name = (enriched.get("exchangeName") or enriched.get("name") or "").lower()
        candidate = candidate_map.get(base_name)
        if candidate and candidate.uuid:
            enriched["referenceToFlowDataSet"] = {
                "@refObjectId": candidate.uuid,
                "comment": candidate.general_comment,
            }
        if candidate:
            enriched.setdefault("matchingDetail", asdict(candidate))
        merged.append(enriched)
    return merged


def _ensure_list(exchanges: Iterable[dict[str, Any]] | dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(exchanges, list):
        return exchanges
    if isinstance(exchanges, dict):
        return [exchanges]
    return list(exchanges or [])


def determine_functional_unit(exchanges: list[dict[str, Any]]) -> str | None:
    for exchange in exchanges:
        name = (exchange.get("exchangeName") or "").lower()
        if not name:
            continue
        if _is_waste(name):
            continue
        amount = exchange.get("resultingAmount") or exchange.get("amount")
        unit = exchange.get("unit") or exchange.get("resultingAmountUnit")
        if amount and unit:
            return f"{amount} {unit} {exchange.get('exchangeName')}"
    return None


def _is_waste(name: str) -> bool:
    waste_keywords = ["waste", "slag", "flue gas", "residue"]
    return any(keyword in name for keyword in waste_keywords)
