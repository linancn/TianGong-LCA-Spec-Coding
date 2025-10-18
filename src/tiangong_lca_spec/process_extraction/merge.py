"""Merger utilities for combining extraction results with flow matches."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Iterable

from tiangong_lca_spec.core.models import FlowCandidate, ProcessDataset


def _resolve_base_name(name_block: Any) -> str | None:
    if isinstance(name_block, dict):
        base = name_block.get("baseName")
        if isinstance(base, dict):
            text = base.get("#text")
            if text:
                return text
        elif base:
            return str(base)
        text = name_block.get("#text")
        if text:
            return str(text)
    elif isinstance(name_block, list) and name_block:
        return _resolve_base_name(name_block[0])
    elif isinstance(name_block, str):
        return name_block
    return None


def merge_results(
    process_blocks: list[dict[str, Any]],
    matched_lookup: dict[str, list[FlowCandidate]],
    origin_exchanges: dict[str, list[dict[str, Any]]],
) -> list[ProcessDataset]:
    datasets: list[ProcessDataset] = []
    for block in process_blocks:
        process_dataset = block.get("processDataSet")
        if not isinstance(process_dataset, dict):
            raise ValueError("Expected `processDataSet` in process block")
        process_information = process_dataset.get("processInformation", {})
        modelling = process_dataset.get("modellingAndValidation", {})
        administrative = process_dataset.get("administrativeInformation", {})
        base_exchanges = process_dataset.get("exchanges", {}).get("exchange") or []

        process_name = _extract_process_name_from_dataset(process_dataset, block)
        exchanges = origin_exchanges.get(process_name) or base_exchanges
        exchanges_list = _ensure_list(exchanges)
        merged_exchanges = _merge_exchange_candidates(
            exchanges_list,
            matched_lookup.get(process_name, []),
        )

        dataset = ProcessDataset(
            process_information=process_information,
            modelling_and_validation=modelling,
            administrative_information=administrative,
            exchanges=merged_exchanges,
            notes=block.get("notes"),
            process_data_set=process_dataset,
        )
        datasets.append(dataset)
    return datasets


def _extract_process_name_from_dataset(
    process_dataset: dict[str, Any],
    block: dict[str, Any],
) -> str:
    process_info = process_dataset.get("processInformation", {})
    dataset_info = process_info.get("dataSetInformation", {})
    name_block = dataset_info.get("name")
    resolved = _resolve_base_name(name_block)
    if resolved:
        return resolved
    return block.get("process_name", "unknown_process")


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
