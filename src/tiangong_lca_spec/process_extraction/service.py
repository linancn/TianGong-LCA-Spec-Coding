"""High-level process extraction orchestration built on sequential stages."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, TypedDict

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import ProcessExtractionError, SpecCodingError
from tiangong_lca_spec.core.logging import get_logger

from .extractors import (
    LanguageModelProtocol,
    LocationNormalizer,
    ParentProcessExtractor,
    ProcessClassifier,
    SectionExtractor,
)
from .hints import enrich_exchange_hints
from .tidas_mapping import build_tidas_process_dataset

LOGGER = get_logger(__name__)
_YEAR_PATTERN = re.compile(r"\b(19|20)\d{2}\b")
_REFERENCE_YEAR_WINDOW = 5000

PROCESS_ID_KEYS = (
    "processId",
    "processID",
    "process_id",
    "processIdentifier",
    "process_identifier",
    "processUUID",
    "processUuid",
    "uuid",
    "UUID",
    "id",
)


class ExtractionState(TypedDict, total=False):
    clean_text: str
    sections: dict[str, Any]
    process_blocks: list[dict[str, Any]]
    parent_processes: list[dict[str, Any]]
    fallback_reference_year: int


class ProcessExtractionService:
    """Coordinates process extraction by running staged helpers sequentially."""

    def __init__(
        self,
        llm: LanguageModelProtocol,
        settings: Settings | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._section_extractor = SectionExtractor(llm)
        self._parent_extractor = ParentProcessExtractor(llm)
        self._classifier = ProcessClassifier(llm)
        self._location_normalizer = LocationNormalizer(llm)

    def extract(self, clean_text: str) -> list[dict[str, Any]]:
        state: ExtractionState = {"clean_text": clean_text}
        fallback_year = _infer_reference_year_from_text(clean_text)
        if fallback_year is not None:
            state["fallback_reference_year"] = fallback_year
        state = self._extract_sections(state)
        state = self._classify_process(state)
        state = self._normalize_location(state)
        state = self._finalize(state)
        blocks = state.get("process_blocks") or []
        if not blocks:
            raise ProcessExtractionError("No process blocks generated")
        return blocks

    def _extract_sections(self, state: ExtractionState) -> ExtractionState:
        clean_text = state.get("clean_text")
        if not clean_text:
            raise ProcessExtractionError("Clean text missing for extraction")

        parent_summary = self._parent_extractor.run(clean_text)
        parents = _normalise_parent_processes(parent_summary)
        state["parent_processes"] = parents

        sections: dict[str, Any] | None = None
        if parents:
            parent_results: list[tuple[dict[str, Any], dict[str, Any]]] = []
            missing_parents: list[str] = []
            for parent in parents:
                context = _slice_text_for_parent(clean_text, parent)
                section = self._section_extractor.run(
                    context,
                    focus_parent=parent["name"],
                    parent_aliases=parent.get("aliases"),
                )
                if not _has_process_datasets(section):
                    section = self._section_extractor.run(
                        clean_text,
                        focus_parent=parent["name"],
                        parent_aliases=parent.get("aliases"),
                    )
                if not _has_process_datasets(section):
                    missing_parents.append(parent["name"])
                parent_results.append((parent, section))
            sections = _combine_parent_sections(parent_results, parent_summary)
            if missing_parents:
                LOGGER.warning(
                    "process_extraction.parents_uncovered",
                    missing_parents=missing_parents,
                )
        else:
            sections = self._section_extractor.run(clean_text)

        state["sections"] = sections

        dataset_entries = _collect_datasets(sections)
        if not dataset_entries and parents:
            sections = self._section_extractor.run(clean_text)
            state["sections"] = sections
            dataset_entries = _collect_datasets(sections)
        if not dataset_entries:
            raise ProcessExtractionError(
                "Section extraction must return `processDataSets` or `processDataSet`"
            )

        blocks: list[dict[str, Any]] = []
        for process_id, dataset in dataset_entries:
            process_information = dataset.setdefault("processInformation", {})
            administrative = dataset.setdefault("administrativeInformation", {})
            modelling = dataset.setdefault("modellingAndValidation", {})

            block: dict[str, Any] = {
                "processDataSet": dataset,
                "process_information": process_information,
                "administrative_information": administrative,
                "modelling_and_validation": modelling,
            }
            if process_id:
                block["process_id"] = process_id
            blocks.append(block)

        state["process_blocks"] = blocks
        return state

    def _classify_process(self, state: ExtractionState) -> ExtractionState:
        blocks = state.get("process_blocks") or []
        if not blocks:
            LOGGER.warning("process_extraction.missing_process_blocks")
            return state

        for block in blocks:
            dataset = block.get("processDataSet")
            if not isinstance(dataset, dict):
                LOGGER.warning("process_extraction.invalid_dataset_block")
                continue
            process_info = dataset.setdefault("processInformation", {})
            if not process_info:
                LOGGER.warning("process_extraction.missing_process_information")
                continue
            classification = self._classifier.run(process_info)
            data_info = process_info.setdefault("dataSetInformation", {})
            classification_info = data_info.setdefault("classificationInformation", {})
            class_entries = (
                list(classification) if isinstance(classification, list) else [classification]
            )
            classification_info["common:classification"] = {"common:class": class_entries}
            classification_info.pop("classification", None)
            block["classification"] = classification

        state["process_blocks"] = blocks
        return state

    def _normalize_location(self, state: ExtractionState) -> ExtractionState:
        blocks = state.get("process_blocks") or []
        if not blocks:
            return state

        for block in blocks:
            dataset = block.get("processDataSet")
            if not isinstance(dataset, dict):
                continue
            process_info = dataset.setdefault("processInformation", {})
            if not process_info:
                continue
            try:
                geography = self._location_normalizer.run(process_info)
            except SpecCodingError as exc:
                LOGGER.warning("process_extraction.location_parse_failed", error=str(exc))
                geography = {}
            if isinstance(geography, str):
                geography = {"description": geography}
            process_info.setdefault("geography", {}).update(geography)
            block["geography"] = geography

        state["process_blocks"] = blocks
        return state

    def _finalize(self, state: ExtractionState) -> ExtractionState:
        blocks = state.get("process_blocks") or []
        if not blocks:
            raise ProcessExtractionError("Process dataset missing at finalize step")

        fallback_year = state.get("fallback_reference_year")
        final_blocks: list[dict[str, Any]] = []
        for block in blocks:
            process_dataset = block.get("processDataSet")
            if not isinstance(process_dataset, dict):
                raise ProcessExtractionError("Process dataset missing in block")

            if fallback_year is not None:
                _apply_reference_year_fallback(process_dataset, fallback_year)
            normalized_dataset = build_tidas_process_dataset(process_dataset)

            process_name = _extract_process_name(normalized_dataset)
            geography_hint = _extract_geography(normalized_dataset)
            exchanges_container = normalized_dataset.get("exchanges") or {}
            if isinstance(exchanges_container, dict):
                exchange_items = exchanges_container.get("exchange", [])
                if isinstance(exchange_items, list):
                    for exchange in exchange_items:
                        enrich_exchange_hints(
                            exchange, process_name=process_name, geography=geography_hint
                        )

            final_block: dict[str, Any] = {
                "processDataSet": normalized_dataset,
            }
            if process_id := block.get("process_id"):
                final_block["process_id"] = process_id
            final_blocks.append(final_block)

        state["process_blocks"] = final_blocks
        return state


def _infer_reference_year_from_text(text: str) -> int | None:
    current_year = datetime.now().year
    if not text:
        return current_year

    window = text[:_REFERENCE_YEAR_WINDOW]
    candidates: list[tuple[int, int]] = []
    for match in _YEAR_PATTERN.finditer(window):
        try:
            year_value = int(match.group(0))
        except ValueError:
            continue
        if 1900 <= year_value <= current_year + 1:
            candidates.append((match.start(), year_value))
    if not candidates:
        return current_year

    for position, year_value in candidates:
        if position <= 300:
            return year_value

    candidates.sort(key=lambda item: (-item[1], item[0]))
    return candidates[0][1]


def _apply_reference_year_fallback(dataset: dict[str, Any], fallback_year: int) -> None:
    if fallback_year is None:
        return

    process_info = dataset.get("processInformation")
    if not isinstance(process_info, dict):
        process_info = {}
        dataset["processInformation"] = process_info

    time_info: dict[str, Any]
    existing_time = process_info.get("time")
    if isinstance(existing_time, dict):
        time_info = existing_time
    else:
        time_info = {}
        process_info["time"] = time_info

    existing_year = _coerce_year(time_info.get("referenceYear"))
    if existing_year is not None:
        time_info["referenceYear"] = existing_year
        return

    existing_year = _coerce_year(time_info.get("common:referenceYear"))
    if existing_year is not None:
        time_info["common:referenceYear"] = existing_year
        return

    valid_fallback = _coerce_year(fallback_year)
    if valid_fallback is not None:
        time_info["referenceYear"] = valid_fallback


def _coerce_year(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        year_value = int(value)
    elif isinstance(value, str):
        match = _YEAR_PATTERN.search(value)
        if not match:
            return None
        try:
            year_value = int(match.group(0))
        except ValueError:
            return None
    else:
        return None

    current_year = datetime.now().year
    if 1900 <= year_value <= current_year + 1:
        return year_value
    return None


def _normalise_parent_processes(summary: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not summary:
        return []
    raw_parents = summary.get("parentProcesses") or summary.get("parent_processes") or []
    normalised: list[dict[str, Any]] = []
    for entry in _ensure_list(raw_parents):
        if not isinstance(entry, dict):
            continue
        name = _stringify(entry.get("name") or entry.get("title"))
        if not name:
            continue
        aliases_raw = entry.get("aliases") or entry.get("alias") or []
        keywords_raw = entry.get("keywords") or entry.get("keyTerms") or []
        hints_raw = entry.get("subprocessHints") or entry.get("subProcesses") or []
        aliases = [_stringify(alias) for alias in _ensure_list(aliases_raw) if _stringify(alias)]
        keywords = [
            _stringify(keyword) for keyword in _ensure_list(keywords_raw) if _stringify(keyword)
        ]
        hints = [_stringify(hint) for hint in _ensure_list(hints_raw) if _stringify(hint)]
        normalised.append(
            {
                "name": name,
                "aliases": aliases,
                "keywords": keywords,
                "subprocessHints": hints,
                "summary": _stringify(entry.get("summary") or entry.get("description")),
            }
        )
    return normalised


def _slice_text_for_parent(clean_text: str, parent: dict[str, Any]) -> str:
    keywords = {parent.get("name", "")}
    keywords.update(parent.get("aliases") or [])
    keywords.update(parent.get("keywords") or [])
    filtered_keywords = {kw.strip() for kw in keywords if kw and isinstance(kw, str)}
    if not filtered_keywords:
        return clean_text

    paragraphs = [paragraph.strip() for paragraph in clean_text.split("\n\n") if paragraph.strip()]
    matched_indices: list[int] = [
        index
        for index, paragraph in enumerate(paragraphs)
        if any(keyword.lower() in paragraph.lower() for keyword in filtered_keywords)
    ]
    if not matched_indices:
        return clean_text
    selected: list[str] = []
    for index in matched_indices:
        start = max(0, index - 1)
        end = min(len(paragraphs), index + 2)
        selected.extend(paragraphs[start:end])
        selected.append("")
    context = "\n\n".join(chunk for chunk in selected if chunk)
    if len(context) < 400:
        return clean_text
    return context


def _combine_parent_sections(
    parent_sections: list[tuple[dict[str, Any], dict[str, Any]]],
    parent_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    aggregated_datasets: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for parent, section in parent_sections:
        dataset_list = section.get("processDataSets")
        if not dataset_list:
            dataset_list = section.get("processDataSet")
        dataset_entries = _normalise_dataset_list(dataset_list)
        if not dataset_entries:
            continue
        for dataset in dataset_entries:
            identifier = _derive_dataset_identifier(dataset) or json.dumps(
                dataset, sort_keys=True, default=str
            )
            if identifier in seen_keys:
                continue
            seen_keys.add(identifier)
            aggregated_datasets.append(dataset)

    combined: dict[str, Any] = {}
    if parent_summary:
        combined["parentProcesses"] = parent_summary.get("parentProcesses") or parent_summary
    if aggregated_datasets:
        combined["processDataSets"] = aggregated_datasets
    if not combined:
        combined = parent_sections[0][1] if parent_sections else {}
    return combined


def _has_process_datasets(section: dict[str, Any]) -> bool:
    if not isinstance(section, dict):
        return False
    if _normalise_dataset_list(section.get("processDataSets")):
        return True
    if _normalise_dataset_list(section.get("processDataSet")):
        return True
    return False


def _collect_datasets(
    sections: dict[str, Any],
) -> list[tuple[str | None, dict[str, Any]]]:
    datasets = _normalise_dataset_list(sections.get("processDataSets"))
    if not datasets:
        datasets = _normalise_dataset_list(sections.get("processDataSet"))
    if datasets:
        return [(_derive_dataset_identifier(dataset), dataset) for dataset in datasets]

    return _merge_modules_into_datasets(sections)


def _normalise_dataset_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _merge_modules_into_datasets(
    sections: dict[str, Any],
) -> list[tuple[str | None, dict[str, Any]]]:
    module_keys = (
        "processInformation",
        "administrativeInformation",
        "modellingAndValidation",
        "exchanges",
    )
    module_entries = {key: _collect_module_entries(sections.get(key), key) for key in module_keys}
    if not any(module_entries.values()):
        return []

    dataset_map: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    fallback: dict[str, list[Any]] = {key: [] for key in module_keys}

    for key in module_keys:
        for process_id, payload in module_entries[key]:
            if process_id:
                dataset = dataset_map.setdefault(process_id, {})
                dataset[key] = payload
                if process_id not in order:
                    order.append(process_id)
            else:
                fallback[key].append(payload)

    if not dataset_map:
        max_length = max((len(entries) for entries in module_entries.values()), default=0)
        merged_entries: list[tuple[str | None, dict[str, Any]]] = []
        for index in range(max_length):
            dataset: dict[str, Any] = {}
            for key in module_keys:
                entries = module_entries[key]
                if index < len(entries):
                    _, payload = entries[index]
                    dataset[key] = payload
            merged_entries.append(_normalise_merged_dataset(None, dataset))
        return merged_entries

    for key in module_keys:
        for payload in fallback[key]:
            assigned = False
            for process_id in order:
                dataset = dataset_map.setdefault(process_id, {})
                if key not in dataset:
                    dataset[key] = payload
                    assigned = True
                    break
            if not assigned:
                new_id = f"auto_{len(dataset_map) + 1}"
                dataset_map[new_id] = {key: payload}
                order.append(new_id)

    for process_id in dataset_map:
        if process_id not in order:
            order.append(process_id)

    merged_entries: list[tuple[str | None, dict[str, Any]]] = []
    for process_id in order:
        dataset = dataset_map.get(process_id, {})
        clean_id = process_id if isinstance(process_id, str) else None
        if clean_id and clean_id.startswith("auto_"):
            clean_id = None
        merged_entries.append(_normalise_merged_dataset(clean_id, dataset))
    return merged_entries


def _collect_module_entries(
    module_value: Any,
    module_key: str,
) -> list[tuple[str | None, Any]]:
    if module_value is None:
        return []

    if isinstance(module_value, dict):
        if module_key in module_value and isinstance(module_value[module_key], (dict, list)):
            module_value = module_value[module_key]
        elif "items" in module_value:
            module_value = module_value["items"]
        elif "processes" in module_value:
            module_value = module_value["processes"]

    entries: list[tuple[str | None, Any]] = []
    for item in _ensure_list(module_value):
        if not isinstance(item, dict):
            continue
        process_id = _extract_process_id(item)
        payload = item.get(module_key)
        if payload is None:
            payload = {key: value for key, value in item.items() if key not in PROCESS_ID_KEYS}
        entries.append((process_id, payload))
    return entries


def _normalise_merged_dataset(
    process_id: str | None,
    dataset: dict[str, Any],
) -> tuple[str | None, dict[str, Any]]:
    merged = dict(dataset)
    merged["processInformation"] = _ensure_dict(merged.get("processInformation"))
    merged["administrativeInformation"] = _ensure_dict(merged.get("administrativeInformation"))
    merged["modellingAndValidation"] = _ensure_dict(merged.get("modellingAndValidation"))
    merged["exchanges"] = _normalise_exchange_container(merged.get("exchanges"))
    return process_id, merged


def _normalise_exchange_container(exchanges: Any) -> dict[str, Any]:
    if isinstance(exchanges, dict):
        if "exchange" in exchanges:
            exchange_value = exchanges["exchange"]
        elif "exchanges" in exchanges:
            exchange_value = exchanges["exchanges"]
        else:
            exchange_value = exchanges
    elif isinstance(exchanges, list):
        exchange_value = exchanges
    elif exchanges is None:
        exchange_value = []
    else:
        exchange_value = [exchanges]

    if isinstance(exchange_value, list):
        values = [item for item in exchange_value if isinstance(item, dict)]
        return {"exchange": values}
    if isinstance(exchange_value, dict):
        return {"exchange": [exchange_value]}
    return {"exchange": []}


def _extract_process_name(dataset: dict[str, Any]) -> str | None:
    process_info = dataset.get("processInformation")
    if not isinstance(process_info, dict):
        return None
    data_info = process_info.get("dataSetInformation")
    if not isinstance(data_info, dict):
        return None
    name = data_info.get("name")
    if isinstance(name, dict):
        base_name = name.get("baseName")
        if isinstance(base_name, dict):
            return _stringify(
                base_name.get("#text") or base_name.get("@value") or base_name.get("text")
            )
        return _stringify(base_name)
    return _stringify(name)


def _extract_geography(dataset: dict[str, Any]) -> str | None:
    process_info = dataset.get("processInformation")
    if not isinstance(process_info, dict):
        return None
    geography = process_info.get("geography")
    if isinstance(geography, dict):
        for key in ("shortName", "#text", "description", "locationOfOperation", "region"):
            if key in geography:
                value = _stringify(geography[key])
                if value:
                    return value
    value = _stringify(geography)
    return value or None


def _extract_process_id(container: dict[str, Any]) -> str | None:
    for key in PROCESS_ID_KEYS:
        value = container.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    data_info = container.get("dataSetInformation")
    if isinstance(data_info, dict):
        for key in ("identifierOfSubDataSet", "common:UUID"):
            value = data_info.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    process_info = container.get("processInformation")
    if isinstance(process_info, dict):
        data_info = process_info.get("dataSetInformation")
        if isinstance(data_info, dict):
            for key in ("identifierOfSubDataSet", "common:UUID"):
                value = data_info.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return None


def _derive_dataset_identifier(dataset: dict[str, Any]) -> str | None:
    identifier = _extract_process_id(dataset)
    if identifier:
        return identifier

    process_info = dataset.get("processInformation")
    if isinstance(process_info, dict):
        data_info = process_info.get("dataSetInformation")
        if isinstance(data_info, dict):
            for key in ("identifierOfSubDataSet", "common:UUID"):
                value = data_info.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    return None


def _ensure_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def _ensure_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()
