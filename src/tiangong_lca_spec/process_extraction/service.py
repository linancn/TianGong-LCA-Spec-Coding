"""High-level process extraction orchestration built on LangGraph."""

from __future__ import annotations

from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import ProcessExtractionError, SpecCodingError
from tiangong_lca_spec.core.logging import get_logger

from .extractors import (
    LanguageModelProtocol,
    LocationNormalizer,
    ProcessClassifier,
    SectionExtractor,
)
from .tidas_mapping import build_tidas_process_dataset

LOGGER = get_logger(__name__)


class ExtractionState(TypedDict, total=False):
    clean_text: str
    sections: dict[str, Any]
    process_blocks: list[dict[str, Any]]


class ProcessExtractionService:
    """Coordinates process extraction by composing LangGraph nodes."""

    def __init__(
        self,
        llm: LanguageModelProtocol,
        settings: Settings | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._section_extractor = SectionExtractor(llm)
        self._classifier = ProcessClassifier(llm)
        self._location_normalizer = LocationNormalizer(llm)
        self._graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        graph = StateGraph(ExtractionState)
        graph.add_node("extract_sections", self._extract_sections)
        graph.add_node("classify_process", self._classify_process)
        graph.add_node("normalize_location", self._normalize_location)
        graph.add_node("finalize", self._finalize)

        graph.set_entry_point("extract_sections")
        graph.add_edge("extract_sections", "classify_process")
        graph.add_edge("classify_process", "normalize_location")
        graph.add_edge("normalize_location", "finalize")
        graph.add_edge("finalize", END)

        return graph.compile()

    def extract(self, clean_text: str) -> list[dict[str, Any]]:
        state = self._graph.invoke({"clean_text": clean_text})
        blocks = state.get("process_blocks") or []
        if not blocks:
            raise ProcessExtractionError("No process blocks generated")
        return blocks

    def _extract_sections(self, state: ExtractionState) -> ExtractionState:
        clean_text = state.get("clean_text")
        if not clean_text:
            raise ProcessExtractionError("Clean text missing for extraction")
        sections = self._section_extractor.run(clean_text)
        state["sections"] = sections

        datasets: list[dict[str, Any]] = []
        raw_datasets = sections.get("processDataSets")
        if isinstance(raw_datasets, list):
            datasets.extend(item for item in raw_datasets if isinstance(item, dict))
        else:
            single_dataset = sections.get("processDataSet")
            if isinstance(single_dataset, dict):
                datasets.append(single_dataset)
        if not datasets:
            raise ProcessExtractionError(
                "Section extraction must return `processDataSets` or `processDataSet`"
            )

        notes = sections.get("notes")
        if isinstance(notes, list) and len(notes) == len(datasets):
            note_values = notes
        else:
            note_values = [notes] * len(datasets)

        blocks: list[dict[str, Any]] = []
        for index, dataset in enumerate(datasets):
            process_information = dataset.setdefault("processInformation", {})
            administrative = dataset.setdefault("administrativeInformation", {})
            modelling = dataset.setdefault("modellingAndValidation", {})
            exchanges = dataset.get("exchanges", {}).get("exchange")
            exchange_list: list[dict[str, Any]] = []
            if isinstance(exchanges, list):
                exchange_list = exchanges
            elif exchanges:
                exchange_list = [exchanges]

            blocks.append(
                {
                    "processDataSet": dataset,
                    "process_information": process_information,
                    "administrative_information": administrative,
                    "modelling_and_validation": modelling,
                    "exchange_list": exchange_list,
                    "notes": note_values[index] if index < len(note_values) else None,
                }
            )

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
            classification_info["classification"] = classification
            classification_info.setdefault(
                "common:classification",
                {"common:class": classification},
            )
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

        final_blocks: list[dict[str, Any]] = []
        for block in blocks:
            process_dataset = block.get("processDataSet")
            if not isinstance(process_dataset, dict):
                raise ProcessExtractionError("Process dataset missing in block")
            notes = block.get("notes")
            exchange_list = block.get("exchange_list") or []

            normalized_dataset = build_tidas_process_dataset(
                process_dataset,
                notes=notes,
            )

            exchanges = normalized_dataset.get("exchanges", {}).get("exchange")
            if isinstance(exchanges, list):
                exchange_block = {"exchange": exchanges}
            elif exchanges:
                exchange_block = {"exchange": [exchanges]}
            else:
                exchange_block = {"exchange": exchange_list}

            final_blocks.append(
                {
                    "process_information": normalized_dataset.get("processInformation", {}),
                    "administrative_information": normalized_dataset.get(
                        "administrativeInformation", {}
                    ),
                    "modelling_and_validation": normalized_dataset.get(
                        "modellingAndValidation", {}
                    ),
                    "exchange_list": exchange_block.get("exchange") or [],
                    "notes": notes,
                    "processDataSet": normalized_dataset,
                    "exchanges": exchange_block,
                }
            )

        state["process_blocks"] = final_blocks
        return state
