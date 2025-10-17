"""High-level process extraction orchestration built on LangGraph."""

from __future__ import annotations

from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.exceptions import ProcessExtractionError
from tiangong_lca_spec.core.logging import get_logger

from .extractors import LanguageModelProtocol, LocationNormalizer, ProcessClassifier, SectionExtractor

LOGGER = get_logger(__name__)


class ExtractionState(TypedDict, total=False):
    clean_text: str
    sections: dict[str, Any]
    process_information: dict[str, Any]
    administrative_information: dict[str, Any]
    modelling_and_validation: dict[str, Any]
    exchange_list: list[dict[str, Any]]
    notes: Any
    classification: list[dict[str, Any]]
    geography: dict[str, Any]
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
        state.update(sections)
        return state

    def _classify_process(self, state: ExtractionState) -> ExtractionState:
        process_info = state.get("process_information")
        if not process_info:
            LOGGER.warning("process_extraction.missing_process_information")
            return state
        classification = self._classifier.run(process_info)
        process_info.setdefault("classificationInformation", {})["classification"] = classification
        state["classification"] = classification
        return state

    def _normalize_location(self, state: ExtractionState) -> ExtractionState:
        process_info = state.get("process_information")
        if not process_info:
            return state
        geography = self._location_normalizer.run(process_info)
        process_info.setdefault("geography", {}).update(geography)
        state["geography"] = geography
        return state

    def _finalize(self, state: ExtractionState) -> ExtractionState:
        block = {
            "process_information": state.get("process_information", {}),
            "administrative_information": state.get("administrative_information", {}),
            "modelling_and_validation": state.get("modelling_and_validation", {}),
            "exchange_list": state.get("exchange_list") or [],
            "notes": state.get("notes"),
        }
        block["exchanges"] = {"exchange": block["exchange_list"]}
        state["process_blocks"] = [block]
        return state
