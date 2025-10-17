"""LLM-backed extractors used in the process extraction stage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from tiangong_lca_spec.core.json_utils import parse_json_response
from tiangong_lca_spec.core.logging import get_logger

LOGGER = get_logger(__name__)


class LanguageModelProtocol(Protocol):
    """Minimal protocol required from language models used in the pipeline."""

    def invoke(self, input_data: dict[str, Any]) -> Any: ...


SECTION_PROMPT = (
    "You are an expert LCA analyst. Extract structured information from the paper. "
    "Return JSON with keys: process_information, administrative_information, "
    "modelling_and_validation, exchange_list, notes. Ensure JSON is valid."
)

CLASSIFICATION_PROMPT = (
    "Map the process to ISIC classification up to 4 levels. Output JSON array of objects "
    "with keys '@level', '@classId', '#text'."
)

LOCATION_PROMPT = (
    "Normalize the process geography. Return JSON with 'code' and 'description'. "
    "Prefer ISO country or region codes."
)


@dataclass
class SectionExtractor:
    llm: LanguageModelProtocol

    def run(self, clean_text: str) -> dict[str, Any]:
        LOGGER.info("process_extraction.section_extraction")
        response = self.llm.invoke({"prompt": SECTION_PROMPT, "context": clean_text})
        return _ensure_dict(response)


@dataclass
class ProcessClassifier:
    llm: LanguageModelProtocol

    def run(self, process_info: dict[str, Any]) -> list[dict[str, Any]]:
        LOGGER.info("process_extraction.classification")
        response = self.llm.invoke({"prompt": CLASSIFICATION_PROMPT, "context": process_info})
        data = _ensure(response)
        if isinstance(data, dict):
            return [data]
        return list(data)


@dataclass
class LocationNormalizer:
    llm: LanguageModelProtocol

    def run(self, process_info: dict[str, Any]) -> dict[str, Any]:
        LOGGER.info("process_extraction.location_normalization")
        response = self.llm.invoke({"prompt": LOCATION_PROMPT, "context": process_info})
        return _ensure_dict(response)


def _ensure(response: Any) -> Any:
    if hasattr(response, "content"):
        response = getattr(response, "content")
    if isinstance(response, str):
        return parse_json_response(response)
    return response


def _ensure_dict(response: Any) -> dict[str, Any]:
    data = _ensure(response)
    if not isinstance(data, dict):
        raise ValueError("Expected dictionary output from language model")
    return data
