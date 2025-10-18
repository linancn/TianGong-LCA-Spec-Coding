"""LLM-backed extractors used in the process extraction stage."""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache
from typing import Any, Protocol

from tiangong_lca_spec.core.json_utils import parse_json_response
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.tidas import FieldSummary, get_schema_repository

LOGGER = get_logger(__name__)


class LanguageModelProtocol(Protocol):
    """Minimal protocol required from language models used in the pipeline."""

    def invoke(self, input_data: dict[str, Any]) -> Any: ...


def _truncate(text: str, limit: int = 160) -> str:
    cleaned = " ".join(str(text).split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _format_fields(fields: list[FieldSummary], indent: int = 0, depth: int = 2) -> list[str]:
    lines: list[str] = []
    prefix = "  " * indent
    for field in fields:
        type_hint = f" [{field.type}]" if field.type else ""
        required = " (required)" if field.required else ""
        description = f": {_truncate(field.description)}" if field.description else ""
        lines.append(f"{prefix}- {field.name}{type_hint}{required}{description}")
        if field.children and depth > 1:
            lines.extend(_format_fields(field.children, indent + 1, depth - 1))
    return lines


def _render_summary(title: str, pointer: str, depth: int = 2) -> str:
    repo = get_schema_repository()
    fields = repo.summarize_properties("tidas_processes.json", pointer)
    lines = [title]
    lines.extend(_format_fields(fields, indent=1, depth=depth))
    return "\n".join(lines)


@cache
def _build_section_prompt() -> str:
    repo = get_schema_repository()
    metadata_fields = [
        field
        for field in repo.summarize_properties("tidas_processes.json", "/properties/processDataSet")
        if field.name.startswith("@")
    ]
    metadata_lines = ["processDataSet metadata (auto-populated if omitted):"]
    metadata_lines.extend(_format_fields(metadata_fields, indent=1, depth=1))
    metadata = "\n".join(metadata_lines)
    process_info = _render_summary(
        "processInformation fields:",
        "/properties/processDataSet/properties/processInformation",
        depth=2,
    )
    modelling = _render_summary(
        "modellingAndValidation fields:",
        "/properties/processDataSet/properties/modellingAndValidation",
        depth=2,
    )
    administrative = _render_summary(
        "administrativeInformation fields:",
        "/properties/processDataSet/properties/administrativeInformation",
        depth=2,
    )
    exchanges = _render_summary(
        "exchanges.exchange fields:",
        "/properties/processDataSet/properties/exchanges",
        depth=2,
    )
    return (
        "You are an expert LCA analyst. Extract structured content that conforms to the "
        "TIDAS ILCD `processDataSet` schema. Return JSON with a single key `processDataSet` "
        "matching the schema excerpts below. Only include fields supported by the schema and "
        "omit entries that are not supported by evidence in the paper.\n\n"
        f"{metadata}\n\n"
        f"{process_info}\n\n"
        f"{modelling}\n\n"
        f"{administrative}\n\n"
        f"{exchanges}\n\n"
        "Ensure the JSON is valid. Do not wrap the result in Markdown or a code block."
    )


SECTION_PROMPT = _build_section_prompt()

CLASSIFICATION_PROMPT = (
    "Derive the ISIC classification path for the process. Return a JSON array to populate "
    "`dataSetInformation.classificationInformation.common:classification.common:class`, "
    "where each object contains '@level' (string), '@classId', and '#text'. Levels should "
    "progress sequentially from '1'."
)

LOCATION_PROMPT = (
    "Normalize the process geography for the schema field "
    "`processInformation.geography.locationOfOperationSupplyOrProduction`. Return JSON with "
    "keys 'code' (ISO country/region identifier) and 'description' (short context)."
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
