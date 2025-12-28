"""High-level facade and LangGraph assembly for building processes from a reference flow."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypedDict
from uuid import UUID, uuid4

from langgraph.graph import END, StateGraph
from tidas_sdk import create_process
from tidas_sdk.core.multilang import MultiLangList
from tidas_sdk.entities.utils import default_timestamp
from tidas_sdk.generated.tidas_data_types import GlobalReferenceTypeVariant0
from tidas_sdk.generated.tidas_processes import (
    CommonClassItemOption0,
    ComplianceDeclarationsComplianceOption0,
    DataSetInformationClassificationInformationCommonClassification,
    ExchangesExchangeItem,
    ModellingAndValidationValidationReview,
    ProcessDataSetAdministrativeInformationCommonCommissionerAndGoal,
    ProcessDataSetAdministrativeInformationDataEntryBy,
    ProcessDataSetAdministrativeInformationPublicationAndOwnership,
    ProcessDataSetModellingAndValidationComplianceDeclarations,
    ProcessDataSetModellingAndValidationDataSourcesTreatmentAndRepresentativeness,
    ProcessDataSetModellingAndValidationLCIMethodAndAllocation,
    ProcessDataSetModellingAndValidationValidation,
    ProcessDataSetProcessInformationDataSetInformation,
    ProcessDataSetProcessInformationGeography,
    ProcessDataSetProcessInformationQuantitativeReference,
    ProcessDataSetProcessInformationTechnology,
    ProcessDataSetProcessInformationTime,
    Processes,
    ProcessesProcessDataSet,
    ProcessesProcessDataSetAdministrativeInformation,
    ProcessesProcessDataSetExchanges,
    ProcessesProcessDataSetModellingAndValidation,
    ProcessesProcessDataSetProcessInformation,
    ProcessInformationDataSetInformationClassificationInformation,
    ProcessInformationDataSetInformationName,
    ProcessInformationGeographyLocationOfOperationSupplyOrProduction,
)

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.constants import (
    ILCD_FORMAT_SOURCE_SHORT_DESCRIPTION,
    ILCD_FORMAT_SOURCE_URI,
    ILCD_FORMAT_SOURCE_UUID,
    ILCD_FORMAT_SOURCE_VERSION,
)
from tiangong_lca_spec.core.json_utils import parse_json_response
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.mcp_client import MCPToolClient
from tiangong_lca_spec.core.models import FlowCandidate, FlowQuery
from tiangong_lca_spec.core.uris import build_local_dataset_uri, build_portal_uri
from tiangong_lca_spec.flow_alignment.selector import (
    CandidateSelector,
    LanguageModelProtocol,
    LLMCandidateSelector,
    NoFallbackCandidateSelector,
    SimilarityCandidateSelector,
)
from tiangong_lca_spec.flow_search import search_flows
from tiangong_lca_spec.process_extraction.extractors import ProcessClassifier
from tiangong_lca_spec.process_extraction.tidas_mapping import (
    COMPLIANCE_DEFAULT_PREFERENCES,
    ILCD_ENTRY_LEVEL_REFERENCE_ID,
    ILCD_ENTRY_LEVEL_REFERENCE_VERSION,
)
from tiangong_lca_spec.publishing.crud import DatabaseCrudClient
from tiangong_lca_spec.utils.translate import Translator

from .prompts import EXCHANGES_PROMPT, PROCESS_SPLIT_PROMPT, TECH_DESCRIPTION_PROMPT

LOGGER = get_logger(__name__)


def _search_scientific_references(
    query: str,
    *,
    mcp_client: MCPToolClient | None = None,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    """Search scientific literature using tiangong_kb_remote search_Sci_Tool.

    Args:
        query: Search query string describing the technical context
        mcp_client: Optional MCP client instance; creates new one if None
        top_k: Maximum number of references to return

    Returns:
        List of reference dictionaries with keys like 'content', 'metadata', 'score'
    """
    if not query or not query.strip():
        return []

    should_close_client = False
    if mcp_client is None:
        mcp_client = MCPToolClient()
        should_close_client = True

    try:
        result = mcp_client.invoke_json_tool(
            server_name="TianGong_KB_Remote",
            tool_name="Search_Sci_Tool",
            arguments={
                "query": query.strip(),
                "topK": top_k,
            },
        )

        if not result:
            return []

        # Extract references from result structure
        references: list[dict[str, Any]] = []
        if isinstance(result, dict):
            # Handle different possible response structures
            records = result.get("records") or result.get("results") or result.get("data") or []
            if isinstance(records, list):
                references = [item for item in records if isinstance(item, dict)]
        elif isinstance(result, list):
            references = [item for item in result if isinstance(item, dict)]

        LOGGER.info(
            "process_from_flow.search_references",
            query_preview=query[:100],
            count=len(references),
        )
        return references[:top_k]

    except Exception as exc:
        LOGGER.warning(
            "process_from_flow.search_references_failed",
            query_preview=query[:100],
            error=str(exc),
        )
        return []
    finally:
        if should_close_client and mcp_client:
            mcp_client.close()


def _format_references_for_prompt(references: list[dict[str, Any]]) -> str:
    """Format scientific references into a readable string for LLM prompts.

    Args:
        references: List of reference dictionaries from search_Sci_Tool

    Returns:
        Formatted string with numbered references
    """
    if not references:
        return ""

    lines = ["Scientific References:"]
    for idx, ref in enumerate(references, start=1):
        # Extract content and metadata
        content = ref.get("content") or ref.get("text") or ref.get("segment", {}).get("content") or ""
        metadata = ref.get("metadata") or {}

        # Build reference entry
        entry_parts = [f"[{idx}]"]
        if isinstance(metadata, dict):
            meta_str = metadata.get("meta") or ""
            if meta_str:
                entry_parts.append(f"Source: {meta_str}")

        if content:
            # Truncate very long content
            content_preview = content[:500] + "..." if len(content) > 500 else content
            entry_parts.append(f"Content: {content_preview}")

        lines.append(" ".join(entry_parts))
        lines.append("")  # Empty line between references

    return "\n".join(lines)


FlowSearchFn = Callable[[FlowQuery], tuple[list[FlowCandidate], list[object]]]

_FLOW_LABEL_PATTERN = re.compile(r"^f\\d+\\s*[:\\-]\\s*", re.IGNORECASE)
_FLOW_NAME_FIELDS = ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes")
_FLOW_QUALIFIER_FIELDS = ("flowProperties",)
_MEDIA_SUFFIXES = ("to air", "to water", "to soil")
_EMISSION_KEYWORDS = (
    "emission",
    "methane",
    "nitrous oxide",
    "ammonia",
    "carbon dioxide",
    "co2",
    "ch4",
    "n2o",
    "dust",
    "particulate",
    "pm",
    "so2",
    "nox",
    "voc",
)
_WATER_KEYWORDS = ("water", "wastewater", "runoff", "leaching", "leachate", "effluent", "drainage")
_SOIL_KEYWORDS = ("soil", "land", "ground", "field", "sediment")


class ProcessFromFlowState(TypedDict, total=False):
    flow_path: str
    flow_dataset: dict[str, Any]
    flow_summary: dict[str, Any]
    operation: str
    stop_after: str
    technical_description: str
    assumptions: list[str]
    scope: str
    technology_routes: list[dict[str, Any]]
    process_routes: list[dict[str, Any]]
    selected_route_id: str
    processes: list[dict[str, Any]]
    process_exchanges: list[dict[str, Any]]
    matched_process_exchanges: list[dict[str, Any]]
    process_datasets: list[dict[str, Any]]
    step_markers: dict[str, bool]


def _ensure_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = parse_json_response(value)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("Expected a JSON object")


def _language_entry(text: str, lang: str = "en") -> dict[str, str]:
    return {"@xml:lang": lang, "#text": text}


def _normalize_uuid(value: str | None) -> str:
    if not value:
        return str(uuid4())
    try:
        return str(UUID(str(value)))
    except Exception:
        return str(uuid4())


def _pick_lang(value: Any, *, prefer: str = "en") -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, dict):
        text = value.get("#text") or value.get("text") or value.get("@value")
        if isinstance(text, str) and text.strip():
            return text.strip()
        for nested in value.values():
            candidate = _pick_lang(nested, prefer=prefer)
            if candidate:
                return candidate
        return None
    if isinstance(value, list):
        preferred = None
        fallback = None
        for item in value:
            if isinstance(item, dict):
                lang = str(item.get("@xml:lang") or "").strip().lower()
                text = item.get("#text")
                if isinstance(text, str) and text.strip():
                    if lang == prefer.lower() and preferred is None:
                        preferred = text.strip()
                    if fallback is None:
                        fallback = text.strip()
            else:
                if fallback is None:
                    fallback = _pick_lang(item, prefer=prefer)
        return preferred or fallback
    return str(value).strip() or None


def _flow_summary(flow_dataset: dict[str, Any]) -> dict[str, Any]:
    flow = flow_dataset.get("flowDataSet") if isinstance(flow_dataset.get("flowDataSet"), dict) else flow_dataset
    info = flow.get("flowInformation", {}) if isinstance(flow, dict) else {}
    data_info = info.get("dataSetInformation", {}) if isinstance(info, dict) else {}
    name_block = data_info.get("name", {}) if isinstance(data_info, dict) else {}
    admin = flow.get("administrativeInformation", {}) if isinstance(flow, dict) else {}
    publication = admin.get("publicationAndOwnership", {}) if isinstance(admin, dict) else {}

    base_name_en = _pick_lang(name_block.get("baseName"), prefer="en")
    base_name_zh = _pick_lang(name_block.get("baseName"), prefer="zh")
    treatment_en = _pick_lang(name_block.get("treatmentStandardsRoutes"), prefer="en")
    treatment_zh = _pick_lang(name_block.get("treatmentStandardsRoutes"), prefer="zh")
    mix_en = _pick_lang(name_block.get("mixAndLocationTypes"), prefer="en")
    mix_zh = _pick_lang(name_block.get("mixAndLocationTypes"), prefer="zh")
    general_en = _pick_lang(data_info.get("common:generalComment"), prefer="en")
    general_zh = _pick_lang(data_info.get("common:generalComment"), prefer="zh")

    classification: list[dict[str, Any]] = []
    classification_info = data_info.get("classificationInformation") if isinstance(data_info, dict) else None
    if isinstance(classification_info, dict):
        carrier = classification_info.get("common:classification")
        if isinstance(carrier, dict):
            classes = carrier.get("common:class")
            if isinstance(classes, list):
                classification = [item for item in classes if isinstance(item, dict)]

    return {
        "uuid": str(data_info.get("common:UUID") or "").strip() or None,
        "version": str(publication.get("common:dataSetVersion") or "").strip() or None,
        "base_name_en": base_name_en,
        "base_name_zh": base_name_zh,
        "treatment_en": treatment_en,
        "treatment_zh": treatment_zh,
        "mix_en": mix_en,
        "mix_zh": mix_zh,
        "general_comment_en": general_en,
        "general_comment_zh": general_zh,
        "classification": classification,
    }


def _as_multilang_list(value: Any, *, default_lang: str = "en") -> MultiLangList:
    if isinstance(value, MultiLangList):
        return value
    if value is None:
        return MultiLangList()
    if isinstance(value, list):
        out: list[dict[str, str]] = []
        for item in value:
            if isinstance(item, dict) and "#text" in item:
                out.append(_language_entry(str(item.get("#text") or ""), str(item.get("@xml:lang") or default_lang) or default_lang))
            else:
                text = str(item).strip()
                if text:
                    out.append(_language_entry(text, default_lang))
        return MultiLangList([entry for entry in out if entry.get("#text")])
    if isinstance(value, dict) and "#text" in value:
        text = str(value.get("#text") or "").strip()
        if not text:
            return MultiLangList()
        lang = str(value.get("@xml:lang") or default_lang) or default_lang
        return MultiLangList([_language_entry(text, lang)])
    text = str(value).strip()
    return MultiLangList([_language_entry(text, default_lang)]) if text else MultiLangList()


def _global_reference(
    *,
    ref_type: str,
    ref_object_id: str,
    version: str,
    uri: str,
    short_description: Any,
    extra_fields: dict[str, Any] | None = None,
) -> GlobalReferenceTypeVariant0:
    reference = GlobalReferenceTypeVariant0(
        type=ref_type,
        ref_object_id=ref_object_id,
        version=version,
        uri=uri,
        common_short_description=_as_multilang_list(short_description),
    )
    if extra_fields:
        for key, value in extra_fields.items():
            setattr(reference, key, value)
    return reference


def _as_classification_items(entries: list[dict[str, Any]]) -> list[CommonClassItemOption0]:
    items: list[CommonClassItemOption0] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        level = str(entry.get("@level") or entry.get("level") or "0").strip()
        if not (level.isdigit() and len(level) == 1):
            level = "0"
        class_id = str(entry.get("@classId") or entry.get("class_id") or entry.get("classId") or "C").strip() or "C"
        text = str(entry.get("#text") or entry.get("text") or "").strip()
        if not text:
            continue
        items.append(CommonClassItemOption0(level=level, class_id=class_id, text=text))
    if not items:
        items = [CommonClassItemOption0(level="0", class_id="C", text="Manufacturing")]
    return items


def _contact_reference() -> GlobalReferenceTypeVariant0:
    ref_object_id = "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8"
    version = "01.00.000"
    return _global_reference(
        ref_type="contact data set",
        ref_object_id=ref_object_id,
        version=version,
        uri=build_local_dataset_uri("contact data set", ref_object_id, version),
        short_description=[
            _language_entry("Tiangong LCA Data Working Group", "en"),
            _language_entry("天工LCA数据团队", "zh"),
        ],
    )


def _entry_level_compliance_reference() -> GlobalReferenceTypeVariant0:
    return _global_reference(
        ref_type="source data set",
        ref_object_id=ILCD_ENTRY_LEVEL_REFERENCE_ID,
        version=ILCD_ENTRY_LEVEL_REFERENCE_VERSION,
        uri=build_local_dataset_uri(
            "source data set",
            ILCD_ENTRY_LEVEL_REFERENCE_ID,
            ILCD_ENTRY_LEVEL_REFERENCE_VERSION,
        ),
        short_description=[_language_entry("ILCD Data Network - Entry-level", "en")],
    )


def _compliance_declarations() -> ProcessDataSetModellingAndValidationComplianceDeclarations:
    mapped_fields = {
        "common_approval_of_overall_compliance": "common:approvalOfOverallCompliance",
        "common_nomenclature_compliance": "common:nomenclatureCompliance",
        "common_methodological_compliance": "common:methodologicalCompliance",
        "common_review_compliance": "common:reviewCompliance",
        "common_documentation_compliance": "common:documentationCompliance",
        "common_quality_compliance": "common:qualityCompliance",
    }
    values: dict[str, str] = {}
    for field_name, source_key in mapped_fields.items():
        values[field_name] = COMPLIANCE_DEFAULT_PREFERENCES.get(source_key) or "Not defined"
    compliance = ComplianceDeclarationsComplianceOption0(
        common_reference_to_compliance_system=_entry_level_compliance_reference(),
        **values,
    )
    return ProcessDataSetModellingAndValidationComplianceDeclarations(compliance=compliance)


def _dataset_format_reference() -> GlobalReferenceTypeVariant0:
    return _global_reference(
        ref_type="source data set",
        ref_object_id=ILCD_FORMAT_SOURCE_UUID,
        version=ILCD_FORMAT_SOURCE_VERSION,
        uri=ILCD_FORMAT_SOURCE_URI,
        short_description=[ILCD_FORMAT_SOURCE_SHORT_DESCRIPTION],
    )


def _candidate_reference(
    candidate: FlowCandidate,
    *,
    translator: Translator | None = None,
    short_description: Any | None = None,
) -> GlobalReferenceTypeVariant0:
    version = candidate.version or "01.01.000"
    uuid_value = _normalize_uuid(candidate.uuid)
    uri = build_portal_uri("flow", uuid_value, version)
    name = str(candidate.base_name or "Unnamed flow").strip() or "Unnamed flow"
    short_desc = short_description or _build_multilang_entries(name, translator=translator)
    if not short_desc:
        short_desc = [_language_entry(name, "en")]
    return _global_reference(
        ref_type="flow data set",
        ref_object_id=uuid_value,
        version=version,
        uri=uri,
        short_description=short_desc,
    )


def _placeholder_flow_reference(name: str, *, translator: Translator | None = None) -> GlobalReferenceTypeVariant0:
    identifier = _normalize_uuid(None)
    version = "00.00.000"
    uri = build_portal_uri("flow", identifier, version)
    short_desc = _build_multilang_entries(name or "Unnamed flow", translator=translator)
    if not short_desc:
        short_desc = [_language_entry(name or "Unnamed flow", "en")]
    return _global_reference(
        ref_type="flow data set",
        ref_object_id=identifier,
        version=version,
        uri=uri,
        short_description=short_desc,
        extra_fields={"unmatched:placeholder": True},
    )


def _default_exchange_amount() -> str:
    return "1.0"


def _reference_direction(operation: str | None) -> str:
    op = str(operation or "produce").strip().lower()
    if op in {"treat", "dispose", "disposal", "treatment"}:
        return "Input"
    return "Output"


def _build_multilang_entries(
    text: str | None,
    *,
    translator: Translator | None = None,
    zh_text: str | None = None,
) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    base = str(text).strip() if text else ""
    if base:
        entries.append(_language_entry(base, "en"))
    zh_value = str(zh_text).strip() if zh_text else ""
    if not zh_value and translator and base:
        translated = translator.translate(base, "zh")
        if translated:
            zh_value = translated.strip()
    if zh_value and zh_value != base:
        entries.append(_language_entry(zh_value, "zh"))
    return entries


def _merge_lang_maps(target: dict[str, list[str]], source: dict[str, list[str]]) -> dict[str, list[str]]:
    for lang, values in source.items():
        bucket = target.setdefault(lang, [])
        for value in values:
            if value and value not in bucket:
                bucket.append(value)
    return target


def _extract_lang_texts(value: Any) -> dict[str, list[str]]:
    if value is None:
        return {}
    if isinstance(value, list):
        merged: dict[str, list[str]] = {}
        for item in value:
            merged = _merge_lang_maps(merged, _extract_lang_texts(item))
        return merged
    if isinstance(value, dict):
        text = value.get("#text") or value.get("text") or value.get("@value")
        if isinstance(text, str) and text.strip():
            lang = str(value.get("@xml:lang") or value.get("@lang") or "en").strip() or "en"
            return {lang: [text.strip()]}
        merged: dict[str, list[str]] = {}
        for nested in value.values():
            merged = _merge_lang_maps(merged, _extract_lang_texts(nested))
        return merged
    text = str(value).strip()
    return {"en": [text]} if text else {}


def _field_text_by_lang(value: Any, *, separator: str = ", ") -> dict[str, str]:
    lang_map = _extract_lang_texts(value)
    return {lang: separator.join([text for text in values if text]) for lang, values in lang_map.items() if any(values)}


def _compose_flow_name_parts(name_block: dict[str, Any]) -> dict[str, str]:
    parts_by_lang: dict[str, list[str]] = {}
    for field in _FLOW_NAME_FIELDS:
        for lang, text in _field_text_by_lang(name_block.get(field)).items():
            bucket = parts_by_lang.setdefault(lang, [])
            if text and text not in bucket:
                bucket.append(text)
    for qualifier_field in _FLOW_QUALIFIER_FIELDS:
        qualifier_map = _field_text_by_lang(name_block.get(qualifier_field))
        if qualifier_map:
            for lang, text in qualifier_map.items():
                bucket = parts_by_lang.setdefault(lang, [])
                if text and text not in bucket:
                    bucket.append(text)
            break
    return {lang: "; ".join(parts) for lang, parts in parts_by_lang.items() if parts}


def _flow_short_description_from_dataset(flow_dataset: dict[str, Any]) -> list[dict[str, str]] | None:
    flow = flow_dataset.get("flowDataSet") if isinstance(flow_dataset.get("flowDataSet"), dict) else flow_dataset
    info = flow.get("flowInformation", {}) if isinstance(flow, dict) else {}
    data_info = info.get("dataSetInformation", {}) if isinstance(info, dict) else {}
    name_block = data_info.get("name") if isinstance(data_info, dict) else None
    if not isinstance(name_block, dict):
        return None
    parts_by_lang = _compose_flow_name_parts(name_block)
    if not parts_by_lang:
        return None
    en_text = parts_by_lang.get("en")
    zh_text = parts_by_lang.get("zh")
    if en_text:
        entries = [_language_entry(en_text, "en")]
        if zh_text:
            entries.append(_language_entry(zh_text, "zh"))
        return entries
    if zh_text:
        return [_language_entry(zh_text, "zh")]
    for lang, text in parts_by_lang.items():
        if text:
            return [_language_entry(text, lang)]
    return None


def _flow_dataset_version(flow_dataset: dict[str, Any]) -> str | None:
    flow = flow_dataset.get("flowDataSet") if isinstance(flow_dataset.get("flowDataSet"), dict) else flow_dataset
    admin = flow.get("administrativeInformation", {}) if isinstance(flow, dict) else {}
    publication = admin.get("publicationAndOwnership", {}) if isinstance(admin, dict) else {}
    version = str(publication.get("common:dataSetVersion") or "").strip()
    return version or None


def _update_step_markers(state: ProcessFromFlowState, step_name: str) -> dict[str, bool]:
    markers = dict(state.get("step_markers") or {})
    markers[step_name] = True
    return markers


def _clean_string_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, (str, int, float)):
        text = str(values).strip()
        return [text] if text else []
    if isinstance(values, list):
        cleaned: list[str] = []
        for item in values:
            text = str(item).strip()
            if text:
                cleaned.append(text)
        return cleaned
    return []


def _normalize_quantitative_reference(value: Any, fallback_flow_name: str | None) -> str:
    text = str(value).strip() if value is not None else ""
    if not text:
        fallback = fallback_flow_name or "reference flow"
        return f"1 unit of {fallback}"
    if any(ch.isdigit() for ch in text):
        return text
    fallback = fallback_flow_name or text
    return f"1 unit of {fallback}"


def _strip_flow_label(value: str) -> str:
    return _FLOW_LABEL_PATTERN.sub("", value).strip()


def _label_flows(values: list[str], *, prefix: str = "f") -> list[str]:
    labeled: list[str] = []
    for idx, value in enumerate(values, start=1):
        raw = _strip_flow_label(value)
        if not raw:
            continue
        labeled.append(f"{prefix}{idx}: {raw}")
    return labeled


def _normalize_flow_type(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    normalized = {
        "elementary flow": "elementary",
        "emission": "elementary",
        "resource": "elementary",
        "waste flow": "waste",
        "service flow": "service",
    }.get(text, text)
    if normalized in {"product", "elementary", "waste", "service"}:
        return normalized
    return None


def _infer_flow_type(name: str, *, direction: str, is_reference_flow: bool) -> str:
    if is_reference_flow:
        return "product"
    lower = name.lower().strip()
    if any(token in lower for token in _MEDIA_SUFFIXES):
        return "elementary"
    if "labor" in lower or "labour" in lower:
        return "service"
    if direction == "Output" and ("waste" in lower or "residue" in lower or "sludge" in lower):
        return "waste"
    if direction == "Input" and any(token in lower for token in _WATER_KEYWORDS + _SOIL_KEYWORDS):
        return "elementary"
    if direction == "Output" and any(token in lower for token in _EMISSION_KEYWORDS + _WATER_KEYWORDS):
        return "elementary"
    return "product"


def _infer_media_suffix(name: str) -> str | None:
    lower = name.lower()
    if any(token in lower for token in _WATER_KEYWORDS):
        return "water"
    if any(token in lower for token in _SOIL_KEYWORDS):
        return "soil"
    if any(token in lower for token in _EMISSION_KEYWORDS):
        return "air"
    return None


def _ensure_media_suffix(name: str, *, direction: str, flow_type: str, is_reference_flow: bool) -> str:
    if flow_type != "elementary":
        return name
    if is_reference_flow:
        return name
    if direction != "Output":
        return name
    lower = name.lower()
    if any(suffix in lower for suffix in _MEDIA_SUFFIXES):
        return name
    medium = _infer_media_suffix(name)
    return f"{name}, to {medium}" if medium else name


def _build_search_hints(name: str) -> list[str]:
    hints: set[str] = set()
    lower = name.lower()
    if lower == "water, fresh":
        hints.add("Freshwater")
    if "freshwater" in lower:
        hints.add("Water, fresh")
    if "diesel" in lower:
        hints.add("Diesel fuel")
    if "electricity" in lower:
        hints.add("Power, electric")
    if "nitrogen fertilizer" in lower:
        hints.add("Nitrogenous fertilizer")
    if "phosphate fertilizer" in lower or "p2o5" in lower:
        hints.add("Phosphate fertilizer")
    if "potash" in lower or "k2o" in lower:
        hints.add("Potassium fertilizer")
    if "labor" in lower:
        hints.add("Labour")
    if "methane" in lower:
        hints.add("CH4")
    if "pesticide" in lower:
        hints.add("Pesticide")
    cleaned = [hint for hint in hints if hint.lower() != lower]
    return sorted(cleaned)


def _normalize_route_processes(
    processes: list[dict[str, Any]],
    *,
    flow_summary: dict[str, Any],
    route_name: str,
) -> list[dict[str, Any]]:
    flow_name = str(flow_summary.get("base_name_en") or "reference flow").strip() or "reference flow"
    normalized: list[dict[str, Any]] = []

    for idx, proc in enumerate(processes, start=1):
        process_id = str(proc.get("process_id") or "").strip() or f"P{idx}"
        is_reference_flow_process = bool(proc.get("is_reference_flow_process"))
        name_parts = proc.get("name_parts") if isinstance(proc.get("name_parts"), dict) else {}
        structure = proc.get("structure") if isinstance(proc.get("structure"), dict) else {}
        exchange_keywords = proc.get("exchange_keywords") if isinstance(proc.get("exchange_keywords"), dict) else {}

        structure_inputs = [_strip_flow_label(val) for val in _clean_string_list(structure.get("inputs"))]
        structure_outputs = [_strip_flow_label(val) for val in _clean_string_list(structure.get("outputs"))]
        structure_assumptions = _clean_string_list(structure.get("assumptions"))
        exchange_inputs = [_strip_flow_label(val) for val in _clean_string_list(exchange_keywords.get("inputs"))]
        exchange_outputs = [_strip_flow_label(val) for val in _clean_string_list(exchange_keywords.get("outputs"))]

        reference_flow_name = str(proc.get("reference_flow_name") or proc.get("referenceFlowName") or "").strip()
        if is_reference_flow_process:
            reference_flow_name = flow_name
        if not reference_flow_name:
            candidate_outputs = exchange_outputs + structure_outputs
            if candidate_outputs:
                reference_flow_name = candidate_outputs[0]
        if not reference_flow_name:
            base_name_fallback = str(name_parts.get("base_name") or proc.get("name") or "").strip()
            reference_flow_name = f"intermediate product from {base_name_fallback}" if base_name_fallback else "intermediate product"

        if reference_flow_name and reference_flow_name not in structure_outputs:
            structure_outputs.insert(0, reference_flow_name)
        if reference_flow_name and reference_flow_name not in exchange_outputs:
            exchange_outputs.insert(0, reference_flow_name)

        base_name = str(name_parts.get("base_name") or "").strip()
        if not base_name:
            base_name = str(proc.get("name") or "").strip() or reference_flow_name
        treatment_and_route = str(name_parts.get("treatment_and_route") or route_name or "").strip()
        if not treatment_and_route:
            treatment_and_route = "Unspecified route"
        mix_and_location = str(name_parts.get("mix_and_location") or flow_summary.get("mix_en") or "Unspecified mix/location").strip()
        quantitative_reference = _normalize_quantitative_reference(name_parts.get("quantitative_reference"), reference_flow_name)

        name_parts = {
            "base_name": base_name,
            "treatment_and_route": treatment_and_route,
            "mix_and_location": mix_and_location,
            "quantitative_reference": quantitative_reference,
        }
        name = " | ".join([base_name, treatment_and_route, mix_and_location, quantitative_reference])

        description = str(proc.get("description") or "").strip()
        if not description and structure:
            tech = str(structure.get("technology") or "").strip()
            inputs = ", ".join([val for val in structure_inputs if val])
            outputs = ", ".join([val for val in structure_outputs if val])
            boundary = str(structure.get("boundary") or "").strip()
            assumptions = ", ".join([val for val in structure_assumptions if val])
            parts = []
            if tech:
                parts.append(f"Technology: {tech}")
            if inputs:
                parts.append(f"Inputs: {inputs}")
            if outputs:
                parts.append(f"Outputs: {outputs}")
            if boundary:
                parts.append(f"Boundary: {boundary}")
            if assumptions:
                parts.append(f"Assumptions: {assumptions}")
            description = "; ".join(parts)

        normalized.append(
            {
                "process_id": process_id,
                "name": name,
                "description": description,
                "is_reference_flow_process": is_reference_flow_process,
                "reference_flow_name": reference_flow_name,
                "name_parts": name_parts,
                "structure": {
                    **structure,
                    "inputs": structure_inputs,
                    "outputs": structure_outputs,
                    "assumptions": structure_assumptions,
                },
                "exchange_keywords": {
                    "inputs": exchange_inputs,
                    "outputs": exchange_outputs,
                },
            }
        )

    for idx in range(len(normalized) - 1):
        chain_flow = normalized[idx].get("reference_flow_name")
        if not chain_flow:
            continue
        next_proc = normalized[idx + 1]
        next_structure = next_proc.get("structure") if isinstance(next_proc.get("structure"), dict) else {}
        next_exchange = next_proc.get("exchange_keywords") if isinstance(next_proc.get("exchange_keywords"), dict) else {}
        next_inputs = _clean_string_list(next_structure.get("inputs"))
        if chain_flow not in next_inputs:
            next_inputs.insert(0, chain_flow)
        next_exchange_inputs = _clean_string_list(next_exchange.get("inputs"))
        if chain_flow not in next_exchange_inputs:
            next_exchange_inputs.insert(0, chain_flow)
        next_proc["structure"] = {**next_structure, "inputs": next_inputs}
        next_proc["exchange_keywords"] = {**next_exchange, "inputs": next_exchange_inputs}

    for proc in normalized:
        structure = proc.get("structure") if isinstance(proc.get("structure"), dict) else {}
        inputs = _clean_string_list(structure.get("inputs"))
        outputs = _clean_string_list(structure.get("outputs"))
        proc["structure"] = {
            **structure,
            "inputs": _label_flows(inputs, prefix="f"),
            "outputs": _label_flows(outputs, prefix="f"),
        }

    return normalized


def _build_langgraph(
    *,
    llm: LanguageModelProtocol | None,
    settings: Settings,
    flow_search_fn: FlowSearchFn,
    selector: CandidateSelector,
    translator: Translator | None,
    mcp_client: MCPToolClient | None = None,
) -> Any:
    graph = StateGraph(ProcessFromFlowState)
    # Create or use provided MCP client for scientific literature search
    use_mcp_client = mcp_client

    def load_flow(state: ProcessFromFlowState) -> ProcessFromFlowState:
        path = Path(state["flow_path"])
        dataset = json.loads(path.read_text(encoding="utf-8"))
        summary = _flow_summary(dataset)
        LOGGER.info("process_from_flow.load_flow", path=str(path), uuid=summary.get("uuid"))
        return {"flow_dataset": dataset, "flow_summary": summary}

    def describe_technology(state: ProcessFromFlowState) -> ProcessFromFlowState:
        if state.get("technology_routes"):
            return {"step_markers": _update_step_markers(state, "step1")}
        if state.get("technical_description"):
            route = {
                "route_id": "R1",
                "route_name": "Default route",
                "route_summary": str(state.get("technical_description") or "").strip(),
                "key_unit_processes": [],
                "key_inputs": [],
                "key_outputs": [],
                "assumptions": [str(item) for item in (state.get("assumptions") or []) if str(item).strip()],
                "scope": str(state.get("scope") or "").strip(),
            }
            return {
                "technology_routes": [route],
                "step_markers": _update_step_markers(state, "step1"),
            }
        if llm is None:
            summary = state.get("flow_summary") or {}
            base_name = summary.get("base_name_en") or "reference flow"
            operation = str(state.get("operation") or "produce").strip().lower()
            verb = "treatment/disposal" if operation in {"treat", "dispose", "disposal", "treatment"} else "production"
            route_summary = f"Generic {verb} of {base_name}. Assumptions: unspecified technology route; generic foreground process."
            route = {
                "route_id": "R1",
                "route_name": f"Typical {verb} route",
                "route_summary": route_summary,
                "key_unit_processes": [f"{verb.title()} of {base_name}"],
                "key_inputs": [],
                "key_outputs": [base_name],
                "assumptions": ["No quantified inventory available; amounts are placeholders."],
                "scope": "Generic scope",
            }
            return {
                "technical_description": route_summary,
                "assumptions": route["assumptions"],
                "scope": route["scope"],
                "technology_routes": [route],
                "step_markers": _update_step_markers(state, "step1"),
            }
        # Search for scientific references before invoking LLM
        flow_summary = state.get("flow_summary") or {}
        flow_name = flow_summary.get("base_name_en") or flow_summary.get("base_name_zh") or "reference flow"
        operation = state.get("operation") or "produce"

        # Build search query for scientific literature
        search_query = f"{operation} {flow_name} technology process route LCA life cycle assessment"
        references = _search_scientific_references(search_query, mcp_client=use_mcp_client, top_k=5)
        references_text = _format_references_for_prompt(references)

        # Build prompt with references
        enhanced_prompt = TECH_DESCRIPTION_PROMPT
        if references_text:
            enhanced_prompt = f"{TECH_DESCRIPTION_PROMPT}\n\n" f"Use the following scientific references to inform your analysis:\n" f"{references_text}\n"

        payload = {
            "prompt": enhanced_prompt,
            "context": {
                "operation": operation,
                "flow": flow_summary,
            },
            "response_format": {"type": "json_object"},
        }
        raw = llm.invoke(payload)
        data = _ensure_dict(raw)
        routes = data.get("routes")
        cleaned_routes: list[dict[str, Any]] = []
        if isinstance(routes, list):
            for idx, route in enumerate(routes, start=1):
                if not isinstance(route, dict):
                    continue
                route_id = str(route.get("route_id") or route.get("routeId") or "").strip() or f"R{idx}"
                route_name = str(route.get("route_name") or route.get("routeName") or "").strip() or f"Route {route_id}"
                route_summary = str(route.get("route_summary") or route.get("routeSummary") or "").strip()
                key_unit_processes = [str(item).strip() for item in (route.get("key_unit_processes") or route.get("keyUnitProcesses") or []) if str(item).strip()]
                key_inputs = [str(item).strip() for item in (route.get("key_inputs") or route.get("keyInputs") or []) if str(item).strip()]
                key_outputs = [str(item).strip() for item in (route.get("key_outputs") or route.get("keyOutputs") or []) if str(item).strip()]
                assumptions = [str(item).strip() for item in (route.get("assumptions") or []) if str(item).strip()]
                scope = str(route.get("scope") or "").strip()
                cleaned_routes.append(
                    {
                        "route_id": route_id,
                        "route_name": route_name,
                        "route_summary": route_summary,
                        "key_unit_processes": key_unit_processes,
                        "key_inputs": key_inputs,
                        "key_outputs": key_outputs,
                        "assumptions": assumptions,
                        "scope": scope,
                    }
                )
        if cleaned_routes:
            primary = cleaned_routes[0]
            return {
                "technical_description": primary.get("route_summary") or "",
                "assumptions": primary.get("assumptions") or [],
                "scope": primary.get("scope") or "",
                "technology_routes": cleaned_routes,
                "step_markers": _update_step_markers(state, "step1"),
            }
        technical_description = str(data.get("technical_description") or "").strip()
        assumptions = [str(item) for item in (data.get("assumptions") or []) if str(item).strip()]
        scope = str(data.get("scope") or "").strip()
        fallback_route = {
            "route_id": "R1",
            "route_name": "Default route",
            "route_summary": technical_description,
            "key_unit_processes": [],
            "key_inputs": [],
            "key_outputs": [],
            "assumptions": assumptions,
            "scope": scope,
        }
        return {
            "technical_description": technical_description,
            "assumptions": assumptions,
            "scope": scope,
            "technology_routes": [fallback_route],
            "step_markers": _update_step_markers(state, "step1"),
        }

    def split_processes(state: ProcessFromFlowState) -> ProcessFromFlowState:
        if state.get("processes"):
            if not state.get("process_routes"):
                normalized = _normalize_route_processes(
                    [item for item in (state.get("processes") or []) if isinstance(item, dict)],
                    flow_summary=state.get("flow_summary") or {},
                    route_name="Default route",
                )
                default_route = {
                    "route_id": "R1",
                    "route_name": "Default route",
                    "processes": normalized,
                }
                return {
                    "process_routes": [default_route],
                    "selected_route_id": "R1",
                    "processes": normalized,
                    "step_markers": _update_step_markers(state, "step2"),
                }
            return {"step_markers": _update_step_markers(state, "step2")}
        if llm is None:
            summary = state.get("flow_summary") or {}
            flow_name = summary.get("base_name_en") or "reference flow"
            operation = str(state.get("operation") or "produce").strip().lower()
            prefix = "Treatment of" if operation in {"treat", "dispose", "disposal", "treatment"} else "Production of"
            name_parts = {
                "base_name": f"{prefix} {flow_name}",
                "treatment_and_route": "Generic route",
                "mix_and_location": summary.get("mix_en") or "Unspecified mix/location",
                "quantitative_reference": _normalize_quantitative_reference(None, flow_name),
            }
            process_entry = {
                "process_id": "P1",
                "name": f"{name_parts['base_name']} | {name_parts['treatment_and_route']} | {name_parts['mix_and_location']} | {name_parts['quantitative_reference']}",
                "description": state.get("technical_description") or "",
                "is_reference_flow_process": True,
                "reference_flow_name": flow_name,
                "name_parts": name_parts,
                "structure": {
                    "technology": state.get("technical_description") or "",
                    "inputs": [],
                    "outputs": [flow_name],
                    "boundary": state.get("scope") or "",
                    "assumptions": state.get("assumptions") or [],
                },
                "exchange_keywords": {"inputs": [], "outputs": [flow_name]},
            }
            return {
                "processes": [process_entry],
                "process_routes": [{"route_id": "R1", "route_name": "Default route", "processes": [process_entry]}],
                "selected_route_id": "R1",
                "step_markers": _update_step_markers(state, "step2"),
            }
        # Search for scientific references for process splitting
        flow_summary = state.get("flow_summary") or {}
        flow_name = flow_summary.get("base_name_en") or flow_summary.get("base_name_zh") or "reference flow"
        tech_desc = state.get("technical_description") or ""
        operation = state.get("operation") or "produce"

        # Build search query focusing on unit processes and process decomposition
        search_query = f"{flow_name} {operation} unit process decomposition inventory LCA"
        if tech_desc:
            # Add key technical terms from description
            tech_preview = tech_desc[:100].strip()
            search_query = f"{search_query} {tech_preview}"

        references = _search_scientific_references(search_query, mcp_client=use_mcp_client, top_k=5)
        references_text = _format_references_for_prompt(references)

        # Build enhanced prompt with references
        enhanced_prompt = PROCESS_SPLIT_PROMPT
        if references_text:
            enhanced_prompt = f"{PROCESS_SPLIT_PROMPT}\n\n" f"Use the following scientific references to inform your process decomposition:\n" f"{references_text}\n"

        payload = {
            "prompt": enhanced_prompt,
            "context": {
                "flow": flow_summary,
                "technical_description": tech_desc,
                "routes": state.get("technology_routes") or [],
                "operation": operation,
            },
            "response_format": {"type": "json_object"},
        }
        raw = llm.invoke(payload)
        data = _ensure_dict(raw)
        routes = data.get("routes")
        cleaned_routes: list[dict[str, Any]] = []
        if isinstance(routes, list):
            for route_idx, route in enumerate(routes, start=1):
                if not isinstance(route, dict):
                    continue
                route_id = str(route.get("route_id") or route.get("routeId") or "").strip() or f"R{route_idx}"
                route_name = str(route.get("route_name") or route.get("routeName") or "").strip() or f"Route {route_id}"
                processes = route.get("processes") or []
                if not isinstance(processes, list):
                    continue
                cleaned_processes: list[dict[str, Any]] = []
                for proc_idx, item in enumerate(processes, start=1):
                    if not isinstance(item, dict):
                        continue
                    process_id = str(item.get("process_id") or item.get("processId") or "").strip() or f"P{proc_idx}"
                    name_parts = item.get("name_parts") if isinstance(item.get("name_parts"), dict) else {}
                    name = str(item.get("name") or "").strip()
                    description = str(item.get("description") or "").strip()
                    structure = item.get("structure") if isinstance(item.get("structure"), dict) else {}
                    exchange_keywords = item.get("exchange_keywords") if isinstance(item.get("exchange_keywords"), dict) else {}
                    reference_flow_name = str(item.get("reference_flow_name") or item.get("referenceFlowName") or "").strip()
                    cleaned_processes.append(
                        {
                            "process_id": process_id,
                            "name": name,
                            "description": description,
                            "is_reference_flow_process": bool(item.get("is_reference_flow_process")),
                            "reference_flow_name": reference_flow_name,
                            "name_parts": name_parts,
                            "structure": structure,
                            "exchange_keywords": exchange_keywords,
                        }
                    )
                if not cleaned_processes:
                    continue
                if sum(1 for proc in cleaned_processes if proc.get("is_reference_flow_process")) != 1:
                    for proc in cleaned_processes:
                        proc["is_reference_flow_process"] = False
                    cleaned_processes[-1]["is_reference_flow_process"] = True
                cleaned_processes = _normalize_route_processes(
                    cleaned_processes,
                    flow_summary=state.get("flow_summary") or {},
                    route_name=route_name,
                )
                cleaned_routes.append({"route_id": route_id, "route_name": route_name, "processes": cleaned_processes})
        selected_route_id = str(data.get("selected_route_id") or data.get("selectedRouteId") or "").strip()
        selected_route: dict[str, Any] | None = None
        if cleaned_routes:
            if selected_route_id:
                for route in cleaned_routes:
                    if route.get("route_id") == selected_route_id:
                        selected_route = route
                        break
            if selected_route is None:
                selected_route = cleaned_routes[0]
                selected_route_id = str(selected_route.get("route_id") or "")
        if selected_route:
            processes = selected_route.get("processes") or []
            tech_routes = state.get("technology_routes") or []
            selected_summary = None
            if isinstance(tech_routes, list):
                for route in tech_routes:
                    if isinstance(route, dict) and route.get("route_id") == selected_route_id:
                        selected_summary = route.get("route_summary")
                        break
            update: dict[str, Any] = {
                "process_routes": cleaned_routes,
                "selected_route_id": selected_route_id,
                "processes": processes,
                "step_markers": _update_step_markers(state, "step2"),
            }
            if selected_summary and not state.get("technical_description"):
                update["technical_description"] = str(selected_summary).strip()
            return update

        processes = data.get("processes")
        if not isinstance(processes, list):
            raise ValueError("LLM did not return routes[] or processes[]")
        cleaned: list[dict[str, Any]] = []
        for item in processes:
            if not isinstance(item, dict):
                continue
            process_id = str(item.get("process_id") or item.get("processId") or "").strip()
            if not process_id:
                continue
            cleaned.append(
                {
                    "process_id": process_id,
                    "name": str(item.get("name") or "").strip(),
                    "description": str(item.get("description") or "").strip(),
                    "is_reference_flow_process": bool(item.get("is_reference_flow_process")),
                }
            )
        if not cleaned:
            raise ValueError("No valid process entries returned by LLM")
        if sum(1 for proc in cleaned if proc.get("is_reference_flow_process")) != 1:
            cleaned[0]["is_reference_flow_process"] = True
            for proc in cleaned[1:]:
                proc["is_reference_flow_process"] = False
        return {"processes": cleaned, "step_markers": _update_step_markers(state, "step2")}

    def generate_exchanges(state: ProcessFromFlowState) -> ProcessFromFlowState:
        if state.get("process_exchanges"):
            return {"step_markers": _update_step_markers(state, "step3")}
        if llm is None:
            summary = state.get("flow_summary") or {}
            base_name = summary.get("base_name_en") or "reference flow"
            direction = _reference_direction(state.get("operation"))
            return {
                "process_exchanges": [
                    {
                        "process_id": "P1",
                        "exchanges": [
                            {
                                "exchangeDirection": direction,
                                "exchangeName": base_name,
                                "generalComment": summary.get("general_comment_en") or "",
                                "unit": None,
                                "amount": None,
                                "is_reference_flow": True,
                            }
                        ],
                    }
                ],
                "step_markers": _update_step_markers(state, "step3"),
            }
        # Search for scientific references for exchange generation
        flow_summary = state.get("flow_summary") or {}
        flow_name = flow_summary.get("base_name_en") or flow_summary.get("base_name_zh") or "reference flow"
        tech_desc = state.get("technical_description") or ""
        operation = state.get("operation") or "produce"
        processes = state.get("processes") or []

        # Build search query focusing on inventory exchanges, inputs, outputs
        search_query = f"{flow_name} {operation} inventory exchanges inputs outputs emissions resources LCA"
        if processes and isinstance(processes, list):
            # Add process names to search context
            process_names = " ".join([str(p.get("name") or "")[:50] for p in processes[:3] if isinstance(p, dict)])
            search_query = f"{search_query} {process_names}"

        references = _search_scientific_references(search_query, mcp_client=use_mcp_client, top_k=5)
        references_text = _format_references_for_prompt(references)

        # Build enhanced prompt with references
        enhanced_prompt = EXCHANGES_PROMPT
        if references_text:
            enhanced_prompt = f"{EXCHANGES_PROMPT}\n\n" f"Use the following scientific references to identify accurate inventory exchanges:\n" f"{references_text}\n"

        payload = {
            "prompt": enhanced_prompt,
            "context": {
                "flow": flow_summary,
                "technical_description": tech_desc,
                "processes": processes,
                "operation": operation,
            },
            "response_format": {"type": "json_object"},
        }
        raw = llm.invoke(payload)
        data = _ensure_dict(raw)
        processes = data.get("processes")
        if not isinstance(processes, list):
            raise ValueError("LLM did not return processes[] for exchanges")
        process_plan_index = {str(item.get("process_id") or ""): item for item in (state.get("processes") or []) if isinstance(item, dict)}
        target_flow_name = str((state.get("flow_summary") or {}).get("base_name_en") or "reference flow").strip()
        reference_direction = _reference_direction(state.get("operation"))
        cleaned_processes: list[dict[str, Any]] = []
        for proc in processes:
            if not isinstance(proc, dict):
                continue
            process_id = str(proc.get("process_id") or proc.get("processId") or "").strip()
            exchanges = proc.get("exchanges") or []
            if not isinstance(exchanges, list):
                exchanges = []
            plan = process_plan_index.get(process_id) or {}
            plan_reference_flow = str(plan.get("reference_flow_name") or "").strip()
            is_reference_flow_process = bool(plan.get("is_reference_flow_process"))
            if is_reference_flow_process:
                plan_reference_flow = target_flow_name
            structure = plan.get("structure") if isinstance(plan.get("structure"), dict) else {}
            structure_inputs = {_strip_flow_label(value).strip().lower() for value in _clean_string_list(structure.get("inputs")) if _strip_flow_label(value).strip()}
            structure_outputs = {_strip_flow_label(value).strip().lower() for value in _clean_string_list(structure.get("outputs")) if _strip_flow_label(value).strip()}
            cleaned_exchanges: list[dict[str, Any]] = []
            matched_reference = False
            for exchange in exchanges:
                if not isinstance(exchange, dict):
                    continue
                name = _strip_flow_label(str(exchange.get("exchangeName") or "").strip())
                raw_flow_type = _normalize_flow_type(exchange.get("flow_type") or exchange.get("flowType"))
                unit = str(exchange.get("unit") or "").strip() or "unit"
                amount = exchange.get("amount")
                if amount in (None, "", 0):
                    amount = "1"
                exchange_direction = str(exchange.get("exchangeDirection") or "").strip()
                name_key = name.lower()
                if name_key:
                    in_inputs = name_key in structure_inputs
                    in_outputs = name_key in structure_outputs
                    if in_inputs and not in_outputs:
                        exchange_direction = "Input"
                    elif in_outputs and not in_inputs:
                        exchange_direction = "Output"
                is_reference = False
                if plan_reference_flow:
                    is_reference = name_key == plan_reference_flow.lower()
                if is_reference:
                    matched_reference = True
                if is_reference and reference_direction:
                    exchange_direction = reference_direction
                flow_type = raw_flow_type or _infer_flow_type(
                    name,
                    direction=exchange_direction or "",
                    is_reference_flow=is_reference,
                )
                name = _ensure_media_suffix(
                    name,
                    direction=exchange_direction or "",
                    flow_type=flow_type,
                    is_reference_flow=is_reference,
                )
                search_hints = exchange.get("search_hints") or exchange.get("searchHints") or _build_search_hints(name)
                cleaned_exchanges.append(
                    {
                        **exchange,
                        "exchangeName": name,
                        "unit": unit,
                        "amount": amount,
                        "is_reference_flow": is_reference,
                        "exchangeDirection": exchange_direction,
                        "flow_type": flow_type,
                        "search_hints": search_hints,
                    }
                )
            if plan_reference_flow and not matched_reference:
                cleaned_exchanges.append(
                    {
                        "exchangeDirection": reference_direction,
                        "exchangeName": plan_reference_flow,
                        "generalComment": "Reference flow for this unit process.",
                        "unit": "unit",
                        "amount": "1",
                        "is_reference_flow": True,
                        "flow_type": "product",
                        "search_hints": _build_search_hints(plan_reference_flow),
                    }
                )
            cleaned_processes.append({"process_id": process_id, "exchanges": cleaned_exchanges})
        return {
            "process_exchanges": cleaned_processes,
            "step_markers": _update_step_markers(state, "step3"),
        }

    def match_flows(state: ProcessFromFlowState) -> ProcessFromFlowState:
        if state.get("matched_process_exchanges"):
            return {}
        matched: list[dict[str, Any]] = []
        flow_summary = state.get("flow_summary") or {}
        reference_name = flow_summary.get("base_name_en") or ""

        for proc in state.get("process_exchanges") or []:
            process_id = str(proc.get("process_id") or proc.get("processId") or "").strip()
            exchanges = proc.get("exchanges") or []
            if not process_id or not isinstance(exchanges, list):
                continue
            matched_exchanges: list[dict[str, Any]] = []
            for exchange in exchanges:
                if not isinstance(exchange, dict):
                    continue
                name = str(exchange.get("exchangeName") or "").strip()
                comment = str(exchange.get("generalComment") or "").strip() or None
                search_hints = exchange.get("search_hints") or []
                if isinstance(search_hints, list) and search_hints:
                    hint_text = ", ".join([str(item) for item in search_hints if str(item).strip()])
                    if hint_text:
                        comment = f"{comment} | search_hints: {hint_text}" if comment else f"search_hints: {hint_text}"
                query = FlowQuery(exchange_name=name or reference_name or "unknown_exchange", description=comment)
                candidates, unmatched = flow_search_fn(query)
                candidates = candidates[:10]
                # Build a minimal exchange dict for selector context.
                selector_exchange = {
                    "exchangeName": query.exchange_name,
                    "generalComment": comment,
                    "flow_type": exchange.get("flow_type"),
                    "search_hints": exchange.get("search_hints") or [],
                }
                decision = selector.select(query, selector_exchange, candidates)
                selected = decision.candidate
                selected_reason = decision.reasoning
                if not selected_reason:
                    if selected is not None:
                        selected_reason = "Selected by LLM."
                    else:
                        selected_reason = "No suitable candidate selected by LLM."
                matched_exchanges.append(
                    {
                        **exchange,
                        "flow_search": {
                            "query": {"exchange_name": query.exchange_name, "description": comment},
                            "candidates": [
                                {
                                    "uuid": cand.uuid,
                                    "base_name": cand.base_name,
                                    "treatment_standards_routes": cand.treatment_standards_routes,
                                    "mix_and_location_types": cand.mix_and_location_types,
                                    "flow_properties": cand.flow_properties,
                                    "flow_type": cand.flow_type,
                                    "version": cand.version,
                                    "geography": cand.geography,
                                    "classification": cand.classification,
                                    "general_comment": cand.general_comment,
                                }
                                for cand in candidates
                            ],
                            "selected_uuid": selected.uuid if selected else None,
                            "selected_reason": selected_reason,
                            "selector": decision.strategy,
                            "unmatched": [getattr(item, "base_name", None) for item in (unmatched or [])],
                        },
                    }
                )
            matched.append({"process_id": process_id, "exchanges": matched_exchanges})
        return {"matched_process_exchanges": matched}

    def build_process_datasets(state: ProcessFromFlowState) -> ProcessFromFlowState:
        if state.get("process_datasets"):
            return {}
        flow_summary = state.get("flow_summary") or {}
        target_flow_name = flow_summary.get("base_name_en") or "reference flow"
        target_flow_name_zh = flow_summary.get("base_name_zh")
        tech_description = state.get("technical_description") or ""
        scope = state.get("scope") or ""
        assumptions = state.get("assumptions") or []
        reference_direction = _reference_direction(state.get("operation"))

        process_plans = {str(item.get("process_id") or ""): item for item in (state.get("processes") or []) if isinstance(item, dict)}
        exchange_plans = {str(item.get("process_id") or ""): item for item in (state.get("matched_process_exchanges") or []) if isinstance(item, dict)}
        results: list[dict[str, Any]] = []
        crud_client: DatabaseCrudClient | None = None
        flow_name_cache: dict[str, dict[str, Any]] = {}
        if exchange_plans:
            crud_client = DatabaseCrudClient(settings)

        try:
            for process_id, plan in process_plans.items():
                name_parts = plan.get("name_parts") if isinstance(plan.get("name_parts"), dict) else {}
                process_name = str(plan.get("name") or "").strip()
                base_name = str(name_parts.get("base_name") or process_name or f"Process {process_id}").strip()
                treatment_route = str(name_parts.get("treatment_and_route") or scope or "Unspecified treatment").strip()
                mix_location = str(name_parts.get("mix_and_location") or flow_summary.get("mix_en") or "Unspecified mix/location").strip()
                quantitative_ref = str(name_parts.get("quantitative_reference") or "").strip()
                if name_parts:
                    name_bits = [bit for bit in [base_name, treatment_route, mix_location, quantitative_ref] if bit]
                    process_name = " | ".join(name_bits) if name_bits else base_name
                if not process_name:
                    process_name = base_name
                base_name_for_dataset = base_name or process_name or f"Process {process_id}"
                process_desc = str(plan.get("description") or "").strip() or tech_description
                is_reference_flow_process = bool(plan.get("is_reference_flow_process"))
                process_reference_flow = str(plan.get("reference_flow_name") or "").strip()
                if is_reference_flow_process or not process_reference_flow:
                    process_reference_flow = target_flow_name

                proc_uuid = str(uuid4())
                version = "01.01.000"

                process_info_for_classifier = {
                    "dataSetInformation": {
                        "name": {
                            "baseName": base_name_for_dataset,
                            "treatmentStandardsRoutes": treatment_route,
                            "mixAndLocationTypes": mix_location,
                        },
                        "common:generalComment": process_desc,
                    }
                }
                classification_path: list[dict[str, Any]] = []
                if llm is not None:
                    try:
                        classifier = ProcessClassifier(llm)
                        classification_path = classifier.run(process_info_for_classifier)
                    except Exception as exc:  # pylint: disable=broad-except
                        LOGGER.warning("process_from_flow.classification_failed", process_id=process_id, error=str(exc))
                if not classification_path:
                    classification_path = [{"@level": "0", "@classId": "C", "#text": "Manufacturing"}]

                matched_entry = exchange_plans.get(process_id) or {}
                exchanges_raw = matched_entry.get("exchanges") or []
                exchange_items: list[ExchangesExchangeItem] = []
                reference_internal_id: str | None = None
                next_internal_id = 1
                for exchange in exchanges_raw:
                    if not isinstance(exchange, dict):
                        continue
                    internal_id = str(next_internal_id)
                    next_internal_id += 1
                    name = str(exchange.get("exchangeName") or "").strip() or "unknown_exchange"
                    direction = str(exchange.get("exchangeDirection") or "").strip()
                    if direction not in {"Input", "Output"}:
                        direction = "Input"
                    if bool(exchange.get("is_reference_flow")):
                        direction = reference_direction
                    selected_uuid = None
                    selected_version = None
                    selected_base_name = None
                    flow_search_block = exchange.get("flow_search") if isinstance(exchange.get("flow_search"), dict) else {}
                    if isinstance(flow_search_block, dict):
                        selected_uuid = flow_search_block.get("selected_uuid")
                        # Try to resolve selected version from candidates list.
                        candidates = flow_search_block.get("candidates")
                        if isinstance(candidates, list) and selected_uuid:
                            for cand in candidates:
                                if isinstance(cand, dict) and cand.get("uuid") == selected_uuid:
                                    selected_version = cand.get("version")
                                    selected_base_name = cand.get("base_name")
                                    break
                    if selected_uuid:
                        flow_uuid = str(selected_uuid)
                        cached = flow_name_cache.get(flow_uuid)
                        if cached is None:
                            flow_dataset = None
                            if crud_client:
                                try:
                                    flow_dataset = crud_client.select_flow(flow_uuid)
                                except Exception as exc:  # pylint: disable=broad-except
                                    LOGGER.warning(
                                        "process_from_flow.flow_select_failed",
                                        flow_id=flow_uuid,
                                        error=str(exc),
                                    )
                            short_desc = _flow_short_description_from_dataset(flow_dataset) if flow_dataset else None
                            version_override = _flow_dataset_version(flow_dataset) if flow_dataset else None
                            cached = {"short_description": short_desc, "version": version_override}
                            flow_name_cache[flow_uuid] = cached
                        candidate = FlowCandidate(
                            uuid=flow_uuid,
                            base_name=str(selected_base_name or name),
                            version=str(cached.get("version") or selected_version) if (cached.get("version") or selected_version) else None,
                        )
                        reference = _candidate_reference(
                            candidate,
                            translator=translator,
                            short_description=cached.get("short_description"),
                        )
                    else:
                        reference = _placeholder_flow_reference(name, translator=translator)

                    amount = exchange.get("amount")
                    amount_text = _default_exchange_amount() if amount in (None, "", 0) else str(amount)

                    comment_text = str(exchange.get("generalComment") or "").strip()
                    exchange_item = ExchangesExchangeItem(
                        data_set_internal_id=internal_id,
                        reference_to_flow_data_set=reference,
                        exchange_direction=direction,
                        mean_amount=amount_text,
                        resulting_amount=amount_text,
                        data_derivation_type_status="Estimated",
                    )
                    if comment_text:
                        comment_entries = _build_multilang_entries(comment_text, translator=translator)
                        exchange_item.general_comment = _as_multilang_list(comment_entries or comment_text)
                    exchange_items.append(exchange_item)

                    if bool(exchange.get("is_reference_flow")):
                        reference_internal_id = internal_id

                if reference_internal_id is None:
                    # Ensure a reference exchange exists even if LLM failed to mark it.
                    reference_internal_id = str(next_internal_id)
                    exchange_items.append(
                        ExchangesExchangeItem(
                            data_set_internal_id=reference_internal_id,
                            reference_to_flow_data_set=_placeholder_flow_reference(
                                process_reference_flow,
                                translator=translator,
                            ),
                            exchange_direction=reference_direction,
                            mean_amount=_default_exchange_amount(),
                            resulting_amount=_default_exchange_amount(),
                            data_derivation_type_status="Estimated",
                            general_comment=_as_multilang_list(
                                _build_multilang_entries(
                                    flow_summary.get("general_comment_en") or "",
                                    translator=translator,
                                    zh_text=flow_summary.get("general_comment_zh"),
                                )
                            ),
                        )
                    )

                functional_unit = quantitative_ref or f"1 unit of {process_reference_flow}".strip()
                if is_reference_flow_process:
                    if reference_direction == "Input":
                        functional_unit = quantitative_ref or f"1 unit of {target_flow_name} treated"
                    else:
                        functional_unit = quantitative_ref or f"1 unit of {target_flow_name}"

                name_entries = _build_multilang_entries(base_name_for_dataset, translator=translator)
                treatment_entries = _build_multilang_entries(treatment_route, translator=translator)
                mix_entries = _build_multilang_entries(
                    mix_location,
                    translator=translator,
                    zh_text=flow_summary.get("mix_zh"),
                )
                comment_entries = _build_multilang_entries(process_desc, translator=translator)
                functional_unit_zh = None
                if target_flow_name_zh:
                    if reference_direction == "Input":
                        functional_unit_zh = f"处理 1 单位 {target_flow_name_zh}"
                    else:
                        functional_unit_zh = f"1 单位 {target_flow_name_zh}"
                functional_unit_entries = _build_multilang_entries(
                    functional_unit,
                    translator=translator,
                    zh_text=functional_unit_zh,
                )
                tech_text = "; ".join([text for text in [tech_description, process_desc, *assumptions] if text]).strip()
                tech_entries = _build_multilang_entries(tech_text, translator=translator)

                classification_items = _as_classification_items(classification_path)
                classification = DataSetInformationClassificationInformationCommonClassification(common_class=classification_items)
                classification_info = ProcessInformationDataSetInformationClassificationInformation(common_classification=classification)
                dataset_name = ProcessInformationDataSetInformationName(
                    base_name=_as_multilang_list(name_entries or process_name),
                    treatment_standards_routes=_as_multilang_list(treatment_entries or (scope or "Unspecified treatment")),
                    mix_and_location_types=_as_multilang_list(mix_entries or (flow_summary.get("mix_en") or "Unspecified mix/location")),
                )
                data_set_information = ProcessDataSetProcessInformationDataSetInformation(
                    common_uuid=proc_uuid,
                    name=dataset_name,
                    classification_information=classification_info,
                    common_general_comment=_as_multilang_list(comment_entries or process_desc),
                )
                quantitative_reference = ProcessDataSetProcessInformationQuantitativeReference(
                    type="Reference flow(s)",
                    reference_to_reference_flow=reference_internal_id or "1",
                    functional_unit_or_other=_as_multilang_list(functional_unit_entries or functional_unit),
                )
                time_info = ProcessDataSetProcessInformationTime(common_reference_year=int(datetime.now(timezone.utc).strftime("%Y")))
                location = ProcessInformationGeographyLocationOfOperationSupplyOrProduction(location="GLO")
                geography = ProcessDataSetProcessInformationGeography(location_of_operation_supply_or_production=location)
                process_info_kwargs = {
                    "data_set_information": data_set_information,
                    "quantitative_reference": quantitative_reference,
                    "time": time_info,
                    "geography": geography,
                }
                if tech_entries or tech_text:
                    process_info_kwargs["technology"] = ProcessDataSetProcessInformationTechnology(technology_description_and_included_processes=_as_multilang_list(tech_entries or tech_text))
                process_information = ProcessesProcessDataSetProcessInformation(**process_info_kwargs)

                exchanges = ProcessesProcessDataSetExchanges(exchange=exchange_items)
                modelling_and_validation = ProcessesProcessDataSetModellingAndValidation(
                    lci_method_and_allocation=ProcessDataSetModellingAndValidationLCIMethodAndAllocation(type_of_data_set="Unit process, single operation"),
                    data_sources_treatment_and_representativeness=(
                        ProcessDataSetModellingAndValidationDataSourcesTreatmentAndRepresentativeness(reference_to_data_source=_entry_level_compliance_reference())
                    ),
                    validation=ProcessDataSetModellingAndValidationValidation(review=ModellingAndValidationValidationReview(type="Not reviewed")),
                    compliance_declarations=_compliance_declarations(),
                )
                administrative_information = ProcessesProcessDataSetAdministrativeInformation(
                    common_commissioner_and_goal=ProcessDataSetAdministrativeInformationCommonCommissionerAndGoal(common_reference_to_commissioner=_contact_reference()),
                    data_entry_by=ProcessDataSetAdministrativeInformationDataEntryBy(
                        common_time_stamp=default_timestamp(),
                        common_reference_to_data_set_format=_dataset_format_reference(),
                        common_reference_to_person_or_entity_entering_the_data=_contact_reference(),
                    ),
                    publication_and_ownership=ProcessDataSetAdministrativeInformationPublicationAndOwnership(
                        common_data_set_version=version,
                        common_permanent_data_set_uri=build_portal_uri("process", proc_uuid, version),
                        common_reference_to_ownership_of_data_set=_contact_reference(),
                        common_copyright="false",
                        common_license_type="Free of charge for all users and uses",
                    ),
                )
                process_dataset = ProcessesProcessDataSet(
                    xmlns="http://lca.jrc.it/ILCD/Process",
                    xmlns_common="http://lca.jrc.it/ILCD/Common",
                    xmlns_xsi="http://www.w3.org/2001/XMLSchema-instance",
                    version="1.1",
                    locations="../ILCDLocations.xml",
                    xsi_schema_location="http://lca.jrc.it/ILCD/Process ../../schemas/ILCD_ProcessDataSet.xsd",
                    process_information=process_information,
                    exchanges=exchanges,
                    modelling_and_validation=modelling_and_validation,
                    administrative_information=administrative_information,
                )
                process_model = Processes(process_data_set=process_dataset)

                validated_on_init = False
                try:
                    entity = create_process(process_model, validate=True)
                    validated_on_init = True
                except Exception as exc:  # pylint: disable=broad-except
                    LOGGER.warning("process_from_flow.process_validation_failed", process_id=process_id, error=str(exc))
                    entity = create_process(process_model, validate=False)

                if validated_on_init:
                    errors = entity.last_validation_error()
                    if errors:
                        LOGGER.warning("process_from_flow.process_not_valid", process_id=process_id, error=str(errors))
                else:
                    valid = entity.validate(mode="pydantic")
                    if not valid:
                        errors = entity.last_validation_error()
                        LOGGER.warning("process_from_flow.process_not_valid", process_id=process_id, error=str(errors))
                results.append(entity.model.model_dump(mode="json", by_alias=True, exclude_none=True))
        finally:
            if crud_client:
                crud_client.close()

        return {"process_datasets": results}

    graph.add_node("load_flow", load_flow)
    graph.add_node("describe_technology", describe_technology)
    graph.add_node("split_processes", split_processes)
    graph.add_node("generate_exchanges", generate_exchanges)
    graph.add_node("match_flows", match_flows)
    graph.add_node("build_process_datasets", build_process_datasets)

    graph.set_entry_point("load_flow")
    graph.add_edge("load_flow", "describe_technology")
    graph.add_conditional_edges(
        "describe_technology",
        lambda state: END if (str(state.get("stop_after") or "").strip().lower() == "tech") else "split_processes",
    )
    graph.add_conditional_edges(
        "split_processes",
        lambda state: END if (str(state.get("stop_after") or "").strip().lower() == "processes") else "generate_exchanges",
    )
    graph.add_conditional_edges(
        "generate_exchanges",
        lambda state: END if (str(state.get("stop_after") or "").strip().lower() == "exchanges") else "match_flows",
    )
    graph.add_conditional_edges(
        "match_flows",
        lambda state: END if (str(state.get("stop_after") or "").strip().lower() == "matches") else "build_process_datasets",
    )
    graph.add_edge("build_process_datasets", END)

    return graph.compile()


@dataclass(slots=True)
class ProcessFromFlowService:
    """Facade that builds ILCD process datasets from a reference flow via LangGraph."""

    llm: LanguageModelProtocol | None = None
    settings: Settings | None = None
    flow_search_fn: FlowSearchFn | None = None
    selector: CandidateSelector | None = None
    translator: Translator | None = None
    mcp_client: MCPToolClient | None = None

    def run(
        self,
        *,
        flow_path: str | Path,
        operation: str = "produce",
        initial_state: dict[str, Any] | None = None,
        stop_after: str | None = None,
    ) -> ProcessFromFlowState:
        settings = self.settings or get_settings()
        flow_search_fn = self.flow_search_fn or search_flows
        selector: CandidateSelector
        if self.selector is not None:
            selector = self.selector
        elif self.llm is not None:
            selector = LLMCandidateSelector(self.llm, fallback=NoFallbackCandidateSelector())
        else:
            selector = SimilarityCandidateSelector()

        # Create MCP client if not provided and we want to use scientific references
        mcp_client = self.mcp_client
        should_close_mcp = False
        if mcp_client is None and self.llm is not None:
            # Only create MCP client when LLM is available (scientific references only useful with LLM)
            try:
                mcp_client = MCPToolClient(settings)
                should_close_mcp = True
                LOGGER.info("process_from_flow.mcp_client_created", service="TianGong_KB_Remote")
            except Exception as exc:
                LOGGER.warning("process_from_flow.mcp_client_creation_failed", error=str(exc))
                mcp_client = None

        try:
            app = _build_langgraph(
                llm=self.llm,
                settings=settings,
                flow_search_fn=flow_search_fn,
                selector=selector,
                translator=self.translator,
                mcp_client=mcp_client,
            )
            initial: ProcessFromFlowState = {"flow_path": str(flow_path), "operation": operation}
            if stop_after:
                initial["stop_after"] = stop_after
            if initial_state:
                initial.update({k: v for k, v in initial_state.items() if k not in {"flow_path", "operation"}})
            return app.invoke(initial)
        finally:
            if should_close_mcp and mcp_client:
                mcp_client.close()
                LOGGER.info("process_from_flow.mcp_client_closed")
