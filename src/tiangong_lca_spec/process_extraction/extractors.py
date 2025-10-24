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
    process_guidelines = (
        "Process extraction guidelines:\n"
        "1. Process definition: a concrete activity that produces a product or service, "
        "consumes resources (energy, materials, land, transport, services), and has "
        "quantified LCI exchanges.\n"
        "2. Only create a process when quantifiable LCI data is provided; descriptive text "
        "without amounts is ignored.\n"
        "3. When both parent and subprocess data are present, create entries for each "
        "subprocess. Treat the parent dataset as the aggregation of its subprocesses and "
        "document that relation in `common:generalComment` or `notes` instead of creating "
        "an additional record.\n"
        "4. Do not promote a single exchange from another dataset into its own process "
        "unless the literature provides an independent LCI for it.\n"
        "5. If the same activity has distinct LCI variants by geography, year, or technology "
        "route, create separate records; otherwise merge them.\n"
        "6. Always capture which subprocesses are bundled together, the functional unit, "
        "and key allocation or shared-resource notes inside `common:generalComment` or "
        "`notes`.\n"
        "7. Treat shared preparation steps, raw material staging, or utility supply that "
        "lack their own functional unit as supplemental information. Write such details "
        "into the relevant subprocess `common:generalComment` instead of creating a new "
        "process entry.\n"
        "8. Only introduce a new process when the document explicitly labels a unit "
        "operation (in tables, section headings, or prose) and associates it with its own "
        "inventory or functional output.\n"
        "9. Stage 3 flow alignment performs serial MCP lookups; consolidate table rows "
        "into roughly 8-12 representative exchanges per process, but carry detailed "
        "sub-rows (amounts, units, qualifiers) into `generalComment1` or process "
        "notes so no information is lost.\n"
        "10. Normalize exchange names to Tiangong/ILCD canonical wording (e.g., "
        '"Electricity, medium voltage", "Carbon dioxide, fossil") and enrich '
        "`generalComment1` with common synonyms, aliases/abbreviations (e.g., "
        '"COG", "DAC"), chemical formulas or CAS numbers, and bilingual descriptors '
        "to improve flow search hit-rate."
    )
    module_guidelines = (
        "Populate these required fields whenever evidence exists:\n"
        "- processInformation.dataSetInformation:\n"
        '  * `name`: "Subprocess for Parent process" (e.g., "Coal mining and '
        'processing for Coal Gasification to MeOH (CGTM)").\n'
        "  * `specinfo.baseName`: core activity label.\n"
        "  * `specinfo.treatmentStandardsRoutes`: technical route, feedstock, or "
        "standards.\n"
        "  * `specinfo.mixAndLocationTypes`: market or geographic qualifier "
        '(e.g., "at plant, Germany").\n'
        "  * `specinfo.functionalUnitFlowProperties`: quantitative reference / "
        "functional unit.\n"
        "  * `time.referenceYear`: explicit reference year; fall back to publication "
        "year; if still missing, leave empty and the system will normalise.\n"
        '  * `geography.@location`: explicit ISO/ILCD location; if absent, use "GLO" '
        "to match normalisation defaults.\n"
        "  * `technology`: short description of included technology and system "
        "boundary.\n"
        "- administrativeInformation:\n"
        "  * `common:commissionerAndGoal.common:intendedApplications`: summarise the "
        "stated intended applications of the dataset.\n"
        "- modellingAndValidation:\n"
        "  * `LCIMethodAndAllocation.typeOfDataSet`, `LCIMethodAndAllocation."
        "LCIMethodPrinciple`, and related allocation notes.\n"
        "  * `dataSourcesTreatmentAndRepresentativeness."
        "dataCutOffAndCompletenessPrinciples` and the list of `referenceToDataSource` "
        "entries (short descriptions of cited sources).\n"
        "- exchanges.exchange (for each flow):\n"
        '  * `exchangeDirection`: "Input" or "Output".\n'
        "  * `meanAmount`, `unit`, and `resultingAmount`.\n"
        "  * `exchangeName` / `flowName`: align with wording in the paper.\n"
        "  * `generalComment1`: capture data source, representativeness, quality, and key "
        "modelling assumptions succinctly.\n"
        "  * Omit `referenceToFlowDataSet` and other `referenceTo...` placeholders; Stage 3 "
        "will populate flow references after alignment.\n"
        '  * `@dataSetInternalID`: sequential identifiers as strings starting from "0".'
    )
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
        "TIDAS ILCD `processDataSet` schema. Before filling fields, follow the guidelines "
        "below.\n\n"
        f"{process_guidelines}\n\n"
        f"{module_guidelines}\n\n"
        "Return JSON with a top-level key `processDataSets` whose value is an array of one or more "
        "objects matching the schema excerpts below. If only a single process is identified, still "
        "return it as a single-element array. Only include fields supported by the schema and omit "
        "entries that are not supported by evidence in the paper. Ensure the JSON is valid and do "
        "not wrap the result in Markdown or a code block.\n\n"
        f"{metadata}\n\n"
        f"{process_info}\n\n"
        f"{modelling}\n\n"
        f"{administrative}\n\n"
        f"{exchanges}"
    )


SECTION_PROMPT = _build_section_prompt()

PARENT_PROMPT = (
    "You are analysing a life cycle assessment document. Identify every top-level or parent "
    "process system described (for example, production routes, technology options, or supply "
    "chains that contain multiple subprocesses with their own LCIs). Return JSON with the key "
    "`parentProcesses`, whose value is an array. Each item must include `name` (string), optional "
    "`aliases` (array of alternative names), optional `keywords` (array of distinguishing terms), "
    "and optional `subprocessHints` (array summarising important subprocesses mentioned). Only "
    "include parents that have at least one quantified subprocess in the text. Ensure every "
    "parent mentioned in the document appears exactly once."
)

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

    def run(
        self,
        clean_text: str,
        *,
        focus_parent: str | None = None,
        parent_aliases: list[str] | None = None,
    ) -> dict[str, Any]:
        LOGGER.info("process_extraction.section_extraction")
        prompt = SECTION_PROMPT
        if focus_parent:
            alias_text = ""
            if parent_aliases:
                filtered_aliases = [alias for alias in parent_aliases if alias]
                if filtered_aliases:
                    alias_text = f" (aliases: {', '.join(filtered_aliases)})"
            focus_directive = (
                "Focus exclusively on the parent process "
                f"`{focus_parent}`{alias_text}. Extract every subprocess that the document "
                "explicitly assigns to this parent (headings, tables, or prose with a named "
                "unit process). Do not split out generic raw-material staging or shared "
                "utilities unless the text states they operate as distinct unit processes. "
                "Capture supplemental materials or shared resources in `common:generalComment`."
            )
            prompt = f"{SECTION_PROMPT}\n\n{focus_directive}"
        payload = {"prompt": prompt, "context": clean_text}
        response = self.llm.invoke(payload)
        raw_content = getattr(response, "content", response)
        truncated = False
        if isinstance(raw_content, str):
            stripped = raw_content.strip()
            if stripped.endswith("...") or stripped.count("{") != stripped.count("}"):
                truncated = True
        data = _ensure_dict(response)
        if truncated:
            LOGGER.warning(
                "process_extraction.section_extraction_truncated",
                focus_parent=focus_parent,
            )
        return data


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


@dataclass
class ParentProcessExtractor:
    llm: LanguageModelProtocol

    def run(self, clean_text: str) -> dict[str, Any]:
        LOGGER.info("process_extraction.parent_process_identification")
        response = self.llm.invoke({"prompt": PARENT_PROMPT, "context": clean_text})
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
