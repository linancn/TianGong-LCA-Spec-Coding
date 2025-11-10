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


def _schema_type_hint(schema: dict[str, Any] | None) -> str:
    if not isinstance(schema, dict):
        return ""
    type_value = schema.get("type")
    if isinstance(type_value, list):
        # Preserve order but avoid duplicate fragments
        seen: list[str] = []
        for item in type_value:
            if item not in seen:
                seen.append(str(item))
        type_hint = " | ".join(seen)
    elif isinstance(type_value, str):
        type_hint = type_value
    elif "enum" in schema:
        type_hint = "enum"
    else:
        return ""
    return f" [{type_hint}]"


def _schema_is_array(schema: dict[str, Any] | None) -> bool:
    if not isinstance(schema, dict):
        return False
    type_value = schema.get("type")
    if isinstance(type_value, list):
        return "array" in type_value
    return type_value == "array"


def _render_enum_values(values: list[Any], indent: int) -> list[str]:
    prefix = "  " * indent
    formatted = ", ".join(str(value) for value in values)
    return [f"{prefix}- allowed values: {formatted}"]


def _is_multilang_field(field: FieldSummary, schema: dict[str, Any] | None) -> bool:
    if field.reference and "MultiLang" in field.reference:
        return True
    if not isinstance(schema, dict):
        return False
    options = schema.get("anyOf") or schema.get("oneOf") or schema.get("allOf")
    if not isinstance(options, list):
        return False
    for option in options:
        if not isinstance(option, dict):
            continue
        properties = option.get("properties")
        if isinstance(properties, dict) and "@xml:lang" in properties and "#text" in properties:
            return True
    return False


def _render_schema_details(schema: dict[str, Any] | None, indent: int, seen: set[int]) -> list[str]:
    if not isinstance(schema, dict):
        return []
    schema_id = id(schema)
    if schema_id in seen:
        prefix = "  " * indent
        return [f"{prefix}- ... (recursive reference)"]
    seen.add(schema_id)

    lines: list[str] = []
    prefix = "  " * indent

    for key in ("allOf", "anyOf", "oneOf"):
        options = schema.get(key)
        if isinstance(options, list) and options:
            for index, option in enumerate(options, start=1):
                option_hint = _schema_type_hint(option)
                lines.append(f"{prefix}- {key} option {index}{option_hint}")
                lines.extend(_render_schema_details(option, indent + 1, seen))
            seen.remove(schema_id)
            return lines

    if _schema_is_array(schema) and isinstance(schema.get("items"), dict):
        extras: list[str] = []
        if schema.get("uniqueItems"):
            extras.append("uniqueItems")
        if "minItems" in schema:
            extras.append(f"minItems={schema['minItems']}")
        if "maxItems" in schema:
            extras.append(f"maxItems={schema['maxItems']}")
        extras_text = f" ({', '.join(extras)})" if extras else ""
        item_schema = schema["items"]
        lines.append(f"{prefix}- items{_schema_type_hint(item_schema)}{extras_text}")
        lines.extend(_render_schema_details(item_schema, indent + 1, seen))
        seen.remove(schema_id)
        return lines

    properties = schema.get("properties")
    if isinstance(properties, dict) and properties:
        required = set(schema.get("required", []))
        for name, child_schema in properties.items():
            type_hint = _schema_type_hint(child_schema)
            flags: list[str] = []
            if name in required:
                flags.append("required")
            if "maxLength" in child_schema:
                flags.append(f"maxLength={child_schema['maxLength']}")
            if "minLength" in child_schema:
                flags.append(f"minLength={child_schema['minLength']}")
            if "pattern" in child_schema:
                flags.append(f"pattern={child_schema['pattern']}")
            suffix = f" ({'; '.join(flags)})" if flags else ""
            lines.append(f"{prefix}- {name}{type_hint}{suffix}")
            enum_values = child_schema.get("enum")
            if isinstance(enum_values, list) and enum_values:
                lines.extend(_render_enum_values(enum_values, indent + 1))
            lines.extend(_render_schema_details(child_schema, indent + 1, seen))

    seen.remove(schema_id)
    return lines


def _format_fields(
    fields: list[FieldSummary],
    schema_node: dict[str, Any] | None,
    *,
    indent: int = 0,
    depth: int = 2,
) -> list[str]:
    lines: list[str] = []
    prefix = "  " * indent
    properties = schema_node.get("properties") if isinstance(schema_node, dict) else {}
    for field in fields:
        field_schema = properties.get(field.name, {}) if isinstance(properties, dict) else {}
        type_hint = f" [{field.type}]" if field.type else ""
        required = " (required)" if field.required else ""
        description = f": {_truncate(field.description)}" if field.description else ""
        lines.append(f"{prefix}- {field.name}{type_hint}{required}{description}")

        enum_values = field_schema.get("enum")
        if isinstance(enum_values, list) and enum_values:
            lines.extend(_render_enum_values(enum_values, indent + 1))

        if _is_multilang_field(field, field_schema):
            lines.extend(_render_schema_details(field_schema, indent + 1, seen=set()))

        if field.children and depth > 1:
            next_schema = field_schema
            if _schema_is_array(field_schema) and isinstance(field_schema.get("items"), dict):
                next_schema = field_schema["items"]
            lines.extend(
                _format_fields(
                    field.children,
                    next_schema,
                    indent=indent + 1,
                    depth=depth - 1,
                )
            )
    return lines


def _render_summary(title: str, pointer: str, depth: int = 2) -> str:
    repo = get_schema_repository()
    fields = repo.summarize_properties("tidas_processes.json", pointer)
    schema_node = repo.resolve_with_references("tidas_processes.json", pointer)
    lines = [title]
    lines.extend(_format_fields(fields, schema_node, indent=1, depth=depth))
    return "\n".join(lines)


@cache
def _build_section_prompt() -> str:
    repo = get_schema_repository()
    process_guidelines = (
        "Process extraction guidelines:\n"
        "1. Process Definition: a concrete activity that produces a product or service, "
        "consumes resources (energy, materials, land, transport, services), and has "
        "quantified LCI exchanges.\n"
        "2. Only create a process when quantifiable LCI data is provided; descriptive text "
        "without amounts is ignored.\n"
        "3. Only introduce a new process when the document explicitly labels a unit "
        "operation (in tables, section headings, or prose) and associates it with its own "
        "inventory or functional output.\n"
        "4. Do not promote a single exchange from another dataset's inventory into its own "
        "process unless the literature provides an independent LCI for that activity.\n"
        "5. When both parent (aggregated system) and subprocess data are present, create "
        "entries for each subprocess. Treat the parent dataset as the aggregation of its "
        "subprocesses and document that relation in `common:generalComment` instead of "
        "creating an additional record.\n"
        "6. If the literature mentions subprocesses in the text but provides only a total, "
        "system-boundary inventory in the data tables (black-box), create only one process "
        "representing the entire system, and do not create entries for the subprocesses "
        "lacking independent LCI data.\n"
        "7. Treat shared preparation steps, raw material staging, or unallocated "
        "\"common\" flows that lack their own functional unit as supplemental information. "
        "Do not create a separate Process for them. Write such details, or their total "
        "values, into the relevant subprocess `common:generalComment`.\n"
        "8. Every Process created must define one, and only one, primary product or service "
        "output directly related to its function, which serves as the **Reference Flow**.\n"
        "9. When identifying the Reference Flow, do not blindly assume the overall table "
        "header is the functional unit. You **must** look for the unique functional output "
        "(name and amount) explicitly associated with **this specific unit process** within "
        "the prose, table structure, or dedicated captions. The exchange amount **must be "
        "the exact numerical value specified in the literature**.\n"
        "10. The **Reference Flow** must not be an environmental emission or resource "
        "consumption (Elementary Flow); it **must** be a Product/Service flow "
        "(Technosphere Flow).\n"
        "11. If a Process yields multiple valuable products, you **must** clearly document "
        "the allocation method and basis described in the literature (e.g., \"allocation by "
        "economic value,\" \"mass allocation\") inside the `common:generalComment`.\n"
        "12. If the same activity has distinct LCI variants by geography, year, or technology "
        "route, create separate records; otherwise merge them.\n"
        "13. Always capture which subprocesses are bundled together, the functional unit, "
        "and key allocation or shared-resource notes inside the process-level "
        "`common:generalComment`.\n"
        "14. Stage 3 flow alignment performs serial MCP lookups; therefore you **must** "
        "reproduce each table row or inventory line as its own `exchange` entry. Never "
        "merge, drop, or average distinct rows—even if values are similar. Preserve the "
        "original units, qualifiers, scenario labels, and footnotes inside `generalComment` "
        "so downstream alignment can trace every source datum.\n"
        "15. Normalize exchange names to Tiangong/ILCD canonical wording (e.g., "
        "\"Electricity, medium voltage\", \"Carbon dioxide, fossil\") and ensure every "
        "`generalComment` begins with the exact prefix `FlowSearch hints:` followed by "
        "the pipe-delimited template "
        "`en_synonyms=... | zh_synonyms=... | abbreviation=... | formula_or_CAS=... | "
        "state_purity=... | source_or_pathway=... | usage_context=...`. Populate each "
        "slot with rich bilingual synonyms, aliases/abbreviations, chemical identifiers, "
        "and state/source descriptors gathered from the paper; if a field is unknown, "
        "write `NA`. Append the original table reference, assumptions, or calculation "
        "notes after the template so downstream alignment retains provenance. High-use "
        "utilities (grid electricity, water, steam, oxygen, hydrogen, natural gas, etc.) "
        "must list at least two English and two Chinese descriptors or usage scenarios to "
        "maximise MCP recall."
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
        "  * `generalComment`: output a single string in the format described above "
        "(`FlowSearch hints: en_synonyms=... | ... | usage_context=...`). Include "
        "bilingual synonyms, abbreviations, chemical identifiers, state/purity, source "
        "or supply pathway, and explicit usage context. If any element is missing, keep "
        "the placeholder `NA`. After the structured segments, append concise notes on "
        "data provenance, allocation, conversions, or table references.\n"
        "  * Omit `referenceToFlowDataSet` and other `referenceTo...` placeholders; Stage 3 "
        "will populate flow references after alignment.\n"
        '  * `@dataSetInternalID`: sequential identifiers as strings starting from "0".'
    )
    metadata_schema = repo.resolve_with_references("tidas_processes.json", "/properties/processDataSet")
    metadata_fields = [
        field
        for field in repo.summarize_properties("tidas_processes.json", "/properties/processDataSet")
        if field.name.startswith("@")
    ]
    metadata_lines = ["processDataSet metadata (auto-populated if omitted):"]
    metadata_lines.extend(_format_fields(metadata_fields, metadata_schema, indent=1, depth=1))
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

AGGREGATE_SYSTEM_PROMPT = (
    "You are analysing a life cycle assessment document. Identify every top-level or parent "
    "process system described (for example, production routes, technology options, or supply "
    "chains that contain multiple subprocesses with their own LCIs). Return JSON with the key "
    "`parentProcesses`, whose value is an array. Each item must include `name` (string), optional "
    "`aliases` (array of alternative names), optional `keywords` (array of distinguishing terms), "
    "and optional `subprocessHints` (array summarising important subprocesses mentioned). Only "
    "include parents that have at least one quantified subprocess in the text, and skip shared "
    "preparation steps or utilities that lack independent LCIs or a functional unit. If the "
    "document only provides a single black-box inventory with no decomposed subprocesses, return "
    "an empty array. Ensure every qualifying parent mentioned in the document appears exactly once."
)

CLASSIFICATION_PROMPT = (
    "Derive the ISIC classification path for the process. Return a JSON array to populate "
    "`dataSetInformation.classificationInformation.common:classification.common:class`, "
    "where each object contains '@level' (string), '@classId', and '#text'. Levels should "
    "progress sequentially from '0'."
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
        focus_system: str | None = None,
        system_aliases: list[str] | None = None,
    ) -> dict[str, Any]:
        LOGGER.info("process_extraction.section_extraction")
        prompt = SECTION_PROMPT
        if focus_system:
            alias_text = ""
            if system_aliases:
                filtered_aliases = [alias for alias in system_aliases if alias]
                if filtered_aliases:
                    alias_text = f" (aliases: {', '.join(filtered_aliases)})"
            focus_directive = (
                "Focus exclusively on the top-level system "
                f"`{focus_system}`{alias_text}. Extract every subprocess or unit process that the document "
                "explicitly assigns to this parent (headings, tables, or prose with a named "
                "unit process). Do not split out generic raw-material staging or shared "
                "utilities unless the text states they operate as distinct unit processes. "
                "Capture supplemental materials or shared resources in `common:generalComment`."
            )
            prompt = f"{SECTION_PROMPT}\n\n{focus_directive}"
        payload = {
            "prompt": prompt,
            "context": clean_text,
            "response_format": {"type": "json_object"},
        }
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
                focus_system=focus_system,
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
        response = self.llm.invoke(
            {
                "prompt": LOCATION_PROMPT,
                "context": process_info,
                "response_format": {"type": "json_object"},
            }
        )
        return _ensure_dict(response)


@dataclass
class AggregateSystemExtractor:
    llm: LanguageModelProtocol

    def run(self, clean_text: str) -> dict[str, Any]:
        LOGGER.info("process_extraction.aggregate_system_identification")
        response = self.llm.invoke(
            {
                "prompt": AGGREGATE_SYSTEM_PROMPT,
                "context": clean_text,
                "response_format": {"type": "json_object"},
            }
        )
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
