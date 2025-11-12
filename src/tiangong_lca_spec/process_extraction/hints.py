"""Utilities to normalise and enrich FlowSearch hint strings."""

from __future__ import annotations

import json
import re
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

from tiangong_lca_spec.core.config import get_settings
from tiangong_lca_spec.core.logging import get_logger

HINT_FIELDS = (
    "en_synonyms",
    "zh_synonyms",
    "abbreviation",
    "formula_or_CAS",
    "state_purity",
    "source_or_pathway",
    "usage_context",
)

LOGGER = get_logger(__name__)

HINT_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "en_synonyms": (
        "en_synonyms",
        "enSynonyms",
        "synonyms_en",
        "synonymsEn",
        "synonyms",
        "aliases",
        "alias",
        "alternateNames",
        "alternateName",
    ),
    "zh_synonyms": (
        "zh_synonyms",
        "zhSynonyms",
        "synonyms_zh",
        "synonymsZh",
        "chinese_synonyms",
        "chineseSynonyms",
        "name_zh",
        "nameZh",
        "zhName",
        "nameCN",
        "cnName",
    ),
    "abbreviation": (
        "abbreviation",
        "abbreviations",
        "abbr",
        "abbrs",
        "short_name",
        "shortName",
        "shortLabel",
        "alias",
    ),
    "formula_or_CAS": (
        "formula_or_CAS",
        "formulaOrCas",
        "formula",
        "chemicalFormula",
        "molecularFormula",
        "cas",
        "casNumber",
        "CAS",
        "identifiers",
    ),
    "state_purity": (
        "state_purity",
        "statePurity",
        "state",
        "phase",
        "purity",
        "grade",
        "quality",
        "concentration",
        "specification",
        "temperature",
        "pressure",
    ),
    "source_or_pathway": (
        "source_or_pathway",
        "sourceOrPathway",
        "source",
        "pathway",
        "origin",
        "supplier",
        "provenance",
        "location",
        "geography",
        "productionRoute",
        "technology",
    ),
    "usage_context": (
        "usage_context",
        "usageContext",
        "usage",
        "context",
        "application",
        "scenario",
        "notes",
    ),
}

CHINESE_CHAR_PATTERN = re.compile(r"[\u4e00-\u9fff]")


def _flow_hint_catalog() -> dict[str, dict[str, list[str] | str]]:
    return _load_flow_hint_catalog()


@lru_cache(maxsize=1)
def _load_flow_hint_catalog() -> dict[str, dict[str, list[str] | str]]:
    settings = get_settings()
    path = settings.flow_hint_catalog_path
    if not path:
        return {}
    path_obj = Path(path)
    if not path_obj.exists():
        LOGGER.warning("Flow hint catalogue not found at %s", path_obj)
        return {}
    try:
        raw = _read_catalog_file(path_obj)
    except Exception as exc:  # noqa: BLE001 - surface parse issues in logs
        LOGGER.warning("Failed to read flow hint catalogue %s: %s", path_obj, exc)
        return {}
    return _normalise_catalog(raw)


def _read_catalog_file(path: Path) -> Any:
    with path.open("rb") as handle:
        suffix = path.suffix.lower()
        if suffix in {".json"}:
            return json.load(handle)
        if suffix in {".toml"}:
            return tomllib.load(handle)
        raise ValueError(f"Unsupported catalogue format: {path.suffix}")


def _normalise_catalog(raw: Any) -> dict[str, dict[str, list[str] | str]]:
    catalogue: dict[str, dict[str, list[str] | str]] = {}
    items: Iterable[tuple[str | None, Any]]
    if isinstance(raw, dict):
        items = raw.items()
    elif isinstance(raw, list):
        unpacked = []
        for entry in raw:
            if isinstance(entry, dict):
                flow_name = _stringify(entry.get("flow_name") or entry.get("flowName") or entry.get("flow") or entry.get("name"))
                unpacked.append((flow_name or None, entry))
        items = unpacked
    else:
        return catalogue

    for key, entry in items:
        if not isinstance(entry, dict):
            continue
        flow_name = _stringify(key) or _stringify(entry.get("flow_name") or entry.get("flowName") or entry.get("flow"))
        if not flow_name:
            continue
        canonical: dict[str, list[str] | str] = {}
        for field in HINT_FIELDS:
            values = _collect_field_values(entry, (field,) + HINT_FIELD_ALIASES.get(field, ()))
            if values:
                canonical[field] = values
                continue
            raw_value = entry.get(field)
            if raw_value is not None:
                canonical[field] = _format_field(raw_value)
        catalogue[flow_name.lower()] = canonical
    return catalogue


def _collect_field_values(source: Any, keys: Iterable[str]) -> list[str]:
    results: list[str] = []
    if not isinstance(source, (dict, list)):
        return results
    key_set = {key.lower() for key in keys}
    stack: list[Any] = [source]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            for key, value in current.items():
                if key.lower() in key_set:
                    if isinstance(value, dict):
                        stack.append(value)
                        continue
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, (dict, list)):
                                stack.append(item)
                            else:
                                results.extend(_normalise_items(item))
                        continue
                    results.extend(_normalise_items(value))
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            stack.extend(current)
    return results


def enrich_exchange_hints(
    exchange: dict[str, Any],
    *,
    process_name: str | None = None,
    geography: str | None = None,
) -> None:
    """Ensure every exchange carries a structured FlowSearch hint block."""

    if not isinstance(exchange, dict):
        return

    existing_text = _extract_text(exchange.get("generalComment"))
    parsed_fields, notes = _parse_existing_fields(existing_text)

    base_name = _stringify(exchange.get("exchangeName"))
    hints: dict[str, str] = {field: "NA" for field in HINT_FIELDS}

    catalog_entry = _lookup_catalog_entry(base_name)
    if catalog_entry:
        for field, values in catalog_entry.items():
            hints[field] = _format_field(values)

    heuristic = _heuristic_hint_candidates(exchange, base_name, geography)
    for field, values in heuristic.items():
        hints[field] = _merge_field_values(hints.get(field), values, prefer_new_if_na=True)

    for field, value in parsed_fields.items():
        if field in HINT_FIELDS:
            hints[field] = _merge_field_values(hints.get(field), value)

    if geography:
        hints["source_or_pathway"] = _merge_field_values(hints.get("source_or_pathway"), geography)

    default_usage = _default_usage_context(exchange, process_name)
    usage_candidate = parsed_fields.get("usage_context") or default_usage
    hints["usage_context"] = _merge_field_values(hints.get("usage_context"), usage_candidate, prefer_new_if_na=True)

    formatted = _format_hints(hints)
    remainder = notes or _strip_hint_prefix(existing_text)
    if remainder:
        formatted = f"{formatted}. {remainder}"

    exchange["generalComment"] = {"@xml:lang": "en", "#text": formatted}


def _lookup_catalog_entry(flow_name: str | None) -> dict[str, list[str] | str] | None:
    if not flow_name:
        return None
    entry = _flow_hint_catalog().get(flow_name.lower())
    if not entry:
        return None
    return {field: value for field, value in entry.items() if field in HINT_FIELDS}


def _heuristic_hint_candidates(
    exchange: dict[str, Any],
    base_name: str | None,
    geography: str | None,
) -> dict[str, list[str] | str]:
    candidates: dict[str, list[str] | str] = {}

    en_synonyms = _generate_en_synonyms(base_name)
    en_synonyms.extend(_collect_field_values(exchange, HINT_FIELD_ALIASES["en_synonyms"]))
    if en_synonyms:
        candidates["en_synonyms"] = en_synonyms

    zh_synonyms = _generate_zh_synonyms(base_name)
    zh_synonyms.extend(_collect_field_values(exchange, HINT_FIELD_ALIASES["zh_synonyms"]))
    if zh_synonyms:
        candidates["zh_synonyms"] = zh_synonyms

    abbreviations = _collect_field_values(exchange, HINT_FIELD_ALIASES["abbreviation"])
    abbreviations.extend(_derive_abbreviations(base_name))
    if abbreviations:
        candidates["abbreviation"] = abbreviations

    formulas = _collect_field_values(exchange, HINT_FIELD_ALIASES["formula_or_CAS"])
    formulas.extend(_extract_formula_tokens(base_name))
    if formulas:
        candidates["formula_or_CAS"] = formulas

    state_purity = _collect_field_values(exchange, HINT_FIELD_ALIASES["state_purity"])
    state_purity.extend(_extract_state_tokens(base_name))
    if state_purity:
        candidates["state_purity"] = state_purity

    source_terms = _collect_field_values(exchange, HINT_FIELD_ALIASES["source_or_pathway"])
    if geography:
        source_terms.append(geography)
    if source_terms:
        candidates["source_or_pathway"] = source_terms

    usage_terms = _collect_field_values(exchange, HINT_FIELD_ALIASES["usage_context"])
    if usage_terms:
        candidates["usage_context"] = usage_terms

    return {field: _deduplicate(values) for field, values in candidates.items()}


def _generate_en_synonyms(flow_name: str | None) -> list[str]:
    if not flow_name:
        return []
    cleaned = flow_name.strip()
    if not cleaned:
        return []
    variants = [cleaned]
    lower = cleaned.lower()
    title = cleaned.title()
    if lower != cleaned:
        variants.append(lower)
    if title != cleaned and title != lower:
        variants.append(title)
    variants.extend(_split_variants(cleaned, delimiters=("/", "|", ";")))
    variants.extend(_reorder_comma_phrase(cleaned))
    variants.extend(_parenthetical_segments(cleaned))
    return _deduplicate(variants)


def _generate_zh_synonyms(flow_name: str | None) -> list[str]:
    if not flow_name or not CHINESE_CHAR_PATTERN.search(flow_name):
        return []
    segments = re.split(r"[、，；;,/|]", flow_name)
    variants = [segment.strip() for segment in segments if segment.strip()]
    variants.extend(_parenthetical_segments(flow_name, chinese_only=True))
    return _deduplicate(variants)


def _derive_abbreviations(flow_name: str | None) -> list[str]:
    if not flow_name:
        return []
    candidates = []
    candidates.extend(_parenthetical_segments(flow_name, min_length=2))
    uppercase_tokens = re.findall(r"\b[A-Z][A-Z0-9]{1,5}\b", flow_name)
    candidates.extend(uppercase_tokens)
    initials = "".join(token[0] for token in re.findall(r"[A-Za-z]+", flow_name))
    if len(initials) >= 2 and initials.isupper():
        candidates.append(initials)
    return _deduplicate(candidates)


def _extract_formula_tokens(flow_name: str | None) -> list[str]:
    if not flow_name:
        return []
    candidates = re.findall(r"(?:CAS\s*\d{2,7}-\d{2}-\d)|(?:[A-Z][a-z]?\d{0,3})", flow_name)
    return _deduplicate(candidates)


def _extract_state_tokens(flow_name: str | None) -> list[str]:
    if not flow_name:
        return []
    tokens = []
    for match in re.findall(r"\b\d+(?:\.\d+)?\s?(?:kV|V|MPa|kPa|°C|K|ppm|%)\b", flow_name):
        tokens.append(match)
    qualifiers = re.findall(
        r"\b(low|medium|high|liquid|gaseous|gas|solid|aqueous|cryogenic|saturated|superheated)\b",
        flow_name,
        flags=re.IGNORECASE,
    )
    tokens.extend(qualifiers)
    return _deduplicate(tokens)


def _split_variants(text: str, delimiters: tuple[str, ...]) -> list[str]:
    pattern = "|".join(re.escape(delim) for delim in delimiters)
    segments = re.split(pattern, text)
    return [segment.strip() for segment in segments if segment.strip()]


def _reorder_comma_phrase(text: str) -> list[str]:
    parts = [part.strip() for part in text.split(",") if part.strip()]
    if len(parts) < 2:
        return []
    reordered = [" ".join(parts)]
    reversed_join = " ".join(parts[::-1])
    if reversed_join not in reordered:
        reordered.append(reversed_join)
    return reordered


def _parenthetical_segments(text: str, *, chinese_only: bool = False, min_length: int = 1) -> list[str]:
    segments = []
    for raw in re.findall(r"\(([^)]+)\)", text):
        cleaned = raw.strip()
        if len(cleaned) < min_length:
            continue
        if chinese_only and not CHINESE_CHAR_PATTERN.search(cleaned):
            continue
        segments.append(cleaned)
    return segments


def _deduplicate(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        item = value.strip()
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _format_hints(hints: dict[str, str]) -> str:
    parts = [f"{field}={_safe_value(hints.get(field))}" for field in HINT_FIELDS]
    return "FlowSearch hints: " + " | ".join(parts)


def _parse_existing_fields(text: str | None) -> tuple[dict[str, str], str]:
    if not text:
        return {}, ""
    stripped = text.strip()
    if not stripped.startswith("FlowSearch hints:"):
        return {}, stripped

    body = stripped[len("FlowSearch hints:") :].strip()
    segments = [segment.strip() for segment in body.split("|")]
    fields: dict[str, str] = {}
    notes = ""
    for segment in segments:
        if "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key not in HINT_FIELDS:
            continue
        if key == "usage_context":
            usage_value, remainder = _separate_notes(value)
            fields[key] = usage_value
            if remainder:
                notes = remainder
        else:
            fields[key] = value

    if not notes:
        tail_marker = "usage_context="
        marker_index = body.find(tail_marker)
        if marker_index != -1:
            tail_text = body[marker_index + len(tail_marker) :].strip()
            _, remainder = _separate_notes(tail_text)
            if remainder:
                notes = remainder
    return fields, notes


def _separate_notes(value: str) -> tuple[str, str]:
    for separator in (". ", "; ", "。", "\n"):
        idx = value.find(separator)
        if idx != -1:
            usage = value[:idx].strip()
            remainder = value[idx + len(separator) :].strip()
            return usage, remainder
    return value.strip(), ""


def _merge_field_values(
    existing: str | None,
    incoming: str | list[str] | None,
    *,
    prefer_new_if_na: bool = False,
) -> str:
    existing_items = _normalise_items(existing)
    incoming_items = _normalise_items(incoming)

    if prefer_new_if_na and (not existing_items or existing_items == ["NA"]):
        base_items: list[str] = []
    else:
        base_items = existing_items

    merged: list[str] = []
    for item in base_items + incoming_items:
        clean = item.strip()
        if not clean or clean.lower() == "na":
            continue
        if clean not in merged:
            merged.append(clean)

    if not merged:
        return "NA"
    return "; ".join(merged)


def _normalise_items(value: str | list[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item.strip() for item in value if item and str(item).strip()]
    text = str(value)
    stripped = text.strip()
    if not stripped:
        return []
    if (stripped.startswith("{") and stripped.endswith("}")) or (stripped.startswith("[") and stripped.endswith("]")):
        return []
    cleaned = stripped.replace("|", " ")
    parts = re.split(r"[;,]", cleaned)
    return [part.strip() for part in parts if part.strip()]


def _format_field(value: list[str] | str) -> str:
    if isinstance(value, list):
        return "; ".join(item for item in value if item)
    return str(value)


def _default_usage_context(exchange: dict[str, Any], process_name: str | None) -> str:
    direction = _stringify(exchange.get("exchangeDirection")).lower()
    flow_name = _stringify(exchange.get("exchangeName")) or "exchange"
    unit = _stringify(exchange.get("unit"))
    amount = _stringify(exchange.get("meanAmount"))

    if direction == "input":
        prefix = "Input to"
    elif direction == "output":
        prefix = "Output from"
    else:
        prefix = "Exchange in"

    process_segment = f" {process_name}" if process_name else " the process"
    quantity = f" {amount} {unit}" if amount and unit else ""
    return f"{prefix}{process_segment} ({flow_name}{quantity})"


def _safe_value(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip()


def _strip_hint_prefix(text: str | None) -> str:
    if not text:
        return ""
    stripped = text.strip()
    if not stripped.startswith("FlowSearch hints:"):
        return stripped
    _, remainder = _separate_notes(stripped[len("FlowSearch hints:") :].strip())
    return remainder


def _extract_text(value: Any) -> str:
    if isinstance(value, dict):
        if "#text" in value:
            return _stringify(value["#text"])
        return _stringify(value.get("text"))
    return _stringify(value)


def _stringify(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("#text", "@value", "text", "baseName", "shortName", "name"):
            if key in value:
                return _stringify(value[key])
        return ""
    if value is None:
        return ""
    return str(value).strip()
