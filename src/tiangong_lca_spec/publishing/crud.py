"""Thin wrappers to publish flows and processes via Database_CRUD_Tool."""

from __future__ import annotations

import json
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Protocol, Sequence

from tiangong_lca_spec.core.config import Settings, get_settings
from tiangong_lca_spec.core.constants import build_dataset_format_reference
from tiangong_lca_spec.core.exceptions import SpecCodingError
from tiangong_lca_spec.core.json_utils import parse_json_response
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.mcp_client import MCPToolClient
from tiangong_lca_spec.core.uris import build_local_dataset_uri, build_portal_uri
from tiangong_lca_spec.tidas.flow_property_registry import FlowPropertyRegistry, get_default_registry
from tiangong_lca_spec.workflow.artifacts import flow_compliance_declarations

LOGGER = get_logger(__name__)

DATABASE_TOOL_NAME = "Database_CRUD_Tool"
PROJECT_ROOT = Path(__file__).resolve().parents[3]
PRODUCT_CATEGORY_SCRIPT = PROJECT_ROOT / "scripts" / "md" / "list_product_flow_category_children.py"


def _utc_timestamp() -> str:
    """Return ISO timestamp in the format required by Supabase validator."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        text = value.get("#text")
        if isinstance(text, str):
            return text.strip()
    return str(value).strip()


def _parse_flowsearch_hints(comment: str | None) -> dict[str, list[str] | str]:
    """Parse 'FlowSearch hints:' into a dict of list values."""
    if not comment:
        return {}
    text = comment.strip()
    prefix = "FlowSearch hints:"
    if text.startswith(prefix):
        text = text[len(prefix) :].strip()
    segments = [segment.strip() for segment in text.split("|") if segment.strip()]
    output: dict[str, list[str] | str] = {}
    for segment in segments:
        key, _, value = segment.partition("=")
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if not value or value == "NA":
            output[key] = []
            continue
        parts = [item.strip() for item in value.split(";") if item.strip()]
        output[key] = parts or [value]
    return output


def _normalize_hint_values(hints: Mapping[str, list[str] | str]) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {}
    for key, value in hints.items():
        if isinstance(value, str):
            text = _coerce_text(value)
            if text:
                normalized[key] = [text]
            continue
        if isinstance(value, Iterable):
            entries = [_coerce_text(item) for item in value if _coerce_text(item)]
            if entries:
                normalized[key] = entries
    return normalized


def _collect_classification_entries(exchange: Mapping[str, Any]) -> list[str]:
    results: list[str] = []
    classification = exchange.get("classificationInformation") or exchange.get("classification")
    if isinstance(classification, Mapping):
        carrier = classification.get("common:classification") or classification.get("classification")
        if isinstance(carrier, Mapping):
            classes = carrier.get("common:class") or carrier.get("class")
        else:
            classes = carrier
    else:
        classes = classification
    if isinstance(classes, list):
        for entry in classes:
            if isinstance(entry, Mapping):
                label = _coerce_text(entry.get("#text") or entry.get("text"))
                level = _coerce_text(entry.get("@level"))
                if label and level:
                    results.append(f"{level}:{label}")
                elif label:
                    results.append(label)
            else:
                text = _coerce_text(entry)
                if text:
                    results.append(text)
    return results


def _collect_tag_entries(exchange: Mapping[str, Any]) -> list[str]:
    tags: list[str] = []
    for key in ("synonyms", "synonym", "CASNumber", "chemFormula", "formula", "additionalInformation"):
        value = exchange.get(key)
        if isinstance(value, str):
            candidate = _coerce_text(value)
            if candidate:
                tags.append(candidate)
        elif isinstance(value, Mapping):
            candidate = _coerce_text(value.get("#text") or value.get("text"))
            if candidate:
                tags.append(candidate)
        elif isinstance(value, Iterable):
            for item in value:
                candidate = _coerce_text(item)
                if candidate:
                    tags.append(candidate)
    return tags


def _collect_reference_summary(exchange: Mapping[str, Any]) -> dict[str, Any] | None:
    reference = exchange.get("referenceToFlowDataSet")
    if not isinstance(reference, Mapping):
        return None
    summary: dict[str, Any] = {}
    for key in ("@refObjectId", "@version", "@uri"):
        text = _coerce_text(reference.get(key))
        if text:
            summary[key] = text
    if reference.get("unmatched:placeholder"):
        summary["placeholder"] = True
    return summary or None


def _collect_candidate_summary(exchange: Mapping[str, Any]) -> dict[str, Any] | None:
    detail = exchange.get("matchingDetail")
    if not isinstance(detail, Mapping):
        return None
    candidate = detail.get("selectedCandidate")
    if not isinstance(candidate, Mapping):
        return None
    summary: dict[str, Any] = {}
    for key in ("uuid", "base_name", "flow_properties", "geography", "classification", "general_comment", "selector", "evaluation_reason"):
        value = candidate.get(key)
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            text = _coerce_text(value)
            if text:
                summary[key] = text
        elif isinstance(value, Mapping):
            sanitized = {k: _coerce_text(v) for k, v in value.items() if isinstance(k, str) and _coerce_text(v)}
            if sanitized:
                summary[key] = sanitized
        elif isinstance(value, Iterable):
            collected = [_coerce_text(item) for item in value if _coerce_text(item)]
            if collected:
                summary[key] = collected
    return summary or None


def _assign_if_value(target: dict[str, Any], key: str, value: str) -> None:
    if value:
        target[key] = value


def _compose_flow_context(exchange: Mapping[str, Any], hints: Mapping[str, list[str] | str]) -> dict[str, Any]:
    normalized_hints = _normalize_hint_values(hints)
    summary: dict[str, Any] = {}
    _assign_if_value(summary, "name", _coerce_text(exchange.get("exchangeName")))
    _assign_if_value(summary, "direction", _coerce_text(exchange.get("exchangeDirection")))
    _assign_if_value(summary, "unit", _resolve_unit(exchange))
    _assign_if_value(
        summary,
        "amount",
        _coerce_text(exchange.get("meanAmount") or exchange.get("resultingAmount") or exchange.get("amount")),
    )
    _assign_if_value(summary, "general_comment", _extract_general_comment(exchange))
    _assign_if_value(summary, "input_group", _coerce_text(exchange.get("inputGroup")))
    _assign_if_value(summary, "output_group", _coerce_text(exchange.get("outputGroup")))
    _assign_if_value(summary, "location", _coerce_text(exchange.get("location")))
    _assign_if_value(summary, "cas_number", _coerce_text(exchange.get("CASNumber")))
    _assign_if_value(summary, "formula", _coerce_text(exchange.get("chemFormula") or exchange.get("formula")))
    summary["classification_path"] = _collect_classification_entries(exchange)
    summary["tags"] = _collect_tag_entries(exchange)
    reference = _collect_reference_summary(exchange)
    if reference:
        summary["reference"] = reference
    candidate = _collect_candidate_summary(exchange)
    if candidate:
        summary["selected_candidate"] = candidate
    return {
        "exchange": summary,
        "flow_search_hints": normalized_hints,
    }


def _ensure_mapping_response(response: Any) -> Mapping[str, Any]:
    payload = getattr(response, "content", response)
    if isinstance(payload, str):
        parsed = parse_json_response(payload)
    else:
        parsed = payload
    if isinstance(parsed, Mapping):
        return parsed
    raise ValueError("Language model returned non-dict payload")


class LanguageModelProtocol(Protocol):
    """Minimal protocol for language models used during publishing."""

    def invoke(self, input_data: dict[str, Any]) -> Any: ...


class FlowTypeClassifier:
    """Infer flow types using an optional language model with heuristic fallback."""

    ALLOWED_TYPES = {"Product flow", "Elementary flow", "Waste flow"}
    ELEMENTARY_KEYWORDS = ("emission", "to air", "to water", "wastewater", "effluent", "flue gas", "to soil", "released")
    WASTE_KEYWORDS = ("waste", "slag", "scrap", "residue", "ash", "sludge")
    PROMPT = (
        "You classify life cycle assessment exchanges into one of three flow types.\n"
        "- Product flow: exchanges that represent technical products, services, materials, or energy "
        "circulating within the technosphere and available for downstream use or with economic value.\n"
        "- Elementary flow: exchanges that connect the technosphere with the natural environment "
        "(emissions to air/water/soil or extractions of natural resources).\n"
        "- Waste flow: outputs that leave the technosphere as wastes or by-products requiring waste "
        "management or treatment by another activity.\n"
        "Use the provided data (exchange metadata, hints, candidate details) to pick the best match. "
        "Return strict JSON with keys `flow_type` (one of Product flow, Elementary flow, Waste flow) "
        "and `reason` summarising the evidence. Do not invent new categories."
    )

    def __init__(self, llm: LanguageModelProtocol | None = None) -> None:
        self._llm = llm

    def infer(self, exchange: Mapping[str, Any], hints: Mapping[str, list[str] | str]) -> str:
        context = self._build_context(exchange, hints)
        if self._llm is not None:
            try:
                response = self._llm.invoke(
                    {
                        "prompt": self.PROMPT,
                        "context": context,
                        "response_format": {"type": "json_object"},
                    }
                )
                payload = _ensure_mapping_response(response)
                flow_type = self._normalise_flow_type(payload.get("flow_type"))
                if flow_type:
                    return flow_type
                LOGGER.warning(
                    "flow_publish.flow_type_invalid_response",
                    response=payload,
                )
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.warning("flow_publish.flow_type_llm_failed", error=str(exc))
        fallback = self._heuristic_infer(context)
        LOGGER.debug("flow_publish.flow_type_fallback", selected=fallback)
        return fallback

    def _build_context(self, exchange: Mapping[str, Any], hints: Mapping[str, list[str] | str]) -> dict[str, Any]:
        return _compose_flow_context(exchange, hints)

    def _normalise_flow_type(self, raw: Any) -> str | None:
        if not isinstance(raw, str):
            return None
        candidate = raw.strip().lower()
        for allowed in self.ALLOWED_TYPES:
            if candidate == allowed.lower():
                return allowed
        if candidate in {"product", "productflow"}:
            return "Product flow"
        if candidate in {"elementary", "elementaryflow"}:
            return "Elementary flow"
        if candidate in {"waste", "wasteflow"}:
            return "Waste flow"
        return None

    def _heuristic_infer(self, context: Mapping[str, Any]) -> str:
        exchange = context.get("exchange", {})
        hints = context.get("flow_search_hints", {})
        direction = _coerce_text(exchange.get("direction")).lower()
        text_parts = [
            _coerce_text(exchange.get("name")),
            _coerce_text(exchange.get("general_comment")),
            " ".join(exchange.get("classification_path", [])),
            " ".join(exchange.get("tags", [])),
        ]
        hint_fragments: list[str] = []
        if isinstance(hints, Mapping):
            for values in hints.values():
                if isinstance(values, list):
                    hint_fragments.extend([_coerce_text(value) for value in values])
        text_parts.append(" ".join(fragment for fragment in hint_fragments if fragment))
        combined = " ".join(part for part in text_parts if part).lower()

        if any(keyword in combined for keyword in self.ELEMENTARY_KEYWORDS):
            return "Elementary flow"
        if "waste" in combined or any(keyword in combined for keyword in self.WASTE_KEYWORDS):
            return "Waste flow"
        if direction == "input" and any(term in combined for term in (" ambient air", "air", "water extraction", "surface water", "groundwater", "raw water", "ore", "crude")):
            return "Elementary flow"
        if direction == "output" and "slag" in combined:
            return "Waste flow"
        return "Product flow"


class FlowProductCategorySelector:
    """Select the most specific product category path using LLM-guided traversal."""

    STOP_CHOICES = {"stop", "none", "n/a", "na", "null", "skip"}
    PROMPT = (
        "You are helping to classify a life cycle assessment product flow into Tiangong's "
        "product category hierarchy. Each step provides the exchange context, any categories "
        "selected so far, and the direct children available at the current level.\n\n"
        "Return strict JSON with keys:\n"
        "- `choice`: the code of the selected option (must match one of the provided `options.code`) "
        "or `STOP` if none fit.\n"
        "- `reason`: short explanation of the selection.\n\n"
        "Prefer the most specific child that aligns with the exchange name, hints, and candidate data. "
        'If no option is appropriate, respond with `choice: "STOP"`.'
    )

    def __init__(
        self,
        llm: LanguageModelProtocol | None = None,
        *,
        script_path: Path | None = None,
        max_depth: int = 6,
    ) -> None:
        self._llm = llm
        self._script_path = Path(script_path) if script_path else PRODUCT_CATEGORY_SCRIPT
        self._max_depth = max(1, max_depth)
        self._cache: dict[str, list[tuple[str, str]]] = {}
        self._script_available = self._script_path.exists()
        if not self._script_available:
            LOGGER.warning("flow_publish.product_category_script_missing", path=str(self._script_path))

    def select_path(
        self,
        exchange: Mapping[str, Any],
        hints: Mapping[str, list[str] | str],
    ) -> list[tuple[str, str]]:
        context = _compose_flow_context(exchange, hints)
        path: list[tuple[str, str]] = []
        current_code: str | None = None
        for depth in range(self._max_depth):
            options = self._list_children(current_code)
            if not options:
                break
            choice = self._choose_option(context, path, options)
            if not choice:
                break
            match = next((item for item in options if item[0].lower() == choice.lower()), None)
            if not match:
                LOGGER.warning(
                    "flow_publish.product_category_invalid_choice",
                    choice=choice,
                    options=[code for code, _ in options],
                )
                break
            path.append(match)
            current_code = match[0]
        return path

    def _list_children(self, code: str | None) -> list[tuple[str, str]]:
        key = code or "__root__"
        if key in self._cache:
            return self._cache[key]
        if not self._script_available:
            self._cache[key] = []
            return []

        args = [sys.executable, str(self._script_path)]
        if code:
            args.append(code)
        try:
            result = subprocess.run(args, capture_output=True, text=True, check=False)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("flow_publish.product_category_call_failed", code=code, error=str(exc))
            children: list[tuple[str, str]] = []
        else:
            if result.returncode != 0:
                LOGGER.warning(
                    "flow_publish.product_category_script_error",
                    code=code,
                    stderr=result.stderr.strip(),
                )
                children = []
            else:
                children = []
                for raw_line in result.stdout.splitlines():
                    line = raw_line.strip()
                    if not line:
                        continue
                    if "\t" in line:
                        cat_code, desc = line.split("\t", 1)
                    else:
                        parts = line.split(None, 1)
                        cat_code = parts[0]
                        desc = parts[1] if len(parts) > 1 else ""
                    cat_code = cat_code.strip()
                    desc = desc.strip()
                    if cat_code:
                        children.append((cat_code, desc))
        self._cache[key] = children
        return children

    def _choose_option(
        self,
        context: Mapping[str, Any],
        path: list[tuple[str, str]],
        options: list[tuple[str, str]],
    ) -> str | None:
        if self._llm is not None:
            try:
                response = self._llm.invoke(
                    {
                        "prompt": self.PROMPT,
                        "context": {
                            "exchange_context": context,
                            "selected_path": self._path_payload(path),
                            "options": self._options_payload(options),
                        },
                        "response_format": {"type": "json_object"},
                    }
                )
                data = _ensure_mapping_response(response)
                choice = self._extract_choice(data, options)
                if choice:
                    return choice
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.warning("flow_publish.product_category_llm_failed", error=str(exc))
        return self._heuristic_option(context, options)

    def _extract_choice(self, data: Mapping[str, Any], options: list[tuple[str, str]]) -> str | None:
        raw_choice = _coerce_text(data.get("choice") or data.get("code") or data.get("class_id"))
        if not raw_choice:
            return None
        candidate = raw_choice.split()[0].strip()
        cleaned = candidate.rstrip(".")
        normalized = cleaned.lower()
        if normalized in self.STOP_CHOICES:
            return None
        for code, _ in options:
            if normalized == code.lower():
                return code
        for code, description in options:
            if normalized and normalized in description.lower():
                return code
        return None

    def _heuristic_option(self, context: Mapping[str, Any], options: list[tuple[str, str]]) -> str | None:
        if not options:
            return None
        exchange = context.get("exchange", {})
        hints = context.get("flow_search_hints", {})
        fragments: list[str] = [
            _coerce_text(exchange.get("name")),
            _coerce_text(exchange.get("general_comment")),
        ]
        fragments.extend(exchange.get("tags", []))
        if isinstance(hints, Mapping):
            for values in hints.values():
                if isinstance(values, list):
                    fragments.extend(values)
        haystack = " ".join(fragment for fragment in fragments if fragment).lower()
        if not haystack:
            return None

        best_code: str | None = None
        best_score = 0.0
        for code, desc in options:
            desc_lower = desc.lower()
            score = 0.0
            if desc_lower and desc_lower in haystack:
                score += 3.0
            if "electric" in desc_lower and "electric" in haystack:
                score += 2.5
            if code.lower() in haystack:
                score += 1.0
            if "transport" in desc_lower and "transport" in haystack:
                score += 1.5
            if score > best_score:
                best_score = score
                best_code = code
        return best_code

    @staticmethod
    def _options_payload(options: list[tuple[str, str]]) -> list[dict[str, str]]:
        return [{"code": code, "description": desc} for code, desc in options]

    @staticmethod
    def _path_payload(path: list[tuple[str, str]]) -> list[dict[str, str]]:
        return [{"code": code, "description": desc} for code, desc in path]


def _infer_flow_type(
    exchange: Mapping[str, Any],
    hints: Mapping[str, list[str] | str],
    *,
    classifier: FlowTypeClassifier | None = None,
    llm: LanguageModelProtocol | None = None,
) -> str:
    engine = classifier or FlowTypeClassifier(llm)
    return engine.infer(exchange, hints)


def _classification_from_path(path: Sequence[tuple[str, str]]) -> dict[str, Any]:
    classes = [
        {
            "@level": str(index),
            "@classId": code,
            "#text": description,
        }
        for index, (code, description) in enumerate(path)
    ]
    if not classes:
        return _default_product_classification()
    return {"common:classification": {"common:class": classes}}


def _default_product_classification() -> dict[str, Any]:
    return {
        "common:classification": {
            "common:class": [
                {
                    "@level": "0",
                    "@classId": "1",
                    "#text": "Ores and minerals; electricity, gas and water",
                }
            ]
        }
    }


def _derive_language_pairs(hints: Mapping[str, list[str] | str], fallback: str) -> tuple[str, str]:
    en_candidates = [item for item in hints.get("en_synonyms", []) or [] if isinstance(item, str)]
    zh_candidates = [item for item in hints.get("zh_synonyms", []) or [] if isinstance(item, str)]
    en = en_candidates[0] if en_candidates else fallback
    zh = zh_candidates[0] if zh_candidates else en
    return en, zh


def _get_nested(mapping: Mapping[str, Any], path: Sequence[str]) -> Any:
    """Return nested value via keys, or None when any level is missing."""
    current: Any = mapping
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _resolve_dataset_root(
    payload: Mapping[str, Any],
    *,
    root_key: str | None,
    dataset_kind: str,
) -> Mapping[str, Any]:
    """Return the ILCD dataset block and validate its structure."""
    if root_key is None:
        target = payload
    else:
        target = payload.get(root_key)
        if target is None:
            raise SpecCodingError(f"{dataset_kind} payload missing '{root_key}'.")
    if not isinstance(target, Mapping):
        location = root_key or "root"
        raise SpecCodingError(f"{dataset_kind} payload must be an object at '{location}'.")
    return target


def _require_uuid(value: Any, dataset_kind: str) -> str:
    uuid_value = _coerce_text(value)
    if not uuid_value:
        raise SpecCodingError(f"Missing common:UUID for {dataset_kind} dataset.")
    return uuid_value


def _build_elementary_classification(hints: Mapping[str, list[str] | str]) -> dict[str, Any]:
    usage = _coerce_text(hints.get("usage_context"))
    usage_lower = usage.lower()
    if "air" in usage_lower or "vent" in usage_lower:
        path = ["Emissions", "Emissions to air", "Emissions to air, unspecified"]
    elif "water" in usage_lower or "effluent" in usage_lower:
        path = ["Emissions", "Emissions to water", "Emissions to water, unspecified"]
    elif "soil" in usage_lower:
        path = ["Emissions", "Emissions to soil", "Emissions to soil, unspecified"]
    else:
        path = ["Emissions", "Emissions to unspecified"]
    categories = []
    for level, label in enumerate(path):
        categories.append({"@level": str(level), "#text": label})
    return {"common:elementaryFlowCategorization": {"common:category": categories}}


def _extract_general_comment(exchange: Mapping[str, Any]) -> str:
    comment = exchange.get("generalComment")
    if isinstance(comment, dict):
        text = comment.get("#text")
        if isinstance(text, str):
            return text.strip()
    if isinstance(comment, str):
        return comment.strip()
    return ""


def _resolve_unit(exchange: Mapping[str, Any]) -> str:
    return _coerce_text(exchange.get("unit"))


def _language_entry(text: str, lang: str = "en") -> dict[str, Any]:
    return {"@xml:lang": lang, "#text": text}


@dataclass(slots=True, frozen=True)
class FlowPropertyOverride:
    """Override entry used to customise flow property selection."""

    flow_property_uuid: str
    mean_value: str | None = None


@dataclass
class FlowPublishPlan:
    """Single flow payload ready for publication."""

    uuid: str
    exchange_name: str
    process_name: str
    dataset: Mapping[str, Any]
    exchange_ref: Mapping[str, Any]
    mode: str = "insert"
    flow_property_uuid: str | None = None


class DatabaseCrudClient:
    """Client wrapper over Database_CRUD_Tool for flows/processes CRUD."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        mcp_client: MCPToolClient | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._mcp = mcp_client or MCPToolClient(self._settings)
        self._server_name = self._settings.flow_search_service_name

    def insert_flow(self, dataset: Mapping[str, Any]) -> dict[str, Any]:
        root_key = "flowDataSet" if "flowDataSet" in dataset else None
        flow_root = _resolve_dataset_root(dataset, root_key=root_key, dataset_kind="flow")
        uuid_value = _require_uuid(
            _get_nested(flow_root, ("flowInformation", "dataSetInformation", "common:UUID")),
            "flow",
        )
        json_payload = dataset if root_key else {"flowDataSet": flow_root}
        return self._invoke(
            {
                "operation": "insert",
                "table": "flows",
                "id": uuid_value,
                "jsonOrdered": json_payload,
            }
        )

    def update_flow(self, dataset: Mapping[str, Any]) -> dict[str, Any]:
        root_key = "flowDataSet" if "flowDataSet" in dataset else None
        flow_root = _resolve_dataset_root(dataset, root_key=root_key, dataset_kind="flow")
        uuid_value = _require_uuid(
            _get_nested(flow_root, ("flowInformation", "dataSetInformation", "common:UUID")),
            "flow",
        )
        version_candidate = _coerce_text(
            _get_nested(
                flow_root,
                ("administrativeInformation", "publicationAndOwnership", "common:dataSetVersion"),
            )
        )
        if not version_candidate:
            version_candidate = "01.01.000"
        json_payload = dataset if root_key else {"flowDataSet": flow_root}
        return self._invoke(
            {
                "operation": "update",
                "table": "flows",
                "id": uuid_value,
                "version": version_candidate,
                "jsonOrdered": json_payload,
            }
        )

    def insert_process(self, dataset: Mapping[str, Any]) -> dict[str, Any]:
        root_key = "processDataSet" if "processDataSet" in dataset else None
        process_root = _resolve_dataset_root(dataset, root_key=root_key, dataset_kind="process")
        uuid_value = _require_uuid(
            _get_nested(process_root, ("processInformation", "dataSetInformation", "common:UUID")),
            "process",
        )
        json_payload = dataset if root_key else {"processDataSet": process_root}
        return self._invoke(
            {
                "operation": "insert",
                "table": "processes",
                "id": uuid_value,
                "jsonOrdered": json_payload,
            }
        )

    def update_process(self, dataset: Mapping[str, Any]) -> dict[str, Any]:
        root_key = "processDataSet" if "processDataSet" in dataset else None
        process_root = _resolve_dataset_root(dataset, root_key=root_key, dataset_kind="process")
        uuid_value = _require_uuid(
            _get_nested(process_root, ("processInformation", "dataSetInformation", "common:UUID")),
            "process",
        )
        version_candidate = _coerce_text(
            _get_nested(
                process_root,
                ("administrativeInformation", "publicationAndOwnership", "common:dataSetVersion"),
            )
        )
        if not version_candidate:
            version_candidate = "01.01.000"
        json_payload = dataset if root_key else {"processDataSet": process_root}
        return self._invoke(
            {
                "operation": "update",
                "table": "processes",
                "id": uuid_value,
                "version": version_candidate,
                "jsonOrdered": json_payload,
            }
        )

    def delete(self, table: str, record_id: str, version: str) -> dict[str, Any]:
        return self._invoke(
            {
                "operation": "delete",
                "table": table,
                "id": record_id,
                "version": version,
            }
        )

    def _invoke(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        LOGGER.debug("crud.invoke", table=payload.get("table"), operation=payload.get("operation"))
        raw = self._mcp.invoke_json_tool(self._server_name, DATABASE_TOOL_NAME, payload)
        if isinstance(raw, str):
            return json.loads(raw)
        if isinstance(raw, dict):
            return raw
        raise SpecCodingError("Unexpected payload returned from Database_CRUD_Tool")

    def close(self) -> None:
        self._mcp.close()


class FlowPublisher:
    """Build and optionally publish flow datasets for unmatched or deficient exchanges."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        crud_client: DatabaseCrudClient | None = None,
        dry_run: bool = True,
        flow_property_registry: FlowPropertyRegistry | None = None,
        default_flow_property_uuid: str | None = None,
        flow_property_overrides: Mapping[tuple[str | None, str], FlowPropertyOverride] | None = None,
        llm: LanguageModelProtocol | None = None,
        flow_type_classifier: FlowTypeClassifier | None = None,
        product_category_selector: FlowProductCategorySelector | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._crud = crud_client or DatabaseCrudClient(self._settings)
        self._dry_run = dry_run
        self._registry = flow_property_registry or get_default_registry()
        self._default_flow_property_uuid = self._resolve_default_property(default_flow_property_uuid)
        self._overrides = dict(flow_property_overrides or {})
        self._flow_type_classifier = flow_type_classifier or FlowTypeClassifier(llm)
        self._product_category_selector = product_category_selector or FlowProductCategorySelector(llm)
        self._prepared: list[FlowPublishPlan] = []

    def _resolve_default_property(self, requested: str | None) -> str:
        if requested:
            try:
                self._registry.get(requested)
                return requested
            except KeyError as exc:  # pragma: no cover - configuration errors
                raise SpecCodingError(f"Unknown default flow property UUID: {requested}") from exc
        # Prefer Mass when available.
        try:
            return self._registry.get("93a60a56-a3c8-11da-a746-0800200b9a66").uuid
        except KeyError:
            descriptors = self._registry.list()
            if not descriptors:
                raise SpecCodingError("Flow property registry is empty")
            return descriptors[0].uuid

    def prepare_from_alignment(self, alignment: Iterable[Mapping[str, Any]]) -> list[FlowPublishPlan]:
        """Generate publication plans for unmatched exchanges and matched flows missing properties."""
        plans: list[FlowPublishPlan] = []
        for entry in alignment:
            process_name = _coerce_text(entry.get("process_name")) or "Unknown process"
            origin = entry.get("origin_exchanges") or {}
            for exchanges in origin.values():
                exchanges_iter = [exchanges] if isinstance(exchanges, Mapping) else list(exchanges or [])
                for exchange in exchanges_iter:
                    if not isinstance(exchange, Mapping):
                        continue
                    exchange_name = _coerce_text(exchange.get("exchangeName")) or "Unnamed exchange"
                    candidate = self._extract_selected_candidate(exchange)
                    property_uuid, mean_value = self._resolve_flow_property(process_name, exchange_name, exchange, candidate)
                    if property_uuid is None:
                        continue
                    ref = exchange.get("referenceToFlowDataSet")
                    if not isinstance(ref, Mapping):
                        ref = {}
                    if self._is_placeholder_reference(ref):
                        plan = self._build_plan(
                            exchange,
                            process_name,
                            property_uuid,
                            mean_value,
                            candidate=None,
                            mode="insert",
                            existing_ref=None,
                        )
                    else:
                        if candidate is None:
                            continue
                        if self._candidate_has_required_property(candidate, property_uuid):
                            continue
                        plan = self._build_plan(
                            exchange,
                            process_name,
                            property_uuid,
                            mean_value,
                            candidate=candidate,
                            mode="update",
                            existing_ref=ref,
                        )
                    if plan is not None:
                        plans.append(plan)
        self._prepared = plans
        LOGGER.info("flow_publish.plans_ready", count=len(plans))
        return plans

    def publish(self) -> list[dict[str, Any]]:
        """Execute inserts or updates for the prepared plans."""
        results: list[dict[str, Any]] = []
        for plan in self._prepared:
            if self._dry_run:
                LOGGER.info(
                    "flow_publish.dry_run",
                    exchange=plan.exchange_name,
                    process=plan.process_name,
                    uuid=plan.uuid,
                    mode=plan.mode,
                )
                continue
            payload = {"flowDataSet": plan.dataset}
            if plan.mode == "update":
                result = self._crud.update_flow(payload)
            else:
                result = self._crud.insert_flow(payload)
            results.append(result)
        return results

    def close(self) -> None:
        self._crud.close()

    @staticmethod
    def _is_placeholder_reference(reference: Mapping[str, Any]) -> bool:
        return bool(reference.get("unmatched:placeholder"))

    @staticmethod
    def _extract_selected_candidate(exchange: Mapping[str, Any]) -> Mapping[str, Any] | None:
        detail = exchange.get("matchingDetail")
        if not isinstance(detail, Mapping):
            return None
        candidate = detail.get("selectedCandidate")
        if isinstance(candidate, Mapping):
            return candidate
        return None

    def _resolve_flow_property(
        self,
        process_name: str,
        exchange_name: str,
        exchange: Mapping[str, Any],
        candidate: Mapping[str, Any] | None,
    ) -> tuple[str | None, str | None]:
        override = self._overrides.get((process_name, exchange_name)) or self._overrides.get((None, exchange_name))
        if override:
            try:
                self._registry.get(override.flow_property_uuid)
            except KeyError as exc:
                raise SpecCodingError(f"Unknown flow property in override: {override.flow_property_uuid}") from exc
            return override.flow_property_uuid, override.mean_value

        candidate_property = _coerce_text(candidate.get("flow_properties")) if candidate else ""
        if candidate_property:
            descriptor = self._registry.fuzzy_match(candidate_property)
            if descriptor:
                return descriptor.uuid, None

        return self._default_flow_property_uuid, None

    def _candidate_has_required_property(
        self,
        candidate: Mapping[str, Any],
        expected_uuid: str,
    ) -> bool:
        candidate_property = _coerce_text(candidate.get("flow_properties"))
        if not candidate_property:
            return False
        descriptor = self._registry.fuzzy_match(candidate_property)
        if descriptor is None:
            return False
        return descriptor.uuid.lower() == expected_uuid.lower()

    def _build_plan(
        self,
        exchange: Mapping[str, Any],
        process_name: str,
        property_uuid: str,
        mean_value: str | None,
        *,
        candidate: Mapping[str, Any] | None,
        mode: str,
        existing_ref: Mapping[str, Any] | None,
    ) -> Optional[FlowPublishPlan]:
        dataset = self._compose_flow_dataset(
            exchange,
            process_name,
            property_uuid,
            mean_value,
            candidate=candidate,
            mode=mode,
            existing_ref=existing_ref,
        )
        if dataset is None:
            return None
        flow_dataset, exchange_ref = dataset
        return FlowPublishPlan(
            uuid=exchange_ref.get("@refObjectId", ""),
            exchange_name=_coerce_text(exchange.get("exchangeName")) or "Unnamed exchange",
            process_name=process_name,
            dataset=flow_dataset,
            exchange_ref=exchange_ref,
            mode=mode,
            flow_property_uuid=property_uuid,
        )

    def _compose_flow_dataset(
        self,
        exchange: Mapping[str, Any],
        process_name: str,
        property_uuid: str,
        mean_value: str | None,
        *,
        candidate: Mapping[str, Any] | None,
        mode: str,
        existing_ref: Mapping[str, Any] | None,
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        exchange_name = _coerce_text(exchange.get("exchangeName")) or "Unnamed exchange"
        comment = _extract_general_comment(exchange)
        hints = _parse_flowsearch_hints(comment)
        flow_type = _infer_flow_type(exchange, hints, classifier=self._flow_type_classifier)
        if flow_type == "Elementary flow":
            LOGGER.warning(
                "flow_publish.skip_elementary",
                exchange=exchange_name,
                process=process_name,
                reason="Elementary flows must reuse existing records.",
            )
            return None

        uuid_value = self._resolve_flow_uuid(candidate, existing_ref)
        version = self._resolve_flow_version(candidate, existing_ref, mode)
        en_name, zh_name = self._resolve_language_pairs(candidate, hints, exchange_name)
        classification = self._resolve_classification(flow_type, hints, candidate, exchange)
        comment_entries = self._resolve_comments(comment, candidate, exchange_name)
        flow_property_block = self._registry.build_flow_property_block(
            property_uuid,
            mean_value=mean_value or "1.0",
        )
        unit = _resolve_unit(exchange)
        if unit and property_uuid == self._default_flow_property_uuid and property_uuid == "93a60a56-a3c8-11da-a746-0800200b9a66" and unit.lower() in {"kwh", "mj", "gj"}:
            LOGGER.warning(
                "flow_publish.energy_property_placeholder",
                unit=unit,
                note="Flow property defaults to mass; please update energy reference manually.",
            )

        dataset = {
            "@xmlns": "http://lca.jrc.it/ILCD/Flow",
            "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
            "@xmlns:ecn": "http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "@locations": "../ILCDLocations.xml",
            "@version": "1.1",
            "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
            "flowInformation": {
                "dataSetInformation": {
                    "common:UUID": uuid_value,
                    "name": self._build_name_block(candidate, hints, en_name, zh_name),
                    "common:synonyms": self._build_synonyms(hints, en_name, zh_name),
                    "common:generalComment": comment_entries,
                    "classificationInformation": classification,
                },
                "quantitativeReference": {
                    "referenceToReferenceFlowProperty": "0",
                },
            },
            "modellingAndValidation": self._build_modelling_section(flow_type),
            "administrativeInformation": self._build_administrative_section(version),
            "flowProperties": flow_property_block,
        }

        uri = build_portal_uri("flow", uuid_value, version)
        exchange_ref = {
            "@type": "flow data set",
            "@uri": uri,
            "@refObjectId": uuid_value,
            "@version": version,
            "common:shortDescription": _language_entry(exchange_name),
        }
        return dataset, exchange_ref

    @staticmethod
    def _resolve_flow_uuid(
        candidate: Mapping[str, Any] | None,
        existing_ref: Mapping[str, Any] | None,
    ) -> str:
        candidate_uuid = _coerce_text(candidate.get("uuid")) if candidate else ""
        if candidate_uuid:
            return candidate_uuid
        if existing_ref:
            uuid_value = _coerce_text(existing_ref.get("@refObjectId"))
            if uuid_value:
                return uuid_value
        return str(uuid.uuid4())

    def _resolve_flow_version(
        self,
        candidate: Mapping[str, Any] | None,
        existing_ref: Mapping[str, Any] | None,
        mode: str,
    ) -> str:
        base_version = _coerce_text(candidate.get("version")) if candidate else ""
        if not base_version and existing_ref:
            base_version = _coerce_text(existing_ref.get("@version"))
        if not base_version:
            base_version = "01.01.000"
        if mode == "update":
            return _bump_version(base_version)
        return base_version

    @staticmethod
    def _resolve_language_pairs(
        candidate: Mapping[str, Any] | None,
        hints: Mapping[str, list[str] | str],
        fallback: str,
    ) -> tuple[str, str]:
        candidate_name = _coerce_text(candidate.get("base_name")) if candidate else ""
        base = candidate_name or fallback
        en_name, zh_name = _derive_language_pairs(hints, base)
        return en_name, zh_name

    def _resolve_classification(
        self,
        flow_type: str,
        hints: Mapping[str, list[str] | str],
        candidate: Mapping[str, Any] | None,
        exchange: Mapping[str, Any],
    ) -> dict[str, Any]:
        if flow_type == "Elementary flow":
            return _build_elementary_classification(hints)
        classification_data = candidate.get("classification") if isinstance(candidate, Mapping) else None
        if isinstance(classification_data, list) and classification_data:
            classes: list[dict[str, Any]] = []
            for index, item in enumerate(classification_data):
                if not isinstance(item, Mapping):
                    continue
                level = _coerce_text(item.get("@level")) or str(index)
                class_entry = {
                    "@level": level,
                    "#text": _coerce_text(item.get("#text")) or "",
                }
                class_id = _coerce_text(item.get("@classId"))
                if class_id:
                    class_entry["@classId"] = class_id
                classes.append(class_entry)
            if classes:
                return {"common:classification": {"common:class": classes}}
        if flow_type != "Product flow":
            return _default_product_classification()
        path: list[tuple[str, str]] = []
        try:
            path = self._product_category_selector.select_path(exchange, hints)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("flow_publish.product_category_select_failed", error=str(exc))
            path = []
        if path:
            return _classification_from_path(path)
        LOGGER.debug("flow_publish.product_category_fallback", reason="no_path_selected")
        return _default_product_classification()

    @staticmethod
    def _resolve_comments(comment: str, candidate: Mapping[str, Any] | None, exchange_name: str) -> list[dict[str, Any]]:
        candidate_comment = _coerce_text(candidate.get("general_comment")) if candidate else ""
        if candidate_comment:
            return [_language_entry(candidate_comment)]
        if comment:
            return [_language_entry(comment)]
        return [_language_entry(f"Auto-generated for {exchange_name}")]

    def _build_name_block(
        self,
        candidate: Mapping[str, Any] | None,
        hints: Mapping[str, list[str] | str],
        en_name: str,
        zh_name: str,
    ) -> dict[str, Any]:
        treatment = _coerce_text(candidate.get("treatment_standards_routes")) if candidate else ""
        treatment_values = hints.get("treatmentStandardsRoutes") if isinstance(hints.get("treatmentStandardsRoutes"), list) else []
        mix = _coerce_text(candidate.get("mix_and_location_types")) if candidate else ""
        mix_values = hints.get("mixAndLocationTypes") if isinstance(hints.get("mixAndLocationTypes"), list) else []
        return {
            "baseName": [
                _language_entry(en_name, "en"),
                _language_entry(zh_name, "zh"),
            ],
            "treatmentStandardsRoutes": [
                _language_entry(treatment or treatment_values[0] if treatment_values else en_name, "en"),
            ],
            "mixAndLocationTypes": [
                _language_entry(mix or mix_values[0] if mix_values else en_name, "en"),
            ],
        }

    @staticmethod
    def _build_synonyms(
        hints: Mapping[str, list[str] | str],
        en_name: str,
        zh_name: str,
    ) -> list[dict[str, Any]]:
        en_values = hints.get("en_synonyms") or []
        zh_values = hints.get("zh_synonyms") or []
        if isinstance(en_values, str):
            en_values = [en_values]
        if isinstance(zh_values, str):
            zh_values = [zh_values]
        en_synonyms = "; ".join(en_values or [en_name])
        zh_synonyms = "; ".join(zh_values or [zh_name])
        return [
            _language_entry(en_synonyms, "en"),
            _language_entry(zh_synonyms, "zh"),
        ]

    @staticmethod
    def _build_modelling_section(flow_type: str) -> dict[str, Any]:
        modelling_section: dict[str, Any] = {
            "LCIMethod": {
                "typeOfDataSet": flow_type,
            },
        }
        compliance_block = flow_compliance_declarations()
        if compliance_block:
            modelling_section["complianceDeclarations"] = compliance_block
        return modelling_section

    @staticmethod
    def _build_administrative_section(version: str) -> dict[str, Any]:
        def _default_contact_reference() -> dict[str, Any]:
            contact_uuid = "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8"
            contact_version = "01.00.000"
            return {
                "@type": "contact data set",
                "@refObjectId": contact_uuid,
                "@uri": build_local_dataset_uri("contact data set", contact_uuid, contact_version),
                "@version": contact_version,
                "common:shortDescription": [_language_entry("Tiangong LCA Data Working Group")],
            }

        return {
            "dataEntryBy": {
                "common:timeStamp": _utc_timestamp(),
                "common:referenceToDataSetFormat": build_dataset_format_reference(),
                "common:referenceToPersonOrEntityEnteringTheData": _default_contact_reference(),
            },
            "publicationAndOwnership": {
                "common:dataSetVersion": version,
                "common:referenceToOwnershipOfDataSet": _default_contact_reference(),
            },
        }


def _bump_version(version: str) -> str:
    """Increment the patch component of an ILCD version string."""
    parts = version.split(".")
    if len(parts) != 3:
        return version
    major, minor, patch = parts
    try:
        patch_int = int(patch)
    except ValueError:
        return version
    return f"{major}.{minor}.{patch_int + 1:03d}"


class ProcessPublisher:
    """Publish final process datasets once validation passes."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        crud_client: DatabaseCrudClient | None = None,
        dry_run: bool = True,
    ) -> None:
        self._settings = settings or get_settings()
        self._crud = crud_client or DatabaseCrudClient(self._settings)
        self._dry_run = dry_run

    def publish(self, datasets: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for dataset in datasets:
            process_payload: Mapping[str, Any]
            if isinstance(dataset, Mapping):
                candidate = dataset.get("process_data_set") or dataset.get("processDataSet")
                if isinstance(candidate, Mapping):
                    process_payload = candidate
                else:
                    process_payload = dataset
            else:
                raise SpecCodingError("Process dataset must be a mapping.")
            payload = {"processDataSet": process_payload}
            process_info = process_payload.get("processInformation", {})
            name_block = process_info.get("dataSetInformation", {}).get("name", {})
            process_name = _coerce_text(name_block.get("baseName"))
            uuid_value = _coerce_text(process_info.get("dataSetInformation", {}).get("common:UUID"))
            if self._dry_run:
                LOGGER.info("process_publish.dry_run", name=process_name)
                continue
            try:
                result = self._crud.insert_process(payload)
            except SpecCodingError:
                try:
                    result = self._crud.update_process(payload)
                except SpecCodingError as exc:  # pragma: no cover - network errors bubbled up
                    raise SpecCodingError(f"Failed to publish process '{process_name or uuid_value}' ({uuid_value})") from exc
            results.append(result)
        return results

    def close(self) -> None:
        self._crud.close()
