#!/usr/bin/env python
# ruff: noqa: E402
"""Stage 1 (JSON-LD): LLM-assisted conversion of OpenLCA JSON-LD into ILCD datasets."""

from __future__ import annotations

import argparse
import json
import re
import importlib.resources as resources
import sys
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))
SRC_ROOT = REPO_ROOT / "src"
if SRC_ROOT.exists() and str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

import jsonschema
from jsonschema import Draft7Validator

try:
    from scripts.md._workflow_common import (  # type: ignore
        OpenAIResponsesLLM,
        dump_json,
        ensure_run_cache_dir,
        load_secrets,
        run_cache_path,
        save_latest_run_id,
    )
except ModuleNotFoundError:  # pragma: no cover
    from _workflow_common import (  # type: ignore
        OpenAIResponsesLLM,
        dump_json,
        ensure_run_cache_dir,
        load_secrets,
        run_cache_path,
        save_latest_run_id,
    )
from tiangong_lca_spec.core.exceptions import ProcessExtractionError
from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.core.uris import build_portal_uri
from tiangong_lca_spec.jsonld import JSONLDFlowExtractor, JSONLDProcessExtractor, JSONLDSourceExtractor
from tiangong_lca_spec.jsonld.converters import (
    JSONLDFlowConverter,
    JSONLDProcessConverter,
    JSONLDSourceConverter,
    collect_jsonld_files,
)
from tiangong_lca_spec.jsonld.process_overrides import apply_jsonld_process_overrides
from tiangong_lca_spec.location import extract_location_response, get_location_catalog
from tiangong_lca_spec.process_extraction.extractors import (
    LocationNormalizer,
    ProcessClassifier,
    ProductFlowClassifier,
)
from tiangong_lca_spec.process_extraction.tidas_mapping import build_tidas_process_dataset
from tiangong_lca_spec.tidas.flow_classification_registry import ensure_valid_product_flow_classification
from tiangong_lca_spec.workflow.artifacts import (
    DEFAULT_DATA_SET_VERSION,
    flow_compliance_declarations,
)

FLOW_XMLNS = {
    "@xmlns": "http://lca.jrc.it/ILCD/Flow",
    "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
    "@xmlns:ecn": "http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber",
    "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
    "@version": "1.1",
    "@locations": "../ILCDLocations.xml",
}
SOURCE_XMLNS = {
    "@xmlns": "http://lca.jrc.it/ILCD/Source",
    "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
    "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Source ../../schemas/ILCD_SourceDataSet.xsd",
    "@version": "1.1",
}
DEFAULT_LICENSE = "Free of charge for all users and uses"
MASS_FLOW_PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200b9a66"
MASS_FLOW_PROPERTY_VERSION = "03.00.003"
# Transport service property (kg*km)
MASS_DISTANCE_FLOW_PROPERTY_UUID = "118f2a40-50ec-457c-aa60-9bc6b6af9931"
MASS_DISTANCE_FLOW_PROPERTY_VERSION = "01.01.000"
FLOW_PROPERTY_VERSION_OVERRIDES: dict[str, str] = {
    "838aaa23-0117-11db-92e3-0800200c9a66": "03.00.000",
    "01846770-4cfe-4a25-8ad9-919d8d378345": "03.00.004",
    "16764bbb-d1ea-4eb4-9911-13f0ecd3dfad": "01.01.000",
    "341fd786-b2ad-4552-a762-5eafcab45dee": "01.00.003",
    "441238a3-ba09-46ec-b35b-c30cfba746d1": "02.00.003",
    MASS_DISTANCE_FLOW_PROPERTY_UUID: MASS_DISTANCE_FLOW_PROPERTY_VERSION,
    "93a60a56-a3c8-11da-a746-0800200c9a66": "03.00.003",
}
TIANGONG_CONTACT_UUID = "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8"
TIANGONG_CONTACT_VERSION = "01.00.000"
ILCD_FORMAT_SOURCE_UUID = "a97a0155-0234-4b87-b4ce-a45da52f2a40"
ILCD_FORMAT_SOURCE_VERSION = "03.00.003"
ILCD_COMPLIANCE_SOURCE_UUID = "d92a1a12-2545-49e2-a585-55c259997756"
ILCD_COMPLIANCE_SOURCE_VERSION = "20.20.002"
VERSION_SUFFIX_RE = re.compile(r"_[0-9]{2}\.[0-9]{2}\.[0-9]{3}$")
REFERENCE_YEAR_RE = re.compile(r"(18|19|20|21)\d{2}")
GENERIC_PROCESS_CLASS_PATH = [
    {"@level": "0", "@classId": "C", "#text": "Manufacturing"},
    {"@level": "1", "@classId": "27", "#text": "Manufacture of electrical equipment"},
    {"@level": "2", "@classId": "272", "#text": "Manufacture of batteries and accumulators"},
    {"@level": "3", "@classId": "2720", "#text": "Manufacture of batteries and accumulators"},
]

BATTERY_PROCESS_CLASS_PATH = [
    {"@level": "0", "@classId": "C", "#text": "Manufacturing"},
    {"@level": "1", "@classId": "27", "#text": "Manufacture of electrical equipment"},
    {"@level": "2", "@classId": "272", "#text": "Manufacture of batteries and accumulators"},
    {"@level": "3", "@classId": "2720", "#text": "Manufacture of batteries and accumulators"},
]
GENERIC_FLOW_CLASS_PATH = [
    {"@level": "0", "@classId": "C", "#text": "Manufacturing"},
    {"@level": "1", "@classId": "27", "#text": "Manufacture of electrical equipment"},
    {"@level": "2", "@classId": "272", "#text": "Manufacture of batteries and accumulators"},
    {"@level": "3", "@classId": "2720", "#text": "Manufacture of batteries and accumulators"},
    {"@level": "4", "@classId": "272000", "#text": "Generic battery products"},
]
BATTERY_FLOW_CLASS_PATH = GENERIC_FLOW_CLASS_PATH
SOURCE_CATEGORY_CLASS_MAP: dict[str, tuple[str, str]] = {
    "images": ("0", "Images"),
    "image": ("0", "Images"),
    "data set formats": ("1", "Data set formats"),
    "dataset formats": ("1", "Data set formats"),
    "databases": ("2", "Databases"),
    "compliance systems": ("3", "Compliance systems"),
    "compliance system": ("3", "Compliance systems"),
    "statistical classifications": ("4", "Statistical classifications"),
    "publications and communications": ("5", "Publications and communications"),
    "publication": ("5", "Publications and communications"),
    "publications": ("5", "Publications and communications"),
    "other source types": ("6", "Other source types"),
    "other": ("6", "Other source types"),
}
SOURCE_PUBLICATION_TYPE_MAP: dict[str, str] = {
    "images": "Other unpublished and grey literature",
    "data set formats": "Software or database",
    "dataset formats": "Software or database",
    "databases": "Software or database",
    "compliance systems": "Other unpublished and grey literature",
    "statistical classifications": "Other unpublished and grey literature",
    "publications and communications": "Article in periodical",
    "publication": "Article in periodical",
    "publications": "Article in periodical",
}
DEFAULT_SOURCE_CLASS = ("6", "Other source types")
DEFAULT_PUBLICATION_TYPE = "Undefined"

PROCESS_ROOT_SECTIONS: tuple[str, ...] = ("exchanges", "modellingAndValidation", "administrativeInformation", "LCIAResults")

LOGGER = get_logger(__name__)
SCHEMA_DIR = Path(resources.files("tidas_tools.tidas.schemas"))
FLOW_SCHEMA_FILE = "tidas_flows.json"
PROCESS_SCHEMA_FILE = "tidas_processes.json"
SOURCE_SCHEMA_FILE = "tidas_sources.json"

_STRICT_SCHEMA_STORE: dict[str, dict[str, Any]] | None = None
_VALIDATOR_CACHE: dict[str, Draft7Validator] = {}

PROCESS_NAME_RECOVERY_PROMPT = """
You are filling in missing ILCD process naming fields. The JSON context includes the current
`processInformation` plus the original OpenLCA JSON-LD payload. Use all available evidence to
infer concise English text for the requested fields. Only respond with JSON containing any
of the keys `baseName`, `treatmentStandardsRoutes`, and `mixAndLocationTypes`. Each value
must be a short phrase that would make sense in the ILCD name block. Do not invent data beyond
what can be inferred from the context, and prefer specific technical descriptors over placeholders.
"""


def _language_entry(text: str | None, lang: str = "en") -> dict[str, str]:
    value = (text or "").strip() or "Unspecified"
    return {"@xml:lang": lang, "#text": value}


def _current_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


FLOW_LCI_METHOD_MAP = {
    "PRODUCT_FLOW": "Product flow",
    "PRODUCT FLOW": "Product flow",
    "PRODUCT": "Product flow",
    "WASTE_FLOW": "Waste flow",
    "WASTE FLOW": "Waste flow",
    "WASTE": "Waste flow",
    "ELEMENTARY_FLOW": "Elementary flow",
    "ELEMENTARY FLOW": "Elementary flow",
    "ELEMENTARY": "Elementary flow",
}


def _contact_reference() -> dict[str, Any]:
    return {
        "@refObjectId": TIANGONG_CONTACT_UUID,
        "@type": "contact data set",
        "@uri": f"../contacts/{TIANGONG_CONTACT_UUID}_{TIANGONG_CONTACT_VERSION}.xml",
        "@version": TIANGONG_CONTACT_VERSION,
        "common:shortDescription": [
            _language_entry("Tiangong LCA Data Working Group"),
            _language_entry("天工LCA数据团队", "zh"),
        ],
    }


def _format_reference() -> dict[str, Any]:
    return {
        "@refObjectId": ILCD_FORMAT_SOURCE_UUID,
        "@type": "source data set",
        "@uri": f"../sources/{ILCD_FORMAT_SOURCE_UUID}_{ILCD_FORMAT_SOURCE_VERSION}.xml",
        "@version": ILCD_FORMAT_SOURCE_VERSION,
        "common:shortDescription": _language_entry("ILCD format"),
    }


def _ownership_reference() -> dict[str, Any]:
    return deepcopy(_contact_reference())


def _compliance_reference() -> dict[str, Any]:
    return {
        "@refObjectId": ILCD_COMPLIANCE_SOURCE_UUID,
        "@type": "source data set",
        "@uri": f"../sources/{ILCD_COMPLIANCE_SOURCE_UUID}_{ILCD_COMPLIANCE_SOURCE_VERSION}.xml",
        "@version": ILCD_COMPLIANCE_SOURCE_VERSION,
        "common:shortDescription": _language_entry("ILCD Data Network - Entry-level"),
    }


def _permanent_dataset_uri(kind: str, uuid_value: str, version: str) -> str:
    return build_portal_uri(kind, uuid_value, version)


def _coerce_dataset_version(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("#text", "@value", "value", "text"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    elif isinstance(value, (int, float)):
        candidate = f"{value}".strip()
        if candidate:
            return candidate
    elif isinstance(value, str) and value.strip():
        return value.strip()
    return DEFAULT_DATA_SET_VERSION


def _extract_admin_version(admin_block: dict[str, Any]) -> str:
    publication_block = admin_block.get("publicationAndOwnership")
    if isinstance(publication_block, dict):
        return _coerce_dataset_version(publication_block.get("common:dataSetVersion"))
    return DEFAULT_DATA_SET_VERSION


def _merge_template(target: dict[str, Any], template: dict[str, Any]) -> None:
    for key, value in template.items():
        if isinstance(value, dict):
            child = target.setdefault(key, {})
            if isinstance(child, dict):
                _merge_template(child, value)
        elif isinstance(value, list):
            target.setdefault(key, deepcopy(value))
        else:
            target.setdefault(key, value)


class _SourceExchangeIndex:
    """Index JSON-LD exchanges for later metadata reconciliation."""

    def __init__(self, exchanges: list[dict[str, Any]]) -> None:
        self._by_flow_direction: dict[tuple[str, bool], deque[dict[str, Any]]] = {}
        self._by_flow: dict[str, deque[dict[str, Any]]] = {}
        self._all: deque[dict[str, Any]] = deque([exchange for exchange in exchanges if isinstance(exchange, dict)])
        self._matched: set[int] = set()
        for exchange in exchanges:
            if not isinstance(exchange, dict):
                continue
            flow = exchange.get("flow", {})
            flow_id = _strip_version_suffix(flow.get("@id") or flow.get("id"))
            if not flow_id:
                continue
            is_input = bool(exchange.get("isInput"))
            self._by_flow_direction.setdefault((flow_id, is_input), deque()).append(exchange)
            self._by_flow.setdefault(flow_id, deque()).append(exchange)

    def match(self, flow_id: str | None, is_input: bool | None) -> dict[str, Any] | None:
        if flow_id:
            candidate = self._pop(self._by_flow_direction.get((flow_id, bool(is_input))))
            if candidate:
                return candidate
            candidate = self._pop(self._by_flow.get(flow_id))
            if candidate:
                return candidate
        return self._pop(self._all)

    def _pop(self, bucket: deque[dict[str, Any]] | None) -> dict[str, Any] | None:
        if not bucket:
            return None
        while bucket:
            candidate = bucket.popleft()
            ident = id(candidate)
            if ident in self._matched:
                continue
            self._matched.add(ident)
            return candidate
        return None


def _strip_version_suffix(value: str | None) -> str | None:
    if not isinstance(value, str):
        return value
    if VERSION_SUFFIX_RE.search(value):
        return VERSION_SUFFIX_RE.sub("", value)
    return value


def _extract_category_text(payload: Any) -> str:
    if isinstance(payload, dict):
        value = payload.get("category")
    else:
        value = None
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if isinstance(item, str) and item.strip()]
        return " / ".join(parts)
    return ""


def _classification_from_category(category: str) -> list[dict[str, str]] | None:
    if not category:
        return None
    entries: list[dict[str, str]] = []
    for idx, segment in enumerate(category.split("/")):
        segment = segment.strip()
        if not segment:
            continue
        if ":" in segment:
            code, text = segment.split(":", 1)
        else:
            code, text = segment, segment
        code = code.strip()
        text = text.strip() or code
        entries.append(
            {
                "@level": str(idx),
                "@classId": code or f"LEVEL_{idx}",
                "#text": text,
            }
        )
    return entries or None


def _extract_flow_description(payload: Any) -> str:
    if isinstance(payload, dict):
        description = payload.get("description")
        if isinstance(description, str) and description.strip():
            return description
    return ""


def _apply_strict_mode(schema: Any) -> None:
    if isinstance(schema, dict):
        schema_type = schema.get("type")
        if schema_type == "object" or "properties" in schema or "patternProperties" in schema:
            schema.setdefault("additionalProperties", False)
            for subschema in schema.get("properties", {}).values():
                _apply_strict_mode(subschema)
            for subschema in schema.get("patternProperties", {}).values():
                _apply_strict_mode(subschema)
            additional = schema.get("additionalProperties")
            if isinstance(additional, dict):
                _apply_strict_mode(additional)
        if "items" in schema:
            items = schema["items"]
            if isinstance(items, list):
                for subschema in items:
                    _apply_strict_mode(subschema)
            elif isinstance(items, dict):
                _apply_strict_mode(items)
        for key in ("allOf", "anyOf", "oneOf", "not", "if", "then", "else", "dependentSchemas"):
            value = schema.get(key)
            if isinstance(value, list):
                for subschema in value:
                    _apply_strict_mode(subschema)
            elif isinstance(value, dict):
                _apply_strict_mode(value)
    elif isinstance(schema, list):
        for item in schema:
            _apply_strict_mode(item)


def _build_strict_schema_store() -> dict[str, dict[str, Any]]:
    store: dict[str, dict[str, Any]] = {}
    for path in SCHEMA_DIR.glob("*.json"):
        raw = json.loads(path.read_text(encoding="utf-8"))
        strict_copy = deepcopy(raw)
        _apply_strict_mode(strict_copy)
        store[path.as_uri()] = strict_copy
    return store


def _get_validator(schema_filename: str) -> Draft7Validator:
    global _STRICT_SCHEMA_STORE
    if _STRICT_SCHEMA_STORE is None:
        _STRICT_SCHEMA_STORE = _build_strict_schema_store()
    if schema_filename in _VALIDATOR_CACHE:
        return _VALIDATOR_CACHE[schema_filename]
    schema_path = SCHEMA_DIR / schema_filename
    schema_uri = schema_path.as_uri()
    schema_data = deepcopy(_STRICT_SCHEMA_STORE[schema_uri])
    resolver = jsonschema.RefResolver(base_uri=schema_uri, referrer=schema_data, store=_STRICT_SCHEMA_STORE)
    validator = Draft7Validator(schema_data, resolver=resolver)
    _VALIDATOR_CACHE[schema_filename] = validator
    return validator


def _prune_instance_to_schema(instance: Any, schema_filename: str) -> None:
    validator = _get_validator(schema_filename)
    while True:
        removed = False
        for error in validator.iter_errors(instance):
            if error.validator == "additionalProperties":
                target = error.instance
                if not isinstance(target, dict):
                    continue
                schema_fragment = error.schema or {}
                allowed = set(schema_fragment.get("properties", {}).keys())
                pattern_props = schema_fragment.get("patternProperties", {}) or {}
                compiled_patterns = [re.compile(pattern) for pattern in pattern_props]
                extras: list[str] = []
                for key in list(target.keys()):
                    if key in allowed:
                        continue
                    if any(pattern.match(key) for pattern in compiled_patterns):
                        continue
                    extras.append(key)
                for key in extras:
                    target.pop(key, None)
                    removed = True
        if not removed:
            break
    remaining = [err for err in validator.iter_errors(instance) if err.validator == "additionalProperties"]
    if remaining:
        raise SystemExit(f"Unable to prune unsupported properties for schema {schema_filename}")


def _prune_dataset(root_key: str, node: dict[str, Any], schema_filename: str) -> None:
    wrapper = {root_key: node}
    _prune_instance_to_schema(wrapper, schema_filename)


def _first_text(node: Any) -> str:
    if isinstance(node, dict):
        return str(node.get("#text") or "").strip()
    if isinstance(node, list):
        for item in node:
            text = _first_text(item)
            if text:
                return text
    if isinstance(node, str):
        return node.strip()
    return ""


def _has_text_entry(value: Any) -> bool:
    if isinstance(value, dict):
        return bool(_first_text(value))
    if isinstance(value, list):
        return any(_has_text_entry(entry) for entry in value)
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _source_label(source_path: Path | str | None) -> str:
    if source_path is None:
        return "JSON-LD payload"
    return str(source_path)


def _collect_exchange_entries(node: Any) -> list[dict[str, Any]]:
    if isinstance(node, dict):
        exchanges = node.get("exchange")
        if isinstance(exchanges, list):
            return [entry for entry in exchanges if isinstance(entry, dict)]
        if isinstance(exchanges, dict):
            return [exchanges]
    if isinstance(node, list):
        return [entry for entry in node if isinstance(entry, dict)]
    return []


def _require_multilang_field(value: Any, field_name: str, source_path: Path | str | None) -> None:
    if not _has_text_entry(value):
        raise SystemExit(f"[jsonld-stage1] Missing {field_name} in { _source_label(source_path) }")


def _require_string_field(value: Any, field_name: str, source_path: Path | str | None) -> None:
    if isinstance(value, str) and value.strip():
        return
    if isinstance(value, (int, float)):
        return
    raise SystemExit(f"[jsonld-stage1] Missing {field_name} in { _source_label(source_path) }")


def _apply_process_template_fields(process_dataset: dict[str, Any], uuid_value: str) -> None:
    modelling = process_dataset.get("modellingAndValidation")
    if isinstance(modelling, list):
        modelling = modelling[0] if modelling else {}
        process_dataset["modellingAndValidation"] = modelling
    elif not isinstance(modelling, dict):
        modelling = {}
        process_dataset["modellingAndValidation"] = modelling
    _merge_template(modelling, {"validation": {"review": {"@type": "Not reviewed"}}})
    lci_block = modelling.get("LCIMethodAndAllocation")
    if not isinstance(lci_block, dict):
        lci_block = {}
        modelling["LCIMethodAndAllocation"] = lci_block
    if not _clean_text(lci_block.get("typeOfDataSet")):
        lci_block["typeOfDataSet"] = "Unit process, single operation"
    if not _clean_text(lci_block.get("LCIMethodPrinciple")):
        lci_block["LCIMethodPrinciple"] = "Not defined"
    compliance_declarations = modelling.get("complianceDeclarations")
    if isinstance(compliance_declarations, list):
        compliance_declarations = compliance_declarations[0] if compliance_declarations else {}
        modelling["complianceDeclarations"] = compliance_declarations
    elif not isinstance(compliance_declarations, dict):
        compliance_declarations = {}
        modelling["complianceDeclarations"] = compliance_declarations
    compliance_node = compliance_declarations.get("compliance")
    if isinstance(compliance_node, list):
        compliance_node = compliance_node[0] if compliance_node else {}
        compliance_declarations["compliance"] = compliance_node
    elif not isinstance(compliance_node, dict):
        compliance_node = {}
        compliance_declarations["compliance"] = compliance_node
    compliance_template = {
        "common:referenceToComplianceSystem": _compliance_reference(),
        "common:approvalOfOverallCompliance": "Fully compliant",
        "common:nomenclatureCompliance": "Fully compliant",
        "common:methodologicalCompliance": "Not defined",
        "common:reviewCompliance": "Not defined",
        "common:documentationCompliance": "Not defined",
        "common:qualityCompliance": "Not defined",
    }
    _merge_template(compliance_node, compliance_template)

    admin = process_dataset.setdefault("administrativeInformation", {})
    timestamp = _current_timestamp()
    version = _extract_admin_version(admin)
    admin_template = {
        "dataEntryBy": {
            "common:referenceToDataSetFormat": _format_reference(),
            "common:referenceToPersonOrEntityEnteringTheData": _contact_reference(),
            "common:timeStamp": timestamp,
        },
        "common:commissionerAndGoal": {
            "common:referenceToCommissioner": _contact_reference(),
        },
        "publicationAndOwnership": {
            "common:dataSetVersion": version,
            "common:permanentDataSetURI": _permanent_dataset_uri("process", uuid_value, version),
            "common:referenceToOwnershipOfDataSet": _ownership_reference(),
            "common:licenseType": DEFAULT_LICENSE,
            "common:copyright": "false",
        },
    }
    _merge_template(admin, admin_template)


def _apply_flow_template_fields(flow_dataset: dict[str, Any], uuid_value: str) -> None:
    for key, value in FLOW_XMLNS.items():
        flow_dataset.setdefault(key, value)
    flow_info = flow_dataset.setdefault("flowInformation", {})
    flow_info.setdefault("quantitativeReference", {})

    modelling = flow_dataset.get("modellingAndValidation")
    if isinstance(modelling, list):
        modelling = modelling[0] if modelling else {}
        flow_dataset["modellingAndValidation"] = modelling
    elif not isinstance(modelling, dict):
        modelling = {}
        flow_dataset["modellingAndValidation"] = modelling
    compliance_declarations = modelling.get("complianceDeclarations")
    if isinstance(compliance_declarations, list):
        compliance_declarations = compliance_declarations[0] if compliance_declarations else {}
        modelling["complianceDeclarations"] = compliance_declarations
    elif not isinstance(compliance_declarations, dict):
        compliance_declarations = {}
        modelling["complianceDeclarations"] = compliance_declarations
    compliance_block = flow_compliance_declarations()
    if compliance_block:
        _merge_template(compliance_declarations, compliance_block)
    compliance_defaults = {
        "common:nomenclatureCompliance": "Fully compliant",
        "common:methodologicalCompliance": "Not defined",
        "common:reviewCompliance": "Not defined",
        "common:documentationCompliance": "Not defined",
        "common:qualityCompliance": "Not defined",
    }
    compliance_node = compliance_declarations.get("compliance")
    if isinstance(compliance_node, list):
        compliance_node = compliance_node[0] if compliance_node else {}
        compliance_declarations["compliance"] = compliance_node
    elif not isinstance(compliance_node, dict):
        compliance_node = {}
        compliance_declarations["compliance"] = compliance_node
    _merge_template(compliance_node, compliance_defaults)

    admin = flow_dataset.setdefault("administrativeInformation", {})
    timestamp = _current_timestamp()
    version = _extract_admin_version(admin)
    admin_template = {
        "dataEntryBy": {
            "common:referenceToDataSetFormat": _format_reference(),
            "common:referenceToPersonOrEntityEnteringTheData": _contact_reference(),
            "common:timeStamp": timestamp,
        },
        "publicationAndOwnership": {
            "common:dataSetVersion": version,
            "common:permanentDataSetURI": _permanent_dataset_uri("flow", uuid_value, version),
            "common:referenceToOwnershipOfDataSet": _ownership_reference(),
            "common:licenseType": DEFAULT_LICENSE,
        },
    }
    _merge_template(admin, admin_template)


def _apply_source_template_fields(source_dataset: dict[str, Any], uuid_value: str) -> None:
    for key, value in SOURCE_XMLNS.items():
        source_dataset.setdefault(key, value)
    source_info = source_dataset.setdefault("sourceInformation", {})
    data_info = source_info.setdefault("dataSetInformation", {})
    data_info.setdefault("referenceToContact", _contact_reference())

    admin = source_dataset.setdefault("administrativeInformation", {})
    timestamp = _current_timestamp()
    version = _extract_admin_version(admin)
    admin_template = {
        "dataEntryBy": {
            "common:referenceToDataSetFormat": _format_reference(),
            "common:referenceToPersonOrEntityEnteringTheData": _contact_reference(),
            "common:timeStamp": timestamp,
        },
        "publicationAndOwnership": {
            "common:dataSetVersion": version,
            "common:permanentDataSetURI": _permanent_dataset_uri("source", uuid_value, version),
            "common:referenceToOwnershipOfDataSet": _ownership_reference(),
            "common:licenseType": DEFAULT_LICENSE,
        },
    }
    _merge_template(admin, admin_template)


def _validate_process_dataset(dataset: dict[str, Any], source_path: Path | str | None) -> None:
    process_info = dataset.get("processInformation")
    if not isinstance(process_info, dict):
        raise SystemExit(f"[jsonld-stage1] processInformation missing in { _source_label(source_path) }")
    data_info = process_info.get("dataSetInformation")
    if not isinstance(data_info, dict):
        raise SystemExit(f"[jsonld-stage1] dataSetInformation missing in { _source_label(source_path) }")
    name_block = data_info.get("name")
    if not isinstance(name_block, dict):
        raise SystemExit(f"[jsonld-stage1] name block missing in { _source_label(source_path) }")
    for field in ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes", "functionalUnitFlowProperties"):
        _require_multilang_field(name_block.get(field), f"name.{field}", source_path)
    _require_multilang_field(data_info.get("common:generalComment"), "common:generalComment", source_path)

    quant_ref = process_info.get("quantitativeReference")
    if not isinstance(quant_ref, dict):
        raise SystemExit(f"[jsonld-stage1] quantitativeReference missing in { _source_label(source_path) }")
    _require_string_field(quant_ref.get("referenceToReferenceFlow"), "quantitativeReference.referenceToReferenceFlow", source_path)
    _require_multilang_field(quant_ref.get("functionalUnitOrOther"), "quantitativeReference.functionalUnitOrOther", source_path)

    time_info = process_info.get("time")
    if not isinstance(time_info, dict) or "common:referenceYear" not in time_info:
        raise SystemExit(f"[jsonld-stage1] time.common:referenceYear missing in { _source_label(source_path) }")

    geography = process_info.get("geography")
    if not isinstance(geography, dict):
        raise SystemExit(f"[jsonld-stage1] geography missing in { _source_label(source_path) }")
    location_block = geography.get("locationOfOperationSupplyOrProduction")
    if not isinstance(location_block, dict) or not location_block.get("@location"):
        raise SystemExit(f"[jsonld-stage1] geography.locationOfOperationSupplyOrProduction.@location missing in { _source_label(source_path) }")

    exchanges_node = dataset.get("exchanges")
    exchanges = _collect_exchange_entries(exchanges_node)
    if not exchanges:
        raise SystemExit(f"[jsonld-stage1] exchanges missing in { _source_label(source_path) }")
    for exchange in exchanges:
        _require_string_field(exchange.get("meanAmount"), "exchange.meanAmount", source_path)
        ref = exchange.get("referenceToFlowDataSet")
        if not isinstance(ref, dict) or not ref.get("@refObjectId"):
            raise SystemExit(f"[jsonld-stage1] exchange.referenceToFlowDataSet missing in { _source_label(source_path) }")

    admin_info = dataset.get("administrativeInformation")
    if not isinstance(admin_info, dict):
        raise SystemExit(f"[jsonld-stage1] administrativeInformation missing in { _source_label(source_path) }")
    commissioner = admin_info.get("common:commissionerAndGoal")
    if not isinstance(commissioner, dict):
        raise SystemExit(f"[jsonld-stage1] administrativeInformation.common:commissionerAndGoal missing in { _source_label(source_path) }")
    intended = commissioner.get("common:intendedApplications")
    _require_multilang_field(intended, "common:commissionerAndGoal.common:intendedApplications", source_path)

    modelling = dataset.get("modellingAndValidation")
    if not isinstance(modelling, dict):
        raise SystemExit(f"[jsonld-stage1] modellingAndValidation missing in { _source_label(source_path) }")
    lci_block = modelling.get("LCIMethodAndAllocation")
    if not isinstance(lci_block, dict):
        raise SystemExit(f"[jsonld-stage1] LCIMethodAndAllocation missing in { _source_label(source_path) }")
    _require_string_field(lci_block.get("typeOfDataSet"), "LCIMethodAndAllocation.typeOfDataSet", source_path)
    _require_string_field(lci_block.get("LCIMethodPrinciple"), "LCIMethodAndAllocation.LCIMethodPrinciple", source_path)


def _validate_flow_dataset(
    dataset: dict[str, Any],
    source_path: Path | str | None,
) -> None:
    flow_info = dataset.get("flowInformation")
    if not isinstance(flow_info, dict):
        raise SystemExit(f"[jsonld-stage1] flowInformation missing in { _source_label(source_path) }")
    data_info = flow_info.get("dataSetInformation")
    if not isinstance(data_info, dict):
        raise SystemExit(f"[jsonld-stage1] flow dataSetInformation missing in { _source_label(source_path) }")
    name_block = data_info.get("name")
    if not isinstance(name_block, dict):
        raise SystemExit(f"[jsonld-stage1] flow name block missing in { _source_label(source_path) }")
    for field in ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes"):
        _require_multilang_field(name_block.get(field), f"flow.name.{field}", source_path)
    _require_multilang_field(data_info.get("common:generalComment"), "flow common:generalComment", source_path)

    flow_props_node = dataset.get("flowProperties") or flow_info.get("flowProperties")
    flow_props = []
    if isinstance(flow_props_node, dict):
        raw = flow_props_node.get("flowProperty")
        if isinstance(raw, list):
            flow_props = [entry for entry in raw if isinstance(entry, dict)]
        elif isinstance(raw, dict):
            flow_props = [raw]
    if not flow_props:
        raise SystemExit(f"[jsonld-stage1] flowProperties missing in { _source_label(source_path) }")
    for entry in flow_props:
        ref = entry.get("referenceToFlowPropertyDataSet")
        if not isinstance(ref, dict) or not ref.get("@refObjectId"):
            raise SystemExit(f"[jsonld-stage1] flowProperty.referenceToFlowPropertyDataSet missing in { _source_label(source_path) }")

    quant_ref = flow_info.get("quantitativeReference")
    if not isinstance(quant_ref, dict) or not quant_ref.get("referenceToReferenceFlowProperty"):
        raise SystemExit(f"[jsonld-stage1] flow quantitativeReference missing in { _source_label(source_path) }")

    modelling = dataset.get("modellingAndValidation")
    if not isinstance(modelling, dict):
        raise SystemExit(f"[jsonld-stage1] flow modellingAndValidation missing in { _source_label(source_path) }")
    lci_block = modelling.get("LCIMethod")
    if not isinstance(lci_block, dict):
        raise SystemExit(f"[jsonld-stage1] flow LCIMethod missing in { _source_label(source_path) }")
    _require_string_field(lci_block.get("typeOfDataSet"), "LCIMethod.typeOfDataSet", source_path)


def _validate_source_dataset(
    dataset: dict[str, Any],
    source_path: Path | str | None,
) -> None:
    source_info = dataset.get("sourceInformation")
    if not isinstance(source_info, dict):
        raise SystemExit(f"[jsonld-stage1] sourceInformation missing in { _source_label(source_path) }")
    data_info = source_info.get("dataSetInformation")
    if not isinstance(data_info, dict):
        raise SystemExit(f"[jsonld-stage1] source dataSetInformation missing in { _source_label(source_path) }")
    _require_multilang_field(data_info.get("common:shortName"), "source.common:shortName", source_path)
    classification = data_info.get("classificationInformation", {}).get("common:classification", {})
    classes = classification.get("common:class") if isinstance(classification, dict) else None
    if not classes:
        raise SystemExit(f"[jsonld-stage1] source classification missing in { _source_label(source_path) }")
    _require_string_field(data_info.get("sourceCitation"), "source.sourceCitation", source_path)
    _require_string_field(data_info.get("publicationType"), "source.publicationType", source_path)
    _require_multilang_field(data_info.get("sourceDescriptionOrComment"), "source.sourceDescriptionOrComment", source_path)


def _mass_flow_property_reference() -> dict[str, Any]:
    return {
        "@type": "flow property data set",
        "@refObjectId": MASS_FLOW_PROPERTY_UUID,
        "@uri": f"../flowproperties/{MASS_FLOW_PROPERTY_UUID}_{MASS_FLOW_PROPERTY_VERSION}.xml",
        "@version": MASS_FLOW_PROPERTY_VERSION,
        "common:shortDescription": _language_entry("Mass"),
    }


def _flow_property_version(uuid_value: str | None) -> str:
    if not isinstance(uuid_value, str):
        return MASS_FLOW_PROPERTY_VERSION
    key = uuid_value.strip().lower()
    if not key:
        return MASS_FLOW_PROPERTY_VERSION
    return FLOW_PROPERTY_VERSION_OVERRIDES.get(key, MASS_FLOW_PROPERTY_VERSION)


def _flow_property_reference(uuid_value: str, name: str | None = None) -> dict[str, Any]:
    version = _flow_property_version(uuid_value)
    return {
        "@type": "flow property data set",
        "@refObjectId": uuid_value,
        "@uri": f"../flowproperties/{uuid_value}_{version}.xml",
        "@version": version,
        "common:shortDescription": _language_entry(name or uuid_value),
    }


def _stringify_number(value: Any, default: str = "1") -> str:
    if isinstance(value, (int, float)):
        return f"{value}"
    if isinstance(value, str):
        text = value.strip()
        return text or default
    return default


def _flow_properties_from_payload(payload: Any) -> tuple[list[dict[str, Any]], str | None]:
    if not isinstance(payload, dict):
        return [], None
    raw_props = payload.get("flowProperties")
    if isinstance(raw_props, dict):
        raw_props = [raw_props]
    if not isinstance(raw_props, list):
        return [], None
    entries: list[dict[str, Any]] = []
    reference_id: str | None = None
    for entry in raw_props:
        if not isinstance(entry, dict):
            continue
        flow_property = entry.get("flowProperty")
        if not isinstance(flow_property, dict):
            continue
        raw_uuid = flow_property.get("@id") or flow_property.get("id")
        if not isinstance(raw_uuid, str):
            continue
        clean_uuid = _strip_version_suffix(raw_uuid) or raw_uuid
        dataset_id = str(len(entries) + 1)
        name = flow_property.get("name") or clean_uuid
        conversion_value = _stringify_number(entry.get("conversionFactor"), "1")
        flow_entry = {
            "@dataSetInternalID": dataset_id,
            "meanValue": conversion_value,
            "referenceToFlowPropertyDataSet": _flow_property_reference(clean_uuid, name),
        }
        entries.append(flow_entry)
        if reference_id is None:
            flag = entry.get("isRefFlowProperty")
            if isinstance(flag, str):
                normalized_flag = flag.strip().lower() in {"true", "1", "yes"}
            else:
                normalized_flag = bool(flag)
            if normalized_flag:
                reference_id = dataset_id
    if not entries:
        return [], None
    if reference_id is None:
        reference_id = entries[0]["@dataSetInternalID"]
    return entries, reference_id


def _ensure_flow_property_factors(
    flow_dataset: dict[str, Any],
    source_payload: dict[str, Any],
    source_path: Path | str | None,
) -> None:
    flow_props_node = flow_dataset.get("flowProperties")
    if not isinstance(flow_props_node, dict):
        flow_props_node = {}
        flow_dataset["flowProperties"] = flow_props_node

    extracted_props, reference_id = _flow_properties_from_payload(source_payload)
    if extracted_props:
        flow_props_node["flowProperty"] = extracted_props
        chosen_id = reference_id or extracted_props[0]["@dataSetInternalID"]
        quant_ref = flow_dataset.get("flowInformation", {}).get("quantitativeReference")
        if isinstance(quant_ref, dict) and not quant_ref.get("referenceToReferenceFlowProperty"):
            quant_ref["referenceToReferenceFlowProperty"] = chosen_id
        return

    flow_props = flow_props_node.get("flowProperty")
    if isinstance(flow_props, dict):
        flow_props = [flow_props]
    elif not isinstance(flow_props, list):
        flow_props = []

    normalized: list[dict[str, Any]] = []
    for idx, entry in enumerate(flow_props, start=1):
        if not isinstance(entry, dict):
            continue
        entry.setdefault("@dataSetInternalID", str(entry.get("@dataSetInternalID") or idx))
        normalized.append(entry)

    if not normalized:
        raise SystemExit(f"[jsonld-stage1] flowProperties missing and could not be inferred for {_source_label(source_path)}")

    flow_props_node["flowProperty"] = normalized
    chosen_id = normalized[0]["@dataSetInternalID"]
    quant_ref = flow_dataset.get("flowInformation", {}).get("quantitativeReference")
    if isinstance(quant_ref, dict) and not quant_ref.get("referenceToReferenceFlowProperty"):
        quant_ref["referenceToReferenceFlowProperty"] = chosen_id


def _hoist_process_root_sections(process_dataset: dict[str, Any]) -> None:
    if not isinstance(process_dataset, dict):
        return
    info = process_dataset.get("processInformation")
    containers: list[dict[str, Any]] = []
    if isinstance(info, dict):
        containers.append(info)
        data_info = info.get("dataSetInformation")
        if isinstance(data_info, dict):
            containers.append(data_info)
    for container in containers:
        for key in PROCESS_ROOT_SECTIONS:
            if not isinstance(container, dict):
                continue
            if key in container and key not in process_dataset:
                process_dataset[key] = container.pop(key)


def _ensure_process_exchanges(process_dataset: dict[str, Any], source_payload: dict[str, Any]) -> None:
    exchanges_container = process_dataset.setdefault("exchanges", {})
    exchanges = exchanges_container.get("exchange")
    if isinstance(exchanges, dict):
        return
    if isinstance(exchanges, list) and exchanges:
        return
    source_exchanges = source_payload.get("exchanges")
    if not isinstance(source_exchanges, list) or not source_exchanges:
        return
    ilcd_exchanges: list[dict[str, Any]] = []
    for idx, source_exchange in enumerate(source_exchanges, start=1):
        if not isinstance(source_exchange, dict):
            continue
        exchange: dict[str, Any] = {"@dataSetInternalID": str(idx)}
        exchange["exchangeDirection"] = "Input" if source_exchange.get("isInput") else "Output"
        amount = source_exchange.get("amount")
        if isinstance(amount, (int, float)):
            amount_text = f"{amount}"
        else:
            amount_text = _clean_text(amount) or "0"
        exchange["meanAmount"] = amount_text
        exchange["resultingAmount"] = amount_text
        unit = source_exchange.get("unit")
        unit_name = None
        if isinstance(unit, dict):
            unit_name = _clean_text(unit.get("name"))
        elif isinstance(unit, str):
            unit_name = unit.strip()
        if unit_name:
            exchange["unit"] = unit_name
            exchange["resultingAmountUnit"] = unit_name
        flow_block = source_exchange.get("flow", {})
        flow_id = None
        flow_name = None
        if isinstance(flow_block, dict):
            flow_id = _strip_version_suffix(flow_block.get("@id") or flow_block.get("id"))
            flow_name = flow_block.get("name")
        if flow_id:
            exchange["referenceToFlowDataSet"] = {
                "@type": "flow data set",
                "@refObjectId": flow_id,
                "@version": "01.01.000",
                "@uri": f"../flows/{flow_id}_01.01.000.xml",
                "common:shortDescription": _language_entry(flow_name or flow_id),
            }
        if flow_name:
            exchange["exchangeName"] = flow_name
        property_ref = _build_flow_property_reference_from_source(source_exchange.get("flowProperty"))
        if property_ref:
            exchange["referenceToFlowPropertyDataSet"] = property_ref
        if source_exchange.get("isQuantitativeReference"):
            exchange["isQuantitativeReference"] = True
        ilcd_exchanges.append(exchange)
    if ilcd_exchanges:
        exchanges_container["exchange"] = ilcd_exchanges


def _merge_source_exchange_metadata(process_dataset: dict[str, Any], source_payload: dict[str, Any]) -> None:
    exchanges_container = process_dataset.get("exchanges")
    if not isinstance(exchanges_container, dict):
        return
    exchanges = exchanges_container.get("exchange")
    if isinstance(exchanges, dict):
        exchanges = [exchanges]
    if not isinstance(exchanges, list) or not exchanges:
        return
    source_exchanges = source_payload.get("exchanges")
    if not isinstance(source_exchanges, list) or not source_exchanges:
        return
    index = _SourceExchangeIndex([exchange for exchange in source_exchanges if isinstance(exchange, dict)])
    for exchange in exchanges:
        if not isinstance(exchange, dict):
            continue
        ref = exchange.get("referenceToFlowDataSet")
        ref_uuid = None
        if isinstance(ref, dict):
            ref_uuid = _strip_version_suffix(ref.get("@refObjectId"))
        direction_field = exchange.get("exchangeDirection")
        direction = None
        if isinstance(direction_field, str):
            direction = direction_field.strip().lower() == "input"
        source = index.match(ref_uuid, direction)
        if not isinstance(source, dict):
            continue
        _apply_source_exchange_metadata(exchange, source)


def _apply_source_exchange_metadata(exchange: dict[str, Any], source_exchange: dict[str, Any]) -> None:
    amount = source_exchange.get("amount")
    if amount is not None:
        amount_str = f"{amount}"
        exchange["meanAmount"] = amount_str
        exchange["resultingAmount"] = amount_str
    is_input = bool(source_exchange.get("isInput"))
    exchange["exchangeDirection"] = "Input" if is_input else "Output"
    unit = source_exchange.get("unit", {})
    unit_name = unit.get("name") if isinstance(unit, dict) else None
    if unit_name:
        exchange["unit"] = unit_name
        exchange.setdefault("resultingAmountUnit", unit_name)
    flow_property_reference = _build_flow_property_reference_from_source(source_exchange.get("flowProperty"))
    if flow_property_reference:
        exchange["referenceToFlowPropertyDataSet"] = flow_property_reference
    flow_block = source_exchange.get("flow", {})
    flow_id = _strip_version_suffix(flow_block.get("@id") or flow_block.get("id"))
    reference = exchange.get("referenceToFlowDataSet")
    if isinstance(reference, dict):
        if flow_id and not reference.get("@refObjectId"):
            reference["@refObjectId"] = flow_id
        reference["@type"] = reference.get("@type") or "flow data set"
        reference["@version"] = reference.get("@version") or "01.01.000"
        if reference.get("@refObjectId") and not reference.get("@uri"):
            version = reference.get("@version") or "01.01.000"
            reference["@uri"] = f"../flows/{reference['@refObjectId']}_{version}.xml"
        if "common:shortDescription" not in reference and flow_block.get("name"):
            reference["common:shortDescription"] = _language_entry(flow_block["name"])
    elif flow_id:
        exchange["referenceToFlowDataSet"] = {
            "@type": "flow data set",
            "@refObjectId": flow_id,
            "@version": "01.01.000",
            "@uri": f"../flows/{flow_id}_01.01.000.xml",
            "common:shortDescription": _language_entry(flow_block.get("name") or flow_id),
        }
    if flow_block.get("name"):
        exchange["exchangeName"] = flow_block["name"]


def _flow_lci_type_from_value(value: Any) -> str | None:
    candidate = _clean_text(value)
    if not candidate:
        return None
    mapped = FLOW_LCI_METHOD_MAP.get(candidate.upper())
    if mapped:
        return mapped
    if candidate in FLOW_LCI_METHOD_MAP.values():
        return candidate
    return None


def _flow_lci_type_hint(flow_dataset: dict[str, Any], source_payload: dict[str, Any] | None) -> str | None:
    if isinstance(flow_dataset, dict):
        modelling = flow_dataset.get("modellingAndValidation")
        if isinstance(modelling, dict):
            lci_block = modelling.get("LCIMethod")
            if isinstance(lci_block, dict):
                hint = _flow_lci_type_from_value(lci_block.get("typeOfDataSet"))
                if hint:
                    return hint
        flow_info = flow_dataset.get("flowInformation")
        if isinstance(flow_info, dict):
            hint = _flow_lci_type_from_value(flow_info.get("flowType"))
            if hint:
                return hint
    if isinstance(source_payload, dict):
        hint = _flow_lci_type_from_value(source_payload.get("flowType"))
        if hint:
            return hint
    return None


def _ensure_flow_lci_block(flow_dataset: dict[str, Any], source_payload: dict[str, Any] | None) -> None:
    modelling = flow_dataset.get("modellingAndValidation")
    if isinstance(modelling, list):
        modelling = modelling[0] if modelling else {}
        flow_dataset["modellingAndValidation"] = modelling
    elif not isinstance(modelling, dict):
        modelling = {}
        flow_dataset["modellingAndValidation"] = modelling

    lci_block = modelling.get("LCIMethod")
    if isinstance(lci_block, list):
        lci_block = lci_block[0] if lci_block else {}
        modelling["LCIMethod"] = lci_block
    elif not isinstance(lci_block, dict):
        lci_block = {}
        modelling["LCIMethod"] = lci_block

    current = _clean_text(lci_block.get("typeOfDataSet"))
    if current:
        lci_block["typeOfDataSet"] = _flow_lci_type_from_value(current) or current
        return

    hint = _flow_lci_type_hint(flow_dataset, source_payload)
    lci_block["typeOfDataSet"] = hint or "Product flow"


def _source_name_from_payload(source_payload: dict[str, Any] | None) -> str | None:
    if not isinstance(source_payload, dict):
        return None
    name_value = source_payload.get("name")
    if isinstance(name_value, list):
        parts = [part.strip() for part in name_value if isinstance(part, str) and part.strip()]
        if parts:
            return "; ".join(parts)
    elif isinstance(name_value, str) and name_value.strip():
        return name_value.strip()
    description = source_payload.get("description")
    if isinstance(description, str) and description.strip():
        return description.strip()
    return None


def _classification_entry_from_category(category: Any) -> dict[str, str]:
    category_text = _clean_text(category)
    if category_text:
        lookup = category_text.lower()
        class_id, label = SOURCE_CATEGORY_CLASS_MAP.get(lookup, DEFAULT_SOURCE_CLASS)
    else:
        class_id, label = DEFAULT_SOURCE_CLASS
    return {"@level": "0", "@classId": class_id, "#text": label}


def _publication_type_from_category(category: Any) -> str:
    category_text = _clean_text(category)
    if category_text:
        lookup = category_text.lower()
        mapped = SOURCE_PUBLICATION_TYPE_MAP.get(lookup)
        if mapped:
            return mapped
    return DEFAULT_PUBLICATION_TYPE


def _ensure_source_fields(source_dataset: dict[str, Any], source_payload: dict[str, Any] | None) -> None:
    source_info = source_dataset.setdefault("sourceInformation", {})
    data_info = source_info.setdefault("dataSetInformation", {})
    uuid_value = data_info.get("common:UUID")
    if not _clean_text(uuid_value):
        uuid_value = source_payload.get("@id") if isinstance(source_payload, dict) else None
        if not _clean_text(uuid_value):
            uuid_value = str(uuid4())
        data_info["common:UUID"] = uuid_value
    else:
        uuid_value = _clean_text(uuid_value)

    short_name_block = data_info.get("common:shortName")
    if not _has_text_entry(short_name_block):
        short_name_text = _source_name_from_payload(source_payload) or uuid_value or "Unspecified source"
        data_info["common:shortName"] = _language_entry(short_name_text)

    classification_info = data_info.setdefault("classificationInformation", {})
    classification = classification_info.get("common:classification")
    if isinstance(classification, list):
        classification = classification[0] if classification else {}
        classification_info["common:classification"] = classification
    elif not isinstance(classification, dict):
        classification = {}
        classification_info["common:classification"] = classification
    if not isinstance(classification.get("common:class"), dict):
        category_value = source_payload.get("category") if isinstance(source_payload, dict) else None
        classification["common:class"] = _classification_entry_from_category(category_value)

    cite_value = _clean_text(data_info.get("sourceCitation"))
    if not cite_value and isinstance(source_payload, dict):
        cite_value = _clean_text(source_payload.get("textReference")) or _source_name_from_payload(source_payload)
    if not cite_value:
        cite_value = uuid_value or "Unspecified reference"
    data_info["sourceCitation"] = cite_value

    publication_type = _clean_text(data_info.get("publicationType"))
    if not publication_type:
        category_value = source_payload.get("category") if isinstance(source_payload, dict) else None
        data_info["publicationType"] = _publication_type_from_category(category_value)

    description_block = data_info.get("sourceDescriptionOrComment")
    if not _has_text_entry(description_block):
        description_text = None
        if isinstance(source_payload, dict):
            description_text = _clean_text(source_payload.get("description")) or _source_name_from_payload(source_payload)
        description_text = description_text or cite_value or "Converted from JSON-LD source payload"
        data_info["sourceDescriptionOrComment"] = _language_entry(description_text)

    if isinstance(source_payload, dict):
        external_file = _clean_text(source_payload.get("externalFile"))
        if external_file and not data_info.get("referenceToDigitalFile"):
            data_info["referenceToDigitalFile"] = {"@uri": external_file}


def _clean_text(value: Any) -> str | None:
    if isinstance(value, str):
        candidate = value.strip()
        if candidate:
            return candidate
    return None


def _normalize_synonym_list(values: Any) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        candidates = re.split(r"[;,]", values)
    elif isinstance(values, list):
        candidates = values
    else:
        candidates = [values]
    normalized: list[str] = []
    for value in candidates:
        text = _clean_text(value)
        if text:
            normalized.append(text)
    return normalized


def _format_functional_unit(exchange: dict[str, Any]) -> str | None:
    if not isinstance(exchange, dict):
        return None
    amount = exchange.get("amount")
    if isinstance(amount, (int, float)):
        amount_text = f"{amount}".rstrip("0").rstrip(".") if isinstance(amount, float) else str(amount)
    else:
        amount_text = _clean_text(amount)
    unit = exchange.get("unit")
    if isinstance(unit, dict):
        unit_name = _clean_text(unit.get("name"))
    else:
        unit_name = _clean_text(unit)
    flow_block = exchange.get("flow", {})
    if isinstance(flow_block, dict):
        flow_name = _clean_text(flow_block.get("name"))
    else:
        flow_name = None
    components = [component for component in (amount_text, unit_name) if component]
    if flow_name:
        components.append(f"of {flow_name}")
    text = " ".join(components).strip()
    return text or None


def _extract_functional_unit_hint(source_payload: dict[str, Any]) -> str | None:
    exchanges = source_payload.get("exchanges")
    if not isinstance(exchanges, list):
        return None
    prioritized: list[dict[str, Any]] = []
    fallback: list[dict[str, Any]] = []
    for exchange in exchanges:
        if not isinstance(exchange, dict):
            continue
        if exchange.get("isQuantitativeReference") or (exchange.get("isInput") is False):
            prioritized.append(exchange)
        else:
            fallback.append(exchange)
    for bucket in (prioritized, fallback):
        for exchange in bucket:
            text = _format_functional_unit(exchange)
            if text:
                return text
    return None


def _ensure_multilang_entry(container: dict[str, Any], key: str, text: str | None) -> None:
    if not isinstance(container, dict):
        return
    existing = container.get(key)
    if existing and _has_text_entry(existing):
        return
    container[key] = _language_entry(text or "Unspecified")


def _extract_location_hint(process_dataset: dict[str, Any], source_payload: dict[str, Any]) -> str | None:
    process_info = process_dataset.get("processInformation", {})
    if isinstance(process_info, dict):
        geography = process_info.get("geography", {})
        if isinstance(geography, dict):
            location_block = geography.get("locationOfOperationSupplyOrProduction")
            if isinstance(location_block, dict):
                candidate = _clean_text(location_block.get("name")) or _clean_text(location_block.get("@location"))
                if candidate:
                    return candidate
    if isinstance(source_payload, dict):
        location = source_payload.get("location")
        if isinstance(location, dict):
            candidate = _clean_text(location.get("name")) or _clean_text(location.get("code"))
            if candidate:
                return candidate
        process_doc = source_payload.get("processDocumentation")
        if isinstance(process_doc, dict):
            geo_desc = _clean_text(process_doc.get("geographyDescription"))
            if geo_desc:
                return geo_desc
        category_hint = _clean_text(source_payload.get("category"))
        if category_hint:
            return category_hint
    return _clean_text(process_dataset.get("category"))


def _build_location_candidates(raw_hint: str | None) -> list[dict[str, str]]:
    catalog = get_location_catalog()
    return catalog.build_candidate_list(raw_hint)


def _ensure_process_descriptive_fields(process_dataset: dict[str, Any], source_payload: dict[str, Any]) -> None:
    if not isinstance(process_dataset, dict):
        return
    if not isinstance(source_payload, dict):
        source_payload = {}
    process_info = process_dataset.setdefault("processInformation", {})
    data_info = process_info.setdefault("dataSetInformation", {})
    name_block = data_info.get("name") if isinstance(data_info.get("name"), dict) else {}
    base_name_hint = _clean_text(source_payload.get("name")) or _clean_text(source_payload.get("category")) or "Unnamed process"
    description_hint = _clean_text(source_payload.get("description")) or base_name_hint
    location_hint = _extract_location_hint(process_dataset, source_payload) or base_name_hint
    functional_unit_hint = _extract_functional_unit_hint(source_payload) or description_hint
    _ensure_multilang_entry(name_block, "baseName", base_name_hint)
    _ensure_multilang_entry(name_block, "treatmentStandardsRoutes", description_hint)
    _ensure_multilang_entry(name_block, "mixAndLocationTypes", location_hint)
    if functional_unit_hint:
        _ensure_multilang_entry(name_block, "functionalUnitFlowProperties", functional_unit_hint)
    data_info["name"] = name_block
    description_value = _clean_text(source_payload.get("description"))
    if description_value:
        data_info["common:generalComment"] = _language_entry(description_value)
    elif not _has_text_entry(data_info.get("common:generalComment")):
        data_info["common:generalComment"] = _language_entry(description_hint)


def _ensure_intended_applications(process_dataset: dict[str, Any], source_payload: dict[str, Any]) -> None:
    if not isinstance(process_dataset, dict):
        return
    if not isinstance(source_payload, dict):
        source_payload = {}
    admin = process_dataset.setdefault("administrativeInformation", {})
    commissioner = admin.setdefault("common:commissionerAndGoal", {})
    intended = commissioner.get("common:intendedApplications")
    if _has_text_entry(intended):
        return
    summary = (
        _clean_text(source_payload.get("processDocumentation", {}).get("useAdvice") if isinstance(source_payload.get("processDocumentation"), dict) else None)
        or _clean_text(source_payload.get("useAdvice"))
        or _clean_text(source_payload.get("description"))
        or "Life cycle dataset prepared for Tiangong JSON-LD workflow automation"
    )
    commissioner["common:intendedApplications"] = [_language_entry(summary)]


def _match_reference_year(value: Any) -> str | None:
    if isinstance(value, (int, float)):
        candidate = int(value)
        if 1800 <= candidate <= 2200:
            return str(candidate)
        return None
    text = _clean_text(value)
    if not text:
        return None
    match = REFERENCE_YEAR_RE.search(text)
    if match:
        return match.group(0)
    return None


def _extract_reference_year(source_payload: dict[str, Any]) -> str:
    if not isinstance(source_payload, dict):
        return datetime.now(timezone.utc).strftime("%Y")
    documentation = source_payload.get("processDocumentation")
    if isinstance(documentation, dict):
        for key in ("validFrom", "validUntil", "timeDescription"):
            year = _match_reference_year(documentation.get(key))
            if year:
                return year
    for key in ("lastChange", "creationDate", "version", "description", "name"):
        year = _match_reference_year(source_payload.get(key))
        if year:
            return year
    return datetime.now(timezone.utc).strftime("%Y")


def _ensure_process_temporal_fields(process_dataset: dict[str, Any], source_payload: dict[str, Any]) -> None:
    if not isinstance(process_dataset, dict):
        return
    if not isinstance(source_payload, dict):
        source_payload = {}
    process_info = process_dataset.setdefault("processInformation", {})
    time_block = process_info.get("time") if isinstance(process_info.get("time"), dict) else {}
    if not _clean_text(time_block.get("common:referenceYear")):
        time_block["common:referenceYear"] = _extract_reference_year(source_payload)
    process_info["time"] = time_block


def _ensure_process_geography(process_dataset: dict[str, Any], source_payload: dict[str, Any]) -> str | None:
    if not isinstance(process_dataset, dict):
        return None
    process_info = process_dataset.setdefault("processInformation", {})
    geography = process_info.get("geography")
    if not isinstance(geography, dict):
        geography = {}
    location_block = geography.get("locationOfOperationSupplyOrProduction")
    if not isinstance(location_block, dict):
        location_block = {}
    location_hint = _extract_location_hint(process_dataset, source_payload)
    if location_hint:
        if not _clean_text(location_block.get("@location")):
            location_block["@location"] = location_hint
        if not _clean_text(location_block.get("name")):
            location_block["name"] = location_hint
    geography["locationOfOperationSupplyOrProduction"] = location_block
    process_info["geography"] = geography
    return location_hint


def _recover_missing_process_names(
    process_dataset: dict[str, Any],
    source_payload: dict[str, Any],
    llm: OpenAIResponsesLLM,
) -> None:
    process_info = process_dataset.get("processInformation")
    if not isinstance(process_info, dict):
        return
    data_info = process_info.get("dataSetInformation")
    if not isinstance(data_info, dict):
        return
    name_block = data_info.get("name")
    if not isinstance(name_block, dict):
        return
    required_keys = ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes")
    missing = [key for key in required_keys if not _has_text_entry(name_block.get(key))]
    if not missing:
        return
    context = {
        "missingFields": missing,
        "processInformation": process_info,
        "sourcePayload": source_payload,
    }
    try:
        response = llm.invoke(
            {
                "prompt": PROCESS_NAME_RECOVERY_PROMPT,
                "context": context,
                "response_format": {"type": "json_object"},
            }
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("jsonld.process_name_recovery_failed", error=str(exc))
        return
    recovery = response if isinstance(response, dict) else {}
    for key in missing:
        value = recovery.get(key)
        text = _clean_text(value)
        if text:
            _ensure_multilang_entry(name_block, key, text)


def _ensure_exchange_ids(exchanges: list[dict[str, Any]]) -> None:
    for idx, exchange in enumerate(exchanges, start=1):
        dataset_id = exchange.get("@dataSetInternalID")
        if not isinstance(dataset_id, str) or not dataset_id.strip():
            exchange["@dataSetInternalID"] = str(idx)


def _find_reference_flow_id(process_dataset: dict[str, Any]) -> str:
    exchanges = _collect_exchange_entries(process_dataset.get("exchanges"))
    if not exchanges:
        return "1"
    _ensure_exchange_ids(exchanges)
    for exchange in exchanges:
        if exchange.get("isQuantitativeReference"):
            return exchange["@dataSetInternalID"]
    return exchanges[0]["@dataSetInternalID"]


def _ensure_process_quantitative_reference(process_dataset: dict[str, Any], source_payload: dict[str, Any]) -> None:
    if not isinstance(process_dataset, dict):
        return
    if not isinstance(source_payload, dict):
        source_payload = {}
    process_info = process_dataset.setdefault("processInformation", {})
    quant_ref = process_info.get("quantitativeReference")
    if not isinstance(quant_ref, dict):
        quant_ref = {}
    if not _clean_text(quant_ref.get("referenceToReferenceFlow")):
        quant_ref["referenceToReferenceFlow"] = _find_reference_flow_id(process_dataset)
    if not _has_text_entry(quant_ref.get("functionalUnitOrOther")):
        quant_ref["functionalUnitOrOther"] = _language_entry(_extract_functional_unit_hint(source_payload) or "Reference flow of process")
    process_info["quantitativeReference"] = quant_ref


def _build_flow_property_reference_from_source(flow_property: Any) -> dict[str, Any] | None:
    if not isinstance(flow_property, dict):
        return None
    uuid_value = _strip_version_suffix(flow_property.get("@id") or flow_property.get("id"))
    if not uuid_value:
        return None
    key = uuid_value.strip().lower()
    version = FLOW_PROPERTY_VERSION_OVERRIDES.get(key, MASS_FLOW_PROPERTY_VERSION)
    short_desc = flow_property.get("name") or "Flow property"
    return {
        "@type": "flow property data set",
        "@refObjectId": uuid_value,
        "@version": version,
        "@uri": f"../flowproperties/{uuid_value}_{version}.xml",
        "common:shortDescription": _language_entry(short_desc),
    }


def _attach_process_source_references(process_dataset: dict[str, Any], source_payload: dict[str, Any]) -> None:
    """Populate referenceToDataSource with actual JSON-LD sources when available."""

    modelling = process_dataset.setdefault("modellingAndValidation", {})
    dsr = modelling.setdefault("dataSourcesTreatmentAndRepresentativeness", {})
    # Ensure the mandatory JSON-LD field exists even if no detailed note is available.
    if not isinstance(dsr.get("dataCutOffAndCompletenessPrinciples"), (dict, list)):
        dsr["dataCutOffAndCompletenessPrinciples"] = _language_entry("Not specified")

    sources_block = source_payload.get("processDocumentation", {}).get("sources") if isinstance(source_payload, dict) else None
    existing = dsr.get("referenceToDataSource")
    reference_entries: list[dict[str, Any]] = []
    if isinstance(existing, dict):
        reference_entries.append(existing)
    elif isinstance(existing, list):
        reference_entries.extend([entry for entry in existing if isinstance(entry, dict)])

    seen_ids = {entry.get("@refObjectId").strip().lower() for entry in reference_entries if isinstance(entry.get("@refObjectId"), str)}

    if isinstance(sources_block, list):
        for source in sources_block:
            if not isinstance(source, dict):
                continue
            source_id = source.get("@id") or source.get("id")
            if not isinstance(source_id, str):
                continue
            clean_id = _strip_version_suffix(source_id) or source_id
            key = clean_id.lower()
            if key in seen_ids:
                continue
            short_name = source.get("name") or source.get("shortName") or clean_id
            reference_entries.append(
                {
                    "@type": "source data set",
                    "@refObjectId": clean_id,
                    "@version": "01.01.000",
                    "@uri": f"../sources/{clean_id}_01.01.000.xml",
                    "common:shortDescription": [_language_entry(short_name)],
                }
            )
            seen_ids.add(key)

    if reference_entries:
        dsr["referenceToDataSource"] = reference_entries
    else:
        dsr["referenceToDataSource"] = [_format_reference()]


def _extract_multilang_text(node: Any) -> str:
    if isinstance(node, dict):
        text = node.get("#text") or node.get("text") or node.get("value")
        if isinstance(text, str):
            return text.strip()
    if isinstance(node, list):
        for entry in node:
            text = _extract_multilang_text(entry)
            if text:
                return text
    if isinstance(node, str):
        return node.strip()
    return ""


def _extract_name_text(value: Any) -> str:
    if isinstance(value, dict):
        text = value.get("#text") or value.get("text") or value.get("value")
        if isinstance(text, str) and text.strip():
            return text.strip()
        for candidate in value.values():
            extracted = _extract_name_text(candidate)
            if extracted:
                return extracted
        return ""
    if isinstance(value, list):
        for entry in value:
            extracted = _extract_name_text(entry)
            if extracted:
                return extracted
        return ""
    if isinstance(value, str):
        return value.strip()
    return ""


def _compose_flow_short_description_from_dataset(flow_dataset: dict[str, Any]) -> str:
    info = flow_dataset.get("flowInformation", {}).get("dataSetInformation", {})
    name_block = info.get("name", {})
    if not isinstance(name_block, dict):
        name_block = {}
    parts: list[str] = []
    for key in ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes", "functionalUnitFlowProperties"):
        text = _extract_multilang_text(name_block.get(key))
        if text:
            parts.append(text)
    description = "; ".join(part for part in parts if part)
    if description:
        return description
    fallback = _extract_multilang_text(info.get("common:generalComment"))
    if fallback:
        return fallback
    short_name = _extract_multilang_text(info.get("common:synonyms"))
    if short_name:
        return short_name
    uuid_value = info.get("common:UUID")
    if isinstance(uuid_value, str):
        return uuid_value
    return ""


def _compose_process_display_name(process_dataset: dict[str, Any]) -> str:
    info = process_dataset.get("processInformation", {}).get("dataSetInformation", {})
    name_block = info.get("name", {})
    if not isinstance(name_block, dict):
        name_block = {}
    parts: list[str] = []
    for key in ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes", "functionalUnitFlowProperties"):
        text = _extract_multilang_text(name_block.get(key))
        if text:
            parts.append(text)
    if parts:
        return "; ".join(parts)
    fallback = _extract_multilang_text(info.get("common:generalComment"))
    return fallback or ""


def _compose_flow_display_name(flow_dataset: dict[str, Any]) -> str:
    return _compose_flow_short_description_from_dataset(flow_dataset)


def _compose_source_display_name(source_dataset: dict[str, Any]) -> str:
    info = source_dataset.get("sourceInformation", {}).get("dataSetInformation", {})
    new_name = _extract_multilang_text(info.get("common:shortName"))
    if new_name:
        return new_name
    fallback = _extract_multilang_text(info.get("common:generalComment"))
    return fallback or ""


def _extract_original_name(payload: Any) -> str:
    if isinstance(payload, dict):
        original = _extract_name_text(payload.get("name"))
        if original:
            return original
        description = payload.get("description")
        if isinstance(description, str):
            return description.strip()
    return ""


def _extract_payload_uuid(payload: Any) -> str:
    if isinstance(payload, dict):
        candidate = payload.get("@id") or payload.get("id")
        if isinstance(candidate, str):
            return candidate
    return ""


def _flow_dataset_type(entry: dict[str, Any]) -> str | None:
    dataset = entry.get("flowDataSet") if isinstance(entry, dict) else None
    if not isinstance(dataset, dict):
        return None
    modelling = dataset.get("modellingAndValidation", {})
    lcimethod = modelling.get("LCIMethod", {}) if isinstance(modelling, dict) else {}
    flow_type = lcimethod.get("typeOfDataSet")
    return _clean_text(flow_type)


def _is_elementary_flow_entry(entry: dict[str, Any]) -> bool:
    dataset_type = _flow_dataset_type(entry)
    return bool(dataset_type and dataset_type.lower().startswith("elementary flow"))


def _build_elementary_flow_hint(payload: dict[str, Any]) -> dict[str, Any]:
    hint: dict[str, Any] = {
        "name": _clean_text(payload.get("name")) or "Unnamed flow",
        "category": _clean_text(payload.get("category")) or "",
        "flowType": "elementary",
    }
    cas_number = _clean_text(payload.get("casNumber") or payload.get("cas"))
    formula = _clean_text(payload.get("formula"))
    synonyms = _normalize_synonym_list(payload.get("synonyms"))
    if cas_number:
        hint["cas"] = cas_number
    if formula:
        hint["formula"] = formula
    if synonyms:
        hint["synonyms"] = synonyms
    return hint


def _append_skipped_flow_log(payload: dict[str, Any], source_file: str) -> None:
    return None  # Deprecated in favor of run-scoped skipped UUID whitelist.


def _append_elementary_flow_hint_log(record: dict[str, Any], run_id: str) -> None:
    target = run_cache_path(run_id, "elementary_flow_hints.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing_text = target.read_text(encoding="utf-8")
        records = json.loads(existing_text) if existing_text.strip() else []
    except FileNotFoundError:
        records = []
    except json.JSONDecodeError:
        records = []
    records.append(record)
    target.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def _lookup_elementary_flow_uuid(hint: dict[str, Any], service: Any | None, llm: Any | None) -> tuple[str | None, str]:
    # Stage 1 no longer performs MCP lookup; return empty result for downstream handling.
    return None, DEFAULT_DATA_SET_VERSION


def _rewrite_elementary_flow_references(
    process_blocks: list[dict[str, Any]],
    replacements: dict[str, dict[str, str]],
    metadata: dict[str, dict[str, Any]],
    mapping_records: list[dict[str, Any]],
) -> None:
    # No-op placeholder retained for compatibility; elementary flow replacement now happens in later stages.
    return None


def _relative_source_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _append_stage1_metadata_record(
    records: list[dict[str, Any]],
    *,
    dataset_type: str,
    dataset: dict[str, Any] | None,
    original_uuid: str,
    original_name: str,
    source_file: str,
) -> None:
    if not isinstance(dataset, dict):
        return
    stage1_uuid = ""
    stage1_name = ""
    if dataset_type == "Process":
        info = dataset.get("processInformation", {}).get("dataSetInformation", {})
        stage1_uuid = info.get("common:UUID") or ""
        stage1_name = _compose_process_display_name(dataset)
    elif dataset_type == "Flow":
        info = dataset.get("flowInformation", {}).get("dataSetInformation", {})
        stage1_uuid = info.get("common:UUID") or ""
        stage1_name = _compose_flow_display_name(dataset)
    elif dataset_type == "Source":
        info = dataset.get("sourceInformation", {}).get("dataSetInformation", {})
        stage1_uuid = info.get("common:UUID") or ""
        stage1_name = _compose_source_display_name(dataset)
    else:
        return

    records.append(
        {
            "type": dataset_type,
            "original_uuid": original_uuid,
            "original_name": original_name,
            "stage1_uuid": stage1_uuid,
            "stage1_name": stage1_name,
            "source_file": source_file,
        }
    )


def _write_stage1_metadata_cache(run_id: str, records: list[dict[str, Any]]) -> None:
    """Persist Stage 1 metadata for downstream UUID joins."""
    cache_path = run_cache_path(run_id, "stage1_metadata_cache.json")
    dump_json(records, cache_path)
    print(f"[jsonld-stage1] Metadata cache -> {cache_path}")


def _apply_flow_short_descriptions(process_dataset: dict[str, Any], flow_short_map: dict[str, str]) -> None:
    exchanges = process_dataset.get("exchanges")
    if not isinstance(exchanges, dict):
        return
    exchange_list = exchanges.get("exchange")
    if isinstance(exchange_list, dict):
        exchange_list = [exchange_list]
    if not isinstance(exchange_list, list):
        return
    for exchange in exchange_list:
        if not isinstance(exchange, dict):
            continue
        reference = exchange.get("referenceToFlowDataSet")
        if not isinstance(reference, dict):
            continue
        ref_uuid = reference.get("@refObjectId")
        if not isinstance(ref_uuid, str):
            continue
        key = (_strip_version_suffix(ref_uuid) or ref_uuid).lower()
        summary = flow_short_map.get(key)
        if not summary:
            continue
        reference["common:shortDescription"] = _language_entry(summary)


def _apply_source_reference_short_names(process_dataset: dict[str, Any], source_short_map: dict[str, str]) -> None:
    modelling = process_dataset.get("modellingAndValidation")
    if not isinstance(modelling, dict):
        return
    dsr = modelling.get("dataSourcesTreatmentAndRepresentativeness")
    if not isinstance(dsr, dict):
        return
    references = dsr.get("referenceToDataSource")
    if isinstance(references, dict):
        references = [references]
    if not isinstance(references, list):
        return
    for reference in references:
        if not isinstance(reference, dict):
            continue
        ref_uuid = reference.get("@refObjectId")
        if not isinstance(ref_uuid, str):
            continue
        key = (_strip_version_suffix(ref_uuid) or ref_uuid).lower()
        short_name = source_short_map.get(key)
        if not short_name:
            continue
        reference["common:shortDescription"] = [_language_entry(short_name)]


def _strip_exchange_name_fields(process_dataset: dict[str, Any]) -> None:
    exchanges = process_dataset.get("exchanges")
    if not isinstance(exchanges, dict):
        return
    exchange_list = exchanges.get("exchange")
    if isinstance(exchange_list, dict):
        exchange_list = [exchange_list]
    if not isinstance(exchange_list, list):
        return
    for exchange in exchange_list:
        if not isinstance(exchange, dict):
            continue
        exchange.pop("exchangeName", None)


def _write_flow_property_audit_log(run_id: str, records: list[dict[str, Any]]) -> None:
    if not records:
        return
    log_dir = Path("artifacts") / run_id / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "flow_property_mapping_audit.json"
    with log_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")
    print(f"[jsonld-stage1] Flow property mapping audit -> {log_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--process-dir", type=Path, required=True, help="Directory or file containing OpenLCA JSON-LD process datasets.")
    parser.add_argument("--flow-dir", type=Path, help="Directory or file containing OpenLCA JSON-LD flow datasets.")
    parser.add_argument("--source-dir", type=Path, help="Directory or file containing OpenLCA JSON-LD source datasets.")
    parser.add_argument("--run-id", required=True, help="Run identifier shared across JSON-LD stages.")
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".secrets/secrets.toml"),
        help="Secrets file containing OpenAI credentials.",
    )
    parser.add_argument(
        "--llm-cache",
        type=Path,
        help="Optional override for the Stage 2 JSON-LD LLM cache directory.",
    )
    parser.add_argument("--disable-cache", action="store_true", help="Disable on-disk LLM response caching.")
    parser.add_argument("--output", type=Path, help="Optional override for process blocks JSON path.")
    parser.add_argument("--flow-output", type=Path, help="Optional override for flow dataset JSON path.")
    parser.add_argument("--source-output", type=Path, help="Optional override for source dataset JSON path.")
    parser.add_argument("--resume", action="store_true", help="Skip work when output already exists and appears valid.")
    return parser.parse_args()


def _ensure_process_classification(data_info: dict[str, Any]) -> dict[str, Any]:
    classification_info = data_info.get("classificationInformation")
    if isinstance(classification_info, list):
        merged: dict[str, Any] = {}
        for entry in classification_info:
            if isinstance(entry, dict):
                merged.update(entry)
        classification_info = merged
    elif not isinstance(classification_info, dict):
        classification_info = {}
    data_info["classificationInformation"] = classification_info

    classification = classification_info.get("common:classification")
    if isinstance(classification, list):
        merged_classification: dict[str, Any] = {}
        class_entries: list[Any] = []
        for entry in classification:
            if isinstance(entry, dict) and any(isinstance(key, str) and key.startswith("common:") for key in entry):
                merged_classification.update(entry)
            else:
                class_entries.append(entry)
        if class_entries:
            merged_classification.setdefault("common:class", class_entries)
        if not merged_classification:
            merged_classification = {"common:class": class_entries}
        classification = merged_classification
    elif not isinstance(classification, dict):
        classification = {}
    classification_info["common:classification"] = classification
    return classification


def _normalise_process_classes(candidates: Any) -> list[dict[str, str]]:
    if isinstance(candidates, dict):
        items = [candidates]
    elif isinstance(candidates, list):
        items = candidates
    else:
        items = [candidates]
    normalised: list[dict[str, str]] = []
    for idx, entry in enumerate(items):
        if isinstance(entry, dict):
            level = entry.get("@level") or entry.get("level") or str(idx)
            class_id = entry.get("@classId") or entry.get("classId") or entry.get("code") or entry.get("#text") or f"CLASS_{idx}"
            text = entry.get("#text") or entry.get("description") or entry.get("label") or str(class_id)
            normalised.append({"@level": str(level), "@classId": str(class_id), "#text": str(text)})
        elif isinstance(entry, str):
            normalised.append({"@level": str(idx), "@classId": entry, "#text": entry})
    return normalised


def _fallback_process_classification(source_payload: dict[str, Any]) -> list[dict[str, str]]:
    category_text = (_extract_category_text(source_payload) or "").lower()
    if "battery" in category_text:
        return BATTERY_PROCESS_CLASS_PATH
    return GENERIC_PROCESS_CLASS_PATH


def _fallback_flow_classification(source_payload: dict[str, Any]) -> list[dict[str, str]]:
    if not isinstance(source_payload, dict):
        return GENERIC_FLOW_CLASS_PATH
    category_text = (_extract_category_text(source_payload) or "").lower()
    if "battery" in category_text or "cathode" in category_text or "anode" in category_text:
        return BATTERY_FLOW_CLASS_PATH
    return GENERIC_FLOW_CLASS_PATH
    return GENERIC_PROCESS_CLASS_PATH


def _apply_process_classification(
    dataset: dict[str, Any],
    classifier: ProcessClassifier,
    source_payload: dict[str, Any],
) -> dict[str, Any]:
    process_info = dataset.setdefault("processInformation", {})
    classification_path = classifier.run(process_info)
    data_info = process_info.setdefault("dataSetInformation", {})
    classification = _ensure_process_classification(data_info)
    normalised_path = _normalise_process_classes(classification_path)
    if not normalised_path:
        normalised_path = _normalise_process_classes(_fallback_process_classification(source_payload))
    classification["common:class"] = normalised_path
    return classification


def _apply_location_normalization(
    dataset: dict[str, Any],
    location_normalizer: LocationNormalizer,
    location_hint: str | None,
) -> None:
    if not isinstance(dataset, dict):
        return
    process_info = dataset.setdefault("processInformation", {})
    if not process_info:
        return
    catalog = get_location_catalog()
    initial_code = catalog.best_guess(location_hint)
    candidates = _build_location_candidates(location_hint)
    try:
        context = {"processInformation": process_info}
        geography = location_normalizer.run(
            context,
            hint=location_hint,
            candidates=candidates,
            initial_code=initial_code,
        )
    except Exception as exc:  # noqa: BLE001 - keep pipeline resilient
        LOGGER.warning("jsonld.location_normalization_failed", error=str(exc))
        geography = None
    code_from_response, geography_payload = extract_location_response(geography)
    final_code = catalog.coerce_code(code_from_response) or initial_code
    geography_block = process_info.setdefault("geography", {})
    if isinstance(geography_payload, dict):
        geography_block.update(geography_payload)
    if final_code:
        geography_block["code"] = final_code
        geography_block.setdefault("description", catalog.describe(final_code))
        supply_block = geography_block.setdefault("locationOfOperationSupplyOrProduction", {})
        if isinstance(supply_block, dict):
            supply_block["@location"] = final_code
            if not _clean_text(supply_block.get("name")):
                supply_block["name"] = geography_block.get("description") or location_hint or catalog.describe(final_code)


def _wrap_process_dataset(
    dataset: dict[str, Any],
    classifier: ProcessClassifier,
    location_normalizer: LocationNormalizer,
    llm: OpenAIResponsesLLM,
    source_payload: dict[str, Any],
    source_path: Path,
) -> dict[str, Any]:
    if "processDataSet" not in dataset or not isinstance(dataset["processDataSet"], dict):
        raise SystemExit("LLM response missing 'processDataSet'.")
    node = dataset["processDataSet"]
    info = node.setdefault("processInformation", {}).setdefault("dataSetInformation", {})
    uuid_value = info.get("common:UUID") or str(uuid4())
    info["common:UUID"] = uuid_value
    _hoist_process_root_sections(node)
    _ensure_process_exchanges(node, source_payload)
    _merge_source_exchange_metadata(node, source_payload)
    _ensure_process_descriptive_fields(node, source_payload)
    _ensure_intended_applications(node, source_payload)
    _ensure_process_temporal_fields(node, source_payload)
    location_hint = _ensure_process_geography(node, source_payload)
    _recover_missing_process_names(node, source_payload, llm)
    _ensure_process_quantitative_reference(node, source_payload)
    _apply_process_template_fields(node, uuid_value)
    _validate_process_dataset(node, source_path)
    _attach_process_source_references(node, source_payload)
    classification_block = _apply_process_classification(node, classifier, source_payload)
    _apply_location_normalization(node, location_normalizer, location_hint)
    class_entries = classification_block.get("common:class")
    if not class_entries:
        category_hint = _classification_from_category(_extract_category_text(source_payload))
        if category_hint:
            classification_block["common:class"] = _normalise_process_classes(category_hint)
    apply_jsonld_process_overrides({"processDataSet": node})
    normalized_node = build_tidas_process_dataset(node)
    _prune_dataset("processDataSet", normalized_node, PROCESS_SCHEMA_FILE)
    normalized_uuid = normalized_node.get("processInformation", {}).get("dataSetInformation", {}).get("common:UUID") or uuid_value
    return {
        "processDataSet": normalized_node,
        "process_id": normalized_uuid,
    }


def _normalise_flow_classes(raw: Any) -> list[dict[str, str]]:
    if isinstance(raw, dict):
        candidate = [raw]
    else:
        candidate = list(raw) if isinstance(raw, list) else []

    normalised: list[dict[str, str]] = []
    for idx, entry in enumerate(candidate):
        if isinstance(entry, dict):
            level = entry.get("@level") or entry.get("level") or str(idx)
            class_id = entry.get("@classId") or entry.get("classId") or entry.get("@catId") or entry.get("catId") or entry.get("#text") or f"CLASS_{idx}"
            text = entry.get("#text") or entry.get("text") or entry.get("label") or str(class_id)
            normalised.append(
                {
                    "@level": str(level),
                    "@classId": str(class_id),
                    "#text": str(text),
                }
            )
        elif isinstance(entry, str):
            normalised.append({"@level": str(idx), "@classId": entry, "#text": entry})
    return normalised


def _normalise_flow_type(value: Any) -> str:
    allowed = {"Product flow", "Waste flow", "Elementary flow"}
    if isinstance(value, str):
        candidate = value.strip()
        if candidate in allowed:
            return candidate
    return "Product flow"


def _multilang_to_text(node: Any) -> list[str]:
    texts: list[str] = []
    if isinstance(node, dict):
        value = node.get("#text")
        if isinstance(value, str):
            texts.append(value)
    elif isinstance(node, list):
        for entry in node:
            if isinstance(entry, dict):
                value = entry.get("#text")
                if isinstance(value, str):
                    texts.append(value)
            elif isinstance(entry, str):
                texts.append(entry)
    elif isinstance(node, str):
        texts.append(node)
    return texts


def _collect_flow_textual_context(flow_dataset: dict[str, Any], extra_text: str | None = None) -> str:
    flow_info = flow_dataset.get("flowInformation", {})
    info = flow_info.get("dataSetInformation", {})
    fragments: list[str] = []
    name_block = info.get("name", {})
    if isinstance(name_block, dict):
        for key in (
            "baseName",
            "treatmentStandardsRoutes",
            "mixAndLocationTypes",
            "functionalUnitFlowProperties",
        ):
            fragments.extend(_multilang_to_text(name_block.get(key)))
    fragments.extend(_multilang_to_text(info.get("common:synonyms")))
    fragments.extend(_multilang_to_text(info.get("common:generalComment")))
    technology = flow_info.get("technology", {})
    fragments.extend(_multilang_to_text(technology.get("technologicalApplicability")))
    if extra_text:
        fragments.append(extra_text)
    return " ".join(fragment for fragment in fragments if fragment)


def _ensure_flow_classification(
    flow_dataset: dict[str, Any],
    source: Path | None,
    fallback_text: str | None = None,
) -> None:
    info = flow_dataset.setdefault("flowInformation", {}).setdefault("dataSetInformation", {})
    classification_info = info.setdefault("classificationInformation", {})
    classification = classification_info.get("common:classification")
    if isinstance(classification, list):
        # Some LLM responses mistakenly place the class list directly here; wrap it.
        classification = {"common:class": classification}
        classification_info["common:classification"] = classification
    elif not isinstance(classification, dict):
        classification = {}
        classification_info["common:classification"] = classification

    classes = classification.get("common:class")
    normalised = _normalise_flow_classes(classes)

    hint = f" ({source})" if source else ""
    context_hint = f" [category hint: {fallback_text}]" if fallback_text else ""
    if not normalised:
        raise SystemExit(
            f"Flow dataset{hint}{context_hint} is missing classification entries; "
            "Stage 1 must emit a complete path defined in the TIDAS product flow "
            "classification schema (tidas_tools.tidas.schemas/tidas_flows_product_category.json)."
        )
    try:
        classification["common:class"] = ensure_valid_product_flow_classification(tuple(normalised))
    except ValueError as exc:
        raise SystemExit(
            f"Flow dataset{hint}{context_hint} has invalid product classification: {exc}. "
            "Update the Stage 1 prompt/output so the LLM returns a valid path directly from "
            "tidas_tools.tidas.schemas/tidas_flows_product_category.json."
        ) from exc


def _wrap_flow_dataset(
    dataset: dict[str, Any],
    source: Path | None,
    source_payload: dict[str, Any],
    flow_classifier: ProductFlowClassifier,
) -> dict[str, Any]:
    node = dataset.get("flowDataSet")
    if not isinstance(node, dict):
        raise SystemExit("LLM response missing 'flowDataSet'.")
    flow_info_root = node.setdefault("flowInformation", {})
    info = flow_info_root.setdefault("dataSetInformation", {})
    uuid_value = info.get("common:UUID") or str(uuid4())
    info["common:UUID"] = uuid_value
    _ensure_flow_descriptive_fields(node, source_payload)
    classification_container = info.get("classificationInformation")
    if isinstance(classification_container, list):
        classification_container = classification_container[0] if classification_container else {}
        info["classificationInformation"] = classification_container
    elif not isinstance(classification_container, dict):
        classification_container = {}
        info["classificationInformation"] = classification_container

    classification_info = classification_container.get("common:classification")
    if isinstance(classification_info, list):
        classification_info = {"common:class": classification_info}
        classification_container["common:classification"] = classification_info
    elif not isinstance(classification_info, dict):
        classification_info = {}
        classification_container["common:classification"] = classification_info

    try:
        flow_class_entries = flow_classifier.run(node, source_payload=source_payload)
    except ProcessExtractionError:
        flow_class_entries = []
    if not flow_class_entries:
        fallback_class = _fallback_flow_classification(source_payload)
        flow_class_entries = _normalise_flow_classes(fallback_class)
    classification_info["common:class"] = flow_class_entries
    category_hint = _extract_category_text(source_payload)
    _ensure_flow_classification(node, source, category_hint)
    flow_info_root.setdefault("quantitativeReference", {})
    _ensure_flow_property_factors(node, source_payload, source)
    _apply_flow_template_fields(node, uuid_value)
    _ensure_flow_lci_block(node, source_payload)
    _validate_flow_dataset(node, source)
    _prune_dataset("flowDataSet", node, FLOW_SCHEMA_FILE)
    return {"flowDataSet": node}


def _wrap_source_dataset(
    dataset: dict[str, Any],
    source_path: Path | None,
    source_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    node = dataset.get("sourceDataSet")
    if not isinstance(node, dict):
        raise SystemExit("LLM response missing 'sourceDataSet'.")
    info = node.setdefault("sourceInformation", {}).setdefault("dataSetInformation", {})
    _ensure_source_fields(node, source_payload)
    uuid_value = info.get("common:UUID") or str(uuid4())
    info["common:UUID"] = uuid_value
    _apply_source_template_fields(node, uuid_value)
    _validate_source_dataset(node, source_path)
    _prune_dataset("sourceDataSet", node, SOURCE_SCHEMA_FILE)
    return {"sourceDataSet": node}


def _fallback_process_block(source_path: Path) -> dict[str, Any]:
    converter = JSONLDProcessConverter(source_path)
    return converter.to_process_block()


def _fallback_flow_block(source_path: Path) -> dict[str, Any]:
    converter = JSONLDFlowConverter(source_path)
    return converter.to_flow_dataset()


def _fallback_source_block(source_path: Path) -> dict[str, Any]:
    converter = JSONLDSourceConverter(source_path)
    return converter.to_source_dataset()


def _extract_flow_location_hint(source_payload: dict[str, Any]) -> str | None:
    if not isinstance(source_payload, dict):
        return None
    location = source_payload.get("location")
    if isinstance(location, dict):
        candidate = _clean_text(location.get("name")) or _clean_text(location.get("code"))
        if candidate:
            return candidate
    return None


def _summarize_flow_properties(source_payload: dict[str, Any]) -> str | None:
    flow_props = source_payload.get("flowProperties")
    if not isinstance(flow_props, list):
        return None
    parts: list[str] = []
    for entry in flow_props:
        if not isinstance(entry, dict):
            continue
        flow_property = entry.get("flowProperty")
        name = _clean_text(flow_property.get("name") if isinstance(flow_property, dict) else None)
        factor = entry.get("conversionFactor")
        if name and isinstance(factor, (int, float)) and factor not in (0, 1):
            parts.append(f"{name} ({factor})")
        elif name:
            parts.append(name)
    return ", ".join(parts) if parts else None


def _normalize_synonyms(value: Any) -> list[dict[str, str]]:
    entries: list[str] = []
    if isinstance(value, str):
        candidates = [candidate.strip() for candidate in value.replace("；", ";").replace("，", ",").split(",")]
        entries.extend(candidate for candidate in candidates if candidate)
    elif isinstance(value, list):
        for item in value:
            text = _clean_text(item)
            if text:
                entries.append(text)
    elif isinstance(value, dict):
        text = _clean_text(value.get("#text"))
        if text:
            entries.append(text)
    return [_language_entry(text) for text in entries if text]


def _ensure_flow_descriptive_fields(flow_dataset: dict[str, Any], source_payload: dict[str, Any]) -> None:
    if not isinstance(flow_dataset, dict):
        return
    if not isinstance(source_payload, dict):
        source_payload = {}
    flow_info = flow_dataset.setdefault("flowInformation", {})
    data_info = flow_info.setdefault("dataSetInformation", {})
    name_block = data_info.get("name") if isinstance(data_info.get("name"), dict) else {}
    base_name_hint = _clean_text(source_payload.get("name")) or "Unnamed flow"
    description_hint = _clean_text(source_payload.get("description")) or base_name_hint
    mix_hint = _extract_flow_location_hint(source_payload) or "Production mix"
    property_hint = _summarize_flow_properties(source_payload)
    _ensure_multilang_entry(name_block, "baseName", base_name_hint)
    _ensure_multilang_entry(name_block, "treatmentStandardsRoutes", description_hint)
    _ensure_multilang_entry(name_block, "mixAndLocationTypes", mix_hint)
    if property_hint:
        _ensure_multilang_entry(name_block, "functionalUnitFlowProperties", property_hint)
    data_info["name"] = name_block
    if not _has_text_entry(data_info.get("common:generalComment")) and description_hint:
        data_info["common:generalComment"] = _language_entry(description_hint)
    if not data_info.get("common:synonyms"):
        synonyms_value = source_payload.get("synonyms")
        synonym_entries = _normalize_synonyms(synonyms_value)
        if synonym_entries:
            data_info["common:synonyms"] = synonym_entries
    flow_geo = flow_info.get("geography")
    if not isinstance(flow_geo, dict):
        flow_geo = {}
    location_hint = _extract_flow_location_hint(source_payload)
    if location_hint:
        flow_geo["locationOfSupply"] = location_hint
        flow_info["geography"] = flow_geo


def main() -> None:
    args = parse_args()
    run_id = args.run_id
    ensure_run_cache_dir(run_id)
    save_latest_run_id(run_id, pipeline="jsonld")

    output_path = args.output or run_cache_path(run_id, "stage1_process_blocks.json")
    if args.resume and output_path.exists():
        try:
            payload = json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict) and payload.get("process_blocks"):
            print(f"[jsonld-stage1] Output already present at {output_path}; skipping due to --resume.")
            return

    process_files = collect_jsonld_files(args.process_dir)
    flow_files: list[Path] = []
    if args.flow_dir:
        flow_files = collect_jsonld_files(args.flow_dir)
    source_files: list[Path] = []
    if args.source_dir:
        source_files = collect_jsonld_files(args.source_dir)

    api_key, model, base_url = load_secrets(args.secrets)
    cache_dir = args.llm_cache or run_cache_path(run_id, Path("openai/stage1_jsonld"))
    if args.disable_cache:
        llm_cache = None
    else:
        cache_dir.parent.mkdir(parents=True, exist_ok=True)
        llm_cache = cache_dir

    llm = OpenAIResponsesLLM(
        api_key=api_key,
        model=model,
        cache_dir=llm_cache,
        use_cache=not args.disable_cache,
        base_url=base_url,
    )
    process_classifier = ProcessClassifier(llm)
    flow_classifier = ProductFlowClassifier(llm)
    location_normalizer = LocationNormalizer(llm)
    process_extractor = JSONLDProcessExtractor(llm, process_classifier, location_normalizer)
    flow_extractor = JSONLDFlowExtractor(llm, flow_classifier, location_normalizer)
    source_extractor = JSONLDSourceExtractor(llm)
    process_blocks: list[dict[str, Any]] = []
    flow_datasets: list[dict[str, Any]] = []
    source_datasets: list[dict[str, Any]] = []
    flow_short_descriptions: dict[str, str] = {}
    source_short_names: dict[str, str] = {}
    stage1_metadata_records: list[dict[str, Any]] = []
    elementary_flow_metadata: dict[str, dict[str, Any]] = {}
    skipped_flow_records: list[dict[str, Any]] = []
    skipped_flow_uuids: set[str] = set()

    for json_path in process_files:
        raw_payload = json.loads(json_path.read_text(encoding="utf-8"))
        source_file = _relative_source_path(json_path)
        original_uuid = _extract_payload_uuid(raw_payload)
        original_name = _extract_original_name(raw_payload)
        try:
            process_entries = process_extractor.run(raw_payload)
            for entry in process_entries:
                dataset = entry.get("processDataSet")
                if isinstance(dataset, dict):
                    _attach_process_source_references(dataset, raw_payload)
                    _append_stage1_metadata_record(
                        stage1_metadata_records,
                        dataset_type="Process",
                        dataset=dataset,
                        original_uuid=original_uuid,
                        original_name=original_name,
                        source_file=source_file,
                    )
            process_blocks.extend(process_entries)
        except (SystemExit, ProcessExtractionError) as exc:
            LOGGER.warning(
                "jsonld.process_component_fallback",
                source=str(json_path),
                error=str(exc),
            )
            fallback = _fallback_process_block(json_path)
            process_blocks.append(
                _wrap_process_dataset(
                    fallback,
                    process_classifier,
                    location_normalizer,
                    llm,
                    raw_payload,
                    json_path,
                )
            )
            dataset = process_blocks[-1].get("processDataSet")
            if isinstance(dataset, dict):
                _append_stage1_metadata_record(
                    stage1_metadata_records,
                    dataset_type="Process",
                    dataset=dataset,
                    original_uuid=original_uuid,
                    original_name=original_name,
                    source_file=source_file,
                )

    for json_path in flow_files:
        raw_payload = json.loads(json_path.read_text(encoding="utf-8"))
        source_file = _relative_source_path(json_path)
        original_uuid = _extract_payload_uuid(raw_payload)
        original_name = _extract_original_name(raw_payload)
        try:
            flow_entries = flow_extractor.run(raw_payload)
            for entry in flow_entries:
                if _is_elementary_flow_entry(entry):
                    uuid_key = (original_uuid or _extract_payload_uuid(raw_payload)).strip()
                    if uuid_key:
                        skipped_flow_uuids.add(uuid_key)
                        skipped_flow_records.append(
                            {
                                "uuid": uuid_key,
                                "name": original_name or _clean_text(raw_payload.get("name")) or uuid_key,
                                "source": source_file,
                            }
                        )
                        elementary_flow_metadata[uuid_key] = {"name": original_name or _clean_text(raw_payload.get("name")) or uuid_key}
                    hint = _build_elementary_flow_hint(raw_payload)
                    _append_elementary_flow_hint_log(
                        {
                            "source": source_file,
                            "original_uuid": uuid_key,
                            "hint": hint,
                        },
                        run_id,
                    )
                    continue
                dataset = entry.get("flowDataSet")
                if not isinstance(dataset, dict):
                    continue
                uuid_value = dataset.get("flowInformation", {}).get("dataSetInformation", {}).get("common:UUID")
                summary = _compose_flow_short_description_from_dataset(dataset)
                if isinstance(uuid_value, str) and summary:
                    key = (_strip_version_suffix(uuid_value) or uuid_value).lower()
                    flow_short_descriptions[key] = summary
                _append_stage1_metadata_record(
                    stage1_metadata_records,
                    dataset_type="Flow",
                    dataset=dataset,
                    original_uuid=original_uuid,
                    original_name=original_name,
                    source_file=source_file,
                )
                flow_datasets.append(entry)
        except (SystemExit, ProcessExtractionError) as exc:
            LOGGER.warning(
                "jsonld.flow_component_fallback",
                source=str(json_path),
                error=str(exc),
            )
            fallback_flow = _fallback_flow_block(json_path)
            wrapped = _wrap_flow_dataset(fallback_flow, json_path, raw_payload, flow_classifier)
            dataset = wrapped.get("flowDataSet")
            if _is_elementary_flow_entry(wrapped):
                uuid_key = (original_uuid or _extract_payload_uuid(raw_payload)).strip()
                if uuid_key:
                    skipped_flow_uuids.add(uuid_key)
                    skipped_flow_records.append(
                        {
                            "uuid": uuid_key,
                            "name": original_name or _clean_text(raw_payload.get("name")) or uuid_key,
                            "source": source_file,
                        }
                    )
                    elementary_flow_metadata[uuid_key] = {"name": original_name or _clean_text(raw_payload.get("name")) or uuid_key}
                hint = _build_elementary_flow_hint(raw_payload)
                _append_elementary_flow_hint_log(
                    {
                        "source": source_file,
                        "original_uuid": uuid_key,
                        "hint": hint,
                    },
                    run_id,
                )
                continue
            if isinstance(dataset, dict):
                uuid_value = dataset.get("flowInformation", {}).get("dataSetInformation", {}).get("common:UUID")
                summary = _compose_flow_short_description_from_dataset(dataset)
                if isinstance(uuid_value, str) and summary:
                    key = (_strip_version_suffix(uuid_value) or uuid_value).lower()
                    flow_short_descriptions[key] = summary
                _append_stage1_metadata_record(
                    stage1_metadata_records,
                    dataset_type="Flow",
                    dataset=dataset,
                    original_uuid=original_uuid,
                    original_name=original_name,
                    source_file=source_file,
                )
            flow_datasets.append(wrapped)

    flow_property_audit_records = flow_extractor.drain_flow_property_audit_records()

    for json_path in source_files:
        raw_payload = json.loads(json_path.read_text(encoding="utf-8"))
        source_file = _relative_source_path(json_path)
        original_uuid = _extract_payload_uuid(raw_payload)
        original_name = _extract_original_name(raw_payload)
        try:
            source_entries = source_extractor.run(raw_payload)
            source_datasets.extend(source_entries)
            for entry in source_entries:
                dataset = entry.get("sourceDataSet")
                if not isinstance(dataset, dict):
                    continue
                info = dataset.get("sourceInformation", {}).get("dataSetInformation", {})
                uuid_value = info.get("common:UUID")
                short_name = _extract_multilang_text(info.get("common:shortName"))
                if isinstance(uuid_value, str) and short_name:
                    key = (_strip_version_suffix(uuid_value) or uuid_value).lower()
                    source_short_names[key] = short_name
                _append_stage1_metadata_record(
                    stage1_metadata_records,
                    dataset_type="Source",
                    dataset=dataset,
                    original_uuid=original_uuid,
                    original_name=original_name,
                    source_file=source_file,
                )
        except (SystemExit, ProcessExtractionError) as exc:
            LOGGER.warning(
                "jsonld.source_component_fallback",
                source=str(json_path),
                error=str(exc),
            )
            fallback_source = _fallback_source_block(json_path)
            wrapped = _wrap_source_dataset(fallback_source, json_path, raw_payload)
            dataset = wrapped.get("sourceDataSet")
            if isinstance(dataset, dict):
                info = dataset.get("sourceInformation", {}).get("dataSetInformation", {})
                uuid_value = info.get("common:UUID")
                short_name = _extract_multilang_text(info.get("common:shortName"))
                if isinstance(uuid_value, str) and short_name:
                    key = (_strip_version_suffix(uuid_value) or uuid_value).lower()
                    source_short_names[key] = short_name
                _append_stage1_metadata_record(
                    stage1_metadata_records,
                    dataset_type="Source",
                    dataset=dataset,
                    original_uuid=original_uuid,
                    original_name=original_name,
                    source_file=source_file,
                )
            source_datasets.append(wrapped)

    for block in process_blocks:
        dataset = block.get("processDataSet")
        if not isinstance(dataset, dict):
            continue
        if flow_short_descriptions:
            _apply_flow_short_descriptions(dataset, flow_short_descriptions)
        if source_short_names:
            _apply_source_reference_short_names(dataset, source_short_names)
        _strip_exchange_name_fields(dataset)

    # Write skipped elementary flow whitelist
    skipped_path = run_cache_path(run_id, "stage1_skipped_flow_uuids.json")
    skipped_payload = {
        "skipped_flow_uuids": sorted(skipped_flow_uuids),
        "records": skipped_flow_records,
    }
    dump_json(skipped_payload, skipped_path)
    print(f"[jsonld-stage1] Recorded {len(skipped_flow_uuids)} skipped elementary flow UUID(s) -> {skipped_path}")

    _write_stage1_metadata_cache(run_id, stage1_metadata_records)
    _write_flow_property_audit_log(run_id, flow_property_audit_records)

    dump_json({"process_blocks": process_blocks}, output_path)
    print(f"[jsonld-stage1] Generated {len(process_blocks)} process block(s) -> {output_path} (run_id={run_id})")

    flow_output_path = args.flow_output or run_cache_path(run_id, "stage1_flow_blocks.json")
    if flow_datasets or args.flow_dir:
        dump_json({"flow_datasets": flow_datasets}, flow_output_path)
        print(f"[jsonld-stage1] Generated {len(flow_datasets)} flow dataset(s) -> {flow_output_path} (run_id={run_id})")

    source_output_path = args.source_output or run_cache_path(run_id, "stage1_source_blocks.json")
    if source_datasets:
        dump_json({"source_datasets": source_datasets}, source_output_path)
        print(f"[jsonld-stage1] Generated {len(source_datasets)} source dataset(s) -> {source_output_path} (run_id={run_id})")


if __name__ == "__main__":
    main()
