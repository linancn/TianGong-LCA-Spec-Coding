#!/usr/bin/env python
"""Stage 1 (JSON-LD): LLM-assisted conversion of OpenLCA JSON-LD into ILCD datasets."""

from __future__ import annotations

import argparse
import json
import re
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

try:
    from scripts._workflow_common import (  # type: ignore
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

from tiangong_lca_spec.jsonld.converters import collect_jsonld_files
from tiangong_lca_spec.jsonld.process_overrides import apply_jsonld_process_overrides
from tiangong_lca_spec.process_extraction.extractors import ProcessClassifier, ProductFlowClassifier
from tiangong_lca_spec.tidas.flow_classification_registry import ensure_valid_product_flow_classification

DEFAULT_PROMPT_PATH = Path(".github/prompts/convert_json.prompt.md")

FLOW_XMLNS = {
    "@xmlns": "http://lca.jrc.it/ILCD/Flow",
    "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
    "@xmlns:ecn": "http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber",
    "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
    "@version": "1.1",
    "@locations": "../ILCDLocations.xml",
}

DEFAULT_LICENSE = "Free of charge for all users and uses"
MASS_FLOW_PROPERTY_UUID = "93a60a56-a3c8-11da-a746-0800200b9a66"
MASS_FLOW_PROPERTY_VERSION = "03.00.003"
TIANGONG_CONTACT_UUID = "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8"
TIANGONG_CONTACT_VERSION = "01.00.000"
ILCD_FORMAT_SOURCE_UUID = "a97a0155-0234-4b87-b4ce-a45da52f2a40"
ILCD_FORMAT_SOURCE_VERSION = "03.00.003"
ILCD_COMPLIANCE_SOURCE_UUID = "d92a1a12-2545-49e2-a585-55c259997756"
ILCD_COMPLIANCE_SOURCE_VERSION = "20.20.002"
SOURCE_XMLNS = {
    "@xmlns": "http://lca.jrc.it/ILCD/Source",
    "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
    "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Source ../../schemas/ILCD_SourceDataSet.xsd",
    "@version": "1.1",
}
DEFAULT_SOURCE_DESCRIPTION = "Converted from OpenLCA JSON-LD; bibliographic details preserved for downstream audit."
DEFAULT_SOURCE_PUBLICATION_TYPE = "Article in periodical"
VERSION_SUFFIX_RE = re.compile(r"_[0-9]{2}\.[0-9]{2}\.[0-9]{3}$")


def _language_entry(text: str | None, lang: str = "en") -> dict[str, str]:
    value = (text or "").strip() or "Unspecified"
    return {"@xml:lang": lang, "#text": value}


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


def _multilang_list(value: Any, default_text: str) -> list[dict[str, str]]:
    if isinstance(value, list):
        entries = []
        for item in value:
            if isinstance(item, dict) and "#text" in item:
                entries.append({"@xml:lang": item.get("@xml:lang", "en"), "#text": item.get("#text", "") or default_text})
            elif isinstance(item, str):
                entries.append(_language_entry(item))
        if entries:
            return entries
    elif isinstance(value, dict) and "#text" in value:
        return [{"@xml:lang": value.get("@xml:lang", "en"), "#text": value.get("#text", "") or default_text}]
    elif isinstance(value, str) and value.strip():
        return [_language_entry(value)]
    return [_language_entry(default_text)]


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


def _current_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def _mass_flow_property_reference() -> dict[str, Any]:
    return {
        "@type": "flow property data set",
        "@refObjectId": MASS_FLOW_PROPERTY_UUID,
        "@uri": f"../flowproperties/{MASS_FLOW_PROPERTY_UUID}_{MASS_FLOW_PROPERTY_VERSION}.xml",
        "@version": MASS_FLOW_PROPERTY_VERSION,
        "common:shortDescription": _language_entry("Mass"),
    }


def _ensure_process_defaults(process_dataset: dict[str, Any]) -> None:
    process_info = process_dataset.setdefault("processInformation", {})
    data_info = process_info.setdefault("dataSetInformation", {})
    name_block = data_info.setdefault("name", {})
    base_name = _first_text(name_block.get("baseName")) or "Unnamed process"
    name_block["baseName"] = _multilang_list(name_block.get("baseName"), base_name)[0]
    name_block["treatmentStandardsRoutes"] = _multilang_list(
        name_block.get("treatmentStandardsRoutes"),
        base_name,
    )[0]
    name_block["mixAndLocationTypes"] = _multilang_list(
        name_block.get("mixAndLocationTypes"),
        "Production mix, at plant",
    )[0]
    name_block["functionalUnitFlowProperties"] = _multilang_list(
        name_block.get("functionalUnitFlowProperties"),
        "Functional unit based on reference flow",
    )[0]

    data_info["common:generalComment"] = _multilang_list(
        data_info.get("common:generalComment"),
        "Converted from OpenLCA JSON-LD.",
    )

    quant_ref = process_info.setdefault("quantitativeReference", {})
    ref_flow = quant_ref.get("referenceToReferenceFlow")
    if isinstance(ref_flow, (int, float)):
        ref_flow = str(ref_flow)
    if not isinstance(ref_flow, str) or not ref_flow.strip():
        quant_ref["referenceToReferenceFlow"] = "1"
    else:
        quant_ref["referenceToReferenceFlow"] = ref_flow.strip()
    quant_ref["functionalUnitOrOther"] = _multilang_list(
        quant_ref.get("functionalUnitOrOther"),
        "Reference flow of process",
    )[0]

    time_info = process_info.setdefault("time", {})
    time_info.setdefault("common:referenceYear", 2020)
    time_info.pop("dataSetValidUntil", None)

    geography = process_info.setdefault("geography", {})
    geography.setdefault("locationOfOperationSupplyOrProduction", {"@location": "GLO"})

    modelling = process_info.setdefault("modellingAndValidation", {})
    modelling.setdefault(
        "LCIMethodAndAllocation",
        {
            "typeOfDataSet": "Unit process, single operation",
            "LCIMethodPrinciple": "Attributional",
        },
    )
    completeness = modelling.setdefault("completeness", {})
    completeness["completenessProductModel"] = "No statement"
    modelling.pop("sources", None)

    exchanges_node = process_dataset.setdefault("exchanges", {})
    exchanges = exchanges_node.get("exchange")
    if isinstance(exchanges, dict):
        exchanges = [exchanges]
    elif not isinstance(exchanges, list):
        exchanges = []
    for idx, exchange in enumerate(exchanges, start=1):
        if not isinstance(exchange, dict):
            continue
        exchange["@dataSetInternalID"] = str(exchange.get("@dataSetInternalID") or idx)
        amount = exchange.get("meanAmount")
        mean_amount = str(amount) if amount is not None else "0"
        exchange["meanAmount"] = mean_amount
        exchange.setdefault("resultingAmount", mean_amount)
        if exchange.get("unit") and not exchange.get("resultingAmountUnit"):
            exchange["resultingAmountUnit"] = exchange["unit"]
        ref = exchange.setdefault("referenceToFlowDataSet", {})
        if isinstance(ref, list):
            ref = ref[0] if ref else {}
        if not isinstance(ref, dict):
            ref = {}
        exchange["referenceToFlowDataSet"] = ref
        flow_id = ref.get("@refObjectId")
        if isinstance(flow_id, str):
            flow_id = _strip_version_suffix(flow_id)
        if flow_id:
            version = ref.setdefault("@version", "01.01.000")
            ref["@type"] = ref.get("@type") or "flow data set"
            ref["@refObjectId"] = flow_id
            ref.setdefault("@uri", f"../flows/{flow_id}_{version}.xml")
        else:
            ref.setdefault("@type", "flow data set")
        if "common:shortDescription" not in ref:
            ref["common:shortDescription"] = _language_entry(exchange.get("exchangeName") or base_name)

        prop_ref = exchange.get("referenceToFlowPropertyDataSet")
        if isinstance(prop_ref, dict) and prop_ref.get("@refObjectId"):
            prop_ref.setdefault("@type", "flow property data set")
            prop_version = prop_ref.setdefault("@version", "01.01.000")
            prop_id = prop_ref.get("@refObjectId")
            if prop_id:
                clean_prop_id = _strip_version_suffix(prop_id)
                if clean_prop_id:
                    prop_ref["@refObjectId"] = clean_prop_id
                    prop_id = clean_prop_id
                prop_ref.setdefault("@uri", f"../flowproperties/{prop_id}_{prop_version}.xml")
        else:
            exchange["referenceToFlowPropertyDataSet"] = _mass_flow_property_reference()
    exchanges_node["exchange"] = exchanges
    process_dataset.pop("sources", None)


def _attach_process_source_references(process_dataset: dict[str, Any], source_payload: dict[str, Any]) -> None:
    """Populate referenceToDataSource with actual JSON-LD sources when available."""

    sources_block = source_payload.get("processDocumentation", {}).get("sources") if isinstance(source_payload, dict) else None
    if not isinstance(sources_block, list):
        return

    modelling = process_dataset.setdefault("modellingAndValidation", {})
    dsr = modelling.setdefault("dataSourcesTreatmentAndRepresentativeness", {})
    existing = dsr.get("referenceToDataSource")
    reference_entries: list[dict[str, Any]] = []
    if isinstance(existing, dict):
        reference_entries.append(existing)
    elif isinstance(existing, list):
        reference_entries.extend([entry for entry in existing if isinstance(entry, dict)])

    seen_ids = {entry.get("@refObjectId").strip().lower() for entry in reference_entries if isinstance(entry.get("@refObjectId"), str)}

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
                "common:shortDescription": _language_entry(short_name),
            }
        )
        seen_ids.add(key)

    if reference_entries:
        dsr["referenceToDataSource"] = reference_entries


def _ensure_source_defaults(source_dataset: dict[str, Any], uuid_value: str) -> None:
    for key, value in SOURCE_XMLNS.items():
        source_dataset[key] = value

    source_info = source_dataset.setdefault("sourceInformation", {})
    existing_info = source_info.get("dataSetInformation", {})
    if not isinstance(existing_info, dict):
        existing_info = {}
    data_info: dict[str, Any] = {}
    short_name = _first_text(existing_info.get("common:shortName")) or _first_text(existing_info.get("name")) or "Source"
    data_info["common:UUID"] = uuid_value
    data_info["common:shortName"] = _multilang_list(existing_info.get("common:shortName"), short_name)
    classification = {
        "common:classification": {
            "common:class": {
                "@level": "0",
                "@classId": "5",
                "#text": "Publications and communications",
            }
        }
    }
    data_info["classificationInformation"] = classification
    data_info["sourceCitation"] = short_name
    data_info["publicationType"] = DEFAULT_SOURCE_PUBLICATION_TYPE
    data_info["sourceDescriptionOrComment"] = _multilang_list(
        source_dataset.get("sourceDescriptionOrComment"),
        DEFAULT_SOURCE_DESCRIPTION,
    )
    data_info["referenceToContact"] = _contact_reference()

    source_info.clear()
    source_info["dataSetInformation"] = data_info

    admin = source_dataset.setdefault("administrativeInformation", {})
    data_entry = admin.setdefault("dataEntryBy", {})
    data_entry["common:referenceToDataSetFormat"] = _format_reference()
    data_entry["common:referenceToPersonOrEntityEnteringTheData"] = _contact_reference()
    data_entry["common:timeStamp"] = _current_timestamp()

    publication = admin.setdefault("publicationAndOwnership", {})
    version = "01.01.000"
    publication["common:dataSetVersion"] = version
    publication["common:permanentDataSetURI"] = f"https://lcdn.tiangong.earth/showSource.xhtml?uuid={uuid_value}&version={version}"
    publication["common:referenceToOwnershipOfDataSet"] = _ownership_reference()
    publication["common:licenseType"] = DEFAULT_LICENSE


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--process-dir", type=Path, required=True, help="Directory or file containing OpenLCA JSON-LD process datasets.")
    parser.add_argument("--flow-dir", type=Path, help="Directory or file containing OpenLCA JSON-LD flow datasets.")
    parser.add_argument("--run-id", required=True, help="Run identifier shared across JSON-LD stages.")
    parser.add_argument("--prompt", type=Path, default=DEFAULT_PROMPT_PATH, help="System prompt fed to the LLM during conversion.")
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


def _load_prompt(path: Path) -> str:
    if not path.exists():
        raise SystemExit(f"Prompt file not found: {path}")
    return path.read_text(encoding="utf-8")


def _invoke_llm(llm: OpenAIResponsesLLM, prompt_text: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = llm.invoke(
        {
            "prompt": prompt_text,
            "context": payload,
            "response_format": {"type": "json_object"},
        }
    )
    try:
        return json.loads(response)
    except json.JSONDecodeError as exc:
        snippet = response[:500]
        raise SystemExit(f"LLM output is not valid JSON ({exc}). Snippet: {snippet!r}") from exc


def _extract_dataset_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if "datasets" in payload and isinstance(payload["datasets"], list):
        entries = [item for item in payload["datasets"] if isinstance(item, dict)]
        if entries:
            return entries
    for key in (
        "processDataSet",
        "flowDataSet",
        "flowPropertyDataSet",
        "unitGroupDataSet",
        "sourceDataSet",
    ):
        if key in payload:
            return [payload]
    raise SystemExit("LLM response must contain 'datasets' or at least one ILCD data set object.")


def _apply_process_classification(
    dataset: dict[str, Any],
    classifier: ProcessClassifier,
) -> None:
    process_info = dataset.setdefault("processInformation", {})
    classification_path = classifier.run(process_info)
    data_info = process_info.setdefault("dataSetInformation", {})
    classification = data_info.setdefault("classificationInformation", {}).setdefault("common:classification", {})
    classification["common:class"] = classification_path


def _wrap_process_dataset(
    dataset: dict[str, Any],
    classifier: ProcessClassifier,
    source_payload: dict[str, Any],
) -> dict[str, Any]:
    if "processDataSet" not in dataset or not isinstance(dataset["processDataSet"], dict):
        raise SystemExit("LLM response missing 'processDataSet'.")
    node = dataset["processDataSet"]
    info = node.setdefault("processInformation", {}).setdefault("dataSetInformation", {})
    uuid_value = info.get("common:UUID") or str(uuid4())
    info["common:UUID"] = uuid_value
    _ensure_process_defaults(node)
    _attach_process_source_references(node, source_payload)
    _apply_process_classification(node, classifier)
    class_entries = node.get("processInformation", {}).get("dataSetInformation", {}).get("classificationInformation", {}).get("common:classification", {}).get("common:class")
    if not class_entries:
        category_hint = _classification_from_category(_extract_category_text(source_payload))
        if category_hint:
            classification_info = node.setdefault("processInformation", {}).setdefault("dataSetInformation", {}).setdefault("classificationInformation", {})
            classification_info.setdefault("common:classification", {})["common:class"] = category_hint
    apply_jsonld_process_overrides({"processDataSet": node})
    return {
        "processDataSet": node,
        "process_id": uuid_value,
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


def _ensure_flow_defaults(flow_dataset: dict[str, Any], uuid_value: str) -> None:
    for key, value in FLOW_XMLNS.items():
        flow_dataset[key] = value

    flow_info = flow_dataset.setdefault("flowInformation", {})
    data_info = flow_info.setdefault("dataSetInformation", {})
    cas_value = data_info.get("CASNumber")
    if isinstance(cas_value, str):
        cas_value = cas_value.strip()
        if cas_value:
            data_info["CASNumber"] = cas_value
        else:
            data_info.pop("CASNumber", None)
    else:
        data_info.pop("CASNumber", None)
    name_block = data_info.setdefault("name", {})
    base_name = _first_text(name_block.get("baseName")) or "Unnamed flow"
    name_block["baseName"] = _language_entry(base_name)
    name_block["treatmentStandardsRoutes"] = _language_entry(_first_text(name_block.get("treatmentStandardsRoutes")) or "Standard treatment not specified")
    name_block["mixAndLocationTypes"] = _language_entry(_first_text(name_block.get("mixAndLocationTypes")) or "Production mix, at plant")
    name_block.pop("functionalUnitFlowProperties", None)
    name_block["flowProperties"] = _language_entry("Declared per reference flow property")

    data_info["common:synonyms"] = _multilang_list(data_info.get("common:synonyms"), base_name)
    data_info["common:generalComment"] = _multilang_list(
        data_info.get("common:generalComment"),
        "Converted from OpenLCA JSON-LD.",
    )

    flow_props_node = flow_dataset.setdefault("flowProperties", {})
    flow_props = flow_props_node.get("flowProperty")
    if isinstance(flow_props, dict):
        flow_props = [flow_props]
    elif not isinstance(flow_props, list):
        flow_props = []
    if not flow_props:
        flow_props.append(
            {
                "@dataSetInternalID": "1",
                "meanValue": "1",
                "referenceToFlowPropertyDataSet": _mass_flow_property_reference(),
            }
        )
    for idx, prop in enumerate(flow_props, start=1):
        if not isinstance(prop, dict):
            continue
        prop["@dataSetInternalID"] = str(prop.get("@dataSetInternalID") or idx)
        prop["meanValue"] = str(prop.get("meanValue", "1"))
        ref = prop.get("referenceToFlowPropertyDataSet")
        if not isinstance(ref, dict) or not ref.get("@refObjectId"):
            prop["referenceToFlowPropertyDataSet"] = _mass_flow_property_reference()
        else:
            ref.setdefault("@type", "flow property data set")
            version = ref.setdefault("@version", "01.01.000")
            ref_id = ref.get("@refObjectId")
            if ref_id:
                clean_ref_id = _strip_version_suffix(ref_id)
                if clean_ref_id:
                    ref["@refObjectId"] = clean_ref_id
                    ref_id = clean_ref_id
                ref.setdefault("@uri", f"../flowproperties/{ref_id}_{version}.xml")
            if "common:shortDescription" not in ref:
                ref["common:shortDescription"] = _language_entry(ref.get("name") or base_name)
    flow_props_node["flowProperty"] = flow_props

    quant_ref = flow_info.setdefault("quantitativeReference", {})
    quant_ref["referenceToReferenceFlowProperty"] = flow_props[0]["@dataSetInternalID"]

    geography = flow_info.setdefault("geography", {})
    location = geography.get("locationOfSupply")
    if isinstance(location, dict):
        location = location.get("@location")
    geography["locationOfSupply"] = (location or "GLO").strip()

    technology = flow_info.setdefault("technology", {})
    technology["technologicalApplicability"] = _multilang_list(
        technology.get("technologicalApplicability"),
        "Applicable to generic supply mixes.",
    )

    modelling = flow_dataset.setdefault("modellingAndValidation", {})
    lci_method = modelling.setdefault("LCIMethod", {})
    lci_method["typeOfDataSet"] = _normalise_flow_type(lci_method.get("typeOfDataSet"))
    compliance_block = modelling.setdefault("complianceDeclarations", {})
    compliance_block["compliance"] = {
        "common:referenceToComplianceSystem": _compliance_reference(),
        "common:approvalOfOverallCompliance": "Fully compliant",
        "common:nomenclatureCompliance": "Fully compliant",
        "common:methodologicalCompliance": "Not defined",
        "common:reviewCompliance": "Not defined",
        "common:documentationCompliance": "Not defined",
        "common:qualityCompliance": "Not defined",
    }

    admin = flow_dataset.setdefault("administrativeInformation", {})
    data_entry = admin.setdefault("dataEntryBy", {})
    data_entry["common:referenceToDataSetFormat"] = _format_reference()
    data_entry["common:referenceToPersonOrEntityEnteringTheData"] = _contact_reference()
    data_entry["common:timeStamp"] = _current_timestamp()

    publication = admin.setdefault("publicationAndOwnership", {})
    version = "01.01.000"
    publication["common:dataSetVersion"] = version
    publication["common:permanentDataSetURI"] = f"https://lcdn.tiangong.earth/showFlow.xhtml?uuid={uuid_value}&version={version}"
    publication["common:licenseType"] = DEFAULT_LICENSE
    publication["common:referenceToOwnershipOfDataSet"] = _ownership_reference()


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
            f"Flow dataset{hint}{context_hint} is missing classification entries; " "Stage 1 must emit a complete path defined in src/tidas/schemas/tidas_flows_product_category.json."
        )
    try:
        classification["common:class"] = ensure_valid_product_flow_classification(tuple(normalised))
    except ValueError as exc:
        raise SystemExit(
            f"Flow dataset{hint}{context_hint} has invalid product classification: {exc}. "
            "Update the Stage 1 prompt/output so the LLM returns a valid path directly from "
            "src/tidas/schemas/tidas_flows_product_category.json."
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
    info = node.setdefault("flowInformation", {}).setdefault("dataSetInformation", {})
    uuid_value = info.get("common:UUID") or str(uuid4())
    info["common:UUID"] = uuid_value
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

    classification_info["common:class"] = flow_classifier.run(node, source_payload=source_payload)
    category_hint = _extract_category_text(source_payload)
    _ensure_flow_classification(node, source, category_hint)
    _ensure_flow_defaults(node, uuid_value)
    return {"flowDataSet": node}


def _wrap_source_dataset(dataset: dict[str, Any]) -> dict[str, Any]:
    node = dataset.get("sourceDataSet")
    if not isinstance(node, dict):
        raise SystemExit("LLM response missing 'sourceDataSet'.")
    info = node.setdefault("sourceInformation", {}).setdefault("dataSetInformation", {})
    uuid_value = info.get("common:UUID") or str(uuid4())
    info["common:UUID"] = uuid_value
    _ensure_source_defaults(node, uuid_value)
    return {"sourceDataSet": node}


def _route_datasets(
    datasets: list[dict[str, Any]],
    *,
    source_path: Path,
    source_payload: dict[str, Any],
    process_blocks: list[dict[str, Any]],
    flow_datasets: list[dict[str, Any]],
    source_datasets: list[dict[str, Any]],
    classifier: ProcessClassifier,
    flow_classifier: ProductFlowClassifier,
) -> None:
    for dataset in datasets:
        if "processDataSet" in dataset:
            process_blocks.append(_wrap_process_dataset(dataset, classifier, source_payload))
        elif "flowDataSet" in dataset:
            flow_datasets.append(_wrap_flow_dataset(dataset, source_path, source_payload, flow_classifier))
        elif "sourceDataSet" in dataset:
            source_datasets.append(_wrap_source_dataset(dataset))
        else:
            continue


def main() -> None:
    args = parse_args()
    run_id = args.run_id
    ensure_run_cache_dir(run_id)
    save_latest_run_id(run_id)

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

    prompt_text = _load_prompt(args.prompt)
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

    process_blocks: list[dict[str, Any]] = []
    flow_datasets: list[dict[str, Any]] = []
    source_datasets: list[dict[str, Any]] = []

    for json_path in process_files:
        raw_payload = json.loads(json_path.read_text(encoding="utf-8"))
        response = _invoke_llm(llm, prompt_text, raw_payload)
        datasets = _extract_dataset_entries(response)
        _route_datasets(
            datasets,
            source_path=json_path,
            source_payload=raw_payload,
            process_blocks=process_blocks,
            flow_datasets=flow_datasets,
            source_datasets=source_datasets,
            classifier=process_classifier,
            flow_classifier=flow_classifier,
        )

    for json_path in flow_files:
        raw_payload = json.loads(json_path.read_text(encoding="utf-8"))
        response = _invoke_llm(llm, prompt_text, raw_payload)
        datasets = _extract_dataset_entries(response)
        _route_datasets(
            datasets,
            source_path=json_path,
            source_payload=raw_payload,
            process_blocks=process_blocks,
            flow_datasets=flow_datasets,
            source_datasets=source_datasets,
            classifier=process_classifier,
            flow_classifier=flow_classifier,
        )

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
