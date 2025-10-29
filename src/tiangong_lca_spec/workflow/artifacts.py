"""Utilities to build ILCD artifacts directly from Stage 2/3 outputs."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from tiangong_lca_spec.core.models import FlowCandidate, ProcessDataset
from tiangong_lca_spec.process_extraction.merge import determine_functional_unit, merge_results
from tiangong_lca_spec.tidas_validation import TidasValidationService

DEFAULT_FORMAT_SOURCE_UUID = "00000000-0000-0000-0000-0000000000f0"
TIDAS_PORTAL_BASE = "https://lcdn.tiangong.earth"
SOURCE_CLASSIFICATIONS: dict[str, tuple[str, str]] = {
    "images": ("0", "Images"),
    "data set formats": ("1", "Data set formats"),
    "databases": ("2", "Databases"),
    "compliance systems": ("3", "Compliance systems"),
    "statistical classifications": ("4", "Statistical classifications"),
    "publications and communications": ("5", "Publications and communications"),
    "other source types": ("6", "Other source types"),
}


@dataclass(slots=True)
class ArtifactBuildSummary:
    """Lightweight summary returned after generating artifacts."""

    process_count: int
    flow_count: int
    source_count: int
    validation_report: list[dict[str, Any]]


def generate_artifacts(
    process_blocks: list[dict[str, Any]],
    alignment_entries: list[dict[str, Any]],
    *,
    artifact_root: Path,
    merged_output: Path,
    validation_output: Path,
    workflow_output: Path | None = None,
    format_source_uuid: str = DEFAULT_FORMAT_SOURCE_UUID,
    run_validation: bool = True,
) -> ArtifactBuildSummary:
    """Merge aligned results and materialise ILCD artifacts required by downstream tools."""

    matched_lookup, origin_exchanges = _build_alignment_indexes(alignment_entries)
    datasets = merge_results(process_blocks, matched_lookup, origin_exchanges)
    for dataset in datasets:
        functional_unit = determine_functional_unit(dataset.exchanges)
        if functional_unit:
            info = dict(dataset.process_information)
            processes_block = dict(info.get("processes", {}))
            processes_block["functionalUnit"] = functional_unit
            info["processes"] = processes_block
            dataset.process_information = info

    merged_payload = {"process_datasets": [_serialise_dataset(dataset) for dataset in datasets]}
    _dump_json(merged_payload, merged_output)

    timestamp = _utc_timestamp()
    _ensure_directories(artifact_root)

    source_references: dict[str, dict[str, Any]] = {}
    for dataset in datasets:
        ilcd_dataset = dataset.as_dict()
        uuid_value = (
            ilcd_dataset.get("processInformation", {})
            .get("dataSetInformation", {})
            .get("common:UUID")
        )
        if not uuid_value:
            raise ValueError("Process dataset missing common:UUID.")
        process_path = artifact_root / "processes" / f"{uuid_value}.json"
        _dump_json({"processDataSet": ilcd_dataset}, process_path)

        source_references |= _collect_source_references(ilcd_dataset)

    unmatched_entries = _collect_unmatched_exchanges(alignment_entries)
    flow_count = 0
    for process_name, exchange in unmatched_entries:
        uuid_value, dataset = _build_flow_dataset(
            exchange,
            process_name,
            timestamp,
            format_source_uuid,
        )
        flow_path = artifact_root / "flows" / f"{uuid_value}.json"
        _dump_json(dataset, flow_path)
        flow_count += 1

    format_source_ref = {
        "@type": "source data set",
        "@refObjectId": format_source_uuid,
        "common:shortDescription": _language_entry("ILCD format"),
    }
    source_references.setdefault(format_source_uuid, format_source_ref)

    written_sources = 0
    for uuid_value, reference in source_references.items():
        source_path = artifact_root / "sources" / f"{uuid_value}.json"
        stub = _build_source_stub(uuid_value, reference, timestamp, format_source_uuid)
        _dump_json(stub, source_path)
        written_sources += 1

    if run_validation:
        validation_report = _run_validation(artifact_root)
    else:
        validation_report = []

    _dump_json({"validation_report": validation_report}, validation_output)

    if workflow_output is not None:
        payload = {
            "process_datasets": [dataset.as_dict() for dataset in datasets],
            "alignment": alignment_entries,
            "validation_report": validation_report,
        }
        _dump_json(payload, workflow_output)

    return ArtifactBuildSummary(
        process_count=len(datasets),
        flow_count=flow_count,
        source_count=written_sources,
        validation_report=validation_report,
    )


def _build_alignment_indexes(
    alignment_entries: list[dict[str, Any]],
) -> tuple[dict[str, list[FlowCandidate]], dict[str, list[dict[str, Any]]]]:
    matched_lookup: dict[str, list[FlowCandidate]] = {}
    origin_exchanges: dict[str, list[dict[str, Any]]] = {}
    for entry in alignment_entries:
        process_name = entry.get("process_name") or "unknown_process"
        matched_lookup[process_name] = _hydrate_flow_candidates(entry)
        origin: list[dict[str, Any]] = []
        origin_exchanges_block = entry.get("origin_exchanges") or {}
        if isinstance(origin_exchanges_block, dict):
            for exchanges in origin_exchanges_block.values():
                if isinstance(exchanges, list):
                    origin.extend(exchanges)
                elif isinstance(exchanges, dict):
                    origin.append(exchanges)
        origin_exchanges[process_name] = origin
    return matched_lookup, origin_exchanges


def _hydrate_flow_candidates(entry: dict[str, Any]) -> list[FlowCandidate]:
    candidates_raw = entry.get("matched_flows") or []
    hydrated: list[FlowCandidate] = []
    for item in candidates_raw:
        if isinstance(item, dict):
            hydrated.append(FlowCandidate(**item))
    return hydrated


def _serialise_dataset(dataset: ProcessDataset) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "process_information": deepcopy(dataset.process_information),
        "modelling_and_validation": deepcopy(dataset.modelling_and_validation),
        "administrative_information": deepcopy(dataset.administrative_information),
        "exchanges": [deepcopy(exchange) for exchange in dataset.exchanges],
    }
    if dataset.process_data_set is not None:
        payload["process_data_set"] = deepcopy(dataset.process_data_set)
    return payload


def _language_entry(text: str, lang: str = "en") -> dict[str, str]:
    return {"@xml:lang": lang, "#text": text}


def _normalise_language(value: Any, default_lang: str = "en") -> list[dict[str, str]]:
    if value is None:
        return []
    if isinstance(value, list):
        normalised: list[dict[str, str]] = []
        for item in value:
            if isinstance(item, dict) and "#text" in item:
                lang = item.get("@xml:lang") or default_lang
                normalised.append(_language_entry(str(item["#text"]), lang))
            else:
                normalised.append(_language_entry(str(item), default_lang))
        return normalised
    if isinstance(value, dict) and "#text" in value:
        lang = value.get("@xml:lang") or default_lang
        return [_language_entry(str(value["#text"]), lang)]
    return [_language_entry(str(value), default_lang)]


def _extract_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        text = value.get("#text")
        if isinstance(text, str):
            return text.strip()
    return str(value).strip()


def _parse_flowsearch_hints(comment: Any) -> dict[str, list[str]]:
    text = _extract_text(comment)
    if not text:
        return {}
    prefix = "FlowSearch hints:"
    if text.startswith(prefix):
        text = text[len(prefix) :].strip()
    segments = [segment.strip() for segment in text.split("|") if segment.strip()]
    hints: dict[str, list[str]] = {}
    for segment in segments:
        key, _, value = segment.partition("=")
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if not value or value == "NA":
            hints[key] = []
            continue
        entries = [item.strip() for item in value.split(";") if item.strip()]
        hints[key] = entries or [value]
    return hints


def _infer_flow_type(exchange: dict[str, Any], hints: dict[str, list[str]]) -> str:
    direction = _extract_text(exchange.get("exchangeDirection")).lower()
    name = _extract_text(exchange.get("exchangeName")).lower()
    combined = " ".join(
        [
            name,
            _extract_text(exchange.get("generalComment")).lower(),
            " ".join(hints.get("usage_context", [])).lower(),
            " ".join(hints.get("state_purity", [])).lower(),
        ]
    )
    if any(keyword in combined for keyword in ("emission", "to air", "to water", "wastewater")):
        return "Elementary flow"
    if "waste" in combined or "slag" in combined:
        return "Waste flow"
    if direction == "input" and ("air" in name or "water" in name):
        return "Elementary flow"
    return "Product flow"


def _build_elementary_classification() -> dict[str, Any]:
    categories = [
        {"@level": "0", "#text": "Emissions"},
        {"@level": "1", "#text": "Emissions to unspecified"},
    ]
    return {"common:elementaryFlowCategorization": {"common:category": categories}}


def _build_product_classification() -> dict[str, Any]:
    classes = [
        {"@level": "0", "#text": "Products"},
    ]
    return {"common:classification": {"common:class": classes}}


def _build_waste_classification() -> dict[str, Any]:
    classes = [
        {"@level": "0", "#text": "Waste"},
    ]
    return {"common:classification": {"common:class": classes}}


def _source_classification_entry(class_id: str, label: str) -> dict[str, Any]:
    return {
        "common:classification": {
            "common:class": {
                "@level": "0",
                "@classId": class_id,
                "#text": label,
            }
        }
    }


def _build_source_classification(
    reference_node: dict[str, Any], uuid_value: str, format_source_uuid: str
) -> dict[str, Any]:
    existing = reference_node.get("classificationInformation")
    if isinstance(existing, dict) and existing.get("common:classification"):
        return existing

    ref_uuid = str(reference_node.get("@refObjectId") or "").lower()
    short_desc = _extract_text(reference_node.get("common:shortDescription")).lower()
    uri = str(reference_node.get("@uri") or "").lower()

    def _match_any(haystack: str, keywords: tuple[str, ...]) -> bool:
        return any(keyword in haystack for keyword in keywords if keyword)

    class_id, label = SOURCE_CLASSIFICATIONS["other source types"]
    if uuid_value.lower() == format_source_uuid.lower() or _match_any(
        short_desc, ("format", "schema")
    ):
        class_id, label = SOURCE_CLASSIFICATIONS["data set formats"]
    elif ref_uuid == DEFAULT_FORMAT_SOURCE_UUID:
        class_id, label = SOURCE_CLASSIFICATIONS["data set formats"]
    elif _match_any(short_desc, ("ilcd data network", "compliance", "conformity", "certification")) or _match_any(
        uri, ("compliance", "conformity")
    ):
        class_id, label = SOURCE_CLASSIFICATIONS["compliance systems"]
    elif _match_any(short_desc, ("database", "data bank", "dataset")) or _match_any(
        uri, ("database",)
    ):
        class_id, label = SOURCE_CLASSIFICATIONS["databases"]
    elif _match_any(short_desc, ("nace", "isic", "cpc", "statistical", "classification")) or _match_any(
        uri, ("classification",)
    ):
        class_id, label = SOURCE_CLASSIFICATIONS["statistical classifications"]
    elif _match_any(short_desc, ("image", "photo", "figure", "diagram")) or uri.endswith(
        (".png", ".jpg", ".jpeg", ".gif", ".svg", ".bmp")
    ):
        class_id, label = SOURCE_CLASSIFICATIONS["images"]
    elif _match_any(
        short_desc,
        (
            "publication",
            "report",
            "article",
            "paper",
            "journal",
            "communication",
            "study",
            "thesis",
            "book",
        ),
    ):
        class_id, label = SOURCE_CLASSIFICATIONS["publications and communications"]

    return _source_classification_entry(class_id, label)


def _flow_property_reference() -> dict[str, Any]:
    return {
        "@type": "flow property data set",
        "@refObjectId": "93a60a56-a3c8-11da-a746-0800200b9a66",
        "@uri": "../flowproperties/93a60a56-a3c8-11da-a746-0800200b9a66.xml",
        "@version": "03.00.003",
        "common:shortDescription": _language_entry("Mass"),
    }


def _ownership_reference() -> dict[str, Any]:
    return {
        "@refObjectId": "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8",
        "@type": "contact data set",
        "@uri": "../contacts/f4b4c314-8c4c-4c83-968f-5b3c7724f6a8.xml",
        "@version": "01.00.000",
        "common:shortDescription": [
            _language_entry("Tiangong LCA Data Working Group", "en"),
            _language_entry("天工LCA数据团队", "zh"),
        ],
    }


def _permanent_dataset_uri(dataset_kind: str, uuid_value: str, version: str) -> str:
    suffix_map = {
        "process": "showProcess.xhtml",
        "flow": "showProductFlow.xhtml",
        "source": "showSource.xhtml",
    }
    suffix = suffix_map.get(dataset_kind, "showDataSet.xhtml")
    version_clean = version.strip() or "01.00.000"
    return f"{TIDAS_PORTAL_BASE}/{suffix}?uuid={uuid_value}&version={version_clean}"


def _build_flow_dataset(
    exchange: dict[str, Any],
    process_name: str,
    timestamp: str,
    format_source_uuid: str,
) -> tuple[str, dict[str, Any]]:
    ref = exchange.get("referenceToFlowDataSet") or {}
    uuid_value = ref.get("@refObjectId") or str(uuid4())
    name = _extract_text(exchange.get("exchangeName")) or _extract_text(
        ref.get("common:shortDescription")
    )
    if not name:
        name = "Unnamed flow"
    hints = _parse_flowsearch_hints(exchange.get("generalComment"))
    flow_type = _infer_flow_type(exchange, hints)
    if flow_type == "Elementary flow":
        classification = _build_elementary_classification()
    elif flow_type == "Waste flow":
        classification = _build_waste_classification()
    else:
        classification = _build_product_classification()

    en_synonyms = hints.get("en_synonyms") or []
    zh_synonyms = hints.get("zh_synonyms") or []
    synonyms_block: list[dict[str, str]] = []
    if en_synonyms:
        synonyms_block.append(_language_entry("; ".join(en_synonyms), "en"))
    if zh_synonyms:
        synonyms_block.append(_language_entry("; ".join(zh_synonyms), "zh"))
    if not synonyms_block:
        synonyms_block.append(_language_entry(name, "en"))

    comment_entries = _normalise_language(
        exchange.get("generalComment") or f"Generated for {process_name}"
    )
    name_block = {"baseName": [_language_entry(name, "en")]}
    if zh_synonyms:
        name_block["baseName"].append(_language_entry(zh_synonyms[0], "zh"))

    dataset_version = "01.00.000"
    dataset = {
        "flowDataSet": {
            "@xmlns": "http://lca.jrc.it/ILCD/Flow",
            "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
            "@xmlns:ecn": "http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "@version": "1.1",
            "@locations": "../ILCDLocations.xml",
            "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
            "flowInformation": {
                "dataSetInformation": {
                    "common:UUID": uuid_value,
                    "name": name_block,
                    "common:synonyms": synonyms_block,
                    "common:generalComment": comment_entries,
                    "classificationInformation": classification,
                },
                "quantitativeReference": {
                    "referenceToReferenceFlowProperty": "0",
                },
            },
            "modellingAndValidation": {
                "LCIMethod": {
                    "typeOfDataSet": flow_type,
                }
            },
            "administrativeInformation": {
                "dataEntryBy": {
                    "common:timeStamp": timestamp,
                    "common:referenceToDataSetFormat": {
                        "@type": "source data set",
                        "@refObjectId": format_source_uuid,
                        "@uri": f"../sources/{format_source_uuid}.xml",
                        "@version": "01.00.000",
                        "common:shortDescription": _language_entry("ILCD format"),
                    },
                    "common:referenceToPersonOrEntityEnteringTheData": (
                        "Generated automatically from stage2/3 results"
                    ),
                },
                "publicationAndOwnership": {
                    "common:dataSetVersion": dataset_version,
                    "common:permanentDataSetURI": _permanent_dataset_uri(
                        "flow", uuid_value, dataset_version
                    ),
                    "common:referenceToOwnershipOfDataSet": _ownership_reference(),
                },
            },
            "flowProperties": {
                "flowProperty": {
                    "@dataSetInternalID": "0",
                    "meanValue": "1",
                    "referenceToFlowPropertyDataSet": _flow_property_reference(),
                }
            },
        }
    }
    return uuid_value, dataset


def _collect_unmatched_exchanges(alignment: Iterable[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    collected: dict[str, tuple[str, dict[str, Any]]] = {}
    for entry in alignment:
        process_name = entry.get("process_name") or "Unnamed process"
        origin = entry.get("origin_exchanges") or {}
        if not isinstance(origin, dict):
            continue
        for exchanges in origin.values():
            if isinstance(exchanges, dict):
                exchanges_iter = [exchanges]
            else:
                exchanges_iter = list(exchanges or [])
            for exchange in exchanges_iter:
                if not isinstance(exchange, dict):
                    continue
                ref = exchange.get("referenceToFlowDataSet")
                if not isinstance(ref, dict):
                    continue
                if not ref.get("unmatched:placeholder"):
                    continue
                uuid_value = ref.get("@refObjectId")
                if uuid_value and uuid_value not in collected:
                    collected[uuid_value] = (process_name, exchange)
    return list(collected.values())


def _collect_source_references(process_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    references: dict[str, dict[str, Any]] = {}

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            node_type = node.get("@type")
            ref_id = node.get("@refObjectId")
            if node_type == "source data set" and ref_id:
                references.setdefault(ref_id, node)
            for value in node.values():
                _walk(value)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(process_payload)
    return references


def _build_source_stub(
    uuid_value: str,
    reference_node: dict[str, Any],
    timestamp: str,
    format_source_uuid: str,
) -> dict[str, Any]:
    short_desc = reference_node.get("common:shortDescription")
    description_entries = _normalise_language(short_desc or "Source reference")
    classification = _build_source_classification(reference_node, uuid_value, format_source_uuid)
    dataset_version = "01.00.000"
    dataset = {
        "sourceDataSet": {
            "@xmlns": "http://lca.jrc.it/ILCD/Source",
            "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "@version": "1.1",
            "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Source ../../schemas/ILCD_SourceDataSet.xsd",
            "sourceInformation": {
                "dataSetInformation": {
                    "common:UUID": uuid_value,
                    "common:shortName": description_entries,
                    "classificationInformation": classification,
                }
            },
            "administrativeInformation": {
                "dataEntryBy": {
                    "common:timeStamp": timestamp,
                    "common:referenceToDataSetFormat": {
                        "@type": "source data set",
                        "@refObjectId": format_source_uuid,
                        "@uri": f"../sources/{format_source_uuid}.xml",
                        "@version": "01.00.000",
                        "common:shortDescription": _language_entry("ILCD format"),
                    },
                },
                "publicationAndOwnership": {
                    "common:dataSetVersion": dataset_version,
                    "common:permanentDataSetURI": _permanent_dataset_uri(
                        "source", uuid_value, dataset_version
                    ),
                    "common:referenceToOwnershipOfDataSet": _ownership_reference(),
                },
            },
        }
    }
    return dataset


def _ensure_directories(root: Path) -> None:
    for name in ("processes", "flows", "sources"):
        (root / name).mkdir(parents=True, exist_ok=True)


def _run_validation(artifact_root: Path) -> list[dict[str, Any]]:
    service = TidasValidationService()
    try:
        findings = service.validate_directory(artifact_root)
    finally:
        service.close()

    for finding in findings:
        if finding.severity != "info":
            print(finding.message)
    return [asdict(finding) for finding in findings]


def _dump_json(payload: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
