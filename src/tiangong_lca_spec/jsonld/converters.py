"""Converters that map OpenLCA JSON-LD payloads into ILCD-compatible datasets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from tiangong_lca_spec.tidas.process_classification_registry import ensure_valid_classification_path


ILCD_PROCESS_XMLNS = {
    "@xmlns": "http://lca.jrc.it/ILCD/Process",
    "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
    "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Process ../../schemas/ILCD_ProcessDataSet.xsd",
    "@version": "1.1",
    "@locations": "../ILCDLocations.xml",
}

ILCD_FLOW_XMLNS = {
    "@xmlns": "http://lca.jrc.it/ILCD/Flow",
    "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
    "@xmlns:ecn": "http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber",
    "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
    "@version": "1.1",
    "@locations": "../ILCDLocations.xml",
}

ILCD_SOURCE_XMLNS = {
    "@xmlns": "http://lca.jrc.it/ILCD/Source",
    "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
    "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Source ../../schemas/ILCD_SourceDataSet.xsd",
    "@version": "1.1",
}

TIANGONG_CONTACT_UUID = "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8"
TIANGONG_CONTACT_URI = "../contacts/f4b4c314-8c4c-4c83-968f-5b3c7724f6a8_01.00.000.xml"
TIANGONG_CONTACT_VERSION = "01.00.000"
TIANGONG_CONTACT_SHORT_DESCRIPTION = [
    {"@xml:lang": "en", "#text": "Tiangong LCA Data Working Group"},
    {"@xml:lang": "zh", "#text": "天工LCA数据团队"},
]
ILCD_FORMAT_SOURCE_UUID = "a97a0155-0234-4b87-b4ce-a45da52f2a40"
ILCD_FORMAT_SOURCE_URI = "../sources/a97a0155-0234-4b87-b4ce-a45da52f2a40_03.00.003.xml"
ILCD_FORMAT_SOURCE_VERSION = "03.00.003"
ILCD_COMPLIANCE_SOURCE_UUID = "d92a1a12-2545-49e2-a585-55c259997756"
ILCD_COMPLIANCE_SOURCE_URI = "../sources/d92a1a12-2545-49e2-a585-55c259997756_20.20.002.xml"
ILCD_COMPLIANCE_SOURCE_VERSION = "20.20.002"
DEFAULT_LICENSE = "Free of charge for all users and uses"
DEFAULT_DATA_SET_VERSION = "01.01.000"

FLOW_CLASSIFICATION_PATHS: dict[str, tuple[dict[str, str], ...]] = {
    "generic_goods": (
        {"@level": "0", "@classId": "3", "#text": "Other transportable goods, except metal products, machinery and equipment"},
        {"@level": "1", "@classId": "38", "#text": "Furniture; other transportable goods n.e.c."},
        {"@level": "2", "@classId": "389", "#text": "Other manufactured articles n.e.c."},
        {"@level": "3", "@classId": "3899", "#text": "Other articles"},
        {"@level": "4", "@classId": "38999", "#text": "Articles n.e.c. (including candles, tapers, skins of birds with their feathers, artificial flowers, entertainment articles, hand sieves, hand riddles, vacuum flasks, tailors dummies, animated displays used for shop window dressing, and parts n.e.c.)"},
    ),
    "chemicals": (
        {"@level": "0", "@classId": "3", "#text": "Other transportable goods, except metal products, machinery and equipment"},
        {"@level": "1", "@classId": "34", "#text": "Basic chemicals"},
        {"@level": "2", "@classId": "341", "#text": "Basic organic chemicals"},
        {"@level": "3", "@classId": "3417", "#text": "Ethers, alcohol peroxides, ether peroxides, epoxides, acetals and hemiacetals, and their halogenated, sulphonated, nitrated or nitrosated derivatives; aldehyde-function compounds; ketone-function compounds and quinone-function compounds; enzymes; prepared enzymes n.e.c.; organic compounds n.e.c."},
        {"@level": "4", "@classId": "34170", "#text": "Ethers, alcohol peroxides, ether peroxides, epoxides, acetals and hemiacetals, and their halogenated, sulphonated, nitrated or nitrosated derivatives; aldehyde-function compounds; ketone-function compounds and quinone-function compounds; enzymes; prepared enzymes n.e.c.; organic compounds n.e.c."},
    ),
    "metal": (
        {"@level": "0", "@classId": "4", "#text": "Metal products, machinery and equipment"},
        {"@level": "1", "@classId": "41", "#text": "Basic metals"},
        {"@level": "2", "@classId": "414", "#text": "Copper, nickel, aluminium, alumina, lead, zinc and tin, unwrought"},
        {"@level": "3", "@classId": "4143", "#text": "Aluminium, unwrought; alumina"},
        {"@level": "4", "@classId": "41431", "#text": "Unwrought aluminium"},
    ),
    "electricity": (
        {"@level": "0", "@classId": "1", "#text": "Ores and minerals; electricity, gas and water"},
        {"@level": "1", "@classId": "17", "#text": "Electricity, town gas, steam and hot water"},
        {"@level": "2", "@classId": "171", "#text": "Electrical energy"},
        {"@level": "3", "@classId": "1710", "#text": "Electrical energy"},
        {"@level": "4", "@classId": "17100", "#text": "Electrical energy"},
    ),
    "transport_road": (
        {"@level": "0", "@classId": "6", "#text": "Distributive trade services; accommodation, food and beverage serving services; transport services; and electricity, gas and water distribution services"},
        {"@level": "1", "@classId": "65", "#text": "Freight transport services"},
        {"@level": "2", "@classId": "651", "#text": "Land transport services of freight"},
        {"@level": "3", "@classId": "6511", "#text": "Road transport services of freight"},
        {"@level": "4", "@classId": "65119", "#text": "Other road transport services of freight"},
    ),
    "transport_rail": (
        {"@level": "0", "@classId": "6", "#text": "Distributive trade services; accommodation, food and beverage serving services; transport services; and electricity, gas and water distribution services"},
        {"@level": "1", "@classId": "65", "#text": "Freight transport services"},
        {"@level": "2", "@classId": "651", "#text": "Land transport services of freight"},
        {"@level": "3", "@classId": "6512", "#text": "Railway transport services of freight"},
        {"@level": "4", "@classId": "65129", "#text": "Other railway transport services of freight"},
    ),
}


def collect_jsonld_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    files = sorted(p for p in path.rglob("*.json") if p.is_file())
    if not files:
        raise SystemExit(f"No JSON-LD files found under {path}")
    return files


def _as_language_entry(text: str | None, lang: str = "en") -> dict[str, str]:
    return {"@xml:lang": lang, "#text": (text or "").strip() or "Unnamed"}


def _parse_category_path(category: str | None) -> list[dict[str, str]]:
    if not category:
        return []
    entries: list[dict[str, str]] = []
    segments = [segment.strip() for segment in category.split("/") if segment.strip()]
    for index, segment in enumerate(segments):
        if ":" in segment:
            class_id, label = segment.split(":", 1)
        else:
            class_id, label = segment, segment
        entries.append({"@level": str(index), "@classId": class_id.strip(), "#text": label.strip()})
    return entries


def _process_classification_from_category(category: str | None) -> list[dict[str, str]]:
    entries = _parse_category_path(category)
    if entries:
        try:
            return ensure_valid_classification_path(tuple(entries))
        except ValueError:
            pass
    fallback = (
        {"@level": "0", "@classId": "C", "#text": "Manufacturing"},
        {"@level": "1", "@classId": "27", "#text": "Manufacture of electrical equipment"},
        {"@level": "2", "@classId": "272", "#text": "Manufacture of batteries and accumulators"},
        {"@level": "3", "@classId": "2720", "#text": "Manufacture of batteries and accumulators"},
    )
    return ensure_valid_classification_path(fallback)


def _default_location_code(name: str | None) -> str:
    if not name:
        return "GLO"
    lowered = name.lower()
    if "china" in lowered or lowered == "cn":
        return "CN"
    if "united states" in lowered or lowered == "usa":
        return "US"
    return "GLO"


def _current_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _contact_reference() -> dict[str, Any]:
    return {
        "@refObjectId": TIANGONG_CONTACT_UUID,
        "@type": "contact data set",
        "@uri": TIANGONG_CONTACT_URI,
        "@version": TIANGONG_CONTACT_VERSION,
        "common:shortDescription": [dict(entry) for entry in TIANGONG_CONTACT_SHORT_DESCRIPTION],
    }


def _format_reference() -> dict[str, Any]:
    return {
        "@refObjectId": ILCD_FORMAT_SOURCE_UUID,
        "@type": "source data set",
        "@uri": ILCD_FORMAT_SOURCE_URI,
        "@version": ILCD_FORMAT_SOURCE_VERSION,
        "common:shortDescription": _as_language_entry("ILCD format", "en"),
    }


def _ownership_reference() -> dict[str, Any]:
    return _contact_reference()


def _compliance_reference() -> dict[str, Any]:
    return {
        "@refObjectId": ILCD_COMPLIANCE_SOURCE_UUID,
        "@type": "source data set",
        "@uri": ILCD_COMPLIANCE_SOURCE_URI,
        "@version": ILCD_COMPLIANCE_SOURCE_VERSION,
        "common:shortDescription": {
            "@xml:lang": "en",
            "#text": "ILCD Data Network - Entry-level",
        },
    }


def _default_intended_applications() -> list[dict[str, str]]:
    return [
        _as_language_entry("Life cycle data prepared for Tiangong LCA Spec Coding workflow automation", "en")
    ]


def _flow_classification_from_category(category: str | None) -> list[dict[str, str]]:
    lowered = (category or "").lower()
    if "transport" in lowered or "freight" in lowered or "lorry" in lowered or "rail" in lowered:
        if "rail" in lowered or "train" in lowered:
            return [dict(entry) for entry in FLOW_CLASSIFICATION_PATHS["transport_rail"]]
        return [dict(entry) for entry in FLOW_CLASSIFICATION_PATHS["transport_road"]]
    if any(keyword in lowered for keyword in ("electricity", "electric", "power grid", "voltage")):
        return [dict(entry) for entry in FLOW_CLASSIFICATION_PATHS["electricity"]]
    if any(keyword in lowered for keyword in ("aluminium", "copper", "metal", "foil", "collector")):
        return [dict(entry) for entry in FLOW_CLASSIFICATION_PATHS["metal"]]
    if any(keyword in lowered for keyword in ("chemical", "oxide", "n-methyl", "pvdf", "binder", "paste", "solvent", "carbon", "nanotube")):
        return [dict(entry) for entry in FLOW_CLASSIFICATION_PATHS["chemicals"]]
    return [dict(entry) for entry in FLOW_CLASSIFICATION_PATHS["generic_goods"]]


def _source_classification_from_category(category: str | None) -> dict[str, str]:
    lowered = (category or "").lower()
    if "image" in lowered or "png" in lowered or "jpg" in lowered:
        class_id = "0"
        text = "Images"
    elif "format" in lowered:
        class_id = "1"
        text = "Data set formats"
    elif "database" in lowered:
        class_id = "2"
        text = "Databases"
    elif "compliance" in lowered:
        class_id = "3"
        text = "Compliance systems"
    elif "statistical" in lowered:
        class_id = "4"
        text = "Statistical classifications"
    elif "publication" in lowered or "communication" in lowered or not lowered:
        class_id = "5"
        text = "Publications and communications"
    else:
        class_id = "5"
        text = "Publications and communications"
    return {"@level": "0", "@classId": class_id, "#text": text}


def _derive_short_name(name: str) -> str:
    stripped = (name or "").strip()
    if not stripped:
        return "Source"
    return stripped[:120]


def _guess_publication_type(category: str | None) -> str:
    lowered = (category or "").lower()
    if "article" in lowered or "journal" in lowered or "publication" in lowered:
        return "Article in periodical"
    if "manual" in lowered or "monograph" in lowered:
        return "Monograph"
    if "image" in lowered:
        return "Other unpublished and grey literature"
    if "questionnaire" in lowered:
        return "Questionnaire"
    if "software" in lowered or "database" in lowered:
        return "Software or database"
    return "Article in periodical"


def _ensure_exchange_direction(is_input: bool | None) -> str:
    return "Input" if is_input else "Output"


def _reference_to_flow(flow: dict[str, Any]) -> dict[str, Any]:
    ref_uuid = flow.get("@id") or str(uuid4())
    description = flow.get("name") or "Unnamed flow"
    version = DEFAULT_DATA_SET_VERSION
    return {
        "@type": "flow data set",
        "@refObjectId": ref_uuid,
        "@uri": f"../flows/{ref_uuid}_{version}.xml",
        "@version": version,
        "common:shortDescription": _as_language_entry(description, "en"),
    }


def _reference_to_flow_property(flow_property: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(flow_property, dict):
        return None
    ref_uuid = flow_property.get("@id")
    if not ref_uuid:
        return None
    version = flow_property.get("version") or "01.01.000"
    return {
        "@type": "flow property data set",
        "@refObjectId": ref_uuid,
        "@uri": f"../flowproperties/{ref_uuid}_{version}.xml",
        "@version": version,
        "common:shortDescription": _as_language_entry(flow_property.get("name"), "en"),
    }


def _reference_to_unit_group(unit_group: dict[str, Any]) -> dict[str, Any]:
    ref_uuid = unit_group.get("@id") or str(uuid4())
    version = unit_group.get("version") or "01.01.000"
    return {
        "@type": "unit group data set",
        "@refObjectId": ref_uuid,
        "@uri": f"../unitgroups/{ref_uuid}_{version}.xml",
        "@version": version,
        "common:shortDescription": _as_language_entry(unit_group.get("name") or "Unit group", "en"),
    }


@dataclass(slots=True)
class JSONLDProcessConverter:
    jsonld_path: Path

    def load(self) -> dict[str, Any]:
        return json.loads(self.jsonld_path.read_text(encoding="utf-8"))

    def to_process_block(self) -> dict[str, Any]:
        payload = self.load()
        dataset_uuid = payload.get("@id") or str(uuid4())
        name = payload.get("name") or "Unnamed process"
        description = payload.get("description") or ""
        category = payload.get("category")
        classification = _process_classification_from_category(category)
        location_block = payload.get("location") or {}
        location_code = _default_location_code(location_block.get("name"))

        documentation = payload.get("processDocumentation") or {}
        reference_year = None
        valid_from = documentation.get("validFrom")
        if isinstance(valid_from, str) and valid_from[:4].isdigit():
            reference_year = int(valid_from[:4])

        exchanges_payload = payload.get("exchanges") or []
        exchanges: list[dict[str, Any]] = []
        reference_flow_id: str | None = None
        for idx, exchange in enumerate(exchanges_payload, start=1):
            flow = exchange.get("flow") or {}
            unit = exchange.get("unit") or {}
            amount = exchange.get("amount")
            data_entry: dict[str, Any] = {
                "@dataSetInternalID": str(idx),
                "exchangeDirection": _ensure_exchange_direction(exchange.get("isInput")),
                "meanAmount": str(amount) if amount is not None else "0",
                "unit": unit.get("name") or unit.get("@id") or "",
                "exchangeName": flow.get("name") or "Unnamed flow",
            }
            data_entry["referenceToFlowDataSet"] = _reference_to_flow(flow)
            flow_property = exchange.get("flowProperty") or {}
            property_ref = _reference_to_flow_property(flow_property)
            if property_ref:
                data_entry["referenceToFlowPropertyDataSet"] = property_ref
            if exchange.get("isQuantitativeReference") and reference_flow_id is None:
                reference_flow_id = str(idx)
            exchanges.append(data_entry)

        if not reference_flow_id and exchanges:
            reference_flow_id = exchanges[0]["@dataSetInternalID"]

        process_dataset = {
            **ILCD_PROCESS_XMLNS,
            "processInformation": {
                "dataSetInformation": {
                    "common:UUID": dataset_uuid,
                    "name": {
                        "baseName": _as_language_entry(name, "en"),
                        "treatmentStandardsRoutes": _as_language_entry(name, "en"),
                        "mixAndLocationTypes": _as_language_entry("Production mix", "en"),
                        "functionalUnitFlowProperties": _as_language_entry("Functional unit based on reference flow", "en"),
                    },
                    "identifierOfSubDataSet": "JSONLD",
                    "common:generalComment": _as_language_entry(description or "Converted from OpenLCA JSON-LD.", "en"),
                    "classificationInformation": {
                        "common:classification": {
                            "common:class": classification or [
                                {"@level": "0", "@classId": "Z", "#text": "Unspecified"}
                            ]
                        }
                    },
                },
                "quantitativeReference": {
                    "referenceToReferenceFlow": reference_flow_id or "1",
                    "functionalUnitOrOther": _as_language_entry("Reference flow of process", "en"),
                },
                "time": {
                    "common:referenceYear": reference_year or 2020,
                },
                "geography": {
                    "locationOfOperationSupplyOrProduction": {"@location": location_code},
                },
            },
            "modellingAndValidation": {
                "LCIMethodAndAllocation": {
                    "typeOfDataSet": "Unit process, single operation",
                    "LCIMethodPrinciple": "Attributional",
                }
            },
            "administrativeInformation": {
                "dataEntryBy": {
                    "common:referenceToDataSetFormat": _format_reference(),
                },
                "publicationAndOwnership": {
                    "common:dataSetVersion": payload.get("version") or "01.01.000",
                    "common:permanentDataSetURI": f"https://lcdn.tiangong.earth/showProcess.xhtml?uuid={dataset_uuid}&version=01.01.000",
                },
                "common:commissionerAndGoal": {
                    "common:referenceToCommissioner": _contact_reference(),
                    "common:intendedApplications": _default_intended_applications(),
                },
            },
            "exchanges": {
                "exchange": exchanges,
            },
        }

        return {
            "processDataSet": process_dataset,
            "process_id": dataset_uuid,
        }


@dataclass(slots=True)
class JSONLDFlowConverter:
    jsonld_path: Path

    def load(self) -> dict[str, Any]:
        return json.loads(self.jsonld_path.read_text(encoding="utf-8"))

    def to_flow_dataset(self) -> dict[str, Any]:
        payload = self.load()
        flow_uuid = payload.get("@id") or str(uuid4())
        name = payload.get("name") or "Unnamed flow"
        dataset_version = DEFAULT_DATA_SET_VERSION
        classification = _flow_classification_from_category(payload.get("category"))
        description = payload.get("description") or "Converted from OpenLCA JSON-LD."
        name_block = {
            "baseName": _as_language_entry(name, "en"),
            "treatmentStandardsRoutes": _as_language_entry("Standard treatment not specified", "en"),
            "mixAndLocationTypes": _as_language_entry("Production mix, at plant", "en"),
            "functionalUnitFlowProperties": _as_language_entry("Declared per reference flow property", "en"),
        }
        flow_properties_payload = payload.get("flowProperties") or []
        flow_properties: list[dict[str, Any]] = []
        for idx, factor in enumerate(flow_properties_payload, start=1):
            reference = _reference_to_flow_property(factor.get("flowProperty") or {})
            if reference is None:
                continue
            flow_properties.append(
                {
                    "@dataSetInternalID": str(idx),
                    "meanValue": str(factor.get("conversionFactor", 1)),
                    "referenceToFlowPropertyDataSet": reference,
                }
            )
        if not flow_properties:
            flow_properties.append(
                {
                    "@dataSetInternalID": "1",
                    "meanValue": "1",
                    "referenceToFlowPropertyDataSet": {
                        "@type": "flow property data set",
                        "@refObjectId": "93a60a56-a3c8-11da-a746-0800200b9a66",
                        "@uri": "../flowproperties/93a60a56-a3c8-11da-a746-0800200b9a66_03.00.003.xml",
                        "@version": "03.00.003",
                        "common:shortDescription": _as_language_entry("Mass", "en"),
                    },
                }
            )
        reference_flow_property_id = flow_properties[0]["@dataSetInternalID"]
        location_code = _default_location_code((payload.get("location") or {}).get("name"))

        flow_dataset = {
            "flowDataSet": {
                **ILCD_FLOW_XMLNS,
                "flowInformation": {
                    "dataSetInformation": {
                        "common:UUID": flow_uuid,
                        "name": name_block,
                        "common:synonyms": [_as_language_entry(name, "en")],
                        "common:generalComment": [_as_language_entry(description, "en")],
                        "classificationInformation": {
                            "common:classification": {
                                "common:class": classification
                            }
                        },
                    },
                    "quantitativeReference": {
                        "referenceToReferenceFlowProperty": reference_flow_property_id,
                    },
                    "geography": {
                        "locationOfSupply": location_code,
                    },
                    "technology": {
                        "technologicalApplicability": [_as_language_entry("Applicable to generic supply mixes.", "en")],
                    },
                },
                "flowProperties": {
                    "flowProperty": flow_properties,
                },
                "modellingAndValidation": {
                    "LCIMethod": {
                        "typeOfDataSet": "Product flow",
                    },
                    "complianceDeclarations": {
                        "compliance": {
                            "common:referenceToComplianceSystem": _compliance_reference(),
                            "common:approvalOfOverallCompliance": "Fully compliant",
                            "common:nomenclatureCompliance": "Fully compliant",
                            "common:methodologicalCompliance": "Not defined",
                            "common:reviewCompliance": "Not defined",
                            "common:documentationCompliance": "Not defined",
                            "common:qualityCompliance": "Not defined",
                        }
                    }
                },
                "administrativeInformation": {
                    "dataEntryBy": {
                        "common:referenceToDataSetFormat": _format_reference(),
                        "common:referenceToPersonOrEntityEnteringTheData": _contact_reference(),
                        "common:timeStamp": _current_timestamp(),
                    },
                    "publicationAndOwnership": {
                        "common:dataSetVersion": dataset_version,
                        "common:permanentDataSetURI": f"https://lcdn.tiangong.earth/showFlow.xhtml?uuid={flow_uuid}&version={dataset_version}",
                        "common:licenseType": DEFAULT_LICENSE,
                        "common:copyright": "false",
                        "common:referenceToOwnershipOfDataSet": _ownership_reference(),
                    },
                },
            }
        }
        return flow_dataset


@dataclass(slots=True)
class JSONLDFlowPropertyConverter:
    jsonld_path: Path

    def load(self) -> dict[str, Any]:
        return json.loads(self.jsonld_path.read_text(encoding="utf-8"))

    def to_flow_property_dataset(self) -> dict[str, Any]:
        payload = self.load()
        property_uuid = payload.get("@id") or str(uuid4())
        name = payload.get("name") or "Flow property"
        category = payload.get("category")
        classification = _parse_category_path(category)
        unit_group = payload.get("unitGroup") or {}
        dataset = {
            "flowPropertyDataSet": {
                **ILCD_FLOW_PROPERTY_XMLNS,
                "flowPropertiesInformation": {
                    "dataSetInformation": {
                        "common:UUID": property_uuid,
                        "common:name": _as_language_entry(name, "en"),
                        "common:synonyms": [_as_language_entry(name, "en")],
                        "classificationInformation": {
                            "common:classification": {
                                "common:class": classification or [
                                    {"@level": "0", "@classId": "Z", "#text": "Unspecified"}
                                ]
                            }
                        },
                    },
                    "quantitativeReference": {
                        "referenceToReferenceUnitGroup": _reference_to_unit_group(unit_group),
                    },
                },
                "administrativeInformation": {
                    "dataEntryBy": {
                        "common:referenceToDataSetFormat": {
                            "@refObjectId": "a97a0155-0234-4b87-b4ce-a45da52f2a40",
                            "@type": "source data set",
                            "@uri": "../sources/a97a0155-0234-4b87-b4ce-a45da52f2a40_03.00.003.xml",
                            "@version": "03.00.003",
                            "common:shortDescription": _as_language_entry("ILCD format", "en"),
                        }
                    },
                    "publicationAndOwnership": {
                        "common:dataSetVersion": payload.get("version") or "01.01.000",
                    },
                },
            }
        }
        return dataset


@dataclass(slots=True)
class JSONLDUnitGroupConverter:
    jsonld_path: Path

    def load(self) -> dict[str, Any]:
        return json.loads(self.jsonld_path.read_text(encoding="utf-8"))

    def to_unit_group_dataset(self) -> dict[str, Any]:
        payload = self.load()
        group_uuid = payload.get("@id") or str(uuid4())
        name = payload.get("name") or "Unit group"
        category = payload.get("category")
        classification = _parse_category_path(category)
        units_payload = payload.get("units") or []
        units: list[dict[str, Any]] = []
        for idx, unit in enumerate(units_payload):
            units.append(
                {
                    "@dataSetInternalID": str(idx),
                    "name": unit.get("name") or unit.get("@id") or f"unit_{idx}",
                    "meanValue": str(unit.get("conversionFactor", 1)),
                }
            )
        if not units:
            units.append({"@dataSetInternalID": "0", "name": "1", "meanValue": "1"})

        dataset = {
            "unitGroupDataSet": {
                **ILCD_UNIT_GROUP_XMLNS,
                "unitGroupInformation": {
                    "dataSetInformation": {
                        "common:UUID": group_uuid,
                        "common:name": _as_language_entry(name, "en"),
                        "classificationInformation": {
                            "common:classification": {
                                "common:class": classification or [
                                    {"@level": "0", "@classId": "Z", "#text": "Unspecified"}
                                ]
                            }
                        },
                    },
                    "quantitativeReference": {
                        "referenceToReferenceUnit": "0",
                    },
                },
                "administrativeInformation": {
                    "dataEntryBy": {
                        "common:referenceToDataSetFormat": {
                            "@refObjectId": "a97a0155-0234-4b87-b4ce-a45da52f2a40",
                            "@type": "source data set",
                            "@uri": "../sources/a97a0155-0234-4b87-b4ce-a45da52f2a40_03.00.003.xml",
                            "@version": "03.00.003",
                            "common:shortDescription": _as_language_entry("ILCD format", "en"),
                        }
                    },
                    "publicationAndOwnership": {
                        "common:dataSetVersion": payload.get("version") or "01.01.000",
                    },
                },
                "units": {"unit": units},
            }
        }
        return dataset


@dataclass(slots=True)
class JSONLDSourceConverter:
    jsonld_path: Path

    def load(self) -> dict[str, Any]:
        return json.loads(self.jsonld_path.read_text(encoding="utf-8"))

    def to_source_dataset(self) -> dict[str, Any]:
        payload = self.load()
        source_uuid = payload.get("@id") or str(uuid4())
        name = payload.get("name") or "Source"
        category = payload.get("category")
        classification = _source_classification_from_category(category)
        dataset_version = payload.get("version") or "01.01.000"
        short_name = _derive_short_name(name)
        description = payload.get("textReference") or payload.get("description") or "Converted from OpenLCA JSON-LD."
        publication_type = _guess_publication_type(category)
        citation = payload.get("name") or short_name
        dataset = {
            "sourceDataSet": {
                **ILCD_SOURCE_XMLNS,
                "sourceInformation": {
                    "dataSetInformation": {
                        "common:UUID": source_uuid,
                        "common:shortName": _as_language_entry(short_name, "en"),
                        "classificationInformation": {
                            "common:classification": {
                                "common:class": classification
                            }
                        },
                        "sourceCitation": citation,
                        "publicationType": publication_type,
                        "sourceDescriptionOrComment": [_as_language_entry(description, "en")],
                        "referenceToContact": _contact_reference(),
                    }
                },
                "administrativeInformation": {
                    "dataEntryBy": {
                        "common:timeStamp": _current_timestamp(),
                        "common:referenceToDataSetFormat": _format_reference(),
                    },
                    "publicationAndOwnership": {
                        "common:dataSetVersion": dataset_version,
                        "common:permanentDataSetURI": f"https://lcdn.tiangong.earth/showSource.xhtml?uuid={source_uuid}&version={dataset_version}",
                        "common:referenceToOwnershipOfDataSet": _ownership_reference(),
                    },
                },
            }
        }
        return dataset


def convert_process_directory(path: Path) -> list[dict[str, Any]]:
    files = collect_jsonld_files(path)
    return [JSONLDProcessConverter(file_path).to_process_block() for file_path in files]


def convert_flow_directory(path: Path) -> list[dict[str, Any]]:
    files = collect_jsonld_files(path)
    return [JSONLDFlowConverter(file_path).to_flow_dataset() for file_path in files]


def convert_flow_property_directory(path: Path) -> list[dict[str, Any]]:
    files = collect_jsonld_files(path)
    return [JSONLDFlowPropertyConverter(file_path).to_flow_property_dataset() for file_path in files]


def convert_unit_group_directory(path: Path) -> list[dict[str, Any]]:
    files = collect_jsonld_files(path)
    return [JSONLDUnitGroupConverter(file_path).to_unit_group_dataset() for file_path in files]


def convert_source_directory(path: Path) -> list[dict[str, Any]]:
    files = collect_jsonld_files(path)
    return [JSONLDSourceConverter(file_path).to_source_dataset() for file_path in files]
