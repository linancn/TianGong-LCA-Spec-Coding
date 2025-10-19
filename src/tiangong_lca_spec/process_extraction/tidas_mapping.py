"""Utilities for normalising process datasets to the TIDAS ILCD schema."""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any
from uuid import uuid4

BASE_METADATA = {
    "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
    "@xmlns": "http://lca.jrc.it/ILCD/Process",
    "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
    "@version": "1.1",
    "@locations": "../ILCDLocations.xml",
    "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Process ../../schemas/ILCD_ProcessDataSet.xsd",
}

DEFAULT_LOCATION = "GLO"
DEFAULT_REFERENCE_TYPE = "Reference flow(s)"
DEFAULT_REFERENCE_ID = "0"
DEFAULT_LANGUAGE = "en"


def build_tidas_process_dataset(
    process_dataset: dict[str, Any],
    *,
    notes: Any | None = None,
) -> dict[str, Any]:
    """Return a normalised deep copy of the provided process dataset."""

    dataset = _apply_root_metadata(process_dataset)
    dataset["@locations"] = BASE_METADATA["@locations"]
    process_information = dataset.setdefault("processInformation", {})
    dataset["processInformation"] = _normalise_process_information(process_information, notes=notes)
    dataset_uuid = dataset["processInformation"].get("dataSetInformation", {}).get("common:UUID")
    dataset["modellingAndValidation"] = _normalise_modelling_and_validation(
        dataset.get("modellingAndValidation")
    )
    dataset["administrativeInformation"] = _normalise_administrative_information(
        dataset.get("administrativeInformation"),
        dataset_uuid=dataset_uuid,
    )
    dataset["exchanges"] = _normalise_exchanges(dataset.get("exchanges"))
    dataset["LCIAResults"] = _normalise_lcia_results(dataset.get("LCIAResults"))
    return dataset


def _apply_root_metadata(process_dataset: dict[str, Any]) -> dict[str, Any]:
    dataset = deepcopy(process_dataset if isinstance(process_dataset, dict) else {})
    for key, value in BASE_METADATA.items():
        dataset.setdefault(key, value)
    return dataset


def _normalise_process_information(
    process_information: Any,
    *,
    notes: Any | None,
) -> dict[str, Any]:
    info = _ensure_dict(process_information)
    info["dataSetInformation"] = _normalise_dataset_information(
        info.get("dataSetInformation"),
        notes=notes,
    )
    info["quantitativeReference"] = _normalise_quantitative_reference(
        info.get("quantitativeReference")
    )
    info["time"] = _normalise_time(info.get("time"))
    info["geography"] = _normalise_geography(info.get("geography"))
    info["technology"] = _normalise_technology(info.get("technology"))
    if "technology" in info and not info["technology"]:
        info.pop("technology")
    if "mathematicalRelations" in info and not info["mathematicalRelations"]:
        info.pop("mathematicalRelations")
    return info


def _normalise_dataset_information(data_info: Any, *, notes: Any | None) -> dict[str, Any]:
    info = _ensure_dict(data_info)

    uuid_value = info.get("common:UUID")
    if not isinstance(uuid_value, str) or not _is_valid_uuid(uuid_value):
        uuid_value = str(uuid4())
    info["common:UUID"] = uuid_value

    identifier = _stringify(info.get("identifierOfSubDataSet")).strip()
    if not identifier:
        identifier = uuid_value
    info["identifierOfSubDataSet"] = identifier

    name_block = info.get("name")
    if isinstance(name_block, str):
        name_block = {"baseName": name_block}
    name_block = _ensure_dict(name_block)
    name_block["baseName"] = _ensure_multilang(
        name_block.get("baseName"),
        fallback="Unnamed process",
    )
    name_block["treatmentStandardsRoutes"] = _ensure_multilang(
        name_block.get("treatmentStandardsRoutes"), fallback="Not specified"
    )
    name_block["mixAndLocationTypes"] = _ensure_multilang(
        name_block.get("mixAndLocationTypes"), fallback="Unknown mix"
    )
    if "functionalUnitFlowProperties" in name_block:
        name_block["functionalUnitFlowProperties"] = _ensure_multilang(
            name_block.get("functionalUnitFlowProperties")
        )
    info["name"] = name_block

    classification_info = _ensure_dict(info.get("classificationInformation"))
    classification = classification_info.get("classification")
    common_class = classification_info.get("common:classification")
    if isinstance(classification, list):
        classification_info["common:classification"] = {"common:class": classification}
    elif isinstance(common_class, dict) and "common:class" in common_class:
        pass
    else:
        classification_info.setdefault("common:classification", {"common:class": []})

    current_classes = classification_info.get("common:classification", {}).get("common:class")
    if not current_classes:
        candidate: Any = classification
        if candidate is None:
            candidate = info.get("classification")
        if candidate is None:
            raw_class = info.get("classificationInformation")
            if isinstance(raw_class, dict):
                candidate = raw_class.get("classification") or raw_class.get("common:class")
        if isinstance(candidate, list) and candidate:
            classification_info["common:classification"] = {"common:class": candidate}

    classes = classification_info.get("common:classification", {}).get("common:class")
    if isinstance(classes, list) and len(classes) > 4:
        classification_info["common:classification"]["common:class"] = classes[:4]

    info["classificationInformation"] = classification_info

    general_comment = info.get("common:generalComment") or ""
    note_text = _stringify(notes)
    if note_text:
        general_comment = (
            f"{general_comment}\n{note_text}".strip() if general_comment else note_text
        )
    info["common:generalComment"] = general_comment

    info.pop("referenceToExternalDocumentation", None)

    mathematical_relations = info.get("common:mathematicalRelationsOrReasonsForDataDerivation")
    if not isinstance(mathematical_relations, dict):
        info["common:mathematicalRelationsOrReasonsForDataDerivation"] = {
            "common:other": "Not specified"
        }

    scope = _ensure_dict(info.get("scope"))
    scope.setdefault("defaultAllocationMethod", "Not specified")
    info["scope"] = scope

    owner = _ensure_dict(info.get("dataSetOwner"))
    owner.setdefault("nameOfDataSetOwner", _ensure_multilang("Unknown owner"))
    info["dataSetOwner"] = owner
    return info


def _normalise_quantitative_reference(section: Any) -> dict[str, Any]:
    ref = _ensure_dict(section)
    ref["@type"] = DEFAULT_REFERENCE_TYPE
    reference_id = ref.get("referenceToReferenceFlow") or DEFAULT_REFERENCE_ID
    try:
        numeric_id = int(str(reference_id))
    except (ValueError, TypeError):
        numeric_id = int(DEFAULT_REFERENCE_ID)
    ref["referenceToReferenceFlow"] = str(numeric_id)
    functional_unit = ref.get("functionalUnitOrOther")
    if functional_unit:
        ref["functionalUnitOrOther"] = _to_multilang(functional_unit)
    return ref


def _normalise_time(section: Any) -> dict[str, Any]:
    time_info = _ensure_dict(section)
    reference_year = time_info.get("referenceYear") or time_info.get("common:referenceYear")
    year_value: int | None = None
    if isinstance(reference_year, (int, float)):
        year_value = int(reference_year)
    elif isinstance(reference_year, str) and reference_year.isdigit():
        year_value = int(reference_year)

    if year_value is None:
        year_value = 1900

    time_info["common:referenceYear"] = year_value
    time_info.pop("referenceYear", None)

    valid_until = time_info.get("common:dataSetValidUntil")
    try:
        valid_until_value = int(valid_until)
    except (TypeError, ValueError):
        valid_until_value = year_value + 5 if year_value is not None else 1905
    time_info["common:dataSetValidUntil"] = valid_until_value
    return time_info


def _normalise_geography(section: Any) -> dict[str, Any]:
    geo = _ensure_dict(section)
    block = _ensure_dict(geo.get("locationOfOperationSupplyOrProduction"))
    raw_code = (
        block.pop("@location", None)
        or block.pop("location", None)
        or geo.pop("code", None)
        or geo.pop("@location", None)
        or DEFAULT_LOCATION
    )
    code, code_description = _extract_location_code(raw_code)
    description = (
        block.pop("description", None)
        or geo.pop("description", None)
        or block.pop("locationName", None)
        or code_description
    )
    sub_location = block.pop("subLocation", None) or geo.pop("subLocation", None)

    normalised_block: dict[str, Any] = {"@location": code}
    if description:
        normalised_block["common:other"] = description
    if sub_location:
        normalised_block.setdefault("common:comment", sub_location)

    return {"locationOfOperationSupplyOrProduction": normalised_block}


def _normalise_technology(section: Any) -> dict[str, Any]:
    technology = _ensure_dict(section)
    description = technology.get("technologyDescriptionAndIncludedProcesses")
    technology["technologyDescriptionAndIncludedProcesses"] = _ensure_multilang(
        description, fallback="Not specified"
    )
    if technology.get("technologicalApplicability"):
        technology["technologicalApplicability"] = _ensure_multilang(
            technology.get("technologicalApplicability")
        )
    _ensure_reference_field(
        technology,
        "referenceToIncludedProcesses",
        ref_type="process",
        description="Included process placeholder",
    )
    _ensure_reference_field(
        technology,
        "referenceToTechnologyPictogramme",
        ref_type="source",
        description="Technology pictogram placeholder",
    )
    _ensure_reference_field(
        technology,
        "referenceToTechnologyFlowDiagrammOrPicture",
        ref_type="source",
        description="Technology flow diagram placeholder",
    )
    technology.setdefault("common:other", "Generated placeholder")
    return technology


def _normalise_exchanges(section: Any) -> dict[str, Any]:
    exchange_container = _ensure_dict(section)
    raw_exchanges = exchange_container.get("exchange")
    if isinstance(raw_exchanges, dict):
        exchanges_iter = [raw_exchanges]
    elif isinstance(raw_exchanges, list):
        exchanges_iter = raw_exchanges
    else:
        exchanges_iter = []

    normalised: list[dict[str, Any]] = []
    for index, exchange in enumerate(exchanges_iter, start=1):
        item = _ensure_dict(exchange)
        item["@dataSetInternalID"] = str(index)
        item.pop("functionType", None)
        if not item.get("exchangeName"):
            name_candidate = _extract_flow_name(item.get("referenceToFlowDataSet"))
            if not name_candidate:
                name_candidate = _stringify(item.get("name"))
            if not name_candidate:
                name_candidate = _stringify(item.get("generalComment")).split(":")[0]
            if not name_candidate:
                name_candidate = f"exchange_{index:06d}"
            item["exchangeName"] = name_candidate
        if "name" in item and isinstance(item["name"], dict) and not item["name"].get("#text"):
            item.pop("name")
        direction = _stringify(item.get("exchangeDirection")).lower()
        item["exchangeDirection"] = "Input" if direction != "output" else "Output"
        item.setdefault("meanAmount", "0")
        item.setdefault("resultingAmount", "0")
        item["dataDerivationTypeStatus"] = _normalise_derivation_status(
            item.get("dataDerivationTypeStatus")
        )
        normalised.append(item)

    return {"exchange": normalised}


def _normalise_lcia_results(section: Any) -> dict[str, Any]:
    results = _ensure_dict(section)
    lcia_result = _ensure_dict(results.get("LCIAResult"))
    mean_amount = lcia_result.get("meanAmount")
    try:
        mean_value = float(mean_amount)
    except (TypeError, ValueError):
        mean_value = 0.0
    lcia_result["meanAmount"] = f"{mean_value}"
    _ensure_reference_field(
        lcia_result,
        "referenceToLCIAMethodDataSet",
        ref_type="method",
        description="Placeholder LCIA method",
    )
    if lcia_result.get("generalComment"):
        lcia_result["generalComment"] = _ensure_multilang(lcia_result.get("generalComment"))
    results["LCIAResult"] = lcia_result
    results.setdefault("common:other", "Generated placeholder")
    return results


def _normalise_modelling_and_validation(section: Any) -> dict[str, Any]:
    mv_raw = _ensure_dict(section)
    mv: dict[str, Any] = {}

    mv["LCIMethodAndAllocation"] = {
        "typeOfDataSet": "Unit process, black box",
        "LCIMethodPrinciple": "Attributional",
        "LCIMethodApproaches": "Allocation - physical causality",
    }

    mv["dataSourcesTreatmentAndRepresentativeness"] = {
        "percentageSupplyOrProductionCovered": "0.95",
        "referenceToDataSource": [
            _ensure_global_reference(
                mv_raw.get("dataSourcesTreatmentAndRepresentativeness", {}).get(
                    "referenceToDataSource"
                ),
                ref_type="source",
                description="Not specified",
            )
        ],
    }

    mv["completeness"] = {
        "completenessProductModel": "No statement",
    }

    mv["validation"] = {
        "review": {
            "@type": "Not reviewed",
            "scope": {
                "@name": "Goal and scope definition",
                "method": {"@name": "Documentation"},
            },
        }
    }

    mv["complianceDeclarations"] = {
        "compliance": {
            "common:referenceToComplianceSystem": _ensure_global_reference(
                mv_raw.get("complianceDeclarations", {})
                .get("compliance", {})
                .get("common:referenceToComplianceSystem"),
                ref_type="source",
                description="Compliance system",
            ),
            "common:approvalOfOverallCompliance": "Not defined",
            "common:nomenclatureCompliance": "Not defined",
            "common:methodologicalCompliance": "Not defined",
            "common:reviewCompliance": "Not defined",
            "common:documentationCompliance": "Not defined",
            "common:qualityCompliance": "Not defined",
        }
    }

    other = _ensure_dict(mv_raw).get("common:other")
    if other:
        mv["common:other"] = other
    return mv


def _normalise_administrative_information(
    section: Any,
    *,
    dataset_uuid: str | None,
) -> dict[str, Any]:
    admin = _ensure_dict(section)
    admin["dataGenerator"] = {
        "common:other": "Generated via Tiangong LCA automated workflow",
    }

    admin["common:commissionerAndGoal"] = {
        "common:intendedApplications": ["Life cycle assessment study"],
        "common:referenceToCommissioner": [
            _ensure_global_reference(
                admin.get("common:commissionerAndGoal", {}).get("common:referenceToCommissioner"),
                ref_type="contact",
                description="Commissioning organisation",
            )
        ],
    }

    admin["dataEntryBy"] = {
        "common:timeStamp": "2024-01-01T00:00:00Z",
        "common:referenceToPersonOrEntityEnteringTheData": _ensure_global_reference(
            admin.get("dataEntryBy", {}).get("common:referenceToPersonOrEntityEnteringTheData"),
            ref_type="contact",
            description="Data entry",
        ),
        "common:referenceToDataSetFormat": _ensure_global_reference(
            admin.get("dataEntryBy", {}).get("common:referenceToDataSetFormat"),
            ref_type="documentation",
            description="ILCD 1.1",
        ),
    }

    publication = _ensure_dict(admin.get("publicationAndOwnership"))
    version_value = _stringify(publication.get("common:dataSetVersion")).strip() or "01.00.000"
    publication["common:dataSetVersion"] = version_value
    uri_candidate = _stringify(publication.get("common:permanentDataSetURI")).strip()
    if not uri_candidate or not uri_candidate.startswith(("http://", "https://")):
        publication["common:permanentDataSetURI"] = (
            f"https://tiangong.earth/process/{dataset_uuid}"
            if dataset_uuid
            else "https://tiangong.earth/process/unspecified"
        )
    publication["common:referenceToOwnershipOfDataSet"] = _ensure_global_reference(
        publication.get("common:referenceToOwnershipOfDataSet"),
        ref_type="contact",
        description="Unknown owner",
    )
    publication["common:copyright"] = _normalise_boolean(publication.get("common:copyright"))
    publication["common:licenseType"] = _normalise_license(publication.get("common:licenseType"))
    publication.setdefault("common:accessRestrictions", "Public")
    if not isinstance(publication.get("registrationAuthority"), dict):
        publication["registrationAuthority"] = {
            "name": _ensure_multilang("Tiangong LCA Registry"),
        }
    publication["common:workflowAndPublicationStatus"] = (
        _stringify(publication.get("common:workflowAndPublicationStatus")) or "Working draft"
    )
    _ensure_reference_field(
        publication,
        "common:referenceToUnchangedRepublication",
        ref_type="source",
        description="Original publication",
    )
    _ensure_reference_field(
        publication,
        "common:referenceToRegistrationAuthority",
        ref_type="contact",
        description="Registration authority",
    )
    registration_number = _stringify(publication.get("common:registrationNumber")).strip()
    if not registration_number:
        registration_number = dataset_uuid or "REG-UNSPECIFIED"
    publication["common:registrationNumber"] = registration_number
    admin["publicationAndOwnership"] = publication

    return admin


def _build_reference(ref_type: str, description: str) -> dict[str, Any]:
    identifier = str(uuid4())
    return {
        "@type": ref_type,
        "@refObjectId": identifier,
        "@uri": f"https://tiangong.earth/{ref_type}/{identifier}",
        "@version": "01.00.000",
        "common:shortDescription": _ensure_multilang(description),
    }


def _ensure_global_reference(
    value: Any,
    *,
    ref_type: str,
    description: str,
) -> dict[str, Any]:
    if isinstance(value, dict) and "@refObjectId" in value:
        return value
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict) and "@refObjectId" in item:
                return item
    return _build_reference(ref_type, description)


def _ensure_reference_field(
    container: dict[str, Any],
    key: str,
    *,
    ref_type: str,
    description: str,
) -> None:
    value = container.get(key)
    if _has_reference(value):
        return
    container[key] = _ensure_global_reference(value, ref_type=ref_type, description=description)


def _normalise_derivation_status(value: Any) -> str:
    mapping = {
        "measured": "Measured",
        "measured/calculated": "Calculated",
        "calculated": "Calculated",
        "estimated": "Estimated",
        "unknown": "Unknown derivation",
        "unknown derivation": "Unknown derivation",
        "missing important": "Missing important",
        "missing unimportant": "Missing unimportant",
    }
    text = _stringify(value).strip().lower()
    return mapping.get(text, "Unknown derivation")


def _normalise_license(value: Any) -> str:
    allowed = {
        "free of charge for all users and uses": "Free of charge for all users and uses",
        "free of charge for some user types or use types": (
            "Free of charge for some user types or use types"
        ),
        "free of charge for members only": "Free of charge for members only",
        "license fee": "License fee",
        "other": "Other",
    }
    text = _stringify(value).strip().lower()
    return allowed.get(text, "Other")


def _normalise_boolean(value: Any) -> str:
    text = _stringify(value).strip().lower()
    if text in {"true", "yes", "1"}:
        return "true"
    return "false"


def _select_from_enum(value: Any, allowed: list[str], default: str) -> str:
    text = _stringify(value).strip().lower()
    for candidate in allowed:
        if text == candidate.lower():
            return candidate
    return default


def _is_valid_uuid(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            value,
        )
    )


def _ensure_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _to_multilang(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        if "@xml:lang" not in value:
            value.setdefault("@xml:lang", DEFAULT_LANGUAGE)
        return value
    if isinstance(value, list):
        return {
            "@xml:lang": DEFAULT_LANGUAGE,
            "#text": "; ".join(_stringify(item) for item in value),
        }
    return {"@xml:lang": DEFAULT_LANGUAGE, "#text": _stringify(value)}


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _ensure_multilang(value: Any, *, fallback: str | None = None) -> dict[str, Any]:
    if isinstance(value, dict) and "@xml:lang" in value and "#text" in value:
        return value
    text = _stringify(value)
    if not text and fallback is not None:
        text = fallback
    if not text:
        text = "Not specified"
    return {"@xml:lang": DEFAULT_LANGUAGE, "#text": text}


def _extract_flow_name(reference: Any) -> str | None:
    if isinstance(reference, dict):
        if isinstance(reference.get("common:shortDescription"), dict):
            text = reference["common:shortDescription"].get("#text")
            if text:
                return text
        name = reference.get("name") or reference.get("baseName")
        if isinstance(name, dict):
            return name.get("#text") or name.get("text")
        if isinstance(name, str):
            return name
        for value in reference.values():
            if isinstance(value, str):
                return value
    return None


def _has_reference(value: Any) -> bool:
    if isinstance(value, dict):
        return "@refObjectId" in value
    if isinstance(value, list):
        return any(isinstance(item, dict) and "@refObjectId" in item for item in value)
    return False


def _extract_location_code(value: Any) -> tuple[str, str | None]:
    if isinstance(value, str):
        text = value.strip()
        return (text or DEFAULT_LOCATION, None)
    if isinstance(value, dict):
        fallback_description = (
            _stringify(value.get("description") or value.get("name") or value.get("common:other"))
            or None
        )
        for key in ("code", "@location", "location", "country", "region"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip(), fallback_description
        return DEFAULT_LOCATION, fallback_description
    if isinstance(value, (list, tuple)):
        for item in value:
            code, desc = _extract_location_code(item)
            if code:
                return code, desc
    return DEFAULT_LOCATION, None
