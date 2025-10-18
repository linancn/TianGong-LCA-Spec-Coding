"""Utilities for normalising process datasets to the TIDAS ILCD schema."""

from __future__ import annotations

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
    process_information = dataset.setdefault("processInformation", {})
    dataset["processInformation"] = _normalise_process_information(process_information, notes=notes)
    dataset["modellingAndValidation"] = _ensure_dict(dataset.get("modellingAndValidation"))
    dataset["administrativeInformation"] = _ensure_dict(dataset.get("administrativeInformation"))
    dataset["exchanges"] = _normalise_exchanges(dataset.get("exchanges"))
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
    info["dataSetInformation"] = _normalise_dataset_information(info.get("dataSetInformation"), notes=notes)
    info["quantitativeReference"] = _normalise_quantitative_reference(info.get("quantitativeReference"))
    info["time"] = _normalise_time(info.get("time"))
    info["geography"] = _normalise_geography(info.get("geography"))
    if "technology" in info and not info["technology"]:
        info.pop("technology")
    if "mathematicalRelations" in info and not info["mathematicalRelations"]:
        info.pop("mathematicalRelations")
    return info


def _normalise_dataset_information(data_info: Any, *, notes: Any | None) -> dict[str, Any]:
    info = _ensure_dict(data_info)

    uuid_value = info.get("common:UUID") or str(uuid4())
    info["common:UUID"] = uuid_value

    name_block = info.get("name")
    if isinstance(name_block, str):
        name_block = {"baseName": name_block}
    name_block = _ensure_dict(name_block)
    name_block["baseName"] = _ensure_multilang(name_block.get("baseName"), fallback="Unnamed process")
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

    info["classificationInformation"] = classification_info

    general_comment = info.get("common:generalComment") or ""
    note_text = _stringify(notes)
    if note_text:
        general_comment = f"{general_comment}\n{note_text}".strip() if general_comment else note_text
    info["common:generalComment"] = general_comment
    return info


def _normalise_quantitative_reference(section: Any) -> dict[str, Any]:
    ref = _ensure_dict(section)
    ref.setdefault("@type", DEFAULT_REFERENCE_TYPE)
    reference_id = ref.get("referenceToReferenceFlow") or DEFAULT_REFERENCE_ID
    ref["referenceToReferenceFlow"] = str(reference_id)
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
    return time_info


def _normalise_geography(section: Any) -> dict[str, Any]:
    geo = _ensure_dict(section)
    if "locationOfOperationSupplyOrProduction" in geo:
        return geo

    code = geo.pop("code", None) or geo.pop("@location", None) or DEFAULT_LOCATION
    description = geo.pop("description", None) or geo.pop("comment", None)
    sub_location = geo.pop("subLocation", None) or geo.pop("sub_location", None)

    block: dict[str, Any] = {"@location": code}
    if description:
        block["descriptionOfRestrictions"] = description
    if sub_location:
        block["common:other"] = sub_location

    return {"locationOfOperationSupplyOrProduction": block}


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
        item.setdefault("@dataSetInternalID", str(index))
        if "referenceToFlowDataSet" in item:
            item["referenceToFlowDataSet"] = item["referenceToFlowDataSet"]
        item.setdefault("exchangeDirection", "Input")
        item.setdefault("meanAmount", "0")
        item.setdefault("resultingAmount", "0")
        item.setdefault("dataDerivationTypeStatus", "Unknown")
        normalised.append(item)

    return {"exchange": normalised}


def _ensure_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _to_multilang(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        if "@xml:lang" not in value:
            value.setdefault("@xml:lang", DEFAULT_LANGUAGE)
        return value
    if isinstance(value, list):
        return {"@xml:lang": DEFAULT_LANGUAGE, "#text": "; ".join(_stringify(item) for item in value)}
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
