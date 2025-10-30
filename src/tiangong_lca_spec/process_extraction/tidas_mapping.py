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
TIDAS_PORTAL_BASE = "https://lcdn.tiangong.earth"

DATA_SET_TYPE_OPTIONS = [
    "Unit process, single operation",
    "Unit process, black box",
    "LCI result",
    "Partly terminated system",
    "Avoided product system",
]
DEFAULT_PROCESS_DATA_SET_TYPE = DATA_SET_TYPE_OPTIONS[0]

LCI_METHOD_PRINCIPLE_OPTIONS = [
    "Attributional",
    "Consequential",
    "Consequential with attributional components",
    "Not applicable",
    "Other",
]

LCI_METHOD_APPROACH_OPTIONS = [
    "Allocation - market value",
    "Allocation - gross calorific value",
    "Allocation - net calorific value",
    "Allocation - exergetic content",
    "Allocation - element content",
    "Allocation - mass",
    "Allocation - volume",
    "Allocation - ability to bear",
    "Allocation - marginal causality",
    "Allocation - physical causality",
    "Allocation - 100% to main function",
    "Allocation - other explicit assignment",
    "Allocation - equal distribution",
    "Allocation - recycled content",
    "Substitution - BAT",
    "Substitution - average, market price correction",
    "Substitution - average, technical properties correction",
    "Substitution - recycling potential",
    "Substitution - average, no correction",
    "Substitution - specific",
    "Consequential effects - other",
    "Not applicable",
    "Other",
]


def build_tidas_process_dataset(process_dataset: dict[str, Any]) -> dict[str, Any]:
    """Return a normalised deep copy of the provided process dataset."""

    dataset = _apply_root_metadata(process_dataset)
    dataset["@locations"] = BASE_METADATA["@locations"]
    process_information = dataset.setdefault("processInformation", {})
    normalised_process_information, name_components = _normalise_process_information(
        process_information
    )
    dataset["processInformation"] = normalised_process_information
    dataset_uuid = normalised_process_information.get("dataSetInformation", {}).get("common:UUID")
    modelling = _normalise_modelling_and_validation(dataset.get("modellingAndValidation"))
    if modelling:
        dataset["modellingAndValidation"] = modelling
    else:
        dataset.pop("modellingAndValidation", None)
    administrative = _normalise_administrative_information(
        dataset.get("administrativeInformation"),
        dataset_uuid=dataset_uuid,
        dataset_kind="process",
    )
    if administrative:
        dataset["administrativeInformation"] = administrative
    else:
        dataset.pop("administrativeInformation", None)
    exchanges, reference_id = _normalise_exchanges(dataset.get("exchanges"), name_components)
    dataset["exchanges"] = exchanges
    if reference_id is not None:
        qref = dataset["processInformation"].setdefault("quantitativeReference", {})
        qref["@type"] = DEFAULT_REFERENCE_TYPE
        qref["referenceToReferenceFlow"] = str(reference_id)
        if "functionalUnitOrOther" in qref:
            qref["functionalUnitOrOther"] = _to_multilang(qref["functionalUnitOrOther"])
    lcia = _normalise_lcia_results(dataset.get("LCIAResults"))
    if lcia:
        dataset["LCIAResults"] = lcia
    else:
        dataset.pop("LCIAResults", None)
    return _strip_common_other(dataset)


def _apply_root_metadata(process_dataset: dict[str, Any]) -> dict[str, Any]:
    dataset = deepcopy(process_dataset if isinstance(process_dataset, dict) else {})
    for key, value in BASE_METADATA.items():
        dataset.setdefault(key, value)
    return dataset


def _normalise_process_information(
    process_information: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    info = _ensure_dict(process_information)
    dataset_info, name_components = _normalise_dataset_information(
        info.get("dataSetInformation"),
    )
    info["dataSetInformation"] = dataset_info
    qref = _normalise_quantitative_reference(info.get("quantitativeReference"))
    info["quantitativeReference"] = qref
    name_components["functional_unit"] = _extract_functional_unit_text(qref)
    info["time"] = _normalise_time(info.get("time"))
    geography = _normalise_geography(info.get("geography"))
    info["geography"] = geography
    _finalise_mix_string(name_components, geography)
    dataset_name = info["dataSetInformation"].get("name", {})
    dataset_name["treatmentStandardsRoutes"] = _ensure_multilang(name_components["treatment"])
    dataset_name["mixAndLocationTypes"] = _ensure_multilang(name_components["mix"])
    info["dataSetInformation"]["name"] = dataset_name
    info["technology"] = _normalise_technology(info.get("technology"))
    if "technology" in info and not info["technology"]:
        info.pop("technology")
    if "mathematicalRelations" in info and not info["mathematicalRelations"]:
        info.pop("mathematicalRelations")
    return info, name_components


def _normalise_dataset_information(
    data_info: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    info = _ensure_dict(data_info)
    specinfo = _ensure_dict(info.pop("specinfo", None))

    uuid_value = info.get("common:UUID")
    if not isinstance(uuid_value, str) or not _is_valid_uuid(uuid_value):
        uuid_value = str(uuid4())
    info["common:UUID"] = uuid_value

    identifier = _stringify(info.get("identifierOfSubDataSet")).strip()
    if not identifier:
        identifier = uuid_value
    info["identifierOfSubDataSet"] = identifier

    name_block = _ensure_dict(info.get("name"))
    base_name_text = _extract_multilang_text(name_block.get("baseName", specinfo.get("baseName")))
    for field in (
        "baseName",
        "treatmentStandardsRoutes",
        "mixAndLocationTypes",
        "functionalUnitFlowProperties",
    ):
        if field in specinfo and specinfo[field]:
            name_block[field] = specinfo[field]
    name_components = _derive_name_components(
        base_name_text,
        specinfo,
        _stringify(info.get("common:generalComment")),
    )
    refreshed_name_block: dict[str, Any] = {}
    refreshed_name_block["baseName"] = _ensure_multilang(
        name_components["base"],
        fallback="Unnamed process",
    )
    refreshed_name_block["treatmentStandardsRoutes"] = _ensure_multilang(
        name_components["treatment"]
    )
    refreshed_name_block["mixAndLocationTypes"] = _ensure_multilang(name_components["mix"])
    functional_properties = name_components.get("functional_unit_properties")
    if functional_properties:
        refreshed_name_block["functionalUnitFlowProperties"] = _ensure_multilang(
            functional_properties
        )
    info["name"] = refreshed_name_block

    classification_info = _ensure_dict(info.get("classificationInformation"))
    specification_text = (
        _stringify(classification_info.pop("specification", None))
        or _stringify(info.pop("specification", None))
    ).strip()
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
    if specification_text:
        classification_info.setdefault("common:classification", {}).setdefault(
            "common:other", specification_text
        )
    allowed_keys = {"common:classification", "common:other"}
    classification_info = {
        key: value for key, value in classification_info.items() if key in allowed_keys
    }

    info["classificationInformation"] = classification_info

    general_comment = _stringify(info.get("common:generalComment")).strip()
    if general_comment:
        info["common:generalComment"] = general_comment
    else:
        info.pop("common:generalComment", None)

    info.pop("referenceToExternalDocumentation", None)

    if not isinstance(info.get("common:mathematicalRelationsOrReasonsForDataDerivation"), dict):
        info.pop("common:mathematicalRelationsOrReasonsForDataDerivation", None)

    scope = _ensure_dict(info.get("scope"))
    scope.pop("defaultAllocationMethod", None)
    if scope:
        info["scope"] = scope
    else:
        info.pop("scope", None)

    owner = _ensure_dict(info.get("dataSetOwner"))
    if owner:
        info["dataSetOwner"] = owner
    else:
        info.pop("dataSetOwner", None)

    allowed_dataset_info_keys = {
        "common:UUID",
        "name",
        "identifierOfSubDataSet",
        "common:synonyms",
        "complementingProcesses",
        "classificationInformation",
        "common:generalComment",
    }
    info = {
        key: value
        for key, value in info.items()
        if key in allowed_dataset_info_keys and value not in (None, "", {}, [])
    }
    return info, name_components


FEEDSTOCK_KEYWORDS = [
    "coal",
    "lignite",
    "biomass",
    "wood",
    "natural gas",
    "shale gas",
    "crude oil",
    "petroleum",
    "diesel",
    "gasoline",
    "ethanol",
    "methanol",
    "hydrogen",
    "steam",
    "water",
    "electricity",
    "limestone",
    "iron ore",
    "aluminium",
    "copper",
]


def _derive_name_components(
    base_name: str,
    specinfo: dict[str, Any],
    general_comment: str,
) -> dict[str, Any]:
    base = base_name.strip() or "Unnamed process"
    product, initial_route = _split_product_and_route(base)
    expanded_sources = []
    for value in specinfo.values():
        expanded_sources.append(_stringify(value))
    if general_comment:
        expanded_sources.append(general_comment)
    route = _resolve_route(product, initial_route, expanded_sources)
    feedstock = _extract_feedstock(expanded_sources, product)
    standards = _shorten_standard_text(_extract_standards(expanded_sources))
    mix_type = _infer_mix_type(expanded_sources)
    location_type = _infer_location_type(expanded_sources)
    treatment_segments = _collect_treatment_segments(product, feedstock, route, standards)
    treatment = _semicolon_join([product] + treatment_segments)
    treatment_short = _semicolon_join(treatment_segments)
    mix = _compose_mix_string(mix_type, location_type, None)
    functional_properties = _stringify(specinfo.get("functionalUnitFlowProperties"))
    return {
        "base": base,
        "product": product,
        "feedstock": feedstock,
        "route": route,
        "standards": standards,
        "mix_type": mix_type,
        "location_type": location_type,
        "treatment": treatment,
        "treatment_segments": treatment_segments,
        "treatment_short": treatment_short,
        "mix": mix,
        "functional_unit": None,
        "functional_unit_properties": functional_properties.strip(),
    }


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
    other_value = _stringify(ref.get("common:other"))
    if not other_value.strip():
        ref.pop("common:other", None)
    return ref


def _normalise_time(section: Any) -> dict[str, Any]:
    time_info = _ensure_dict(section)
    reference_year = time_info.pop("referenceYear", None)
    if reference_year is None:
        reference_year = time_info.get("common:referenceYear")
    year_value: int | None = None
    if isinstance(reference_year, (int, float)):
        year_value = int(reference_year)
    elif isinstance(reference_year, str) and reference_year.isdigit():
        year_value = int(reference_year)
    if year_value is not None:
        time_info["common:referenceYear"] = year_value
    elif "common:referenceYear" in time_info:
        value = time_info["common:referenceYear"]
        if not isinstance(value, (int, float, str)) or (
            isinstance(value, str) and not value.isdigit()
        ):
            time_info.pop("common:referenceYear", None)

    valid_until = time_info.get("common:dataSetValidUntil")
    if isinstance(valid_until, (int, float, str)) and str(valid_until).isdigit():
        time_info["common:dataSetValidUntil"] = int(valid_until)
    elif year_value is not None:
        time_info["common:dataSetValidUntil"] = year_value
    else:
        time_info.pop("common:dataSetValidUntil", None)
    return {k: v for k, v in time_info.items() if v not in (None, "", {})}


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
    sub_location = block.pop("subLocation", None) or geo.pop("subLocation", None)

    normalised_block: dict[str, Any] = {"@location": code}
    if sub_location:
        normalised_block.setdefault("common:comment", sub_location)

    return {"locationOfOperationSupplyOrProduction": normalised_block}


def _normalise_technology(section: Any) -> dict[str, Any]:
    technology = _ensure_dict(section)
    if not technology:
        return {}
    if "technologyDescriptionAndIncludedProcesses" in technology:
        technology["technologyDescriptionAndIncludedProcesses"] = _ensure_multilang(
            technology.get("technologyDescriptionAndIncludedProcesses"), fallback=""
        )
    if technology.get("technologicalApplicability"):
        technology["technologicalApplicability"] = _ensure_multilang(
            technology.get("technologicalApplicability"), fallback=""
        )
    for key in (
        "referenceToIncludedProcesses",
        "referenceToTechnologyPictogramme",
        "referenceToTechnologyFlowDiagrammOrPicture",
    ):
        if key in technology and not _has_reference(technology[key]):
            technology.pop(key, None)
    technology.pop("common:other", None)
    return technology


def _normalise_exchanges(
    section: Any,
    name_components: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    exchange_container = _ensure_dict(section)
    raw_exchanges = exchange_container.get("exchange")
    if isinstance(raw_exchanges, dict):
        exchanges_iter = [raw_exchanges]
    elif isinstance(raw_exchanges, list):
        exchanges_iter = raw_exchanges
    else:
        exchanges_iter = []

    normalised: list[dict[str, Any]] = []
    metadata: list[dict[str, Any]] = []
    for index, exchange in enumerate(exchanges_iter, start=0):
        item = _ensure_dict(exchange)
        item["@dataSetInternalID"] = str(index)
        item.pop("functionType", None)
        exchange_name = _stringify(item.get("exchangeName"))
        if not exchange_name:
            exchange_name = _stringify(item.get("flowName"))
        if not exchange_name:
            name_candidate = _extract_flow_name(item.get("referenceToFlowDataSet"))
            if not name_candidate:
                name_candidate = _stringify(item.get("name"))
            if not name_candidate:
                name_candidate = _stringify(item.get("generalComment")).split(":")[0]
            if not name_candidate:
                name_candidate = f"exchange_{index:06d}"
            exchange_name = name_candidate
            item["exchangeName"] = exchange_name
        if "name" in item and isinstance(item["name"], dict) and not item["name"].get("#text"):
            item.pop("name")
        direction = _stringify(item.get("exchangeDirection")).lower()
        item["exchangeDirection"] = "Input" if direction != "output" else "Output"
        item.setdefault("meanAmount", "0")
        item.setdefault("resultingAmount", "0")
        item["dataDerivationTypeStatus"] = _normalise_derivation_status(
            item.get("dataDerivationTypeStatus")
        )
        short_description = _compose_short_description(exchange_name, item, name_components)
        comment_value = item.get("generalComment")
        if comment_value:
            item["generalComment"] = _ensure_multilang(comment_value, fallback="")
        else:
            item.pop("generalComment", None)
        # Preserve genuine flow references (from alignment) but drop empty placeholders,
        # so Stage 3 can inject authoritative matches when available.
        reference = item.get("referenceToFlowDataSet")
        if not _has_reference(reference):
            item.pop("referenceToFlowDataSet", None)
        if not _stringify(item.get("common:other")).strip():
            item.pop("common:other", None)
        item.pop("exchangeName", None)
        item.pop("flowName", None)
        normalised.append(item)
        metadata.append(
            {
                "id": item["@dataSetInternalID"],
                "name": exchange_name,
                "direction": item.get("exchangeDirection"),
                "mean": item.get("meanAmount"),
                "unit": item.get("unit") or item.get("resultingAmountUnit"),
                "short": short_description,
            }
        )

    reference_id = _select_reference_flow(metadata, name_components)
    return {"exchange": normalised}, reference_id


def _compose_short_description(
    base_name: str,
    exchange: dict[str, Any],
    name_components: dict[str, Any],
) -> str:
    segments: list[str] = []
    base_segment = base_name.strip()
    if base_segment:
        segments.append(base_segment)

    feedstock = name_components.get("feedstock")
    if feedstock:
        segments.append(f"{feedstock} feedstock")

    route = name_components.get("route")
    if route and route.lower() not in {base_segment.lower(), (feedstock or "").lower()}:
        segments.append(route)

    standards = _shorten_standard_text(name_components.get("standards", ""))
    if standards:
        segments.append(standards)

    mix = name_components.get("mix") or _compose_mix_string(
        name_components.get("mix_type", "Production mix"),
        name_components.get("location_type", "at plant"),
        name_components.get("location_code"),
    )
    if mix:
        segments.append(mix)

    flow_properties = _compose_flow_properties(exchange)
    if flow_properties:
        segments.append(flow_properties)

    return "; ".join(_deduplicate_preserve_order(segments))


def _compose_flow_properties(exchange: dict[str, Any]) -> str:
    amount = _stringify(exchange.get("meanAmount") or exchange.get("resultingAmount"))
    unit = _stringify(exchange.get("unit") or exchange.get("resultingAmountUnit"))
    amount_clean = amount.strip()
    unit_clean = unit.strip()
    if amount_clean in {"", "0", "0.0"}:
        amount_clean = ""
    if unit_clean in {"", "0"}:
        unit_clean = ""
    if amount_clean and unit_clean:
        return f"{amount_clean} {unit_clean}".strip()
    if amount_clean:
        return amount_clean
    if unit_clean:
        return unit_clean
    return ""


def _compose_flow_treatment(flow_name: str, name_components: dict[str, Any]) -> str:
    segments: list[str] = []
    feedstock = name_components.get("feedstock")
    if feedstock:
        segments.append(f"{feedstock} feedstock")
    route = name_components.get("route")
    base_lower = flow_name.strip().lower()
    if route and route.lower() not in {base_lower, (feedstock or "").lower()}:
        segments.append(route)
    standards = _shorten_standard_text(name_components.get("standards", ""))
    if standards:
        segments.append(standards)
    return _semicolon_join(segments)


def _normalise_lcia_results(section: Any) -> dict[str, Any]:
    results = _ensure_dict(section)
    lcia_result = _ensure_dict(results.get("LCIAResult"))
    if not lcia_result:
        return {}
    mean_amount = lcia_result.get("meanAmount")
    if mean_amount is not None:
        try:
            mean_value = float(mean_amount)
        except (TypeError, ValueError):
            mean_value = mean_amount
        else:
            lcia_result["meanAmount"] = f"{mean_value}"
    if lcia_result.get("generalComment"):
        lcia_result["generalComment"] = _ensure_multilang(
            lcia_result.get("generalComment"), fallback=""
        )
    if not _has_reference(lcia_result.get("referenceToLCIAMethodDataSet")):
        lcia_result.pop("referenceToLCIAMethodDataSet", None)
    results = {"LCIAResult": lcia_result}
    return results


def _normalise_modelling_and_validation(section: Any) -> dict[str, Any]:
    mv_raw = _ensure_dict(section)
    if not mv_raw:
        return {}
    mv: dict[str, Any] = {}

    lci = _ensure_dict(mv_raw.get("LCIMethodAndAllocation"))
    type_value = _normalise_dataset_type(lci.get("typeOfDataSet"))
    if not type_value:
        type_value = DEFAULT_PROCESS_DATA_SET_TYPE
    lci["typeOfDataSet"] = type_value

    principle_value = _match_allowed_option(
        lci.get("LCIMethodPrinciple"), LCI_METHOD_PRINCIPLE_OPTIONS
    )
    if principle_value:
        lci["LCIMethodPrinciple"] = principle_value
    else:
        lci.pop("LCIMethodPrinciple", None)

    approach_value = _normalise_lci_method_approach(lci.get("LCIMethodApproaches"))
    if approach_value:
        lci["LCIMethodApproaches"] = approach_value
    else:
        lci.pop("LCIMethodApproaches", None)

    lci.pop("common:other", None)
    if lci:
        mv["LCIMethodAndAllocation"] = lci

    dsr = _ensure_dict(mv_raw.get("dataSourcesTreatmentAndRepresentativeness"))
    if dsr:
        mv["dataSourcesTreatmentAndRepresentativeness"] = dsr

    completeness = _ensure_dict(mv_raw.get("completeness"))
    if completeness:
        mv["completeness"] = completeness

    validation = _ensure_dict(mv_raw.get("validation"))
    if validation:
        mv["validation"] = validation

    compliance = _ensure_dict(mv_raw.get("complianceDeclarations"))
    if compliance:
        mv["complianceDeclarations"] = compliance

    return mv


def _normalise_administrative_information(
    section: Any,
    *,
    dataset_uuid: str | None = None,
    dataset_kind: str = "process",
) -> dict[str, Any]:
    admin = _ensure_dict(section)
    admin.pop("dataGenerator", None)

    commissioner = _ensure_dict(admin.get("common:commissionerAndGoal"))
    commissioner["common:referenceToCommissioner"] = _build_commissioner_reference()
    if not commissioner.get("common:intendedApplications"):
        commissioner.pop("common:intendedApplications", None)
    admin["common:commissionerAndGoal"] = commissioner

    data_entry = _ensure_dict(admin.get("dataEntryBy"))
    data_entry.pop("common:other", None)
    data_entry["common:referenceToDataSetFormat"] = _build_dataset_format_reference()
    cleaned_data_entry = {
        key: value for key, value in data_entry.items() if value not in (None, "", {}, [])
    }
    admin["dataEntryBy"] = cleaned_data_entry

    publication = _ensure_dict(admin.get("publicationAndOwnership"))
    version_candidate = _stringify(publication.get("common:dataSetVersion")).strip()
    if not version_candidate:
        version_candidate = "01.00.000"
    if publication or dataset_uuid:
        publication["common:dataSetVersion"] = version_candidate
        if dataset_uuid:
            publication["common:permanentDataSetURI"] = _build_permanent_dataset_uri(
                dataset_kind, dataset_uuid, version_candidate
            )
        if "common:registrationNumber" in publication:
            publication["common:registrationNumber"] = _stringify(
                publication.get("common:registrationNumber")
            ).strip()
        licence_value = publication.get("common:licenseType")
        if licence_value is not None:
            publication["common:licenseType"] = _normalise_license(licence_value)
        publication["common:referenceToOwnershipOfDataSet"] = _build_commissioner_reference()
        publication.pop("common:other", None)
        cleaned_publication = {
            key: value for key, value in publication.items() if value not in (None, "", {}, [])
        }
        if cleaned_publication:
            admin["publicationAndOwnership"] = cleaned_publication
        else:
            admin.pop("publicationAndOwnership", None)
    else:
        admin.pop("publicationAndOwnership", None)

    admin.pop("common:other", None)
    return {k: v for k, v in admin.items() if v not in (None, "", {}, [])}


def _build_reference(ref_type: str, description: str) -> dict[str, Any]:
    identifier = str(uuid4())
    return {
        "@type": ref_type,
        "@refObjectId": identifier,
        "@uri": f"https://tiangong.earth/{ref_type}/{identifier}",
        "@version": "01.00.000",
        "common:shortDescription": _ensure_multilang(description),
    }


def _build_commissioner_reference() -> dict[str, Any]:
    return {
        "@refObjectId": "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8",
        "@type": "contact data set",
        "@uri": "../contacts/f4b4c314-8c4c-4c83-968f-5b3c7724f6a8.xml",
        "@version": "01.00.000",
        "common:shortDescription": [
            {"@xml:lang": "en", "#text": "Tiangong LCA Data Working Group"},
            {"@xml:lang": "zh", "#text": "天工LCA数据团队"},
        ],
    }


def _build_dataset_format_reference() -> dict[str, Any]:
    return {
        "@refObjectId": "a97a0155-0234-4b87-b4ce-a45da52f2a40",
        "@type": "source data set",
        "@uri": "../sources/a97a0155-0234-4b87-b4ce-a45da52f2a40.xml",
        "@version": "03.00.003",
        "common:shortDescription": {"@xml:lang": "en", "#text": "ILCD format"},
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


def _build_permanent_dataset_uri(dataset_kind: str, uuid_value: str, version: str) -> str:
    if not uuid_value:
        return ""
    version_clean = version.strip() or "01.00.000"
    suffix_map = {
        "process": "showProcess.xhtml",
        "flow": "showProductFlow.xhtml",
        "source": "showSource.xhtml",
    }
    suffix = suffix_map.get(dataset_kind, "showDataSet.xhtml")
    return f"{TIDAS_PORTAL_BASE}/{suffix}?uuid={uuid_value}&version={version_clean}"


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


def _extract_multilang_text(value: Any) -> str:
    if isinstance(value, dict):
        if "#text" in value:
            return _stringify(value["#text"])
        if "text" in value:
            return _stringify(value["text"])
    return _stringify(value)


def _ensure_multilang(value: Any, *, fallback: str | None = None) -> dict[str, Any]:
    if isinstance(value, dict) and "@xml:lang" in value and "#text" in value:
        return value
    text = _stringify(value)
    if not text and fallback is not None:
        text = fallback
    if text is None:
        text = ""
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


def _match_allowed_option(value: Any, options: list[str]) -> str | None:
    text = _stringify(value).strip().lower()
    if not text:
        return None
    for option in options:
        candidate = option.lower()
        if text == candidate:
            return option
    for option in options:
        candidate = option.lower()
        if candidate in text or text in candidate:
            return option
    return None


def _normalise_dataset_type(value: Any) -> str | None:
    match = _match_allowed_option(value, DATA_SET_TYPE_OPTIONS)
    if match:
        return match
    text = _stringify(value).strip().lower()
    if not text:
        return None
    if "non" in text and "aggreg" in text:
        return "Unit process, single operation"
    if "single" in text and "operation" in text:
        return "Unit process, single operation"
    if "black" in text and "box" in text:
        return "Unit process, black box"
    if "lci" in text or "inventory" in text:
        return "LCI result"
    if "partly" in text or "terminated" in text:
        return "Partly terminated system"
    if "avoid" in text:
        return "Avoided product system"
    return None


def _normalise_lci_method_approach(value: Any) -> str | None:
    text = _stringify(value).strip()
    if not text:
        return None
    segments = re.split(r"[;,/]| and ", text)
    for segment in segments:
        match = _match_allowed_option(segment, LCI_METHOD_APPROACH_OPTIONS)
        if match:
            return match
    match = _match_allowed_option(text, LCI_METHOD_APPROACH_OPTIONS)
    if match:
        return match
    lowered = text.lower()
    keyword_map = {
        "market value": "Allocation - market value",
        "gross calorific": "Allocation - gross calorific value",
        "net calorific": "Allocation - net calorific value",
        "exergetic": "Allocation - exergetic content",
        "element content": "Allocation - element content",
        "mass": "Allocation - mass",
        "volume": "Allocation - volume",
        "ability to bear": "Allocation - ability to bear",
        "marginal causality": "Allocation - marginal causality",
        "physical causality": "Allocation - physical causality",
        "100%": "Allocation - 100% to main function",
        "other explicit": "Allocation - other explicit assignment",
        "equal distribution": "Allocation - equal distribution",
        "recycled content": "Allocation - recycled content",
        "bat": "Substitution - BAT",
        "market price": "Substitution - average, market price correction",
        "technical properties": "Substitution - average, technical properties correction",
        "recycling potential": "Substitution - recycling potential",
        "no correction": "Substitution - average, no correction",
        "specific": "Substitution - specific",
        "consequential": "Consequential effects - other",
    }
    for keyword, option in keyword_map.items():
        if keyword in lowered:
            return option
    return None


def _split_product_and_route(base_name: str) -> tuple[str, str]:
    cleaned = base_name.strip()
    lower = cleaned.lower()
    for token in [" for ", " to ", " via ", " -> ", " - ", " — ", ":"]:
        idx = lower.find(token)
        if idx != -1:
            product = cleaned[:idx].strip()
            route = cleaned[idx + len(token) :].strip()
            if not product:
                product = cleaned
            if not route:
                route = cleaned
            return product, route
    return cleaned, cleaned


def _extract_feedstock(sources: list[str], product: str) -> str:
    for text in sources:
        match = re.search(
            r"([A-Za-z0-9\s\-/]+?)\s+as\s+feedstock",
            text or "",
            re.IGNORECASE,
        )
        if match:
            candidate = match.group(1).strip(" ,.;:")
            if candidate:
                return _title_case_phrase(_clean_feedstock_phrase(candidate))
    for text in sources:
        match = re.search(
            r"feedstock\s*(?:is|are|:)?\s*([A-Za-z0-9\s\-/]+)", text or "", re.IGNORECASE
        )
        if match:
            candidate = match.group(1).strip(" ,.;:")
            if candidate:
                return _title_case_phrase(_clean_feedstock_phrase(candidate))
    for text in sources:
        match = re.search(r"([A-Za-z0-9\s\-/]+?)\s+feedstock", text or "", re.IGNORECASE)
        if match:
            candidate = match.group(1).strip(" ,.;:")
            if candidate:
                return _title_case_phrase(_clean_feedstock_phrase(candidate))
    for keyword in FEEDSTOCK_KEYWORDS:
        for text in sources:
            if keyword in (text or "").lower():
                return _title_case_phrase(keyword)
    for keyword in FEEDSTOCK_KEYWORDS:
        if keyword in product.lower():
            return _title_case_phrase(keyword)
    return product


def _clean_feedstock_phrase(text: str) -> str:
    cleaned = re.sub(r"^(of|the|a|an)\s+", "", text, flags=re.IGNORECASE).strip()
    cleaned = re.split(r"\b(used|consumed|for)\b", cleaned, maxsplit=1)[0].strip()
    return cleaned or text


def _extract_standards(sources: list[str]) -> str:
    for text in sources:
        if not text:
            continue
        match = re.search(r"([^.]*standard[^.]*)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip(" ;,.")
        match = re.search(r"(ISO\s?\d+(?:[:/]\d+)?)", text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def _infer_mix_type(sources: list[str]) -> str:
    for text in sources:
        if not text:
            continue
        lower = text.lower()
        if "consumption mix" in lower:
            return "Consumption mix"
    return "Production mix"


LOCATION_TYPE_KEYWORDS = {
    "consumer": "to consumer",
    "wholesale": "at wholesale",
    "retail": "at sales point",
    "plant": "at plant",
    "factory": "at plant",
    "gate": "at plant",
}


def _infer_location_type(sources: list[str]) -> str:
    for text in sources:
        if not text:
            continue
        lower = text.lower()
        for keyword, location in LOCATION_TYPE_KEYWORDS.items():
            if keyword in lower:
                return location
    return "at plant"


def _compose_treatment_string(
    product: str,
    feedstock: str,
    route: str,
    standards: str,
) -> str:
    treatment_segments = _collect_treatment_segments(product, feedstock, route, standards)
    return _semicolon_join([product] + treatment_segments)


def _compose_mix_string(mix_type: str, location_type: str, code: str | None) -> str:
    components = [mix_type, location_type]
    mix = ", ".join(filter(None, components))
    if code and code.upper() != "GLO":
        mix = f"{mix}, {code}"
    return mix


def _resolve_route(product: str, route_candidate: str, sources: list[str]) -> str:
    candidate = route_candidate.strip()
    if candidate and candidate.lower() not in {product.lower(), "unnamed process"}:
        return candidate
    for text in sources:
        if not text:
            continue
        match = re.search(
            r"(?:technical|technology)\s+route[:：]\s*([^;\n,]+)", text, re.IGNORECASE
        )
        if match:
            candidate = match.group(1).strip()
            if candidate:
                return candidate
        match = re.search(r"route(?:\s+is|:)?\s*([^;\n,]+)", text, re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            if candidate:
                return candidate
    for text in sources:
        if text and "CGTM" in text.upper():
            return "Coal Gasification to Methanol (CGTM)"
    return route_candidate.strip()


def _collect_treatment_segments(
    product: str,
    feedstock: str,
    route: str,
    standards: str,
) -> list[str]:
    segments: list[str] = []
    feedstock_clean = _clean_feedstock_phrase(feedstock)
    if feedstock_clean:
        segment = f"{feedstock_clean} feedstock"
        if segment.lower() != product.lower():
            segments.append(segment)
    route_clean = route.strip()
    if route_clean and route_clean.lower() not in {product.lower(), feedstock_clean.lower()}:
        segments.append(route_clean)
    standards_clean = standards.strip()
    if standards_clean and standards_clean.lower() not in {
        product.lower(),
        feedstock_clean.lower(),
        route_clean.lower(),
    }:
        segments.append(_shorten_standard_text(standards_clean))
    return segments


def _shorten_standard_text(text: str) -> str:
    cleaned = text.strip()
    if not cleaned:
        return ""
    for delimiter in [";", ","]:
        if delimiter in cleaned:
            cleaned = cleaned.split(delimiter)[0].strip()
    lower = cleaned.lower()
    if "cleaner production" in lower:
        subject = ""
        match = re.search(r"for\s+the\s+(.+)", cleaned, re.IGNORECASE)
        if match:
            subject = match.group(1).strip(" ,.()")
        match = re.search(r"for\s+(.+)", cleaned, re.IGNORECASE)
        if not subject and match:
            subject = match.group(1).strip(" ,.()")
        subject = re.sub(r"industry$", "", subject, flags=re.IGNORECASE).strip()
        cleaned = "Cleaner production standard"
        if subject:
            cleaned = f"{cleaned} ({subject})"
    return cleaned


def _semicolon_join(parts: list[str]) -> str:
    return "; ".join(part.strip() for part in parts if part and part.strip())


def _deduplicate_preserve_order(parts: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for part in parts:
        key = part.strip().lower()
        if not part or key in seen:
            continue
        seen.add(key)
        result.append(part.strip())
    return result


def _strip_common_other(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_common_other(val) for key, val in value.items() if key != "common:other"
        }
    if isinstance(value, list):
        return [_strip_common_other(item) for item in value]
    return value


def _finalise_mix_string(name_components: dict[str, Any], geography: dict[str, Any]) -> None:
    mix_type = name_components.get("mix_type") or "Production mix"
    location_type = name_components.get("location_type") or "at plant"
    location_block = _ensure_dict(geography.get("locationOfOperationSupplyOrProduction"))
    code = location_block.get("@location") or geography.get("code")
    name_components["location_code"] = code
    name_components["mix"] = _compose_mix_string(mix_type, location_type, code)


def _title_case_phrase(text: str) -> str:
    if not text:
        return text
    tokens = [token.strip() for token in text.split() if token.strip()]
    return " ".join(token.capitalize() if len(token) > 1 else token.upper() for token in tokens)


def _extract_functional_unit_text(qref: dict[str, Any]) -> str:
    if not isinstance(qref, dict):
        return ""
    fu = qref.get("functionalUnitOrOther")
    if isinstance(fu, dict):
        return _stringify(fu.get("#text") or fu.get("text"))
    return _stringify(fu)


def _select_reference_flow(
    candidates: list[dict[str, Any]],
    name_components: dict[str, Any],
) -> str | None:
    if not candidates:
        return None
    functional_unit = name_components.get("functional_unit") or ""
    fu_amount, fu_unit = _parse_amount_unit(functional_unit)
    output_candidates = [
        candidate for candidate in candidates if candidate.get("direction", "").lower() == "output"
    ]
    search_pool = output_candidates or candidates

    for candidate in search_pool:
        name = (candidate.get("name") or "").lower()
        if "reference" in name or "functional" in name:
            return candidate["id"]

    if fu_amount is not None and fu_unit:
        match = _match_candidate_by_amount(search_pool, fu_amount, fu_unit)
        if match:
            return match

    product = (name_components.get("product") or "").lower()
    route = (name_components.get("route") or "").lower()
    for candidate in search_pool:
        base = _extract_exchange_base_name(candidate)
        if product and product in base:
            return candidate["id"]
        if route and route in base:
            return candidate["id"]

    if search_pool:
        return search_pool[0]["id"]
    return candidates[0]["id"]


def _parse_amount_unit(text: str) -> tuple[float | None, str | None]:
    if not text:
        return None, None
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Zµμ%/]+)", text)
    if not match:
        return None, None
    try:
        amount = float(match.group(1))
    except (TypeError, ValueError):
        amount = None
    unit = match.group(2)
    return amount, unit


def _match_candidate_by_amount(
    candidates: list[dict[str, Any]],
    fu_amount: float,
    fu_unit: str,
) -> str | None:
    for candidate in candidates:
        amount = candidate.get("mean")
        unit = candidate.get("unit") or ""
        try:
            amount_value = float(amount)
        except (TypeError, ValueError):
            continue
        if abs(amount_value - fu_amount) <= 1e-6 and unit and unit.lower() == fu_unit.lower():
            return candidate["id"]
    return None


def _extract_exchange_base_name(candidate: dict[str, Any]) -> str:
    short_desc = candidate.get("short") or ""
    base = short_desc.split(";")[0].strip().lower()
    name = (candidate.get("name") or "").strip().lower()
    if base:
        return base
    return name
