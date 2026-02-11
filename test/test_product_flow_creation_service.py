from __future__ import annotations

import re

from tiangong_lca_spec.product_flow_creation import ProductFlowCreateRequest, ProductFlowCreationService


def _minimal_classification() -> list[dict[str, str]]:
    return [
        {"@level": "0", "@classId": "011", "#text": "Food, feed and beverages"},
        {"@level": "1", "@classId": "01132", "#text": "Meat products"},
    ]


def test_product_flow_creation_service_builds_valid_payload() -> None:
    service = ProductFlowCreationService()
    request = ProductFlowCreateRequest(
        class_id="01132",
        classification=_minimal_classification(),
        base_name_en="Edible offal",
        base_name_zh="可食用内脏",
    )

    result = service.build(request)
    flow = result.payload["flowDataSet"]
    info = flow["flowInformation"]["dataSetInformation"]

    assert result.flow_uuid
    assert result.version == "01.01.000"
    assert info["common:UUID"] == result.flow_uuid
    assert info["name"]["baseName"][0]["#text"] == "Edible offal"
    assert info["name"]["baseName"][1]["#text"] == "可食用内脏"
    assert info["common:synonyms"][0]["#text"] == "Edible offal"
    assert info["common:synonyms"][1]["#text"] == "可食用内脏"
    assert flow["modellingAndValidation"]["LCIMethod"]["typeOfDataSet"] == "Product flow"
    assert flow["flowProperties"]["flowProperty"]["referenceToFlowPropertyDataSet"]["@refObjectId"] == "93a60a56-a3c8-11da-a746-0800200b9a66"
    timestamp = flow["administrativeInformation"]["dataEntryBy"]["common:timeStamp"]
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$", timestamp)
    assert "<flowDataSet" in result.xml


def test_product_flow_creation_service_normalizes_semicolon_fields() -> None:
    service = ProductFlowCreationService()
    request = ProductFlowCreateRequest(
        class_id="01132",
        classification=_minimal_classification(),
        base_name_en="A;B",
        base_name_zh="甲；乙",
        treatment_en="X;Y",
        mix_en="M;N",
        synonyms_en=["S;1", "S;2"],
        synonyms_zh=["同义；1"],
        comment_en="C;D",
    )

    result = service.build(request)
    info = result.payload["flowDataSet"]["flowInformation"]["dataSetInformation"]
    name = info["name"]
    comments = info["common:generalComment"]

    assert name["baseName"][0]["#text"] == "A,B"
    assert name["baseName"][1]["#text"] == "甲，乙"
    assert name["treatmentStandardsRoutes"][0]["#text"] == "X,Y"
    assert name["mixAndLocationTypes"][0]["#text"] == "M,N"
    assert info["common:synonyms"][0]["#text"] == "S,1; S,2"
    assert info["common:synonyms"][1]["#text"] == "同义，1"
    assert comments[0]["#text"] == "C,D"
    # comment_zh falls back to normalized English comment when not supplied.
    assert comments[1]["#text"] == "C,D"
