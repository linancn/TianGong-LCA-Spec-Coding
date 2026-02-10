from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from tiangong_lca_spec.core.models import FlowCandidate, FlowQuery
from tiangong_lca_spec.process_from_flow import ProcessFromFlowService
from tiangong_lca_spec.process_from_flow.service import (
    FlowReferenceInfo,
    UnitGroupInfo,
    _generate_flow_query_rewrites_with_llm,
    _is_core_mass_exchange,
    _parse_exchange_comment_tags,
    _resolve_exchange_balance_unit,
    _resolve_exchange_comment_tag_unit,
)


class FakeLLM:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def invoke(self, input_data: dict[str, Any]) -> Any:
        prompt = str(input_data.get("prompt") or "")
        self.calls.append(prompt.splitlines()[0] if prompt else "")

        if prompt.startswith("You are an expert LCA practitioner"):
            return {
                "technical_description": "Test technical description.",
                "assumptions": ["No quantitative inventory; placeholders only."],
                "scope": "Generic scope",
            }
        if prompt.startswith("You are clustering scientific references into consistent process systems for process_from_flow."):
            context = input_data.get("context") or {}
            summaries = context.get("reference_summaries") or []
            dois: list[str] = []
            if isinstance(summaries, list):
                for item in summaries:
                    if not isinstance(item, dict):
                        continue
                    doi = str(item.get("doi") or "").strip()
                    if doi:
                        dois.append(doi)
            if not dois:
                return {"clusters": [], "primary_cluster_id": "C1", "selection_guidance": "No DOI data in test."}
            return {
                "clusters": [
                    {
                        "cluster_id": "C1",
                        "dois": dois,
                        "system_boundary": "unspecified",
                        "granularity": "unknown",
                        "key_process_chain": [],
                        "key_intermediate_flows": [],
                        "supported_steps": ["step1"],
                        "recommendation": "primary",
                        "reason": "Test cluster.",
                    }
                ],
                "primary_cluster_id": "C1",
                "selection_guidance": "Use primary cluster C1.",
            }
        if prompt.startswith("You are selecting/using the route options") or prompt.startswith("You are decomposing a technical process description"):
            return {
                "selected_route_id": "R1",
                "routes": [
                    {
                        "route_id": "R1",
                        "route_name": "Test route",
                        "processes": [
                            {
                                "process_id": "P1",
                                "reference_flow_name": "Test flow",
                                "name": "Production of test flow",
                                "description": "Foreground process producing the reference flow.",
                                "is_reference_flow_process": True,
                            }
                        ],
                    }
                ],
            }
        if prompt.startswith("You are defining the inventory exchanges"):
            return {
                "processes": [
                    {
                        "process_id": "P1",
                        "exchanges": [
                            {
                                "exchangeDirection": "Output",
                                "exchangeName": "Test flow",
                                "generalComment": "Reference flow output.",
                                "unit": "kg",
                                "amount": None,
                                "is_reference_flow": True,
                            },
                            {
                                "exchangeDirection": "Input",
                                "exchangeName": "Electricity, medium voltage",
                                "generalComment": "Utility input.",
                                "unit": "kWh",
                                "amount": None,
                                "is_reference_flow": False,
                            },
                        ],
                    }
                ]
            }
        if prompt.startswith("You are writing the intended applications field for an ILCD process dataset."):
            return {
                "intended_applications": {
                    "en": "Intended for unit-process LCA modelling of the test flow based on provided scope and assumptions.",
                    "zh": "用于基于给定范围与假设的测试流单元过程生命周期清单建模。",
                }
            }
        if prompt.startswith("You are writing dataCutOffAndCompletenessPrinciples for an ILCD process dataset."):
            return {
                "data_cut_off_and_completeness_principles": {
                    "en": "Exchange completeness reflects available evidence; unresolved placeholders indicate potential gaps.",
                    "zh": "交换清单完整性基于现有证据，未解析占位符表示可能存在缺口。",
                }
            }
        if prompt.startswith("You are extracting quantitative exchange values from evidence."):
            return {"processes": []}
        if prompt.startswith("You are selecting level"):
            context = input_data.get("context") or {}
            candidates = context.get("candidates") or []
            if not candidates:
                raise AssertionError("Classification prompt missing candidates")
            choice = candidates[0]
            level = choice.get("level", 0)
            return {"@level": str(level), "@classId": choice.get("code", "C"), "#text": choice.get("description", "Manufacturing")}
        raise AssertionError(f"Unexpected prompt: {prompt}")


class NoisyLLM(FakeLLM):
    def invoke(self, input_data: dict[str, Any]) -> Any:
        prompt = str(input_data.get("prompt") or "")
        if prompt.startswith("You are defining the inventory exchanges"):
            return {
                "processes": [
                    {
                        "process_id": "P1",
                        "exchanges": [
                            {
                                "exchangeDirection": "Output",
                                "exchangeName": "Test flow",
                                "generalComment": "Reference flow output.",
                                "unit": "kg",
                                "amount": "1.0",
                                "is_reference_flow": True,
                            },
                            {
                                "exchangeDirection": "Input",
                                "exchangeName": "Test flow; Production mix, at plant",
                                "generalComment": "Duplicate reference flow input.",
                                "unit": "kg",
                                "amount": "0.06",
                                "is_reference_flow": False,
                            },
                            {
                                "exchangeDirection": "Output",
                                "exchangeName": "Ham",
                                "generalComment": "Unplanned co-product.",
                                "unit": "kg",
                                "amount": "2.0",
                                "is_reference_flow": False,
                            },
                            {
                                "exchangeDirection": "Output",
                                "exchangeName": "Carbon dioxide",
                                "generalComment": "Emission output.",
                                "unit": "kg",
                                "amount": "0.05",
                                "is_reference_flow": False,
                            },
                            {
                                "exchangeDirection": "Input",
                                "exchangeName": "Electricity, medium voltage",
                                "generalComment": "Utility input.",
                                "unit": "kWh",
                                "amount": None,
                                "is_reference_flow": False,
                            },
                        ],
                    }
                ]
            }
        return super().invoke(input_data)


class RewriteLLM:
    def __init__(self, response: Any) -> None:
        self.response = response

    def invoke(self, input_data: dict[str, Any]) -> Any:  # noqa: ARG002
        return self.response


def fake_flow_search(query: FlowQuery) -> tuple[list[FlowCandidate], list[object]]:
    return [
        FlowCandidate(
            uuid="00000000-0000-0000-0000-000000000001",
            base_name=query.exchange_name,
            version="01.01.000",
        )
    ], []


def test_process_from_flow_generates_process_dataset(tmp_path: Path) -> None:
    flow_uuid = str(uuid4())
    flow_path = tmp_path / "flow.json"
    flow_path.write_text(
        json.dumps(
            {
                "flowDataSet": {
                    "flowInformation": {
                        "dataSetInformation": {
                            "common:UUID": flow_uuid,
                            "name": {
                                "baseName": [{"@xml:lang": "en", "#text": "Test flow"}],
                                "treatmentStandardsRoutes": [{"@xml:lang": "en", "#text": "Finished product, manufactured"}],
                                "mixAndLocationTypes": [{"@xml:lang": "en", "#text": "Production mix, at plant"}],
                                "flowProperties": [],
                            },
                            "classificationInformation": {"common:classification": {"common:class": [{"@level": "0", "@classId": "0", "#text": "Test"}]}},
                            "common:generalComment": [{"@xml:lang": "en", "#text": "Test flow general comment."}],
                        }
                    },
                    "administrativeInformation": {"publicationAndOwnership": {"common:dataSetVersion": "01.01.000"}},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    llm = FakeLLM()
    service = ProcessFromFlowService(llm=llm, flow_search_fn=fake_flow_search)
    state = service.run(flow_path=flow_path, operation="produce")

    datasets = state.get("process_datasets") or []
    assert isinstance(datasets, list)
    assert len(datasets) == 1
    dataset = datasets[0]
    assert isinstance(dataset, dict)
    assert "processDataSet" in dataset

    process_data_set = dataset["processDataSet"]
    process_info = process_data_set["processInformation"]
    qref = process_info["quantitativeReference"]
    assert qref["@type"] == "Reference flow(s)"
    assert qref["referenceToReferenceFlow"] == "1"

    exchanges = process_data_set["exchanges"]["exchange"]
    assert isinstance(exchanges, list)
    assert any(item.get("@dataSetInternalID") == "1" and item.get("exchangeDirection") == "Output" for item in exchanges)


def test_process_from_flow_treat_sets_reference_flow_as_input(tmp_path: Path) -> None:
    flow_uuid = str(uuid4())
    flow_path = tmp_path / "flow.json"
    flow_path.write_text(
        json.dumps(
            {
                "flowDataSet": {
                    "flowInformation": {
                        "dataSetInformation": {
                            "common:UUID": flow_uuid,
                            "name": {
                                "baseName": [{"@xml:lang": "en", "#text": "Test flow"}],
                                "treatmentStandardsRoutes": [{"@xml:lang": "en", "#text": "Finished product, manufactured"}],
                                "mixAndLocationTypes": [{"@xml:lang": "en", "#text": "Production mix, at plant"}],
                                "flowProperties": [],
                            },
                            "classificationInformation": {"common:classification": {"common:class": [{"@level": "0", "@classId": "0", "#text": "Test"}]}},
                            "common:generalComment": [{"@xml:lang": "en", "#text": "Test flow general comment."}],
                        }
                    },
                    "administrativeInformation": {"publicationAndOwnership": {"common:dataSetVersion": "01.01.000"}},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    llm = FakeLLM()
    service = ProcessFromFlowService(llm=llm, flow_search_fn=fake_flow_search)
    state = service.run(flow_path=flow_path, operation="treat")

    datasets = state.get("process_datasets") or []
    assert isinstance(datasets, list)
    assert len(datasets) == 1
    dataset = datasets[0]
    assert isinstance(dataset, dict)
    assert "processDataSet" in dataset

    process_data_set = dataset["processDataSet"]
    process_info = process_data_set["processInformation"]
    qref = process_info["quantitativeReference"]
    ref_id = qref["referenceToReferenceFlow"]

    exchanges = process_data_set["exchanges"]["exchange"]
    assert isinstance(exchanges, list)
    assert any(item.get("@dataSetInternalID") == ref_id and item.get("exchangeDirection") == "Input" for item in exchanges)


def test_process_from_flow_filters_unplanned_outputs(tmp_path: Path) -> None:
    flow_uuid = str(uuid4())
    flow_path = tmp_path / "flow.json"
    flow_path.write_text(
        json.dumps(
            {
                "flowDataSet": {
                    "flowInformation": {
                        "dataSetInformation": {
                            "common:UUID": flow_uuid,
                            "name": {
                                "baseName": [{"@xml:lang": "en", "#text": "Test flow"}],
                                "treatmentStandardsRoutes": [{"@xml:lang": "en", "#text": "Finished product, manufactured"}],
                                "mixAndLocationTypes": [{"@xml:lang": "en", "#text": "Production mix, at plant"}],
                                "flowProperties": [],
                            },
                            "classificationInformation": {"common:classification": {"common:class": [{"@level": "0", "@classId": "0", "#text": "Test"}]}},
                            "common:generalComment": [{"@xml:lang": "en", "#text": "Test flow general comment."}],
                        }
                    },
                    "administrativeInformation": {"publicationAndOwnership": {"common:dataSetVersion": "01.01.000"}},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    llm = NoisyLLM()
    service = ProcessFromFlowService(llm=llm, flow_search_fn=fake_flow_search)
    state = service.run(flow_path=flow_path, operation="produce")

    process_exchanges = state.get("process_exchanges") or []
    assert isinstance(process_exchanges, list)
    exchanges = process_exchanges[0]["exchanges"]
    names = [str(item.get("exchangeName") or "").lower() for item in exchanges]
    assert sum("test flow" in name for name in names) == 1
    assert not any(name == "ham" for name in names)


def test_process_from_flow_exchange_comments_include_io_tags(tmp_path: Path) -> None:
    flow_uuid = str(uuid4())
    flow_path = tmp_path / "flow.json"
    flow_path.write_text(
        json.dumps(
            {
                "flowDataSet": {
                    "flowInformation": {
                        "dataSetInformation": {
                            "common:UUID": flow_uuid,
                            "name": {
                                "baseName": [{"@xml:lang": "en", "#text": "Test flow"}],
                                "treatmentStandardsRoutes": [{"@xml:lang": "en", "#text": "Finished product, manufactured"}],
                                "mixAndLocationTypes": [{"@xml:lang": "en", "#text": "Production mix, at plant"}],
                                "flowProperties": [],
                            },
                            "classificationInformation": {"common:classification": {"common:class": [{"@level": "0", "@classId": "0", "#text": "Test"}]}},
                            "common:generalComment": [{"@xml:lang": "en", "#text": "Test flow general comment."}],
                        }
                    },
                    "administrativeInformation": {"publicationAndOwnership": {"common:dataSetVersion": "01.01.000"}},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    llm = FakeLLM()
    service = ProcessFromFlowService(llm=llm, flow_search_fn=fake_flow_search)
    state = service.run(flow_path=flow_path, operation="produce")

    datasets = state.get("process_datasets") or []
    assert len(datasets) == 1
    process_data_set = datasets[0]["processDataSet"]
    exchanges = process_data_set["exchanges"]["exchange"]
    assert isinstance(exchanges, list) and exchanges

    for item in exchanges:
        comment_block = item.get("generalComment")
        texts: list[str] = []
        if isinstance(comment_block, list):
            for entry in comment_block:
                if isinstance(entry, dict):
                    text = str(entry.get("#text") or "").strip()
                    if text:
                        texts.append(text)
        elif isinstance(comment_block, dict):
            text = str(comment_block.get("#text") or "").strip()
            if text:
                texts.append(text)
        elif isinstance(comment_block, str) and comment_block.strip():
            texts.append(comment_block.strip())
        assert any("[tg_io_kind_tag=" in text for text in texts)
        assert any("[tg_io_uom_tag=" in text for text in texts)


def test_parse_exchange_comment_tags_extracts_kind_and_uom() -> None:
    comment = "example [tg_io_kind_tag=product] [tg_io_uom_tag=kg]"
    parsed = _parse_exchange_comment_tags(comment)
    assert parsed.get("tg_io_kind_tag") == "product"
    assert parsed.get("tg_io_uom_tag") == "kg"


def test_resolve_exchange_balance_unit_prefers_comment_uom_when_unit_is_ambiguous() -> None:
    exchange = {
        "unit": "unit",
        "generalComment": "input [tg_io_kind_tag=product] [tg_io_uom_tag=kg]",
    }
    resolved, reasons = _resolve_exchange_balance_unit(
        exchange,
        reference_info=None,
        material_role="raw_material",
        flow_kind="product",
    )
    assert resolved == "kg"
    assert "uom_from_comment_tag" in reasons


def test_resolve_exchange_balance_unit_can_fallback_to_flow_reference_unit() -> None:
    reference_info = FlowReferenceInfo(
        flow_property_id="93a60a56-a3c8-11da-a746-0800200b9a66",
        unit_group=UnitGroupInfo(
            unit_group_id="mass",
            name="Units of mass",
            reference_unit="kg",
            units={"kg": 1.0},
        ),
    )
    exchange = {
        "unit": "unit",
        "generalComment": "input [tg_io_kind_tag=product] [tg_io_uom_tag=unit]",
    }
    resolved, reasons = _resolve_exchange_balance_unit(
        exchange,
        reference_info=reference_info,
        material_role="raw_material",
        flow_kind="product",
    )
    assert resolved == "kg"
    assert "assume_flow_reference_unit" in reasons


def test_is_core_mass_exchange_respects_roles_and_exclude() -> None:
    assert _is_core_mass_exchange(material_role="raw_material", flow_kind="product", balance_exclude=False) is True
    assert _is_core_mass_exchange(material_role="product", flow_kind="product", balance_exclude=False) is True
    assert _is_core_mass_exchange(material_role="energy", flow_kind="product", balance_exclude=False) is False
    assert _is_core_mass_exchange(material_role="auxiliary", flow_kind="product", balance_exclude=True) is False


def test_resolve_exchange_comment_tag_unit_prefers_flow_reference_unit() -> None:
    reference_info = FlowReferenceInfo(
        flow_property_id="93a60a56-a3c8-11da-a746-0800200b9a66",
        unit_group=UnitGroupInfo(
            unit_group_id="mass",
            name="Units of mass",
            reference_unit="kg",
            units={"kg": 1.0},
        ),
    )
    exchange = {
        "unit": "unit",
        "generalComment": "input [tg_io_kind_tag=product] [tg_io_uom_tag=unit]",
    }
    resolved = _resolve_exchange_comment_tag_unit(exchange, reference_info=reference_info)
    assert resolved == "kg"


def test_generate_flow_query_rewrites_with_llm_dedupes_and_excludes_original() -> None:
    llm = RewriteLLM(
        {
            "query_variants": [
                "Tap water",
                "tap water",
                "water supply",
                "Drinking water for pigs",
                {"query": "potable water"},
            ]
        }
    )
    rewrites = _generate_flow_query_rewrites_with_llm(
        llm=llm,
        exchange_name="Drinking water for pigs",
        comment="Pig farm input",
        flow_type="product",
        direction="Input",
        unit="m3",
        expected_compartment="water",
        search_hints=["water"],
    )
    assert rewrites == ["Tap water", "water supply", "potable water"]


def test_generate_flow_query_rewrites_with_llm_handles_failures() -> None:
    class BrokenLLM:
        def invoke(self, input_data: dict[str, Any]) -> Any:  # noqa: ARG002
            raise RuntimeError("boom")

    rewrites = _generate_flow_query_rewrites_with_llm(
        llm=BrokenLLM(),
        exchange_name="Drinking water for pigs",
        comment=None,
        flow_type="product",
        direction="Input",
        unit="m3",
        expected_compartment="water",
        search_hints=[],
    )
    assert rewrites == []
