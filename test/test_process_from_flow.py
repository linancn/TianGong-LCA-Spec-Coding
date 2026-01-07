from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from tiangong_lca_spec.core.models import FlowCandidate, FlowQuery
from tiangong_lca_spec.process_from_flow import ProcessFromFlowService


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
