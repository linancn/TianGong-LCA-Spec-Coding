"""Regression tests for JSON-LD converter helpers."""

from __future__ import annotations

import json
from pathlib import Path

from tiangong_lca_spec.jsonld.converters import JSONLDFlowConverter, JSONLDProcessConverter

DATA_DIR = Path(__file__).resolve().parent / "data" / "json_ld"
PROCESS_SAMPLE = DATA_DIR / "processes" / "31c7642b-bf84-4aa3-80be-060112b71590.json"
FLOW_SAMPLE = DATA_DIR / "flows" / "029e59a8-23d3-498a-9452-ecebda86b13d.json"


def test_process_converter_splits_name_fields(tmp_path: Path) -> None:
    payload = json.loads(PROCESS_SAMPLE.read_text(encoding="utf-8"))
    payload["category"] = "C:Manufacturing/20:Manufacture of chemicals and chemical products/202:Manufacture of other chemical products/2029:Manufacture of other chemical products n.e.c."
    process_path = tmp_path / "process.json"
    process_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    converter = JSONLDProcessConverter(process_path)
    block = converter.to_process_block()
    name_block = block["processDataSet"]["processInformation"]["dataSetInformation"]["name"]

    assert name_block["baseName"]["#text"] == "4-LIB cathode production"
    assert name_block["treatmentStandardsRoutes"]["#text"] == "Cathode for NMC622-SiGr battery, Cathode paste for NMC622-SiGr battery"
    assert name_block["mixAndLocationTypes"]["#text"] == "Generic"
    assert "functionalUnitFlowProperties" not in name_block


def test_flow_converter_omits_geography_and_comment_when_missing(tmp_path: Path) -> None:
    payload = json.loads(FLOW_SAMPLE.read_text(encoding="utf-8"))
    payload.pop("location", None)
    payload["description"] = ""
    payload["category"] = "0:Agriculture, forestry and fishery products/01:Products of agriculture, horticulture and market gardening/011:Cereals/0111:Wheat"
    temp_path = tmp_path / "flow.json"
    temp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    dataset = JSONLDFlowConverter(temp_path).to_flow_dataset()["flowDataSet"]
    flow_info = dataset["flowInformation"]

    assert "geography" not in flow_info
    assert "common:generalComment" not in flow_info["dataSetInformation"]
