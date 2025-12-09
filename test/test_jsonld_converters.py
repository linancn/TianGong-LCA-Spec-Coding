"""Regression tests for JSON-LD converter helpers."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from tiangong_lca_spec.jsonld.converters import JSONLDFlowConverter, JSONLDProcessConverter

PROCESS_SAMPLE = {
    "@type": "Process",
    "@id": "31c7642b-bf84-4aa3-80be-060112b71590",
    "name": "2020 ; 4-LIB cathode production ; Cathode for NMC622-SiGr battery, Cathode paste for NMC622-SiGr battery ; Generic",
    "description": "Example process payload for JSON-LD conversion tests.",
    "category": "C:Manufacturing/27:Manufacture of electrical equipment/272:Manufacture of batteries and accumulators/2720:Manufacture of batteries and accumulators",
    "version": "01.00.001",
    "processType": "UNIT_PROCESS",
    "isInfrastructureProcess": False,
    "processDocumentation": {"validFrom": "2020-01-01"},
    "exchanges": [
        {
            "@type": "Exchange",
            "isInput": False,
            "amount": 1.0,
            "flow": {
                "@type": "Flow",
                "@id": "779a79ee-7466-4a8b-8440-db906edf68e7",
                "name": "battery, Li-ion, NMC111, rechargeable",
                "category": "C:Manufacturing/27:Manufacture of electrical equipment/272:Manufacture of batteries and accumulators/2720:Manufacture of batteries and accumulators",
                "flowType": "PRODUCT_FLOW",
                "refUnit": "kg",
            },
            "unit": {"@id": "20aadc24-a391-41cf-b340-3e4529f44bde", "name": "kg"},
            "flowProperty": {
                "@id": "93a60a56-a3c8-11da-a746-0800200b9a66",
                "name": "Mass",
                "category": "Technical flow properties",
                "refUnit": "kg",
            },
            "isQuantitativeReference": True,
        }
    ],
}

FLOW_SAMPLE = {
    "@type": "Flow",
    "@id": "029e59a8-23d3-498a-9452-ecebda86b13d",
    "name": "Wheat grain, at farm",
    "description": "Example flow payload for JSON-LD conversion tests.",
    "category": "0:Agriculture, forestry and fishery products/01:Products of agriculture, horticulture and market gardening",
    "flowProperties": [
        {
            "flowProperty": {
                "@id": "93a60a56-a3c8-11da-a746-0800200b9a66",
                "name": "Mass",
                "category": "Technical flow properties",
                "refUnit": "kg",
                "version": "03.00.003",
            },
            "conversionFactor": 1.0,
        }
    ],
    "location": {
        "@type": "Location",
        "@id": "0b301b0b-5d3e-42d3-951e-d4a1bf6b9cd8",
        "name": "Global",
        "category": "Global",
    },
}


def test_process_converter_splits_name_fields(tmp_path: Path) -> None:
    payload = deepcopy(PROCESS_SAMPLE)
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
    payload = deepcopy(FLOW_SAMPLE)
    payload.pop("location", None)
    payload["description"] = ""
    payload["category"] = "0:Agriculture, forestry and fishery products/01:Products of agriculture, horticulture and market gardening/011:Cereals/0111:Wheat"
    temp_path = tmp_path / "flow.json"
    temp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    dataset = JSONLDFlowConverter(temp_path).to_flow_dataset()["flowDataSet"]
    flow_info = dataset["flowInformation"]

    assert "geography" not in flow_info
    assert "common:generalComment" not in flow_info["dataSetInformation"]
