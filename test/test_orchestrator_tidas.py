"""Integration test ensuring orchestrator outputs TIDAS-compliant datasets."""

from __future__ import annotations

import json
from copy import deepcopy

import jsonschema

from tiangong_lca_spec.orchestrator import WorkflowOrchestrator
from tiangong_lca_spec.tidas import get_schema_repository

SAMPLE_PROCESS_DATASET = {
    "processInformation": {
        "dataSetInformation": {
            "name": {
                "baseName": "Example hydrogen production",
                "treatmentStandardsRoutes": "Steam reforming",
                "mixAndLocationTypes": "at plant",
            },
            "classificationInformation": {"common:classification": {"common:class": []}},
            "common:generalComment": "Integration test dataset",
        },
        "quantitativeReference": {
            "@type": "Reference flow(s)",
            "referenceToReferenceFlow": "1",
            "functionalUnitOrOther": {"@xml:lang": "en", "#text": "1 kg H2"},
        },
        "time": {"referenceYear": "2024"},
        "geography": {"code": "CN", "description": "China mainland"},
    },
    "modellingAndValidation": {
        "LCIMethodAndAllocation": {
            "typeOfDataSet": "Unit process, single operation",
            "LCIMethodPrinciple": "Attributional",
            "LCIMethodApproaches": "Not applicable",
            "dataSourcesTreatmentAndRepresentativeness": {
                "dataCutOffAndCompletenessPrinciples": {
                    "@xml:lang": "en",
                    "#text": "Not specified",
                },
                "referenceToDataSource": {
                    "@type": "Source data set",
                    "@refObjectId": "00000000-0000-0000-0000-000000000000",
                    "@version": "1.0",
                    "@uri": "http://example.com/datasource",
                    "common:shortDescription": {
                        "@xml:lang": "en",
                        "#text": "Placeholder data source",
                    },
                },
            },
        },
        "validation": {
            "review": {
                "@type": "Not reviewed",
                "scope": {
                    "@name": "Documentation",
                    "method": {"@name": "Documentation"},
                },
            },
            "reviewDetails": {
                "@xml:lang": "en",
                "#text": "No formal review conducted; placeholder summary.",
            },
            "common:referenceToNameOfReviewerAndInstitution": {
                "@type": "Contact data set",
                "@refObjectId": "00000000-0000-0000-0000-000000000002",
                "@version": "1.0",
                "@uri": "http://example.com/reviewer",
                "common:shortDescription": {
                    "@xml:lang": "en",
                    "#text": "Placeholder reviewer",
                },
            },
            "common:referenceToCompleteReviewReport": {
                "@type": "Source data set",
                "@refObjectId": "00000000-0000-0000-0000-000000000003",
                "@version": "1.0",
                "@uri": "http://example.com/review-report",
                "common:shortDescription": {
                    "@xml:lang": "en",
                    "#text": "Placeholder review report",
                },
            },
        },
        "complianceDeclarations": {
            "compliance": {
                "common:referenceToComplianceSystem": {
                    "@type": "Compliance system",
                    "@refObjectId": "00000000-0000-0000-0000-000000000001",
                    "@version": "1.0",
                    "@uri": "http://example.com/compliance",
                    "common:shortDescription": {
                        "@xml:lang": "en",
                        "#text": "Placeholder compliance",
                    },
                },
                "common:approvalOfOverallCompliance": "Not defined",
                "common:nomenclatureCompliance": "Not defined",
                "common:methodologicalCompliance": "Not defined",
                "common:reviewCompliance": "Not defined",
                "common:documentationCompliance": "Not defined",
                "common:qualityCompliance": "Not defined",
            }
        },
    },
    "administrativeInformation": {},
    "exchanges": {
        "exchange": [
            {
                "@dataSetInternalID": "1",
                "exchangeDirection": "Input",
                "referenceToFlowDataSet": {
                    "@type": "flow data set",
                    "@refObjectId": "00000000-0000-0000-0000-000000000000",
                    "@version": "00.00.000",
                    "@uri": "https://example.com/flows/electricity",
                    "common:shortDescription": {
                        "@xml:lang": "en",
                        "#text": "Electricity, medium voltage; ; ; 1.0 kWh",
                    },
                },
                "meanAmount": "1.0",
                "resultingAmount": "1.0",
                "dataDerivationTypeStatus": "Measured",
            }
        ]
    },
}


class FakeLLM:
    """Deterministic LLM stub that emits schema-aligned structures."""

    def invoke(self, input_data: dict[str, object]) -> dict[str, object]:
        prompt = str(input_data.get("prompt", ""))
        if prompt.startswith("You are an expert LCA analyst"):
            return {"processDataSet": deepcopy(SAMPLE_PROCESS_DATASET)}
        if prompt.startswith("You are analysing a life cycle assessment document"):
            return {"parentProcesses": []}
        if prompt.startswith("You are selecting level"):
            context = input_data.get("context") or {}
            candidates = context.get("candidates") or []
            if not candidates:
                raise AssertionError("Classification prompt missing candidates")
            choice = candidates[0]
            level = choice.get("level", context.get("level", 0))
            return {
                "@level": str(level),
                "@classId": choice.get("code", "C"),
                "#text": choice.get("description", "Manufacturing"),
            }
        if "Derive the ISIC classification" in prompt:
            return [
                {"@level": "0", "@classId": "C", "#text": "Manufacturing"},
                {
                    "@level": "1",
                    "@classId": "20",
                    "#text": "Manufacture of chemicals and chemical products",
                },
                {
                    "@level": "2",
                    "@classId": "201",
                    "#text": ("Manufacture of basic chemicals, fertilizers and nitrogen compounds, " "plastics and synthetic rubber in primary forms"),
                },
                {
                    "@level": "3",
                    "@classId": "2011",
                    "#text": "Manufacture of basic chemicals",
                },
            ]
        if "Normalize the process geography" in prompt:
            return {"code": "CN", "description": "China (ISO: CN)"}
        raise AssertionError(f"Unexpected prompt: {prompt}")


class DummyFlowAlignment:
    """Bypasses remote flow alignment while preserving structure."""

    def align_exchanges(self, process_dataset: dict[str, object], paper_md: str | None):
        info = process_dataset.get("processInformation", {}) if isinstance(process_dataset, dict) else {}
        data_info = info.get("dataSetInformation", {}) if isinstance(info, dict) else {}
        name_block = data_info.get("name", {}) if isinstance(data_info, dict) else {}
        process_name = "example_process"
        if isinstance(name_block, dict):
            base_entry = name_block.get("baseName")
            if isinstance(base_entry, dict):
                process_name = base_entry.get("#text") or process_name
            elif base_entry:
                process_name = str(base_entry)
        elif name_block:
            process_name = str(name_block)
        exchanges = process_dataset.get("exchanges", {}) if isinstance(process_dataset, dict) else {}
        exchange_values = exchanges.get("exchange", []) if isinstance(exchanges, dict) else []
        if isinstance(exchange_values, dict):
            exchange_values = [exchange_values]
        return {
            "process_name": process_name,
            "matched_flows": [],
            "unmatched_flows": [],
            "origin_exchanges": {process_name: exchange_values},
        }

    def close(self) -> None:  # pragma: no cover - nothing to release
        pass


class SchemaValidationService:
    """Validates datasets against the local TIDAS JSON schema."""

    def __init__(self) -> None:
        repo = get_schema_repository()
        full_schema = repo.resolve_with_references("tidas_processes.json", "/properties/processDataSet")
        partial_schema = {
            "type": "object",
            "properties": {
                "processInformation": full_schema["properties"].get("processInformation", {}),
                "exchanges": full_schema["properties"].get("exchanges", {}),
            },
            "required": ["processInformation", "exchanges"],
        }
        self._validator = jsonschema.Draft7Validator(partial_schema)
        self.calls = 0

    def validate(self, datasets):
        self.calls += 1
        for dataset in datasets:
            document = dataset.as_dict() if hasattr(dataset, "as_dict") else dataset
            self._validator.validate(document)
        return []

    def close(self) -> None:  # pragma: no cover - nothing to release
        pass


def test_orchestrator_outputs_tidas_compliant_dataset():
    orchestrator = WorkflowOrchestrator(FakeLLM())
    original_alignment = orchestrator._flow_alignment  # type: ignore[attr-defined]
    original_alignment.close()
    orchestrator._flow_alignment = DummyFlowAlignment()  # type: ignore[attr-defined]
    original_tidas = orchestrator._tidas  # type: ignore[attr-defined]
    original_tidas.close()
    orchestrator._tidas = SchemaValidationService()  # type: ignore[attr-defined]

    try:
        payload = json.dumps(["Sample paper fragment"])
        result = orchestrator.run(payload)
    finally:
        orchestrator.close()

    assert len(result.process_datasets) == 1
    dataset = result.process_datasets[0].as_dict()
    base_name = dataset["processInformation"]["dataSetInformation"]["name"]["baseName"]
    if isinstance(base_name, dict):
        base_text = base_name.get("#text")
    else:
        base_text = base_name
    assert base_text == "Example hydrogen production"
    assert orchestrator._tidas.calls == 1  # type: ignore[attr-defined]
