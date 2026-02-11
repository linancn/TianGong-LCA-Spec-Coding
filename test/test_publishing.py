from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from scripts.md import stage4_publish
from tiangong_lca_spec.publishing.crud import FlowPublisher

EXPECTED_COMPLIANCE_DECLARATIONS = {
    "compliance": {
        "common:referenceToComplianceSystem": {
            "@refObjectId": "d92a1a12-2545-49e2-a585-55c259997756",
            "@type": "source data set",
            "@uri": "../sources/d92a1a12-2545-49e2-a585-55c259997756_20.20.002.xml",
            "@version": "20.20.002",
            "common:shortDescription": {
                "@xml:lang": "en",
                "#text": "ILCD Data Network - Entry-level",
            },
        },
        "common:approvalOfOverallCompliance": "Fully compliant",
    }
}


class DummyCrudClient:
    def insert_flow(self, dataset):
        raise AssertionError("insert_flow should not be called in dry-run tests")

    def close(self):
        pass


class TrackingCrudClient:
    def __init__(self, *, flow_exists: bool) -> None:
        self.flow_exists = flow_exists
        self.operations: list[str] = []

    def select_flow(self, flow_uuid, *, version=None):
        if not self.flow_exists:
            return None
        return {"flowInformation": {"dataSetInformation": {"common:UUID": flow_uuid}}}

    def insert_flow(self, dataset):
        self.operations.append("insert")
        return {"operation": "insert", "ok": True}

    def update_flow(self, dataset):
        self.operations.append("update")
        return {"operation": "update", "ok": True}

    def close(self):
        pass


class StubLLM:
    def __init__(self):
        self.calls = 0

    def invoke(self, input_data):
        self.calls += 1
        context = input_data.get("context") or {}
        if isinstance(context, dict) and "options" in context:
            options = context.get("options") or []
            for option in options:
                description = (option.get("description") or "").lower()
                if "electric" in description:
                    return {"choice": option.get("code"), "reason": "stub"}
            if options:
                return {"choice": options[0].get("code"), "reason": "stub default"}
            return {"choice": "STOP", "reason": "no options"}
        return {"flow_type": "Product flow", "reason": "stub"}


def _build_alignment_payload():
    return [
        {
            "process_name": "Sample process",
            "origin_exchanges": {
                "Electric power": [
                    {
                        "exchangeName": "Electric power",
                        "exchangeDirection": "Input",
                        "unit": "kWh",
                        "generalComment": {
                            "#text": (
                                "FlowSearch hints: "
                                "basename=Electric power supply "
                                "| treatment=Utility-grade, medium voltage, purchased electricity "
                                "| mix_location=Consumption mix, at plant "
                                "| flow_properties=1 kWh reference flow "
                                "| en_synonyms=Electric power; Grid electricity "
                                "| zh_synonyms=电力; 电网供电 "
                                "| abbreviation=MV electricity "
                                "| state_purity=AC 10-30 kV, 50 Hz "
                                "| source_or_pathway=Regional grid, CN "
                                "| usage_context=Input to Sample process for energy supply"
                            )
                        },
                        "referenceToFlowDataSet": {"unmatched:placeholder": True},
                    }
                ]
            },
        }
    ]


def test_flow_publisher_builds_plan_without_network():
    stub_llm = StubLLM()
    publisher = FlowPublisher(crud_client=DummyCrudClient(), dry_run=True, llm=stub_llm)
    plans = publisher.prepare_from_alignment(_build_alignment_payload())
    assert len(plans) == 1
    plan = plans[0]
    assert plan.exchange_name == "Electric power"
    dataset = plan.dataset
    assert dataset["flowInformation"]["dataSetInformation"]["common:UUID"] == plan.uuid
    assert dataset["modellingAndValidation"]["LCIMethod"]["typeOfDataSet"] == "Product flow"
    compliance = dataset["modellingAndValidation"]["complianceDeclarations"]["compliance"]
    expected = EXPECTED_COMPLIANCE_DECLARATIONS["compliance"]
    assert compliance["common:approvalOfOverallCompliance"] == expected["common:approvalOfOverallCompliance"]
    ref = compliance["common:referenceToComplianceSystem"]
    expected_ref = expected["common:referenceToComplianceSystem"]
    assert ref["@refObjectId"] == expected_ref["@refObjectId"]
    assert ref["@type"] == expected_ref["@type"]
    assert ref["@uri"] == expected_ref["@uri"]
    assert ref["@version"] == expected_ref["@version"]
    short_desc = ref.get("common:shortDescription")
    if isinstance(short_desc, list):
        short_desc = short_desc[0] if short_desc else {}
    assert short_desc["#text"] == "ILCD Data Network - Entry-level"
    classes = dataset["flowInformation"]["dataSetInformation"]["classificationInformation"]["common:classification"]["common:class"]
    codes = [entry.get("@classId") for entry in classes]
    assert "1710" in codes
    assert stub_llm.calls >= 2
    publisher.close()


def test_alignment_updates_replace_placeholders():
    alignment_entries = _build_alignment_payload()
    fake_ref = {
        "@refObjectId": "1234",
        "@uri": "https://lcdn.tiangong.earth/showProductFlow.xhtml?uuid=1234&version=01.01.000",
    }
    updates = {("Sample process", "Electric power"): fake_ref, (None, "Electric power"): fake_ref}
    replacements = stage4_publish._update_alignment_entries(alignment_entries, updates)  # type: ignore[attr-defined]
    assert replacements == 1
    ref = alignment_entries[0]["origin_exchanges"]["Electric power"][0]["referenceToFlowDataSet"]
    assert ref["@refObjectId"] == "1234"


def test_flow_publisher_publish_switches_to_update_when_uuid_exists():
    stub_llm = StubLLM()
    crud = TrackingCrudClient(flow_exists=True)
    publisher = FlowPublisher(crud_client=crud, dry_run=False, llm=stub_llm)
    plans = publisher.prepare_from_alignment(_build_alignment_payload())
    assert len(plans) == 1
    results = publisher.publish()
    assert len(results) == 1
    assert crud.operations == ["update"]
    publisher.close()
