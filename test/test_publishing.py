from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from scripts import stage4_publish
from tiangong_lca_spec.publishing.crud import FlowPublisher

EXPECTED_COMPLIANCE_DECLARATIONS = {
    "compliance": {
        "common:referenceToComplianceSystem": {
            "@refObjectId": "d92a1a12-2545-49e2-a585-55c259997756",
            "@type": "source data set",
            "@uri": "../sources/d92a1a12-2545-49e2-a585-55c259997756.xml",
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
                                "FlowSearch hints: en_synonyms=Electric power; Grid electricity | "
                                "zh_synonyms=电力 | usage_context=Electricity purchased from grid"
                            )
                        },
                        "referenceToFlowDataSet": {"unmatched:placeholder": True},
                    }
                ]
            },
        }
    ]


def test_flow_publisher_builds_plan_without_network():
    publisher = FlowPublisher(crud_client=DummyCrudClient(), dry_run=True)
    plans = publisher.prepare_from_alignment(_build_alignment_payload())
    assert len(plans) == 1
    plan = plans[0]
    assert plan.exchange_name == "Electric power"
    dataset = plan.dataset
    assert dataset["flowInformation"]["dataSetInformation"]["common:UUID"] == plan.uuid
    assert dataset["modellingAndValidation"]["LCIMethod"]["typeOfDataSet"] == "Product flow"
    assert (
        dataset["modellingAndValidation"]["complianceDeclarations"]
        == EXPECTED_COMPLIANCE_DECLARATIONS
    )
    publisher.close()


def test_alignment_updates_replace_placeholders():
    alignment_entries = _build_alignment_payload()
    fake_ref = {"@refObjectId": "1234", "@uri": "https://tiangong.earth/flows/1234"}
    updates = {("Sample process", "Electric power"): fake_ref, (None, "Electric power"): fake_ref}
    replacements = stage4_publish._update_alignment_entries(alignment_entries, updates)  # type: ignore[attr-defined]
    assert replacements == 1
    ref = alignment_entries[0]["origin_exchanges"]["Electric power"][0]["referenceToFlowDataSet"]
    assert ref["@refObjectId"] == "1234"
