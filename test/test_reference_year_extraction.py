from __future__ import annotations

from copy import deepcopy
from datetime import datetime

from tiangong_lca_spec.process_extraction.service import ProcessExtractionService

SAMPLE_DATASET = {
    "processInformation": {
        "dataSetInformation": {
            "name": {"baseName": "Sample methanol production"},
        },
        "quantitativeReference": {"referenceToReferenceFlow": "1"},
        "geography": {"code": "CN"},
    },
    "exchanges": {"exchange": []},
}


class StaticLLM:
    """Stubbed language model for deterministic process extraction tests."""

    def __init__(self, dataset: dict[str, object]) -> None:
        self._dataset = dataset

    def invoke(self, payload: dict[str, object]) -> dict[str, object]:
        prompt = str(payload.get("prompt", ""))
        if prompt.startswith("You are an expert LCA analyst"):
            return {"processDataSet": deepcopy(self._dataset)}
        if prompt.startswith("You are analysing a life cycle assessment document"):
            return {"parentProcesses": []}
        if prompt.startswith("Derive the ISIC classification path"):
            return [
                {"@level": "0", "@classId": "C", "#text": "Manufacturing"},
                {"@level": "1", "@classId": "20", "#text": "Manufacture of chemicals"},
            ]
        if prompt.startswith("Normalize the process geography"):
            return {"code": "CN", "description": "China mainland"}
        raise AssertionError(f"unexpected prompt: {prompt}")


def _extract_time_reference(text: str) -> int:
    llm = StaticLLM(deepcopy(SAMPLE_DATASET))
    service = ProcessExtractionService(llm)
    blocks = service.extract(text)
    time_info = blocks[0]["processDataSet"]["processInformation"]["time"]
    return time_info["common:referenceYear"]


def test_reference_year_prefers_publication_header() -> None:
    text = "Energy Conversion and Management, Volume 302, 2024, 118128."
    assert _extract_time_reference(text) == 2024


def test_reference_year_falls_back_to_current_year() -> None:
    text = "Methanol production overview without explicit year markers."
    before = datetime.now().year
    result = _extract_time_reference(text)
    after = datetime.now().year
    assert result == before or result == after
