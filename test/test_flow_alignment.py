"""Unit tests for the flow alignment service."""

from __future__ import annotations

from tiangong_lca_spec.core.exceptions import FlowSearchError
from tiangong_lca_spec.core.models import FlowCandidate, UnmatchedFlow
from tiangong_lca_spec.flow_alignment.service import FlowAlignmentService


def _build_dataset(exchange_name: str = "Flow A") -> dict[str, object]:
    short_description = {
        "@xml:lang": "en",
        "#text": f"{exchange_name}; ; ; ",
    }
    return {
        "processInformation": {
            "dataSetInformation": {
                "name": {
                    "baseName": {"#text": "Test process"},
                }
            }
        },
        "exchanges": {
            "exchange": [
                {
                    "referenceToFlowDataSet": {
                        "@type": "flow data set",
                        "@refObjectId": "11111111-1111-1111-1111-111111111111",
                        "@version": "00.00.000",
                        "@uri": "https://example.com/flows/default",
                        "common:shortDescription": short_description,
                        "unmatched:placeholder": True,
                    },
                    "exchangeDirection": "Input",
                    "meanAmount": "1.0",
                    "unit": "kg",
                }
            ]
        },
    }


def _candidate_for(query_name: str) -> FlowCandidate:
    return FlowCandidate(
        uuid="123e4567-e89b-12d3-a456-426614174000",
        base_name=query_name,
    )


def test_align_exchanges_does_not_emit_unmatched_on_success() -> None:
    def successful_search(query) -> tuple[list[FlowCandidate], list[UnmatchedFlow]]:
        return ([_candidate_for(query.exchange_name)], [])

    service = FlowAlignmentService(flow_search_fn=successful_search)
    try:
        process_dataset = _build_dataset()
        result = service.align_exchanges(process_dataset)

        assert result["matched_flows"], "Expected matches to be recorded"
        assert result["unmatched_flows"] == [], "No unmatched flows should be emitted"
        origin = result["origin_exchanges"]["Flow A"]
        assert origin[0]["referenceToFlowDataSet"]["common:shortDescription"]["#text"].split(";")[0].strip() == "Flow A"
    finally:
        service.close()


def test_align_exchanges_retries_after_flow_search_error() -> None:
    class FlakySearch:
        def __init__(self) -> None:
            self.calls = 0

        def __call__(self, query) -> tuple[list[FlowCandidate], list[UnmatchedFlow]]:
            self.calls += 1
            if self.calls == 1:
                raise FlowSearchError("temporary failure")
            return ([_candidate_for(query.exchange_name)], [])

    flaky = FlakySearch()
    service = FlowAlignmentService(flow_search_fn=flaky)
    try:
        process_dataset = _build_dataset()
        result = service.align_exchanges(process_dataset)

        assert flaky.calls == 2, "Expected a retry after the initial failure"
        assert result["unmatched_flows"] == []
        assert result["matched_flows"], "Retry should produce matches"
    finally:
        service.close()


def test_align_exchanges_records_unmatched_when_retry_fails() -> None:
    def failing_search(_query) -> tuple[list[FlowCandidate], list[UnmatchedFlow]]:
        raise FlowSearchError("fatal failure")

    service = FlowAlignmentService(flow_search_fn=failing_search)
    try:
        process_dataset = _build_dataset("Flow B")
        result = service.align_exchanges(process_dataset)

        assert result["matched_flows"] == []
        assert len(result["unmatched_flows"]) == 1
        assert result["unmatched_flows"][0].base_name == "Flow B"
        origin = result["origin_exchanges"]["Flow B"][0]
        short_desc = origin["referenceToFlowDataSet"]["common:shortDescription"]["#text"]
        assert short_desc.split(";")[0].strip() == "Flow B"
        assert origin["referenceToFlowDataSet"]["unmatched:placeholder"] is True
    finally:
        service.close()
