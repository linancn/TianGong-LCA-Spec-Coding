from __future__ import annotations

from tiangong_lca_spec.product_flow_creation import FlowDedupService


class _StubLookup:
    def __init__(self, *, exists: bool) -> None:
        self.exists = exists
        self.calls: list[tuple[str, str | None]] = []

    def select_flow(self, flow_uuid: str, *, version: str | None = None):
        self.calls.append((flow_uuid, version))
        if self.exists:
            return {"flowInformation": {"dataSetInformation": {"common:UUID": flow_uuid}}}
        return None


def test_flow_dedup_auto_switches_to_update_when_uuid_exists() -> None:
    lookup = _StubLookup(exists=True)
    dedup = FlowDedupService(lookup)
    decision = dedup.decide(flow_uuid="u-1", version="01.01.000", preferred_action="auto")
    assert decision.action == "update"
    assert decision.exists is True
    assert decision.reason == "auto_exists"


def test_flow_dedup_insert_preference_falls_back_to_insert_when_missing() -> None:
    lookup = _StubLookup(exists=False)
    dedup = FlowDedupService(lookup)
    decision = dedup.decide(flow_uuid="u-2", version="01.01.000", preferred_action="insert")
    assert decision.action == "insert"
    assert decision.exists is False
    assert decision.reason == "preferred_insert_missing"
