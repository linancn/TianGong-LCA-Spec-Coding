from __future__ import annotations

from typing import Any, Callable

from tiangong_lca_spec.core.config import Settings
from tiangong_lca_spec.core.models import FlowQuery
from tiangong_lca_spec.flow_search.service import FlowSearchService


class FakeFlowSearchClient:
    def __init__(
        self,
        *,
        search_handler: Callable[[FlowQuery], list[dict[str, Any]]] | None = None,
        query_handler: Callable[[str], list[dict[str, Any]]] | None = None,
    ) -> None:
        self._search_handler = search_handler or (lambda _query: [])
        self._query_handler = query_handler or (lambda _query: [])
        self.search_calls: list[FlowQuery] = []
        self.query_calls: list[str] = []

    def search(self, query: FlowQuery) -> list[dict[str, Any]]:
        self.search_calls.append(query)
        return self._search_handler(query)

    def search_query_text(self, query_text: str) -> list[dict[str, Any]]:
        self.query_calls.append(query_text)
        return self._query_handler(query_text)

    def close(self) -> None:
        return None


def _elementary_candidate(
    *,
    uuid: str,
    base_name: str,
    cas: str | None,
    category_path: str = "Emissions > Emissions to air > Emissions to air, unspecified",
) -> dict[str, Any]:
    return {
        "uuid": uuid,
        "base_name": base_name,
        "flow_type": "elementary",
        "version": "03.00.004",
        "cas": cas,
        "category_path": category_path,
        "classification": [{"#text": "Emissions"}, {"#text": "Emissions to air"}],
    }


def _product_candidate(
    *,
    uuid: str,
    base_name: str,
    flow_type: str = "product",
) -> dict[str, Any]:
    return {
        "uuid": uuid,
        "base_name": base_name,
        "flow_type": flow_type,
        "version": "01.01.000",
    }


def test_elementary_lookup_prefers_cas_match_and_builds_cas_variant_query() -> None:
    wrong = _elementary_candidate(
        uuid="11111111-1111-1111-1111-111111111111",
        base_name="Difluoro(methoxy)methane",
        cas=None,
    )
    right = _elementary_candidate(
        uuid="22222222-2222-2222-2222-222222222222",
        base_name="Methane, biogenic",
        cas="74-82-8",
    )
    client = FakeFlowSearchClient(query_handler=lambda _query_text: [wrong, right])
    settings = Settings(flow_search_state_code=None)
    service = FlowSearchService(settings=settings, client=client)
    try:
        query = FlowQuery(
            exchange_name="Methane, to air",
            description="constraints: flow_type=elementary; compartment=air; search_hints=CH4",
        )
        matches, unmatched = service.lookup(query)
    finally:
        service.close()

    assert unmatched == []
    assert matches
    assert matches[0].base_name == "Methane, biogenic"
    assert matches[0].cas == "74-82-8"
    assert client.query_calls, "Elementary flow lookup should use query_text variants."
    assert any("cas: 74-82-8" in call for call in client.query_calls)
    assert client.search_calls == []


def test_elementary_lookup_rejects_cas_conflicts() -> None:
    conflict = _elementary_candidate(
        uuid="33333333-3333-3333-3333-333333333333",
        base_name="Methane, to air",
        cas="111-40-0",
    )
    client = FakeFlowSearchClient(query_handler=lambda _query_text: [conflict])
    settings = Settings(flow_search_state_code=None)
    service = FlowSearchService(settings=settings, client=client)
    try:
        query = FlowQuery(
            exchange_name="Methane, to air",
            description="constraints: flow_type=elementary; compartment=air",
        )
        matches, unmatched = service.lookup(query)
    finally:
        service.close()

    assert matches == []
    assert len(unmatched) == 1
    assert unmatched[0].base_name == "Methane, to air"


def test_non_elementary_lookup_uses_standard_search() -> None:
    product_candidate = _product_candidate(
        uuid="44444444-4444-4444-4444-444444444444",
        base_name="Compound pig feed",
    )
    client = FakeFlowSearchClient(search_handler=lambda _query: [product_candidate])
    settings = Settings(flow_search_state_code=None)
    service = FlowSearchService(settings=settings, client=client)
    try:
        query = FlowQuery(exchange_name="Compound pig feed")
        matches, unmatched = service.lookup(query)
    finally:
        service.close()

    assert unmatched == []
    assert len(matches) == 1
    assert matches[0].base_name == "Compound pig feed"
    assert len(client.search_calls) == 1
    assert client.query_calls == []


def test_product_with_compartment_is_not_routed_to_elementary_query() -> None:
    candidate = _product_candidate(
        uuid="55555555-5555-5555-5555-555555555555",
        base_name="Tap water",
    )
    client = FakeFlowSearchClient(search_handler=lambda _query: [candidate], query_handler=lambda _query: [])
    settings = Settings(flow_search_state_code=None)
    service = FlowSearchService(settings=settings, client=client)
    try:
        query = FlowQuery(
            exchange_name="Drinking water for pigs",
            description="constraints: flow_type=product; direction=Input; unit=m3; compartment=water",
        )
        matches, unmatched = service.lookup(query)
    finally:
        service.close()

    assert unmatched == []
    assert matches
    assert matches[0].base_name == "Tap water"
    assert len(client.search_calls) == 1
    assert all("flow_type: elementary flow" not in call for call in client.query_calls)


def test_non_elementary_lookup_rewrites_drinking_water_query() -> None:
    pig = _product_candidate(
        uuid="66666666-6666-6666-6666-666666666666",
        base_name="Pig",
    )
    tap = _product_candidate(
        uuid="77777777-7777-7777-7777-777777777777",
        base_name="Tap water",
    )

    def query_handler(query_text: str) -> list[dict[str, Any]]:
        lowered = query_text.lower()
        if "tap water" in lowered or "water supply" in lowered:
            return [tap]
        return [pig]

    client = FakeFlowSearchClient(search_handler=lambda _query: [pig], query_handler=query_handler)
    settings = Settings(flow_search_state_code=None)
    service = FlowSearchService(settings=settings, client=client)
    try:
        query = FlowQuery(
            exchange_name="Drinking water for pigs",
            description="constraints: flow_type=product; direction=Input; unit=m3; compartment=water",
        )
        matches, unmatched = service.lookup(query)
    finally:
        service.close()

    assert unmatched == []
    assert matches
    assert matches[0].base_name == "Tap water"
    assert any("tap water" in call.lower() for call in client.query_calls)
