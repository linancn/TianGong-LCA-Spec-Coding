from __future__ import annotations

import jsonschema

from tiangong_lca_spec.tidas.flow_property_registry import (
    FlowPropertyRegistry,
    get_default_registry,
)
from tiangong_lca_spec.tidas.schema_loader import TidasSchemaRepository


def test_registry_get_by_uuid_returns_expected_descriptor() -> None:
    registry = get_default_registry()
    descriptor = registry.get("93a60a56-a3c8-11da-a746-0800200b9a66")
    assert descriptor.name.lower() == "mass"
    assert descriptor.unit_group.reference_internal_id == "0"


def test_registry_search_by_unit_matches_mass_from_kg() -> None:
    registry = FlowPropertyRegistry()
    matches = registry.search_by_unit("kg")
    uuids = {descriptor.uuid for descriptor in matches}
    assert "93a60a56-a3c8-11da-a746-0800200b9a66" in uuids


def test_registry_emit_block_validates_against_schema() -> None:
    registry = get_default_registry()
    block = registry.build_flow_property_block("93a60a56-a3c8-11da-a746-0800200b9a66")
    repo = TidasSchemaRepository()
    schema_node = repo.resolve_with_references(
        "tidas_flows.json",
        "/properties/flowDataSet/properties/flowProperties",
    )
    jsonschema.validate(instance=block, schema=schema_node)
