from __future__ import annotations

import json
import runpy
from functools import lru_cache
from io import StringIO
from pathlib import Path

import jsonschema

from tiangong_lca_spec.tidas.schema_loader import TidasSchemaRepository

MASS_UUID = "93a60a56-a3c8-11da-a746-0800200b9a66"


@lru_cache(maxsize=1)
def _load_cli_main():
    module_globals = runpy.run_path(str(Path(__file__).resolve().parents[2] / "scripts" / "flow_property_cli.py"))
    return module_globals["main"]


def test_emit_block_cli_output_validates_against_schema(monkeypatch) -> None:
    buffer = StringIO()
    monkeypatch.setattr("sys.stdout", buffer)
    cli_main = _load_cli_main()
    cli_main(["emit-block", "--uuid", MASS_UUID])
    payload = json.loads(buffer.getvalue())
    repo = TidasSchemaRepository()
    schema_node = repo.resolve_with_references(
        "tidas_flows.json",
        "/properties/flowDataSet/properties/flowProperties",
    )
    jsonschema.validate(instance=payload, schema=schema_node)
