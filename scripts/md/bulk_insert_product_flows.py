#!/usr/bin/env python
"""
Batch insert product flows via tiangong_lca_remote Database_CRUD_Tool.

Interface:
  - Input: JSON array or JSONL with objects containing at minimum:
      { "class_id": "01132", "leaf_name": "...", "desc": "...",                 # required
        "base_en": "...", "base_zh": "...",                                     # optional names
        "en_synonyms": [...], "zh_synonyms": [...],                             # optional synonyms (string or list)
        "treatment": "...", "mix": "...", "comment": "..." }                    # optional overrides
  - Flags:
      --commit    Actually call Database_CRUD_Tool (default: dry-run preview).
      --log-csv   Path to append success/failure rows.
      --select-id UUID  Run a select for the given flow ID and exit.
      --schema    Override product flow category schema (default: packaged tidas_flows_product_category.json).

The script runs sequentially, one MCP call per flow, no implicit retries.
Flow payloads are validated/normalised via tidas_sdk before optional CRUD publish.
"""

from __future__ import annotations

import argparse
import csv
import importlib.resources as res
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from tiangong_lca_spec.core.config import get_settings
from tiangong_lca_spec.core.mcp_client import MCPToolClient
from tiangong_lca_spec.product_flow_creation import ProductFlowCreateRequest, ProductFlowCreationService
from tiangong_lca_spec.publishing.crud import DatabaseCrudClient

# ---------------------------- Data structures ---------------------------- #


@dataclass
class FlowInput:
    class_id: str
    leaf_name: str
    desc: str | None = None
    base_en: str | None = None
    leaf_name_zh: str | None = None
    base_zh: str | None = None
    treatment: str | None = None
    mix: str | None = None
    en_synonyms: list[str] | None = None
    zh_synonyms: list[str] | None = None
    comment: str | None = None


# ---------------------------- Helpers ---------------------------- #

DEFAULT_TREATMENT = "Unspecified treatment"
DEFAULT_MIX = "Production mix, at plant"


def _ensure_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    return []


def load_entries(path: Path) -> list[FlowInput]:
    raw = path.read_text(encoding="utf-8")
    items: list[Any]
    try:
        parsed = json.loads(raw)
        items = parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        items = [json.loads(line) for line in raw.splitlines() if line.strip()]

    entries: list[FlowInput] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        entries.append(
            FlowInput(
                class_id=str(item.get("class_id") or item.get("code") or "").strip(),
                leaf_name=str(item.get("leaf_name") or item.get("name") or "").strip(),
                desc=(item.get("desc") or item.get("description")),
                base_en=item.get("base_en"),
                leaf_name_zh=item.get("leaf_name_zh"),
                base_zh=item.get("base_zh"),
                treatment=item.get("treatment"),
                mix=item.get("mix"),
                en_synonyms=_ensure_list(item.get("en_synonyms")),
                zh_synonyms=_ensure_list(item.get("zh_synonyms")),
                comment=item.get("comment"),
            )
        )
    return [entry for entry in entries if entry.class_id and entry.leaf_name]


def load_product_category_entries(schema_path: Any) -> list[tuple[int, str, str]]:
    doc = json.loads(schema_path.read_text(encoding="utf-8"))
    entries: list[tuple[int, str, str]] = []
    for item in doc.get("oneOf", []):
        props = item.get("properties", {})
        code = next((props.get(key, {}).get("const") for key in ("@classId", "@catId", "@code") if props.get(key)), None)
        level = props.get("@level", {}).get("const")
        desc = props.get("#text", {}).get("const", "")
        if code is None or level is None:
            continue
        try:
            level_int = int(level)
        except ValueError:
            continue
        entries.append((level_int, str(code), str(desc)))
    return entries


def build_classification_path(target_code: str, schema_path: Any) -> list[dict[str, str]]:
    entries = load_product_category_entries(schema_path)
    child_map: dict[str, list[tuple[int, str, str]]] = defaultdict(list)
    roots: list[tuple[int, str, str]] = []
    last_per_level: dict[int, tuple[int, str, str]] = {}

    for level, code, desc in entries:
        if level == 0:
            roots.append((level, code, desc))
            child_map[""].append((level, code, desc))
        else:
            t = level - 1
            parent = None
            while t >= 0 and parent is None:
                parent = last_per_level.get(t)
                t -= 1
            if parent:
                child_map[parent[1]].append((level, code, desc))
        last_per_level[level] = (level, code, desc)

    def _dfs(node: tuple[int, str, str]) -> list[tuple[int, str, str]] | None:
        if node[1] == target_code:
            return [node]
        for child in child_map.get(node[1], []):
            res = _dfs(child)
            if res:
                return [node] + res
        return None

    for root in roots:
        result = _dfs(root)
        if result:
            return [{"@level": str(level), "@classId": code, "#text": desc} for level, code, desc in result]
    raise ValueError(f"Classification code not found: {target_code}")


def write_log(log_path: Path, rows: Iterable[dict[str, str]]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["class_id", "leaf_name", "uuid", "status", "message"]
    write_header = not log_path.exists()
    with log_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


# ---------------------------- Main routine ---------------------------- #


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", "-i", type=Path, help="Path to JSON/JSONL input (required unless --select-id).")
    parser.add_argument("--log-csv", type=Path, default=Path("artifacts/bulk_insert_product_flows_log.csv"))
    parser.add_argument("--schema", type=Path, help="Path to product flow category schema (default: packaged schema).")
    parser.add_argument("--commit", action="store_true", help="Actually call Database_CRUD_Tool (default: dry-run).")
    parser.add_argument("--select-id", help="Run select for given flow UUID and exit.")
    args = parser.parse_args(argv)

    settings = get_settings()
    schema_path = args.schema or (res.files("tidas_tools.tidas.schemas") / "tidas_flows_product_category.json")

    if args.select_id:
        crud = DatabaseCrudClient(settings)
        try:
            result = crud.select_flow_record(args.select_id)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        finally:
            crud.close()
        return 0

    if not args.input:
        parser.error("--input is required unless --select-id is used.")

    entries = load_entries(args.input)
    if not entries:
        print("No valid entries found in input.", file=sys.stderr)
        return 1

    log_rows: list[dict[str, str]] = []
    successes = failures = 0
    flow_builder = ProductFlowCreationService()

    client: MCPToolClient | None = None
    try:
        client = MCPToolClient(settings) if args.commit else None
        for entry in entries:
            try:
                classification = build_classification_path(entry.class_id, schema_path)
                request = ProductFlowCreateRequest(
                    class_id=entry.class_id,
                    classification=classification,
                    base_name_en=(entry.base_en or entry.leaf_name).strip(),
                    base_name_zh=(entry.base_zh or entry.leaf_name_zh or entry.base_en or entry.leaf_name).strip(),
                    treatment_en=(entry.treatment or DEFAULT_TREATMENT).strip(),
                    mix_en=(entry.mix or DEFAULT_MIX).strip(),
                    comment_en=str(entry.comment or entry.desc or entry.leaf_name),
                    synonyms_en=_ensure_list(entry.en_synonyms),
                    synonyms_zh=_ensure_list(entry.zh_synonyms),
                )
                result = flow_builder.build(request)
                flow_uuid = result.flow_uuid
                version = result.version
                payload = {
                    "operation": "insert",
                    "table": "flows",
                    "id": flow_uuid,
                    "jsonOrdered": result.payload,
                }
                if args.commit and client:
                    db_result = client.invoke_json_tool(settings.flow_search_service_name, "Database_CRUD_Tool", payload)
                    message = f"inserted version {version}"
                    status = "success"
                    print(f"[OK] {entry.class_id} {entry.leaf_name} -> {flow_uuid}")
                    successes += 1
                    _ = db_result  # silence lint; result available for future use
                else:
                    status = "dry-run"
                    message = "payload prepared; not sent (use --commit to publish)"
                    print(f"[DRY] {entry.class_id} {entry.leaf_name} -> {flow_uuid}")
                    successes += 1
                log_rows.append(
                    {
                        "class_id": entry.class_id,
                        "leaf_name": entry.leaf_name,
                        "uuid": flow_uuid,
                        "status": status,
                        "message": message,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                failures += 1
                log_rows.append(
                    {
                        "class_id": entry.class_id,
                        "leaf_name": entry.leaf_name,
                        "uuid": "",
                        "status": "error",
                        "message": str(exc),
                    }
                )
                print(f"[ERR] {entry.class_id} {entry.leaf_name}: {exc}", file=sys.stderr)
    finally:
        if client:
            client.close()

    write_log(args.log_csv, log_rows)
    print(f"Done. successes={successes}, failures={failures}, log={args.log_csv}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
