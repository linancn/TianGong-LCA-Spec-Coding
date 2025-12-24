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
"""

from __future__ import annotations

import argparse
import csv
import importlib.resources as res
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from tiangong_lca_spec.core.config import get_settings
from tiangong_lca_spec.core.constants import build_dataset_format_reference
from tiangong_lca_spec.core.mcp_client import MCPToolClient
from tiangong_lca_spec.core.uris import build_local_dataset_uri

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


def _lang_entry(text: str, lang: str = "en") -> dict[str, Any]:
    return {"@xml:lang": lang, "#text": text}


def _contact_reference() -> dict[str, Any]:
    uuid_value = "f4b4c314-8c4c-4c83-968f-5b3c7724f6a8"
    version = "01.00.000"
    return {
        "@type": "contact data set",
        "@refObjectId": uuid_value,
        "@uri": build_local_dataset_uri("contact data set", uuid_value, version),
        "@version": version,
        "common:shortDescription": [
            _lang_entry("Tiangong LCA Data Working Group", "en"),
            _lang_entry("天工LCA数据团队", "zh"),
        ],
    }


def _compliance_block() -> dict[str, Any]:
    uuid_value = "d92a1a12-2545-49e2-a585-55c259997756"
    version = "20.20.002"
    return {
        "compliance": {
            "common:referenceToComplianceSystem": {
                "@refObjectId": uuid_value,
                "@type": "source data set",
                "@uri": build_local_dataset_uri("source", uuid_value, version),
                "@version": version,
                "common:shortDescription": _lang_entry("ILCD Data Network - Entry-level", "en"),
            },
            "common:approvalOfOverallCompliance": "Fully compliant",
        }
    }


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


def build_flow_dataset(entry: FlowInput, classification: list[dict[str, str]]) -> tuple[str, dict[str, Any]]:
    base_en = (entry.base_en or entry.leaf_name).strip()
    base_zh = (entry.base_zh or entry.leaf_name_zh or base_en).strip()
    treatment = (entry.treatment or "Unspecified treatment").strip()
    mix = (entry.mix or "Production mix, at plant").strip()
    en_synonyms = _ensure_list(entry.en_synonyms) or [base_en]
    zh_synonyms = _ensure_list(entry.zh_synonyms) or [base_zh]
    comment = entry.comment or entry.desc or entry.leaf_name

    flow_uuid = str(uuid4())
    version = "01.01.000"
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    dataset = {
        "@xmlns": "http://lca.jrc.it/ILCD/Flow",
        "@xmlns:common": "http://lca.jrc.it/ILCD/Common",
        "@xmlns:ecn": "http://eplca.jrc.ec.europa.eu/ILCD/Extensions/2018/ECNumber",
        "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "@locations": "../ILCDLocations.xml",
        "@version": "1.1",
        "@xsi:schemaLocation": "http://lca.jrc.it/ILCD/Flow ../../schemas/ILCD_FlowDataSet.xsd",
        "flowInformation": {
            "dataSetInformation": {
                "common:UUID": flow_uuid,
                "name": {
                    "baseName": [
                        _lang_entry(base_en, "en"),
                        _lang_entry(base_zh, "zh"),
                    ],
                    "treatmentStandardsRoutes": [
                        _lang_entry(treatment, "en"),
                    ],
                    "mixAndLocationTypes": [
                        _lang_entry(mix, "en"),
                    ],
                },
                "common:synonyms": [
                    _lang_entry("; ".join(en_synonyms), "en"),
                    _lang_entry("; ".join(zh_synonyms), "zh"),
                ],
                "common:generalComment": [
                    _lang_entry(str(comment), "en"),
                ],
                "classificationInformation": {"common:classification": {"common:class": classification}},
            },
            "quantitativeReference": {
                "referenceToReferenceFlowProperty": "0",
            },
        },
        "modellingAndValidation": {
            "LCIMethod": {"typeOfDataSet": "Product flow"},
            "complianceDeclarations": _compliance_block(),
        },
        "administrativeInformation": {
            "dataEntryBy": {
                "common:timeStamp": timestamp,
                "common:referenceToDataSetFormat": build_dataset_format_reference(),
                "common:referenceToPersonOrEntityEnteringTheData": _contact_reference(),
            },
            "publicationAndOwnership": {
                "common:dataSetVersion": version,
                "common:referenceToOwnershipOfDataSet": _contact_reference(),
            },
        },
        "flowProperties": {
            "flowProperty": {
                "@dataSetInternalID": "0",
                "meanValue": "1.0",
                "referenceToFlowPropertyDataSet": {
                    "@type": "flow property data set",
                    "@refObjectId": "93a60a56-a3c8-11da-a746-0800200b9a66",
                    "@uri": "../flowproperties/93a60a56-a3c8-11da-a746-0800200b9a66.xml",
                    "@version": "03.00.003",
                    "common:shortDescription": _lang_entry("Mass", "en"),
                },
            }
        },
    }
    return flow_uuid, dataset


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
        payload = {"operation": "select", "table": "flows", "id": args.select_id}
        with MCPToolClient(settings) as client:
            result = client.invoke_json_tool(settings.flow_search_service_name, "Database_CRUD_Tool", payload)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    if not args.input:
        parser.error("--input is required unless --select-id is used.")

    entries = load_entries(args.input)
    if not entries:
        print("No valid entries found in input.", file=sys.stderr)
        return 1

    log_rows: list[dict[str, str]] = []
    successes = failures = 0

    client: MCPToolClient | None = None
    try:
        client = MCPToolClient(settings) if args.commit else None
        for entry in entries:
            try:
                classification = build_classification_path(entry.class_id, schema_path)
                flow_uuid, dataset = build_flow_dataset(entry, classification)
                payload = {
                    "operation": "insert",
                    "table": "flows",
                    "id": flow_uuid,
                    "jsonOrdered": {"flowDataSet": dataset},
                }
                if args.commit and client:
                    result = client.invoke_json_tool(settings.flow_search_service_name, "Database_CRUD_Tool", payload)
                    message = f"inserted version {dataset['administrativeInformation']['publicationAndOwnership']['common:dataSetVersion']}"
                    status = "success"
                    print(f"[OK] {entry.class_id} {entry.leaf_name} -> {flow_uuid}")
                    successes += 1
                    _ = result  # silence lint; result available for future use
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
