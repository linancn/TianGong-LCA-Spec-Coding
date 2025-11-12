#!/usr/bin/env python
"""Utility CLI for browsing flow property mappings."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Iterable
from tiangong_lca_spec.tidas.flow_property_registry import FlowPropertyRegistry, get_default_registry


def _as_serializable(descriptor) -> dict:
    payload = asdict(descriptor)
    if "unit_group" in payload and isinstance(payload["unit_group"], dict):
        payload["unit_group"]["units"] = list(payload["unit_group"].get("units", ()))
    return payload


def _parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List all flow properties")
    list_parser.add_argument("--classification", help="Filter by classification substring")
    list_parser.add_argument("--contains", help="Filter by property name substring (case-insensitive)")

    show_parser = subparsers.add_parser("show", help="Show a single flow property")
    show_group = show_parser.add_mutually_exclusive_group(required=True)
    show_group.add_argument("--uuid", help="Flow property UUID")
    show_group.add_argument("--name", help="Exact flow property name")

    match_parser = subparsers.add_parser("match-unit", help="Find flow properties containing a unit name")
    match_parser.add_argument("--unit", required=True, help="Unit label to search for (case-insensitive)")

    emit_parser = subparsers.add_parser("emit-block", help="Emit ILCD flowProperties block for a property")
    emit_group = emit_parser.add_mutually_exclusive_group(required=True)
    emit_group.add_argument("--uuid", help="Flow property UUID")
    emit_group.add_argument("--name", help="Exact flow property name")
    emit_parser.add_argument("--mean-value", default="1.0", help="Mean value to embed in the block (default: 1.0)")
    emit_parser.add_argument(
        "--internal-id",
        help="Override the @dataSetInternalID for the flowProperty element",
    )
    emit_parser.add_argument(
        "--version",
        help="Override the flow property dataset version embedded in the reference",
    )

    parser.add_argument("--mapping-path", type=str, help="Optional path to the mapping JSON file")
    return parser.parse_args(list(argv) if argv is not None else None)


def _load_registry(args: argparse.Namespace) -> FlowPropertyRegistry:
    if args.mapping_path:
        return FlowPropertyRegistry(Path(args.mapping_path))
    return get_default_registry()


def cmd_list(args: argparse.Namespace, registry: FlowPropertyRegistry) -> None:
    items = []
    for descriptor in registry.list():
        if args.classification:
            if not any(args.classification.lower() in token.lower() for token in descriptor.classification):
                continue
        if args.contains and args.contains.lower() not in descriptor.name.lower():
            continue
        items.append(_as_serializable(descriptor))
    print(json.dumps(items, indent=2, ensure_ascii=False))


def cmd_show(args: argparse.Namespace, registry: FlowPropertyRegistry) -> None:
    if args.uuid:
        descriptor = registry.get(args.uuid)
    else:
        descriptor = registry.find(args.name)
        if descriptor is None:
            raise SystemExit(f"Flow property not found: {args.name}")
    print(json.dumps(_as_serializable(descriptor), indent=2, ensure_ascii=False))


def cmd_match_unit(args: argparse.Namespace, registry: FlowPropertyRegistry) -> None:
    matches = registry.search_by_unit(args.unit)
    payload = [_as_serializable(descriptor) for descriptor in matches]
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def cmd_emit_block(args: argparse.Namespace, registry: FlowPropertyRegistry) -> None:
    if args.uuid:
        descriptor = registry.get(args.uuid)
    else:
        descriptor = registry.find(args.name)
        if descriptor is None:
            raise SystemExit(f"Flow property not found: {args.name}")
    block = registry.build_flow_property_block(
        descriptor.uuid,
        mean_value=args.mean_value,
        data_set_internal_id=args.internal_id,
        version_override=args.version,
    )
    print(json.dumps(block, indent=2, ensure_ascii=False))


def main(argv: Iterable[str] | None = None) -> None:
    args = _parse_args(argv)
    registry = _load_registry(args)
    if args.command == "list":
        cmd_list(args, registry)
    elif args.command == "show":
        cmd_show(args, registry)
    elif args.command == "match-unit":
        cmd_match_unit(args, registry)
    elif args.command == "emit-block":
        cmd_emit_block(args, registry)
    else:
        raise SystemExit(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:  # pragma: no cover
        sys.exit(130)
