#!/usr/bin/env python3
"""
CLI helper to explore the TIDAS location schema hierarchy.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

LocationEntry = Tuple[str, str]


def load_entries(schema_path: Path) -> List[LocationEntry]:
    with schema_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    entries = []
    for item in data.get("oneOf", []):
        code = item.get("const")
        if not code:
            continue
        desc = item.get("description", "")
        entries.append((code, desc))
    return entries


def build_hierarchy(
    entries: Iterable[LocationEntry],
) -> Tuple[List[LocationEntry], Dict[str, List[LocationEntry]]]:
    child_map: Dict[str, List[LocationEntry]] = defaultdict(list)
    seen_children: Dict[str, set[str]] = defaultdict(set)
    root_children: List[LocationEntry] = []

    for code, desc in entries:
        parts = code.split("-")
        if len(parts) == 1:
            if code not in seen_children[""]:
                child_map[""].append((code, desc))
                root_children.append((code, desc))
                seen_children[""].add(code)
            continue
        parent = "-".join(parts[:-1])
        if code not in seen_children[parent]:
            child_map[parent].append((code, desc))
            seen_children[parent].add(code)
    return root_children, child_map


def get_children(
    key: str | None,
    root_children: List[LocationEntry],
    child_map: Dict[str, List[LocationEntry]],
) -> List[LocationEntry]:
    if not key:
        return child_map.get("", root_children)
    return child_map.get(key, [])


def main(argv: List[str]) -> int:
    repo_root = Path(__file__).resolve().parents[2]
    default_schema = repo_root / "src" / "tidas" / "schemas" / "tidas_locations_category.json"

    parser = argparse.ArgumentParser(
        description="List the next level of location codes from the TIDAS schema.",
    )
    parser.add_argument(
        "key",
        nargs="?",
        help="Location code to inspect. Omit to list the top level (e.g., CN, ZW, ...).",
    )
    parser.add_argument(
        "--schema",
        "-s",
        type=Path,
        default=default_schema,
        help=f"Path to the schema JSON file (default: {default_schema}).",
    )
    args = parser.parse_args(argv)

    if not args.schema.is_file():
        parser.error(f"Schema file not found: {args.schema}")

    entries = load_entries(args.schema)
    entry_map = {code: desc for code, desc in entries}
    root_children, child_map = build_hierarchy(entries)

    key = args.key
    children = get_children(key, root_children, child_map)

    if key and key not in entry_map and key not in child_map:
        print(f"Unknown key: {key}", file=sys.stderr)
        return 1

    if not children:
        if key:
            print(f"No direct children found for {key}")
        else:
            print("No locations found.")
        return 0

    for code, desc in children:
        print(f"{code}\t{desc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
