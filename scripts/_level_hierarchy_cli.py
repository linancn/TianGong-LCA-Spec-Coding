"""
Utilities for CLI scripts that explore level-based TIDAS classification schemas.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


@dataclass
class Entry:
    level: int
    code: str
    description: str


ChildrenMap = Dict[str, List[Tuple[str, str]]]


def load_entries(schema_path: Path) -> List[Entry]:
    with schema_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    entries: List[Entry] = []
    for item in data.get("oneOf", []):
        props = item.get("properties", {})
        code = props.get("@classId", {}).get("const")
        level_str = props.get("@level", {}).get("const")
        description = props.get("#text", {}).get("const", "")
        if code is None or level_str is None:
            continue
        try:
            level = int(level_str)
        except ValueError:
            continue
        entries.append(Entry(level=level, code=code, description=description))
    return entries


def build_child_map(entries: Iterable[Entry]) -> Tuple[List[Tuple[str, str]], ChildrenMap]:
    child_map: ChildrenMap = defaultdict(list)
    last_per_level: Dict[int, Entry] = {}
    root_children: List[Tuple[str, str]] = []

    for entry in entries:
        if entry.level == 0:
            root_children.append((entry.code, entry.description))
            child_map[""].append((entry.code, entry.description))
        else:
            parent = find_parent(entry, last_per_level)
            if parent:
                child_map[parent.code].append((entry.code, entry.description))
        last_per_level[entry.level] = entry
    return root_children, child_map


def find_parent(entry: Entry, last_per_level: Dict[int, Entry]) -> Optional[Entry]:
    target_level = entry.level - 1
    while target_level >= 0:
        parent = last_per_level.get(target_level)
        if parent:
            return parent
        target_level -= 1
    return None


def get_children(
    key: Optional[str],
    root_children: List[Tuple[str, str]],
    child_map: ChildrenMap,
) -> List[Tuple[str, str]]:
    if not key:
        return child_map.get("", root_children)
    return child_map.get(key, [])


def run_cli(argv: List[str], default_schema: Path) -> int:
    parser = argparse.ArgumentParser(
        description="List the next level of codes from a level-based TIDAS schema.",
    )
    parser.add_argument(
        "key",
        nargs="?",
        help="Code to inspect. Omit to list the top level.",
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
    entry_map = {entry.code: entry.description for entry in entries}
    root_children, child_map = build_child_map(entries)

    key = args.key
    children = get_children(key, root_children, child_map)

    if key and key not in entry_map and key not in child_map:
        print(f"Unknown key: {key}", file=sys.stderr)
        return 1

    if not children:
        if key:
            print(f"No direct children found for {key}")
        else:
            print("No codes found.")
        return 0

    for code, desc in children:
        print(f"{code}\t{desc}")
    return 0
