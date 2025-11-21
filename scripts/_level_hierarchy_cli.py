"""
Utilities for CLI scripts that explore level-based TIDAS classification schemas.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

from tiangong_lca_spec.tidas.level_hierarchy import HierarchyNavigator, load_level_entries


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

    entries = load_level_entries(args.schema)
    navigator = HierarchyNavigator(entries)
    entry_map = {entry.code: entry.description for entry in entries}

    key = args.key
    children = navigator.children(key)

    if key and key not in entry_map and not children:
        print(f"Unknown key: {key}", file=sys.stderr)
        return 1

    if not children:
        if key:
            print(f"No direct children found for {key}")
        else:
            print("No codes found.")
        return 0

    for entry in children:
        print(f"{entry.code}\t{entry.description}")
    return 0
