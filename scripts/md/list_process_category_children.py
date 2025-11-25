#!/usr/bin/env python3
"""
CLI helper to explore the TIDAS process category hierarchy.
"""

from __future__ import annotations

import sys
from pathlib import Path

from _level_hierarchy_cli import run_cli


def main(argv: list[str]) -> int:
    repo_root = Path(__file__).resolve().parents[2]
    default_schema = repo_root / "src" / "tidas" / "schemas" / "tidas_processes_category.json"
    return run_cli(argv, default_schema)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
