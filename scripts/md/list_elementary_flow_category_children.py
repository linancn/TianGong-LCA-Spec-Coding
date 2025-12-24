#!/usr/bin/env python3
"""
CLI helper to explore the TIDAS elementary flow category hierarchy.
"""

from __future__ import annotations

import importlib.resources as resources
import sys
from pathlib import Path

from _level_hierarchy_cli import run_cli


def main(argv: list[str]) -> int:
    default_schema = Path(resources.files("tidas_tools.tidas.schemas") / "tidas_flows_elementary_category.json")
    return run_cli(argv, default_schema)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
