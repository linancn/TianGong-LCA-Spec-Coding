#!/usr/bin/env python
"""Stage 1: preprocess the paper markdown payload."""

from __future__ import annotations

import argparse
from pathlib import Path

from _workflow_common import load_paper

from tiangong_lca_spec.process_extraction import preprocess_paper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--paper",
        type=Path,
        default=Path("test/data/test-paper.json"),
        help="Path to the paper markdown JSON payload.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/stage1_clean_text.md"),
        help="Where to store the cleaned markdown file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paper_md_json = load_paper(args.paper)
    clean_text = preprocess_paper(paper_md_json)
    cleaned_markdown = clean_text.strip()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(f"{cleaned_markdown}\n", encoding="utf-8")
    print(f"Clean markdown written to {args.output}")


if __name__ == "__main__":
    main()
