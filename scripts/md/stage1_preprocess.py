#!/usr/bin/env python
"""Stage 1: preprocess the paper markdown payload."""

from __future__ import annotations

import argparse
from pathlib import Path

try:
    from scripts.md._workflow_common import (  # type: ignore
        ensure_run_cache_dir,
        generate_run_id,
        load_paper,
        save_latest_run_id,
    )
except ModuleNotFoundError:  # pragma: no cover - allows direct CLI execution
    from _workflow_common import (
        ensure_run_cache_dir,
        generate_run_id,
        load_paper,
        save_latest_run_id,
    )

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
        "--run-id",
        help=("Identifier used to group artifacts under artifacts/<run_id>/. " "Defaults to a UTC timestamp when omitted."),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=("Optional override for the cleaned markdown path. " "Defaults to artifacts/<run_id>/cache/stage1_clean_text.md."),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_id = args.run_id or generate_run_id()
    cache_dir = ensure_run_cache_dir(run_id)
    save_latest_run_id(run_id)

    paper_md_json = load_paper(args.paper)
    clean_text = preprocess_paper(paper_md_json)
    cleaned_markdown = clean_text.strip()
    output_path = args.output or cache_dir / "stage1_clean_text.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(f"{cleaned_markdown}\n", encoding="utf-8")
    print(f"Run ID: {run_id}")
    print(f"Clean markdown written to {output_path}")


if __name__ == "__main__":
    main()
