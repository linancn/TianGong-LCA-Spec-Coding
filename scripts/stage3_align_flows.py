#!/usr/bin/env python
"""Stage 3: align extracted exchanges against TianGong flow datasets."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from _workflow_common import dump_json

from tiangong_lca_spec.flow_alignment import FlowAlignmentService


def _read_process_blocks(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "process_blocks" not in payload:
        raise SystemExit(f"Process blocks JSON must contain 'process_blocks': {path}")
    blocks = payload["process_blocks"]
    if not isinstance(blocks, list):
        raise SystemExit(f"'process_blocks' must be a list in {path}")
    return blocks


def _read_clean_text(path: Path) -> str:
    raw = path.read_text(encoding="utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if isinstance(payload, dict) and "clean_text" in payload:
        value = payload["clean_text"]
        if not isinstance(value, str):
            raise SystemExit(f"'clean_text' must be a string in {path}")
        return value
    raise SystemExit(f"Unexpected clean text format in {path}")


def _serialise_alignment(entry: dict[str, Any]) -> dict[str, Any]:
    matched = entry.get("matched_flows") or []
    unmatched = entry.get("unmatched_flows") or []
    origin = entry.get("origin_exchanges") or {}
    return {
        "process_name": entry.get("process_name"),
        "matched_flows": [asdict(candidate) for candidate in matched],
        "unmatched_flows": [asdict(item) for item in unmatched],
        "origin_exchanges": origin,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--process-blocks",
        type=Path,
        default=Path("artifacts/stage2_process_blocks.json"),
        help="Process blocks JSON emitted by stage2_extract_processes.",
    )
    parser.add_argument(
        "--clean-text",
        type=Path,
        default=Path("artifacts/stage1_clean_text.json"),
        help="Clean text JSON emitted by stage1_preprocess.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/stage3_alignment.json"),
        help="Where to store the alignment results.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    process_blocks = _read_process_blocks(args.process_blocks)
    clean_text = _read_clean_text(args.clean_text)

    service = FlowAlignmentService()
    alignment_entries: list[dict[str, Any]] = []
    try:
        for block in process_blocks:
            dataset = block.get("processDataSet")
            if not isinstance(dataset, dict):
                raise SystemExit("Each process block must contain 'processDataSet'")
            result = service.align_exchanges(dataset, clean_text)
            alignment_entries.append(_serialise_alignment(result))
    finally:
        service.close()

    dump_json({"alignment": alignment_entries}, args.output)
    print(f"Aligned flows for {len(alignment_entries)} processes -> {args.output}")


if __name__ == "__main__":
    main()
