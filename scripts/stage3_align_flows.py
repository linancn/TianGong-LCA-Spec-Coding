#!/usr/bin/env python
"""Stage 3: align extracted exchanges against TianGong flow datasets."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from _workflow_common import OpenAIResponsesLLM, dump_json, load_secrets

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
    origin = entry.get("origin_exchanges") or {}
    return {
        "process_name": entry.get("process_name"),
        "matched_flows": [asdict(candidate) for candidate in matched],
        "origin_exchanges": origin,
    }


def _maybe_create_llm(path: Path | None) -> OpenAIResponsesLLM | None:
    if path is None or not path.exists():
        return None
    api_key, model = load_secrets(path)
    return OpenAIResponsesLLM(api_key=api_key, model=model)


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
    parser.add_argument(
        "--unmatched-output",
        type=Path,
        default=Path("artifacts/stage3_unmatched_flows.json"),
        help="Deprecated. Unmatched exchanges are no longer written to disk.",
    )
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".secrets/secrets.toml"),
        help="Secrets file containing OpenAI credentials for LLM-based alignment.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    process_blocks = _read_process_blocks(args.process_blocks)
    clean_text = _read_clean_text(args.clean_text)

    llm = _maybe_create_llm(args.secrets)
    service = FlowAlignmentService(llm=llm)
    alignment_entries: list[dict[str, Any]] = []
    total_unmatched = 0
    try:
        for block in process_blocks:
            dataset = block.get("processDataSet")
            if not isinstance(dataset, dict):
                raise SystemExit("Each process block must contain 'processDataSet'")
            result = service.align_exchanges(dataset, clean_text)
            alignment_entries.append(_serialise_alignment(result))
            unmatched = result.get("unmatched_flows") or []
            total_unmatched += len(unmatched)
    finally:
        service.close()

    dump_json({"alignment": alignment_entries}, args.output)
    if total_unmatched:
        print(f"Skipped storing {total_unmatched} unmatched exchanges.")
    print(f"Aligned flows for {len(alignment_entries)} processes -> {args.output}")


if __name__ == "__main__":
    main()
