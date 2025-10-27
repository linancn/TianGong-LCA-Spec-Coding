#!/usr/bin/env python
"""Stage 4: merge process blocks with aligned flow candidates."""

from __future__ import annotations

import argparse
import json
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

from _workflow_common import dump_json

from tiangong_lca_spec.core.models import FlowCandidate, ProcessDataset
from tiangong_lca_spec.process_extraction.merge import (
    determine_functional_unit,
    merge_results,
)


def _read_process_blocks(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "process_blocks" not in payload:
        raise SystemExit(f"Process blocks JSON must contain 'process_blocks': {path}")
    blocks = payload["process_blocks"]
    if not isinstance(blocks, list):
        raise SystemExit(f"'process_blocks' must be a list in {path}")
    for index, block in enumerate(blocks):
        if not isinstance(block, dict):
            raise SystemExit(f"Process block #{index} must be an object: {path}")
        if "processDataSet" not in block:
            raise SystemExit(
                "Each process block must contain 'processDataSet'. Stage 2 now writes "
                "normalised exchanges directly inside the dataset; legacy 'exchange_list' "
                "is no longer emitted."
            )
        if "exchange_list" in block and block["exchange_list"]:
            print(
                "stage4_merge_datasets: ignoring legacy 'exchange_list' data; use "
                "'processDataSet.exchanges' instead.",
                file=sys.stderr,
            )
    return blocks


def _read_alignment(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "alignment" not in payload:
        raise SystemExit(f"Alignment JSON must contain 'alignment': {path}")
    alignment = payload["alignment"]
    if not isinstance(alignment, list):
        raise SystemExit(f"'alignment' must be a list in {path}")
    return alignment


def _hydrate_flow_candidates(entry: dict[str, Any]) -> list[FlowCandidate]:
    candidates = entry.get("matched_flows") or []
    hydrated: list[FlowCandidate] = []
    for item in candidates:
        if not isinstance(item, dict):
            raise SystemExit("Matched flow entries must be objects")
        hydrated.append(FlowCandidate(**item))
    return hydrated


def _serialise_dataset(dataset: ProcessDataset) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "process_information": deepcopy(dataset.process_information),
        "modelling_and_validation": deepcopy(dataset.modelling_and_validation),
        "administrative_information": deepcopy(dataset.administrative_information),
        "exchanges": [deepcopy(exchange) for exchange in dataset.exchanges],
        "process_data_set": deepcopy(dataset.process_data_set),
    }
    notes = getattr(dataset, "notes", None)
    if notes is not None:
        payload["notes"] = deepcopy(notes)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--process-blocks",
        type=Path,
        default=Path("artifacts/stage2_process_blocks.json"),
        help="Process blocks JSON emitted by stage2_extract_processes.",
    )
    parser.add_argument(
        "--alignment",
        type=Path,
        default=Path("artifacts/stage3_alignment.json"),
        help="Alignment JSON emitted by stage3_align_flows.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/stage4_process_datasets.json"),
        help="Where to store merged process dataset structures.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    process_blocks = _read_process_blocks(args.process_blocks)
    alignment_entries = _read_alignment(args.alignment)

    matched_lookup: dict[str, list[FlowCandidate]] = {}
    origin_exchanges: dict[str, list[dict[str, Any]]] = {}
    for entry in alignment_entries:
        process_name = entry.get("process_name") or "unknown_process"
        matched_lookup[process_name] = _hydrate_flow_candidates(entry)
        origin = entry.get("origin_exchanges") or {}
        if not isinstance(origin, dict):
            raise SystemExit("origin_exchanges must be an object keyed by exchange")
        origin_exchanges[process_name] = []
        for exchanges in origin.values():
            if isinstance(exchanges, list):
                origin_exchanges[process_name].extend(exchanges)
            elif isinstance(exchanges, dict):
                origin_exchanges[process_name].append(exchanges)

    datasets = merge_results(process_blocks, matched_lookup, origin_exchanges)
    for dataset in datasets:
        func_unit = determine_functional_unit(dataset.exchanges)
        if func_unit:
            info = dict(dataset.process_information)
            processes = dict(info.get("processes", {}))
            processes["functionalUnit"] = func_unit
            info["processes"] = processes
            dataset.process_information = info

    payload = {"process_datasets": [_serialise_dataset(dataset) for dataset in datasets]}
    dump_json(payload, args.output)
    print(f"Merged {len(datasets)} process datasets -> {args.output}")


if __name__ == "__main__":
    main()
