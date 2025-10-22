#!/usr/bin/env python
"""Stage 6: assemble the final workflow result artifact."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from _workflow_common import dump_json
from tiangong_lca_spec.core.models import ProcessDataset


def _read_process_datasets(path: Path) -> list[ProcessDataset]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "process_datasets" not in payload:
        raise SystemExit(f"Process dataset JSON must contain 'process_datasets': {path}")
    entries = payload["process_datasets"]
    if not isinstance(entries, list):
        raise SystemExit(f"'process_datasets' must be a list in {path}")

    datasets: list[ProcessDataset] = []
    for item in entries:
        if not isinstance(item, dict):
            raise SystemExit("Each process dataset entry must be an object")
        datasets.append(
            ProcessDataset(
                process_information=item.get("process_information", {}),
                modelling_and_validation=item.get("modelling_and_validation", {}),
                administrative_information=item.get("administrative_information", {}),
                exchanges=item.get("exchanges", []),
                notes=item.get("notes"),
                process_data_set=item.get("process_data_set"),
            )
        )
    return datasets


def _read_alignment(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "alignment" not in payload:
        raise SystemExit(f"Alignment JSON must contain 'alignment': {path}")
    alignment = payload["alignment"]
    if not isinstance(alignment, list):
        raise SystemExit(f"'alignment' must be a list in {path}")
    return alignment


def _read_validation(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "validation_report" not in payload:
        raise SystemExit(f"Validation JSON must contain 'validation_report': {path}")
    report = payload["validation_report"]
    if not isinstance(report, list):
        raise SystemExit(f"'validation_report' must be a list in {path}")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--process-datasets",
        type=Path,
        default=Path("artifacts/stage4_process_datasets.json"),
        help="Process dataset JSON emitted by stage4_merge_datasets.",
    )
    parser.add_argument(
        "--alignment",
        type=Path,
        default=Path("artifacts/stage3_alignment.json"),
        help="Alignment JSON emitted by stage3_align_flows.",
    )
    parser.add_argument(
        "--validation",
        type=Path,
        default=Path("artifacts/stage5_validation.json"),
        help="Validation JSON emitted by stage5_validate.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/workflow_result.json"),
        help="Where to store the final workflow artifact.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    datasets = _read_process_datasets(args.process_datasets)
    alignment = _read_alignment(args.alignment)
    validation = _read_validation(args.validation)

    payload = {
        "process_datasets": [dataset.as_dict() for dataset in datasets],
        "alignment": alignment,
        "validation_report": validation,
    }
    dump_json(payload, args.output)
    print(
        f"Workflow artifact written to {args.output} "
        f"(datasets={len(datasets)}, alignment_entries={len(alignment)}, findings={len(validation)})"
    )


if __name__ == "__main__":
    main()
