#!/usr/bin/env python
"""Stage 5: run TIDAS validation against merged process datasets."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from _workflow_common import dump_json

from tiangong_lca_spec.core.models import ProcessDataset
from tiangong_lca_spec.tidas_validation import TidasValidationService


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
                process_data_set=item.get("process_data_set"),
            )
        )
    return datasets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--process-datasets",
        type=Path,
        default=Path("artifacts/stage4_process_datasets.json"),
        help="Process dataset JSON emitted by stage4_merge_datasets.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/stage5_validation.json"),
        help="Where to store the validation report.",
    )
    parser.add_argument(
        "--skip",
        action="store_true",
        help="Skip calling the remote TIDAS validation MCP tool.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    datasets = _read_process_datasets(args.process_datasets)

    if args.skip:
        findings: list[dict[str, Any]] = []
    else:
        service = TidasValidationService()
        try:
            validation = service.validate(datasets)
        finally:
            service.close()
        findings = [asdict(item) for item in validation]

    dump_json({"validation_report": findings}, args.output)
    print(f"Validation findings count={len(findings)} -> {args.output}")


if __name__ == "__main__":
    main()
