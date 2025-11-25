#!/usr/bin/env python
# ruff: noqa: E402
"""Stage 3 (JSON-LD): publish converted ILCD datasets via Database_CRUD_Tool."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

try:
    from scripts.md._workflow_common import resolve_run_id  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    from _workflow_common import resolve_run_id  # type: ignore
from tiangong_lca_spec.core.logging import configure_logging, get_logger
from tiangong_lca_spec.publishing.crud import DatabaseCrudClient

LOGGER = get_logger(__name__)


def _iterate_datasets(directory: Path) -> list[tuple[Path, dict[str, Any]]]:
    if not directory.exists():
        return []
    datasets: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(directory.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"Failed to parse {path}: {exc}") from exc
        if isinstance(payload, dict):
            datasets.append((path, payload))
    return datasets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True, help="Run identifier containing JSON-LD exports.")
    parser.add_argument(
        "--exports",
        type=Path,
        help="Optional override for artifacts/<run_id>/exports path.",
    )
    parser.add_argument(
        "--validation-report",
        type=Path,
        help="Optional override for validation report path (used to ensure publishing only after success).",
    )
    parser.add_argument("--commit", action="store_true", help="Actually invoke Database_CRUD_Tool (default: dry run).")
    parser.add_argument("--skip-processes", action="store_true", help="Skip publishing process datasets.")
    parser.add_argument("--skip-flows", action="store_true", help="Skip publishing flow datasets.")
    parser.add_argument("--skip-flow-properties", action="store_true", help="Skip publishing flow property datasets.")
    parser.add_argument("--skip-unit-groups", action="store_true", help="Skip publishing unit group datasets.")
    parser.add_argument("--skip-sources", action="store_true", help="Skip publishing source datasets.")
    return parser.parse_args()


def _check_validation(report_path: Path | None) -> None:
    if not report_path or not report_path.exists():
        return
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    findings = payload.get("validation_report", [])
    for entry in findings:
        if isinstance(entry, dict) and entry.get("severity") == "error":
            raise SystemExit(f"Validation report {report_path} still contains errors; aborting publish.")


def _publish_dataset(client: DatabaseCrudClient, table: str, payload: dict[str, Any], dry_run: bool) -> None:
    if table == "flows":
        if dry_run:
            return
        client.insert_flow(payload)
    elif table == "processes":
        if dry_run:
            return
        client.insert_process(payload)
    else:
        if dry_run:
            return
        client._invoke(
            {
                "operation": "insert",
                "table": table,
                "id": _resolve_dataset_uuid(table, payload),
                "jsonOrdered": payload,
            }
        )


def _resolve_dataset_uuid(table: str, payload: dict[str, Any]) -> str:
    if table == "flowproperties":
        info = payload.get("flowPropertyDataSet", {}).get("flowPropertiesInformation", {}).get("dataSetInformation", {})
    elif table == "unitgroups":
        info = payload.get("unitGroupDataSet", {}).get("unitGroupInformation", {}).get("dataSetInformation", {})
    elif table == "sources":
        info = payload.get("sourceDataSet", {}).get("sourceInformation", {}).get("dataSetInformation", {})
    else:
        info = {}
    uuid_value = info.get("common:UUID")
    if not uuid_value:
        raise SystemExit(f"Dataset for table {table} missing common:UUID")
    return uuid_value


def main() -> None:
    args = parse_args()
    configure_logging()
    run_id = resolve_run_id(args.run_id)
    exports_dir = args.exports or Path("artifacts") / run_id / "exports"
    validation_report = args.validation_report or Path("artifacts") / run_id / "cache" / "tidas_validation.json"

    _check_validation(validation_report if validation_report.exists() else None)

    dry_run = not args.commit
    client = DatabaseCrudClient()

    try:
        if not args.skip_unit_groups:
            datasets = _iterate_datasets(exports_dir / "unitgroups")
            for path, payload in datasets:
                LOGGER.info("jsonld_stage3.publish_unit_group", path=str(path), dry_run=dry_run)
                _publish_dataset(client, "unitgroups", payload, dry_run)

        if not args.skip_flow_properties:
            datasets = _iterate_datasets(exports_dir / "flowproperties")
            for path, payload in datasets:
                LOGGER.info("jsonld_stage3.publish_flow_property", path=str(path), dry_run=dry_run)
                _publish_dataset(client, "flowproperties", payload, dry_run)

        if not args.skip_flows:
            datasets = _iterate_datasets(exports_dir / "flows")
            for path, payload in datasets:
                LOGGER.info("jsonld_stage3.publish_flow", path=str(path), dry_run=dry_run)
                _publish_dataset(client, "flows", payload, dry_run)

        if not args.skip_processes:
            datasets = _iterate_datasets(exports_dir / "processes")
            for path, payload in datasets:
                LOGGER.info("jsonld_stage3.publish_process", path=str(path), dry_run=dry_run)
                _publish_dataset(client, "processes", payload, dry_run)

        if not args.skip_sources:
            datasets = _iterate_datasets(exports_dir / "sources")
            for path, payload in datasets:
                LOGGER.info("jsonld_stage3.publish_source", path=str(path), dry_run=dry_run)
                _publish_dataset(client, "sources", payload, dry_run)

    finally:
        client.close()

    status = "COMMITTED" if args.commit else "DRY-RUN"
    print(f"[jsonld-stage3] Publish complete ({status}) for run {run_id}")


if __name__ == "__main__":
    main()
