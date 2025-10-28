#!/usr/bin/env python
"""Stage 4: optionally publish unmatched flows and validated processes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from scripts._workflow_common import dump_json  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - executed when run as CLI
    from _workflow_common import dump_json  # type: ignore

from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.publishing import FlowPublisher, ProcessPublisher

LOGGER = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--alignment",
        type=Path,
        default=Path("artifacts/stage3_alignment.json"),
        help="Alignment file emitted by stage3_align_flows.",
    )
    parser.add_argument(
        "--process-datasets",
        type=Path,
        default=Path("artifacts/process_datasets.json"),
        help="Process datasets exported by stage3_align_flows.",
    )
    parser.add_argument(
        "--validation",
        type=Path,
        default=Path("artifacts/tidas_validation.json"),
        help="Validation report written by stage3_align_flows.",
    )
    parser.add_argument(
        "--update-alignment",
        action="store_true",
        help="When publishing flows, replace placeholders inside the alignment file.",
    )
    parser.add_argument(
        "--update-datasets",
        action="store_true",
        help=(
            "When publishing flows, replace placeholders inside process datasets and workflow "
            "result."
        ),
    )
    parser.add_argument(
        "--workflow-result",
        type=Path,
        default=Path("artifacts/workflow_result.json"),
        help="Workflow bundle emitted by stage3_align_flows (used when updating placeholders).",
    )
    parser.add_argument(
        "--publish-flows",
        action="store_true",
        help="Publish unmatched flows discovered in the alignment file.",
    )
    parser.add_argument(
        "--publish-processes",
        action="store_true",
        help="Publish process datasets after confirming the artifact validation report has no blocking errors.",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help=(
            "Actually invoke Database_CRUD_Tool. Without this flag the command runs in dry-run "
            "mode."
        ),
    )
    parser.add_argument(
        "--dry-run-output",
        type=Path,
        default=Path("artifacts/stage4_publish_preview.json"),
        help="Where to dump preview payloads when running in dry-run mode.",
    )
    return parser.parse_args()


def _load_json(path: Path) -> Any:
    if not path.exists():
        raise SystemExit(f"Expected JSON file at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        text = value.get("#text")
        if isinstance(text, str):
            return text.strip()
    return str(value).strip()


def _match_update(
    updates: dict[tuple[str | None, str], dict[str, Any]], process: str, exchange: str
):
    entry = updates.get((process, exchange))
    if entry:
        return entry
    return updates.get((None, exchange))


def _update_alignment_entries(
    alignment_entries: list[dict[str, Any]],
    updates: dict[tuple[str | None, str], dict[str, Any]],
) -> int:
    replacements = 0
    for entry in alignment_entries:
        process_name = entry.get("process_name") or "Unknown process"
        origin = entry.get("origin_exchanges") or {}
        for exchanges in origin.values():
            for exchange in exchanges or []:
                ref = exchange.get("referenceToFlowDataSet")
                if isinstance(ref, dict) and ref.get("unmatched:placeholder"):
                    exchange_name = exchange.get("exchangeName")
                    if not exchange_name:
                        continue
                    replacement = _match_update(updates, process_name, exchange_name)
                    if replacement:
                        exchange["referenceToFlowDataSet"] = replacement
                        replacements += 1
    return replacements


def _update_process_payload(
    payload: Any,
    updates: dict[tuple[str | None, str], dict[str, Any]],
) -> int:
    replacements = 0

    def _walk(node: Any, process_hint: str | None = None) -> None:
        nonlocal replacements
        if isinstance(node, dict):
            if "processInformation" in node:
                info = node.get("processInformation", {})
                name_block = info.get("dataSetInformation", {}).get("name", {})
                base_name = name_block.get("baseName")
                if isinstance(base_name, list) and base_name:
                    process_hint_local = _coerce_text(base_name[0])
                else:
                    process_hint_local = process_hint
            else:
                process_hint_local = process_hint

            if "referenceToFlowDataSet" in node and "exchangeName" in node:
                ref = node.get("referenceToFlowDataSet")
                if isinstance(ref, dict) and ref.get("unmatched:placeholder"):
                    exchange_name = node.get("exchangeName")
                    if exchange_name:
                        replacement = _match_update(
                            updates, process_hint_local or "Unknown process", exchange_name
                        )
                        if replacement:
                            node["referenceToFlowDataSet"] = replacement
                            replacements += 1
            for value in node.values():
                _walk(value, process_hint_local)
        elif isinstance(node, list):
            for item in node:
                _walk(item, process_hint)

    _walk(payload, None)
    return replacements


def main() -> None:
    args = parse_args()
    dry_run = not args.commit

    alignment = _load_json(args.alignment)
    alignment_entries = alignment.get("alignment") or []
    updates: dict[tuple[str | None, str], dict[str, Any]] = {}

    if args.publish_flows:
        flow_publisher = FlowPublisher(dry_run=dry_run)
        try:
            plans = flow_publisher.prepare_from_alignment(alignment_entries)
            for plan in plans:
                key = (plan.process_name, plan.exchange_name)
                updates[key] = plan.exchange_ref
                updates[(None, plan.exchange_name)] = plan.exchange_ref
            results = flow_publisher.publish()
            if dry_run:
                dump_json(
                    {
                        "mode": "dry-run",
                        "flows": [
                            {
                                "exchange_name": plan.exchange_name,
                                "process_name": plan.process_name,
                                "uuid": plan.uuid,
                                "dataset": {"flowDataSet": plan.dataset},
                            }
                            for plan in plans
                        ],
                    },
                    args.dry_run_output,
                )
                LOGGER.info(
                    "stage4.dry_run_saved",
                    path=str(args.dry_run_output),
                    flow_count=len(plans),
                )
            else:
                dump_json(
                    {
                        "mode": "committed",
                        "results": results,
                    },
                    args.dry_run_output,
                )
                LOGGER.info(
                    "stage4.commit_results_saved",
                    path=str(args.dry_run_output),
                    created=len(results),
                )
        finally:
            flow_publisher.close()

    if args.publish_processes:
        validation = _load_json(args.validation)
        findings = validation.get("validation_report") or []
        blocking = [item for item in findings if item.get("severity") == "error"]
        if blocking:
            raise SystemExit("Artifact validation reports blocking errors; publishing aborted.")
        datasets_json = _load_json(args.process_datasets)
        datasets = datasets_json.get("process_datasets") or []
        process_publisher = ProcessPublisher(dry_run=dry_run)
        try:
            results = process_publisher.publish(datasets)
            if dry_run:
                dump_json(
                    {
                        "mode": "dry-run",
                        "processes": datasets,
                    },
                    args.dry_run_output,
                )
            else:
                dump_json(
                    {
                        "mode": "committed",
                        "results": results,
                    },
                    args.dry_run_output,
                )
        finally:
            process_publisher.close()

    if updates:
        if args.update_alignment:
            replacements = _update_alignment_entries(alignment_entries, updates)
            dump_json({"alignment": alignment_entries}, args.alignment)
            LOGGER.info(
                "stage4.alignment_updated",
                path=str(args.alignment),
                replacements=replacements,
            )
        if args.update_datasets:
            process_payload = _load_json(args.process_datasets)
            process_replacements = _update_process_payload(process_payload, updates)
            dump_json(process_payload, args.process_datasets)
            if args.workflow_result.exists():
                workflow_payload = _load_json(args.workflow_result)
                workflow_replacements = _update_process_payload(workflow_payload, updates)
                dump_json(workflow_payload, args.workflow_result)
                LOGGER.info(
                    "stage4.workflow_updated",
                    path=str(args.workflow_result),
                    replacements=workflow_replacements,
                )
            LOGGER.info(
                "stage4.datasets_updated",
                path=str(args.process_datasets),
                replacements=process_replacements,
            )

    if not args.publish_flows and not args.publish_processes:
        LOGGER.info("stage4.noop", message="Nothing selected to publish.")


if __name__ == "__main__":
    main()
