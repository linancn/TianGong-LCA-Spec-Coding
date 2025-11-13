#!/usr/bin/env python
"""Stage 4: optionally publish unmatched flows and validated processes."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts._workflow_common import (  # type: ignore
        dump_json,
        ensure_run_cache_dir,
        resolve_run_id,
        run_cache_path,
        save_latest_run_id,
    )
except ModuleNotFoundError:  # pragma: no cover - executed when run as CLI
    from _workflow_common import (  # type: ignore
        dump_json,
        ensure_run_cache_dir,
        resolve_run_id,
        run_cache_path,
        save_latest_run_id,
    )

from tiangong_lca_spec.core.logging import get_logger
from tiangong_lca_spec.publishing import FlowPropertyOverride, FlowPublisher, ProcessPublisher
from tiangong_lca_spec.workflow.artifacts import build_export_filename, resolve_dataset_version

LOGGER = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-id",
        help=("Identifier used to locate run artifacts under artifacts/<run_id>/. " "Defaults to the most recent run recorded by earlier stages."),
    )
    parser.add_argument(
        "--alignment",
        type=Path,
        help=("Optional override for the Stage 3 alignment path. " "Defaults to artifacts/<run_id>/cache/stage3_alignment.json."),
    )
    parser.add_argument(
        "--process-datasets",
        type=Path,
        help=("Optional override for the merged process datasets path. " "Defaults to artifacts/<run_id>/cache/process_datasets.json."),
    )
    parser.add_argument(
        "--validation",
        type=Path,
        help=("Optional override for the validation report emitted by Stage 3. " "Defaults to artifacts/<run_id>/cache/tidas_validation.json."),
    )
    parser.add_argument(
        "--update-alignment",
        action="store_true",
        help="When publishing flows, replace placeholders inside the alignment file.",
    )
    parser.add_argument(
        "--update-datasets",
        action="store_true",
        help=("When publishing flows, replace placeholders inside process datasets and workflow " "result."),
    )
    parser.add_argument(
        "--workflow-result",
        type=Path,
        help=("Optional override for the workflow result bundle emitted by Stage 3. " "Defaults to artifacts/<run_id>/cache/workflow_result.json."),
    )
    parser.add_argument(
        "--publish-flows",
        action="store_true",
        help="Publish unmatched flows discovered in the alignment file.",
    )
    parser.add_argument(
        "--publish-processes",
        action="store_true",
        help=("Publish process datasets after confirming the artifact validation report has no " "blocking errors."),
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help=("Actually invoke Database_CRUD_Tool. Without this flag the command runs in dry-run " "mode."),
    )
    parser.add_argument(
        "--dry-run-output",
        type=Path,
        help=("Optional override for the dry-run preview payload path. " "Defaults to artifacts/<run_id>/cache/stage4_publish_preview.json."),
    )
    parser.add_argument(
        "--default-flow-property",
        dest="default_flow_property",
        help="Fallback flow property UUID when no hints or overrides resolve (default: Mass).",
    )
    parser.add_argument(
        "--flow-property-overrides",
        type=Path,
        help=("Path to a JSON file describing flow property overrides. " "Each entry must contain 'exchange', 'flow_property_uuid', and optional 'process', 'mean_value'."),
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


def _match_update(updates: dict[tuple[str | None, str], dict[str, Any]], process: str, exchange: str):
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

            exchange_name = node.get("exchangeName")
            if not exchange_name:
                candidate_fields = [
                    node.get("name"),
                    node.get("flowName"),
                ]
                for field in candidate_fields:
                    candidate_text = _coerce_text(field)
                    if candidate_text:
                        exchange_name = candidate_text
                        break
            if not exchange_name:
                ref_block = node.get("referenceToFlowDataSet")
                if isinstance(ref_block, dict):
                    short_desc = _coerce_text(ref_block.get("common:shortDescription"))
                    if short_desc:
                        exchange_name = short_desc.split(";")[0].strip()
            if not exchange_name:
                selected = node.get("matchingDetail", {})
                if isinstance(selected, dict):
                    candidate = selected.get("selectedCandidate") or {}
                    exchange_name = _coerce_text(candidate.get("base_name"))

            if exchange_name:
                ref = node.get("referenceToFlowDataSet")
                needs_update = False
                if ref is None:
                    needs_update = True
                elif isinstance(ref, dict) and ref.get("unmatched:placeholder"):
                    needs_update = True
                if needs_update:
                    replacement = _match_update(updates, process_hint_local or "Unknown process", exchange_name)
                    if replacement:
                        replacement = dict(replacement)
                        replacement.pop("unmatched:placeholder", None)
                        node["referenceToFlowDataSet"] = replacement
                        if isinstance(ref, dict):
                            ref.pop("unmatched:placeholder", None)
                        replacements += 1
            for value in node.values():
                _walk(value, process_hint_local)
        elif isinstance(node, list):
            for item in node:
                _walk(item, process_hint)

    _walk(payload, None)
    return replacements


def _load_flow_property_overrides(path: Path | None) -> dict[tuple[str | None, str], FlowPropertyOverride]:
    overrides: dict[tuple[str | None, str], FlowPropertyOverride] = {}
    if path is None:
        return overrides
    if not path.exists():
        raise SystemExit(f"Flow property overrides file not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        entries = payload.get("overrides") or payload.get("entries") or payload.get("data")
        if entries is None:
            entries = [payload]
    else:
        entries = payload

    if isinstance(entries, dict):
        entries = [entries]
    if not isinstance(entries, list):
        raise SystemExit("Flow property overrides must be a list or an object containing an 'overrides' array.")

    for index, item in enumerate(entries):
        if not isinstance(item, dict):
            raise SystemExit(f"Override entry #{index} must be an object.")
        exchange = item.get("exchange")
        flow_property_uuid = item.get("flow_property_uuid")
        if not exchange or not flow_property_uuid:
            raise SystemExit(f"Override entry #{index} must define 'exchange' and 'flow_property_uuid'.")
        process = item.get("process")
        process_key = str(process).strip() if process not in (None, "") else None
        mean_value = item.get("mean_value")
        overrides[(process_key, str(exchange))] = FlowPropertyOverride(
            flow_property_uuid=str(flow_property_uuid),
            mean_value=str(mean_value) if mean_value not in (None, "") else None,
        )
    return overrides


def main() -> None:
    args = parse_args()
    dry_run = not args.commit
    run_id = resolve_run_id(args.run_id)
    ensure_run_cache_dir(run_id)
    save_latest_run_id(run_id)

    alignment_path = args.alignment or run_cache_path(run_id, "stage3_alignment.json")
    process_datasets_path = args.process_datasets or run_cache_path(run_id, "process_datasets.json")
    validation_path = args.validation or run_cache_path(run_id, "tidas_validation.json")
    workflow_result_path = args.workflow_result or run_cache_path(run_id, "workflow_result.json")
    dry_run_output_path = args.dry_run_output or run_cache_path(run_id, "stage4_publish_preview.json")

    alignment = _load_json(alignment_path)
    alignment_entries = alignment.get("alignment") or []
    updates: dict[tuple[str | None, str], dict[str, Any]] = {}
    for entry in alignment_entries:
        process_name = entry.get("process_name") or "Unknown process"
        origin = entry.get("origin_exchanges") or {}
        for exchanges in origin.values():
            for exchange in exchanges or []:
                if not isinstance(exchange, dict):
                    continue
                exchange_name = exchange.get("exchangeName")
                if not exchange_name:
                    continue
                ref = exchange.get("referenceToFlowDataSet")
                if isinstance(ref, dict) and not ref.get("unmatched:placeholder"):
                    updates[(process_name, exchange_name)] = ref
                    updates.setdefault((None, exchange_name), ref)
    process_payload: dict[str, Any] | None = None
    workflow_payload: dict[str, Any] | None = None
    process_replacements = 0
    workflow_replacements = 0
    flow_plans: list = []
    flow_results: list[dict[str, Any]] = []
    process_results: list[dict[str, Any]] = []
    publish_datasets: list[dict[str, Any]] = []
    flow_property_overrides = _load_flow_property_overrides(args.flow_property_overrides)

    if args.publish_flows:
        flow_publisher = FlowPublisher(
            dry_run=dry_run,
            default_flow_property_uuid=args.default_flow_property,
            flow_property_overrides=flow_property_overrides,
        )
        try:
            plans = flow_publisher.prepare_from_alignment(alignment_entries)
            flow_plans = plans
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
                                "publication_mode": plan.mode,
                                "flow_property_uuid": plan.flow_property_uuid,
                                "dataset": {"flowDataSet": plan.dataset},
                            }
                            for plan in plans
                        ],
                    },
                    dry_run_output_path,
                )
                LOGGER.info(
                    "stage4.dry_run_saved",
                    path=str(dry_run_output_path),
                    flow_count=len(plans),
                )
            else:
                dump_json(
                    {
                        "mode": "committed",
                        "results": results,
                    },
                    dry_run_output_path,
                )
                LOGGER.info(
                    "stage4.commit_results_saved",
                    path=str(dry_run_output_path),
                    created=len(results),
                )
                flow_results = results
        finally:
            flow_publisher.close()

    if args.publish_processes:
        validation = _load_json(validation_path)
        findings = validation.get("validation_report") or []
        blocking = [item for item in findings if item.get("severity") == "error"]
        if blocking:
            raise SystemExit("Artifact validation reports blocking errors; publishing aborted.")
        process_payload = process_payload or _load_json(process_datasets_path)
        if updates:
            process_replacements = _update_process_payload(process_payload, updates)
        datasets = process_payload.get("process_datasets") or []
        exports_root = Path("artifacts") / run_id / "exports" / "processes"
        for dataset_entry in datasets:
            ilcd = dataset_entry.get("process_data_set")
            if not isinstance(ilcd, dict):
                continue
            uuid_value = _coerce_text(ilcd.get("processInformation", {}).get("dataSetInformation", {}).get("common:UUID"))
            if uuid_value:
                dataset_version = resolve_dataset_version(ilcd)
                export_filename = build_export_filename(uuid_value, dataset_version)
                export_path = exports_root / export_filename
                if export_path.exists():
                    export_payload = _load_json(export_path)
                    export_dataset = export_payload.get("processDataSet")
                    if isinstance(export_dataset, dict):
                        ilcd = export_dataset
            exchanges_block = ilcd.get("exchanges", {}).get("exchange")
            if not exchanges_block:
                LOGGER.warning(
                    "process_publish.skipped_empty_exchanges",
                    uuid=uuid_value,
                    name=_coerce_text(ilcd.get("processInformation", {}).get("dataSetInformation", {}).get("name", {}).get("baseName")),
                )
                continue
            _update_process_payload(ilcd, updates)
            publish_datasets.append(ilcd)
        process_publisher = ProcessPublisher(dry_run=dry_run)
        try:
            results = process_publisher.publish(publish_datasets)
            if dry_run:
                dump_json(
                    {
                        "mode": "dry-run",
                        "processes": datasets,
                    },
                    dry_run_output_path,
                )
            else:
                dump_json(
                    {
                        "mode": "committed",
                        "results": results,
                    },
                    dry_run_output_path,
                )
                process_results = results
        finally:
            process_publisher.close()

    if updates:
        if args.update_alignment:
            replacements = _update_alignment_entries(alignment_entries, updates)
            dump_json({"alignment": alignment_entries}, alignment_path)
            LOGGER.info(
                "stage4.alignment_updated",
                path=str(alignment_path),
                replacements=replacements,
            )
        if args.update_datasets:
            if process_payload is None:
                process_payload = _load_json(process_datasets_path)
                process_replacements = _update_process_payload(process_payload, updates)
            dump_json(process_payload, process_datasets_path)
            if workflow_result_path.exists():
                workflow_payload = workflow_payload or _load_json(workflow_result_path)
                workflow_replacements = _update_process_payload(workflow_payload, updates)
                dump_json(workflow_payload, workflow_result_path)
                LOGGER.info(
                    "stage4.workflow_updated",
                    path=str(workflow_result_path),
                    replacements=workflow_replacements,
                )
            LOGGER.info(
                "stage4.datasets_updated",
                path=str(process_datasets_path),
                replacements=process_replacements,
            )

    if not dry_run and (args.publish_flows or args.publish_processes):
        flag_path = run_cache_path(run_id, "published.json")
        summary_payload = {
            "published_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "flows_planned": len(flow_plans),
            "flows_committed": len(flow_results),
            "processes_planned": len(publish_datasets),
            "processes_committed": len(process_results),
        }
        flag_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
        LOGGER.info("stage4.published_flag_written", path=str(flag_path))

    if not args.publish_flows and not args.publish_processes:
        LOGGER.info("stage4.noop", message="Nothing selected to publish.")


if __name__ == "__main__":
    main()
