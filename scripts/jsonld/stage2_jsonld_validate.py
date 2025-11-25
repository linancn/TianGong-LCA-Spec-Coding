#!/usr/bin/env python
# ruff: noqa: E402
"""Stage 2 (JSON-LD): export ILCD artifacts, remap UUIDs, run validation, optionally auto-publish."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

try:  # Allow execution via `python` or `python -m`
    from scripts.md._workflow_common import (  # type: ignore
        dump_json,
        ensure_run_cache_dir,
        ensure_run_exports_dir,
        resolve_run_id,
        run_cache_path,
        save_latest_run_id,
    )
except ModuleNotFoundError:  # pragma: no cover
    from _workflow_common import (  # type: ignore
        dump_json,
        ensure_run_cache_dir,
        ensure_run_exports_dir,
        resolve_run_id,
        run_cache_path,
        save_latest_run_id,
    )

from tiangong_lca_spec.jsonld.converters import (
    convert_flow_directory,
    convert_source_directory,
)
from tiangong_lca_spec.jsonld.process_overrides import (
    apply_jsonld_process_overrides,
    auto_fix_from_validation,
)
from tiangong_lca_spec.jsonld.uuid_utils import UUIDMapper
from tiangong_lca_spec.tidas.process_classification_registry import ensure_valid_classification_path
from tiangong_lca_spec.tidas_validation import TidasValidationService
from tiangong_lca_spec.workflow.artifacts import (
    DEFAULT_DATA_SET_VERSION,
    build_export_filename,
    generate_artifacts,
    resolve_dataset_version,
)


def _read_process_blocks(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "process_blocks" not in payload:
        raise SystemExit(f"Process blocks JSON must contain 'process_blocks': {path}")
    blocks = payload["process_blocks"]
    if not isinstance(blocks, list):
        raise SystemExit(f"'process_blocks' must be a list in {path}")
    for index, block in enumerate(blocks):
        if not isinstance(block, dict) or "processDataSet" not in block:
            raise SystemExit(f"Process block #{index} is invalid in {path}")
        _validate_classification(block["processDataSet"], index, path)
        apply_jsonld_process_overrides(block)
    return blocks


def _read_flow_datasets(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "flow_datasets" not in payload:
        raise SystemExit(f"Flow dataset JSON must contain 'flow_datasets': {path}")
    datasets = payload["flow_datasets"]
    if not isinstance(datasets, list):
        raise SystemExit(f"'flow_datasets' must be a list in {path}")
    return [dataset for dataset in datasets if isinstance(dataset, dict)]


def _read_source_datasets(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "source_datasets" not in payload:
        raise SystemExit(f"Source dataset JSON must contain 'source_datasets': {path}")
    datasets = payload["source_datasets"]
    if not isinstance(datasets, list):
        raise SystemExit(f"'source_datasets' must be a list in {path}")
    return [dataset for dataset in datasets if isinstance(dataset, dict)]


def _validate_classification(dataset: dict[str, Any], index: int, source: Path) -> None:
    info = dataset.get("processInformation")
    if not isinstance(info, dict):
        raise SystemExit(f"Process block #{index} missing processInformation in {source}")
    data_info = info.get("dataSetInformation")
    if not isinstance(data_info, dict):
        raise SystemExit(f"Process block #{index} missing dataSetInformation in {source}")
    classification = data_info.get("classificationInformation")
    if not isinstance(classification, dict):
        raise SystemExit(f"Process block #{index} missing classificationInformation in {source}")
    carrier = classification.get("common:classification")
    if not isinstance(carrier, dict):
        raise SystemExit(f"Process block #{index} missing common:classification in {source}")
    classes = carrier.get("common:class")
    if not isinstance(classes, list) or not classes:
        raise SystemExit(f"Process block #{index} must include classification entries in {source}")
    try:
        carrier["common:class"] = ensure_valid_classification_path(tuple(classes))
    except ValueError:
        carrier["common:class"] = [{"@level": "0", "@classId": "Z", "#text": "Unspecified"}]


def _enforce_flow_dataset_version(flow_root: dict[str, Any], uuid_value: str) -> None:
    admin = flow_root.setdefault("administrativeInformation", {})
    publication = admin.setdefault("publicationAndOwnership", {})
    publication["common:dataSetVersion"] = DEFAULT_DATA_SET_VERSION
    publication["common:permanentDataSetURI"] = f"https://lcdn.tiangong.earth/showFlow.xhtml?uuid={uuid_value}&version={DEFAULT_DATA_SET_VERSION}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", help="Run identifier produced by Stage 1 JSON-LD extractor.")
    parser.add_argument(
        "--process-blocks",
        type=Path,
        help="Optional override for the Stage 1 process blocks path.",
    )
    parser.add_argument(
        "--flow-datasets",
        type=Path,
        help="Optional override for the Stage 1 flow datasets path.",
    )
    parser.add_argument(
        "--source-datasets",
        type=Path,
        help="Optional override for the Stage 1 source datasets path.",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        help="Optional override for the exports directory (defaults to artifacts/<run>/exports).",
    )
    parser.add_argument(
        "--process-datasets",
        type=Path,
        help="Optional override for merged process datasets path.",
    )
    parser.add_argument(
        "--validation-output",
        type=Path,
        help="Optional override for TIDAS validation report path.",
    )
    parser.add_argument(
        "--workflow-output",
        type=Path,
        help="Optional override for workflow_result.json path.",
    )
    parser.add_argument(
        "--clean-exports",
        action="store_true",
        help="Clean the exports directory before writing JSON-LD artifacts.",
    )
    parser.add_argument(
        "--json-ld-flows",
        type=Path,
        help="Legacy fallback directory containing OpenLCA JSON-LD flow datasets to convert when Stage 1 flow caches are unavailable.",
    )
    parser.add_argument(
        "--json-ld-sources",
        type=Path,
        help="Directory containing OpenLCA JSON-LD source datasets to convert (optional).",
    )
    parser.add_argument(
        "--skip-auto-publish",
        action="store_true",
        help="Do not automatically invoke Stage 3 JSON-LD publisher after validation succeeds.",
    )
    parser.add_argument(
        "--stage3-script",
        type=Path,
        help="Optional override for the Stage 3 JSON-LD script path.",
    )
    parser.add_argument(
        "--validation-only",
        action="store_true",
        help="Skip conversion of flows/ancillary datasets and only run validation.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_id = resolve_run_id(args.run_id)
    ensure_run_cache_dir(run_id)
    save_latest_run_id(run_id)

    process_blocks_path = args.process_blocks or run_cache_path(run_id, "stage1_process_blocks.json")
    process_datasets_path = args.process_datasets or run_cache_path(run_id, "process_datasets.json")
    flow_datasets_path = args.flow_datasets or run_cache_path(run_id, "stage1_flow_blocks.json")
    source_datasets_path = args.source_datasets or run_cache_path(run_id, "stage1_source_blocks.json")
    validation_output = args.validation_output or run_cache_path(run_id, "tidas_validation.json")
    workflow_output = args.workflow_output or run_cache_path(run_id, "workflow_result.json")

    if args.artifact_root:
        artifact_root = args.artifact_root
        artifact_root.mkdir(parents=True, exist_ok=True)
    else:
        artifact_root = ensure_run_exports_dir(run_id, clean=args.clean_exports)

    if not process_blocks_path.exists():
        raise SystemExit(f"Process blocks file not found: {process_blocks_path}")

    uuid_mapper = UUIDMapper()

    converted_flows: list[dict[str, Any]] = []
    if flow_datasets_path.exists():
        converted_flows = _read_flow_datasets(flow_datasets_path)
        for dataset in converted_flows:
            uuid_mapper.remap_flow_dataset(dataset)
        print(f"[jsonld-stage2] Loaded {len(converted_flows)} flow dataset(s) from {flow_datasets_path}")
    elif args.json_ld_flows:
        converted_flows = convert_flow_directory(args.json_ld_flows)
        for dataset in converted_flows:
            uuid_mapper.remap_flow_dataset(dataset)
        print(f"[jsonld-stage2] Converted {len(converted_flows)} flow dataset(s) from {args.json_ld_flows}")

    converted_sources: list[dict[str, Any]] = []
    if source_datasets_path.exists():
        converted_sources = _read_source_datasets(source_datasets_path)
        for dataset in converted_sources:
            uuid_mapper.remap_source_dataset(dataset)
        print(f"[jsonld-stage2] Loaded {len(converted_sources)} source dataset(s) from {source_datasets_path}")
    elif args.json_ld_sources:
        converted_sources = convert_source_directory(args.json_ld_sources)
        for dataset in converted_sources:
            uuid_mapper.remap_source_dataset(dataset)
        print(f"[jsonld-stage2] Converted {len(converted_sources)} source dataset(s) from {args.json_ld_sources}")

    process_blocks = _read_process_blocks(process_blocks_path)
    for block in process_blocks:
        uuid_mapper.remap_process_block(block)

    alignment_entries: list[dict[str, Any]] = []

    alignment_output = run_cache_path(run_id, "stage3_alignment.json")
    summary = generate_artifacts(
        process_blocks=process_blocks,
        alignment_entries=alignment_entries,
        artifact_root=artifact_root,
        merged_output=process_datasets_path,
        validation_output=validation_output,
        workflow_output=workflow_output,
        run_validation=False,
        primary_source_title=None,
        comment_llm=None,
    )
    print(f"[jsonld-stage2] Process exports complete -> {artifact_root} " f"(processes={summary.process_count}, flows={summary.flow_count}, sources={summary.source_count})")
    dump_json({"alignment": alignment_entries}, alignment_output)

    flow_count = summary.flow_count
    if not args.validation_only and converted_flows:
        flows_dir = artifact_root / "flows"
        flows_dir.mkdir(parents=True, exist_ok=True)
        for dataset in converted_flows:
            flow_root = dataset.get("flowDataSet", {})
            uuid_value = flow_root.get("flowInformation", {}).get("dataSetInformation", {}).get("common:UUID")
            if not uuid_value:
                continue
            _enforce_flow_dataset_version(flow_root, uuid_value)
            dataset_version = resolve_dataset_version(flow_root)
            dump_json(dataset, flows_dir / build_export_filename(uuid_value, dataset_version))
        flow_count += len(converted_flows)
        print(f"[jsonld-stage2] Wrote {len(converted_flows)} remapped flow dataset(s) -> {flows_dir}")

    if not args.validation_only and converted_sources:
        sources_dir = artifact_root / "sources"
        sources_dir.mkdir(parents=True, exist_ok=True)
        for dataset in converted_sources:
            source_root = dataset.get("sourceDataSet", {})
            info = source_root.get("sourceInformation", {}).get("dataSetInformation", {})
            uuid_value = info.get("common:UUID")
            if not uuid_value:
                continue
            dataset_version = resolve_dataset_version(source_root)
            dump_json(dataset, sources_dir / build_export_filename(uuid_value, dataset_version))
        print(f"[jsonld-stage2] Wrote {len(converted_sources)} remapped source dataset(s) -> {sources_dir}")

    validator = TidasValidationService()
    try:
        findings = validator.validate_directory(artifact_root)
    finally:
        validator.close()
    dump_json({"validation_report": [asdict(finding) for finding in findings]}, validation_output)

    def _has_errors(items: list[Any]) -> bool:
        return any(getattr(finding, "severity", None) == "error" for finding in items)

    if _has_errors(findings):
        print(f"[jsonld-stage2] Validation reported blocking errors; attempting automatic fixes via {validation_output}")
        if auto_fix_from_validation(validation_output, artifact_root):
            print("[jsonld-stage2] Applied automatic fixes based on validation findings; re-running validation.")
            validator = TidasValidationService()
            try:
                findings = validator.validate_directory(artifact_root)
            finally:
                validator.close()
            dump_json({"validation_report": [asdict(finding) for finding in findings]}, validation_output)
        if _has_errors(findings):
            print(f"[jsonld-stage2] Validation still failing; see {validation_output}")
            return

    print(f"[jsonld-stage2] Validation succeeded -> {validation_output}")
    if args.skip_auto_publish:
        return

    stage3_script = args.stage3_script or Path(__file__).with_name("stage3_jsonld_publish.py")
    if not stage3_script.exists():
        print("[jsonld-stage2] Stage 3 JSON-LD script not found; skipping auto publish.")
        return

    cmd = [
        sys.executable,
        str(stage3_script),
        "--run-id",
        run_id,
        "--commit",
    ]
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise SystemExit("Stage 3 JSON-LD publishing failed; inspect the logs and rerun manually.")


if __name__ == "__main__":
    main()
