#!/usr/bin/env python
# ruff: noqa: E402
"""Stage 3 (JSON-LD): publish converted ILCD datasets via Database_CRUD_Tool."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

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


def _publish_dataset(client: DatabaseCrudClient, table: str, payload: dict[str, Any], dry_run: bool) -> dict[str, Any] | None:
    if table == "flows":
        if dry_run:
            return None
        return client.insert_flow(payload)
    elif table == "processes":
        if dry_run:
            return None
        return client.insert_process(payload)
    else:
        if dry_run:
            return None
        return client._invoke(
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


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _normalize_uuid(value: str | None) -> str:
    return (value or "").strip().lower()


def _extract_flow_uuid(payload: Mapping[str, Any]) -> str:
    root = payload.get("flowDataSet") if isinstance(payload, Mapping) else None
    if not isinstance(root, Mapping):
        root = payload
    info = root.get("flowInformation", {}).get("dataSetInformation", {})
    return _coerce_text(info.get("common:UUID"))


def _extract_flow_version(payload: Mapping[str, Any]) -> str:
    root = payload.get("flowDataSet") if isinstance(payload, Mapping) else None
    if not isinstance(root, Mapping):
        root = payload
    publication = root.get("administrativeInformation", {}).get("publicationAndOwnership", {})
    version = _coerce_text(publication.get("common:dataSetVersion"))
    return version or "01.01.000"


def _extract_remote_record_id(result: Mapping[str, Any]) -> str:
    for candidate in _iterate_result_candidates(result):
        record_id = _coerce_text(candidate.get("record_id") or candidate.get("recordId"))
        if record_id:
            return record_id
        candidate_id = _coerce_text(candidate.get("id"))
        if candidate_id:
            return candidate_id
    return ""


def _extract_remote_record_version(result: Mapping[str, Any]) -> str | None:
    for candidate in _iterate_result_candidates(result):
        version = _coerce_text(candidate.get("version"))
        if version:
            return version
    return None


def _iterate_result_candidates(result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    candidates: list[Mapping[str, Any]] = []
    if isinstance(result, Mapping):
        candidates.append(result)
        data = result.get("data")
        if isinstance(data, Mapping):
            candidates.append(data)
        elif isinstance(data, list):
            candidates.extend([item for item in data if isinstance(item, Mapping)])
    return candidates


def _extract_multilang_text(node: Any) -> str:
    if isinstance(node, list):
        parts = [_extract_multilang_text(item) for item in node]
        return "; ".join(part for part in parts if part)
    if isinstance(node, dict):
        text = node.get("#text") or node.get("text") or node.get("value")
        if isinstance(text, str):
            return text.strip()
        return ""
    return _coerce_text(node)


def _compose_flow_short_description(payload: Mapping[str, Any]) -> str:
    root = payload.get("flowDataSet") if isinstance(payload, Mapping) else None
    if not isinstance(root, Mapping):
        root = payload
    info = root.get("flowInformation", {}).get("dataSetInformation", {})
    name_block = info.get("name", {}) if isinstance(info.get("name"), Mapping) else {}
    parts: list[str] = []
    for key in ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes", "functionalUnitFlowProperties"):
        if isinstance(name_block, Mapping):
            text = _extract_multilang_text(name_block.get(key))
            if text:
                parts.append(text)
    description = "; ".join(part for part in parts if part)
    if description:
        return description
    fallback = _extract_multilang_text(info.get("common:generalComment"))
    if fallback:
        return fallback
    synonyms = _extract_multilang_text(info.get("common:synonyms"))
    if synonyms:
        return synonyms
    return _coerce_text(info.get("common:UUID"))


def _language_entry(text: str, lang: str = "en") -> dict[str, str] | None:
    cleaned = _coerce_text(text)
    if not cleaned:
        return None
    return {"@xml:lang": lang, "#text": cleaned}


def _rewrite_process_flow_references(process_dir: Path, flow_mapping: Mapping[str, Mapping[str, str]]) -> int:
    if not process_dir.exists():
        return 0
    updated_files = 0
    for path in sorted(process_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        dataset = payload.get("processDataSet")
        target = dataset if isinstance(dataset, dict) else payload
        if not isinstance(target, dict):
            continue
        if _update_flow_references_in_node(target, flow_mapping):
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            updated_files += 1
    return updated_files


def _update_flow_references_in_node(node: Any, flow_mapping: Mapping[str, Mapping[str, str]]) -> bool:
    changed = False
    if isinstance(node, dict):
        ref = node.get("referenceToFlowDataSet")
        if isinstance(ref, dict):
            ref_uuid = _normalize_uuid(ref.get("@refObjectId"))
            mapping = flow_mapping.get(ref_uuid)
            if mapping:
                if _coerce_text(ref.get("@refObjectId")) != mapping.get("remote_id"):
                    ref["@refObjectId"] = mapping["remote_id"]
                    changed = True
                    version = mapping.get("version")
                    if version and _coerce_text(ref.get("@version")) != version:
                        ref["@version"] = version
                        changed = True
                if "unmatched:placeholder" in ref:
                    ref.pop("unmatched:placeholder", None)
                    changed = True
                short_description = mapping.get("short_description")
                if short_description:
                    entry = _language_entry(short_description)
                    if entry and ref.get("common:shortDescription") != entry:
                        ref["common:shortDescription"] = entry
                        changed = True
        for value in node.values():
            if _update_flow_references_in_node(value, flow_mapping):
                changed = True
    elif isinstance(node, list):
        for item in node:
            if _update_flow_references_in_node(item, flow_mapping):
                changed = True
    return changed


def _extract_source_uuid(payload: Mapping[str, Any]) -> str:
    root = payload.get("sourceDataSet") if isinstance(payload, Mapping) else None
    if not isinstance(root, Mapping):
        root = payload
    info = root.get("sourceInformation", {}).get("dataSetInformation", {})
    return _coerce_text(info.get("common:UUID"))


def _extract_source_version(payload: Mapping[str, Any]) -> str:
    root = payload.get("sourceDataSet") if isinstance(payload, Mapping) else None
    if not isinstance(root, Mapping):
        root = payload
    publication = root.get("administrativeInformation", {}).get("publicationAndOwnership", {})
    version = _coerce_text(publication.get("common:dataSetVersion"))
    return version or "01.01.000"


def _rewrite_process_source_references(process_dir: Path, source_mapping: Mapping[str, Mapping[str, str]]) -> int:
    if not process_dir.exists():
        return 0
    updated_files = 0
    for path in sorted(process_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        dataset = payload.get("processDataSet")
        target = dataset if isinstance(dataset, dict) else payload
        if not isinstance(target, dict):
            continue
        if _update_source_references_in_node(target, source_mapping):
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            updated_files += 1
    return updated_files


def _update_source_references_in_node(node: Any, source_mapping: Mapping[str, Mapping[str, str]]) -> bool:
    changed = False
    if isinstance(node, dict):
        modelling = node.get("modellingAndValidation")
        if isinstance(modelling, dict):
            treatment = modelling.get("dataSourcesTreatmentAndRepresentativeness")
            if isinstance(treatment, dict):
                references = treatment.get("referenceToDataSource")
                if isinstance(references, dict):
                    ref_list = [references]
                    single_dict = True
                elif isinstance(references, list):
                    ref_list = [ref for ref in references if isinstance(ref, dict)]
                    single_dict = False
                else:
                    ref_list = []
                    single_dict = False
                updated = False
                for ref in ref_list:
                    ref_uuid = _normalize_uuid(ref.get("@refObjectId"))
                    mapping = source_mapping.get(ref_uuid)
                    if not mapping:
                        continue
                    if _coerce_text(ref.get("@refObjectId")) != mapping["remote_id"]:
                        ref["@refObjectId"] = mapping["remote_id"]
                        updated = True
                    version = mapping.get("version")
                    if version and _coerce_text(ref.get("@version")) != version:
                        ref["@version"] = version
                        updated = True
                if updated:
                    treatment["referenceToDataSource"] = ref_list[0] if single_dict and ref_list else ref_list
                    changed = True
        for value in node.values():
            if _update_source_references_in_node(value, source_mapping):
                changed = True
    elif isinstance(node, list):
        for item in node:
            if _update_source_references_in_node(item, source_mapping):
                changed = True
    return changed


def main() -> None:
    args = parse_args()
    configure_logging()
    run_id = resolve_run_id(args.run_id, pipeline="jsonld")
    exports_dir = args.exports or Path("artifacts") / run_id / "exports"
    validation_report = args.validation_report or Path("artifacts") / run_id / "cache" / "tidas_validation.json"

    _check_validation(validation_report if validation_report.exists() else None)

    dry_run = not args.commit
    client = DatabaseCrudClient()
    flow_publish_records: dict[str, dict[str, str]] = {}
    source_publish_records: dict[str, dict[str, str]] = {}

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
                result = _publish_dataset(client, "flows", payload, dry_run)
                if not dry_run and result:
                    local_uuid = _extract_flow_uuid(payload)
                    if not local_uuid:
                        continue
                    remote_id = _extract_remote_record_id(result)
                    remote_version = _extract_remote_record_version(result) or _extract_flow_version(payload)
                    short_description = _compose_flow_short_description(payload)
                    if remote_id:
                        record = {
                            "remote_id": remote_id,
                            "version": remote_version or "01.01.000",
                        }
                        if short_description:
                            record["short_description"] = short_description
                        flow_publish_records[_normalize_uuid(local_uuid)] = record

        if flow_publish_records and not dry_run and not args.skip_processes:
            updates = _rewrite_process_flow_references(exports_dir / "processes", flow_publish_records)
            LOGGER.info(
                "jsonld_stage3.updated_process_flow_refs",
                files=updates,
            )

        if not args.skip_sources:
            datasets = _iterate_datasets(exports_dir / "sources")
            for path, payload in datasets:
                LOGGER.info("jsonld_stage3.publish_source", path=str(path), dry_run=dry_run)
                result = _publish_dataset(client, "sources", payload, dry_run)
                if not dry_run and result:
                    local_uuid = _extract_source_uuid(payload)
                    if not local_uuid:
                        continue
                    remote_id = _extract_remote_record_id(result)
                    remote_version = _extract_remote_record_version(result) or _extract_source_version(payload)
                    if remote_id:
                        source_publish_records[_normalize_uuid(local_uuid)] = {
                            "remote_id": remote_id,
                            "version": remote_version or "01.01.000",
                        }

        if source_publish_records and not dry_run and not args.skip_processes:
            updates = _rewrite_process_source_references(exports_dir / "processes", source_publish_records)
            LOGGER.info(
                "jsonld_stage3.updated_process_source_refs",
                files=updates,
            )

        if not args.skip_processes:
            datasets = _iterate_datasets(exports_dir / "processes")
            for path, payload in datasets:
                LOGGER.info("jsonld_stage3.publish_process", path=str(path), dry_run=dry_run)
                _publish_dataset(client, "processes", payload, dry_run)

    finally:
        client.close()

    status = "COMMITTED" if args.commit else "DRY-RUN"
    print(f"[jsonld-stage3] Publish complete ({status}) for run {run_id}")


if __name__ == "__main__":
    main()
