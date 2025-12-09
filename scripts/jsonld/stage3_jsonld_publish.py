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
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))

try:
    from scripts.md._workflow_common import (  # type: ignore
        OpenAIResponsesLLM,
        load_secrets,
        resolve_run_id,
        run_cache_path,
    )
except ModuleNotFoundError:  # pragma: no cover
    from _workflow_common import OpenAIResponsesLLM, load_secrets, resolve_run_id, run_cache_path  # type: ignore
from tiangong_lca_spec.core.logging import configure_logging, get_logger
from tiangong_lca_spec.core.models import FlowQuery
from tiangong_lca_spec.core.uris import build_portal_uri
from tiangong_lca_spec.flow_search.client import FlowSearchClient, FlowSearchError
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
    parser.add_argument("--secrets", type=Path, default=Path(".secrets/secrets.toml"), help="Secrets file for LLM/flow search.")
    parser.add_argument("--llm-cache", type=Path, help="Optional cache dir for LLM calls.")
    parser.add_argument("--disable-cache", action="store_true", help="Disable LLM response cache.")
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


def _upsert_dataset(client: DatabaseCrudClient, table: str, payload: dict[str, Any], dry_run: bool) -> dict[str, Any] | None:
    """
    Insert-first strategy with fallback to update for flows/processes when insert conflicts.
    Keeps idempotency for reruns without explicit upsert support.
    """
    try:
        return _publish_dataset(client, table, payload, dry_run)
    except Exception as exc:  # noqa: BLE001
        if dry_run:
            raise
        # Only flows/processes have explicit update handlers
        try:
            if table == "flows":
                return client.update_flow(payload)
            if table == "processes":
                return client.update_process(payload)
        except Exception as update_exc:  # noqa: BLE001
            LOGGER.warning(
                "jsonld_stage3.upsert_failed",
                table=table,
                error=str(update_exc),
                original_error=str(exc),
            )
            raise
        LOGGER.warning(
            "jsonld_stage3.insert_failed_skip_update",
            table=table,
            error=str(exc),
        )
        raise


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


def _normalize_flow_type(value: Any) -> str:
    """Reduce flow type variants (e.g., ELEMENTARY_FLOW vs 'Elementary flow')."""
    text = _coerce_text(value).lower().replace("_", " ")
    text = " ".join(text.split())
    if text.startswith("elementary"):
        return "elementary"
    if text.startswith("product"):
        return "product"
    if text.startswith("waste"):
        return "waste"
    return text


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


def _compose_process_display_name(process_dataset: Mapping[str, Any]) -> str:
    info = process_dataset.get("processInformation", {}).get("dataSetInformation", {}) if isinstance(process_dataset.get("processInformation"), Mapping) else {}
    name_block = info.get("name", {}) if isinstance(info.get("name"), Mapping) else {}
    parts: list[str] = []
    for key in ("baseName", "treatmentStandardsRoutes", "mixAndLocationTypes", "functionalUnitFlowProperties"):
        text = _extract_multilang_text(name_block.get(key))
        if text:
            parts.append(text)
    if parts:
        return "; ".join(parts)
    fallback = _extract_multilang_text(info.get("common:generalComment"))
    return fallback or ""


def _build_hint_description(hint: Mapping[str, Any]) -> str:
    description_parts: list[str] = ["flowType: elementary flow"]
    category = _coerce_text(hint.get("category"))
    cas_number = _coerce_text(hint.get("cas"))
    formula = _coerce_text(hint.get("formula"))
    synonyms = hint.get("synonyms")
    if category:
        description_parts.append(f"category: {category}")
    if cas_number:
        description_parts.append(f"cas: {cas_number}")
    if formula:
        description_parts.append(f"formula: {formula}")
    if isinstance(synonyms, list):
        cleaned_synonyms = ", ".join(_coerce_text(value) for value in synonyms if _coerce_text(value))
        if cleaned_synonyms:
            description_parts.append(f"synonyms: {cleaned_synonyms}")
    elif isinstance(synonyms, str):
        cleaned_synonyms = _coerce_text(synonyms)
        if cleaned_synonyms:
            description_parts.append(f"synonyms: {cleaned_synonyms}")
    return "; ".join(description_parts)


def _load_elementary_flow_hints(exports_dir: Path) -> list[dict[str, Any]]:
    candidates = [
        exports_dir / "elementary_flow_hints.json",
        exports_dir.parent / "cache" / "elementary_flow_hints.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            LOGGER.warning("jsonld_stage3.hints_parse_failed", path=str(path), error=str(exc))
            continue
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
    return []

def _load_substitution_log(log_path: Path) -> list[dict[str, Any]]:
    if not log_path.exists():
        return []
    try:
        payload = json.loads(log_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return []
    return payload if isinstance(payload, list) else []

def _merge_substitution_logs(
    existing: list[dict[str, Any]],
    new_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Keep latest record per (process_id, original_flow_id) to avoid log blow-up.
    """
    merged: dict[tuple[str | None, str | None], dict[str, Any]] = {}
    for record in existing + new_records:
        if not isinstance(record, dict):
            continue
        key = (record.get("process_id"), record.get("original_flow_id"))
        merged[key] = record
    return list(merged.values())


def _preferred_language_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        for item in value:
            text = _preferred_language_text(item)
            if text:
                return text
        return None
    if isinstance(value, dict):
        for key in ("#text", "text", "@value", "value"):
            text = value.get(key)
            if isinstance(text, str) and text.strip():
                return text.strip()
        for item in value.values():
            if isinstance(item, (dict, list)):
                text = _preferred_language_text(item)
                if text:
                    return text
        return None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _normalize_category_path(value: Any) -> str:
    # Accept either a slash-delimited string or a list of dicts/strings; return a unified "/" path.
    if isinstance(value, str):
        parts = [segment.strip() for segment in value.split("/") if segment.strip()]
    elif isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, dict):
                text = item.get("#text") or item.get("text") or ""
                if text:
                    parts.append(str(text).strip())
            elif isinstance(item, str):
                if item.strip():
                    parts.append(item.strip())
    else:
        parts = []
    return "/".join(parts)


def _medium(text: str) -> str | None:
    lowered = text.lower()
    if "air" in lowered:
        return "air"
    if "water" in lowered or "sea" in lowered or "river" in lowered:
        return "water"
    if "soil" in lowered or "ground" in lowered or "land" in lowered:
        return "soil"
    if "resource" in lowered or "material resource" in lowered:
        return "resource"
    return None


def _compartment_info(text: str) -> tuple[str | None, str | None]:
    """Return (medium, kind) where kind is 'resource' or 'emission' when detectable."""
    medium = _medium(text)
    lowered = text.lower()
    kind = None
    if "resource" in lowered:
        kind = "resource"
    elif "emission" in lowered or "emissions" in lowered:
        kind = "emission"
    return medium, kind


def _flatten_flow_candidate(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    flow = payload.get("flowDataSet") if isinstance(payload, Mapping) else None
    if not isinstance(flow, Mapping):
        flow = payload
    if not isinstance(flow, Mapping):
        return None
    info = flow.get("flowInformation", {})
    data_info = info.get("dataSetInformation", {})
    name_block = data_info.get("name") or {}
    base_name = _preferred_language_text(name_block.get("baseName"))
    if not base_name:
        return None
    classification = data_info.get("classificationInformation", {}).get("common:classification", {}).get("common:class")
    # Prefer elementary flow categorization for elementary flows; fall back to product classification.
    category_text = None
    category_path = ""
    elem_cats = data_info.get("classificationInformation", {}).get("common:elementaryFlowCategorization", {}).get("common:category")
    if isinstance(elem_cats, list) and elem_cats:
        last = elem_cats[-1]
        if isinstance(last, dict):
            category_text = last.get("#text") or last.get("text")
        category_path = _normalize_category_path(elem_cats)
    if not category_text and isinstance(classification, list) and classification:
        last = classification[-1]
        if isinstance(last, dict):
            category_text = last.get("#text") or last.get("text")
        if not category_path:
            category_path = _normalize_category_path(classification)
    synonyms_raw = data_info.get("common:synonyms")
    synonyms: list[str] | None = None
    if synonyms_raw:
        syn_list: list[str] = []
        for entry in synonyms_raw if isinstance(synonyms_raw, list) else [synonyms_raw]:
            text = _preferred_language_text(entry) or (entry if isinstance(entry, str) else None)
            if text:
                syn_list.append(text)
        if syn_list:
            synonyms = syn_list
    modelling = flow.get("modellingAndValidation", {})
    lcimethod = modelling.get("LCIMethod", {}) if isinstance(modelling, dict) else {}
    flow_type = lcimethod.get("typeOfDataSet") if isinstance(lcimethod, Mapping) else None
    cas_number = data_info.get("casNumber") or data_info.get("CASNumber") or data_info.get("cas_number")
    return {
        "uuid": data_info.get("common:UUID") or flow.get("@uuid"),
        "base_name": base_name,
        "classification": classification,
        "category": category_text,
        "category_path": category_path,
        "cas": cas_number,
        "flow_type": flow_type,
        "synonyms": synonyms,
        "version": flow.get("administrativeInformation", {}).get("publicationAndOwnership", {}).get("common:dataSetVersion"),
    }


def _flow_search_with_cas(client: FlowSearchClient, query: FlowQuery) -> list[dict[str, Any]]:
    # Use flow_name prefix for JSON-LD alignment without changing core client defaults.
    parts: list[str] = []
    if query.exchange_name:
        parts.append(f"flow_name: {query.exchange_name}")
    if query.description:
        parts.append(f"description: {query.description}")
    joined = " \n".join(parts)
    arguments = {"query": joined or query.exchange_name}
    try:
        raw = client._call_with_retry(arguments)  # type: ignore[attr-defined]
    except Exception as exc:  # pylint: disable=broad-except
        raise FlowSearchError("Flow search invocation failed") from exc
    results: list[dict[str, Any]] = []
    if isinstance(raw, list):
        candidates = raw
    elif isinstance(raw, dict):
        candidates = raw.get("candidates") or raw.get("flows") or raw.get("results") or raw.get("data") or []
        if not isinstance(candidates, list):
            candidates = []
    else:
        candidates = []
    for item in candidates:
        if isinstance(item, dict):
            payload = item.get("json") if isinstance(item.get("json"), dict) else item
            flattened = _flatten_flow_candidate(payload)
            if flattened:
                results.append(flattened)
    return results


def _select_elementary_candidate(hint: Mapping[str, Any], candidates: list[Any], llm: Any | None) -> Any | None:
    if not candidates:
        return None
    hint_name = _coerce_text(hint.get("name")).lower()
    hint_category_raw = _coerce_text(hint.get("category"))
    hint_category_path = _normalize_category_path(hint_category_raw) if hint_category_raw else ""
    hint_cas = _coerce_text(hint.get("cas")).lower()
    raw_synonyms = hint.get("synonyms") or []
    if isinstance(raw_synonyms, str):
        raw_synonyms = [item.strip() for item in raw_synonyms.replace(";", ",").split(",") if item.strip()]
    elif not isinstance(raw_synonyms, list):
        raw_synonyms = []
    hint_synonyms = [text.lower() for text in raw_synonyms if isinstance(text, str)]
    hint_flow_type = _normalize_flow_type(hint.get("flowType"))
    hint_formula = _coerce_text(hint.get("formula")).lower()
    hint_medium, hint_kind = _compartment_info(hint_category_path) if hint_category_path else (None, None)

    def _is_conflict(cand: Mapping[str, Any]) -> bool:
        cand_cas = _coerce_text(cand.get("cas")).lower()
        cand_formula = _coerce_text(cand.get("formula")).lower()
        cand_flow_type = _normalize_flow_type(cand.get("flow_type"))
        cand_category_path = _normalize_category_path(cand.get("category_path") or cand.get("category") or "")
        cand_medium, cand_kind = _compartment_info(cand_category_path) if cand_category_path else (None, None)
        # flowType mismatch
        if hint_flow_type and cand_flow_type and hint_flow_type != cand_flow_type:
            return True
        # resource vs emission mismatch
        if hint_kind and cand_kind and hint_kind != cand_kind:
            return True
        # CAS conflict
        if hint_cas and cand_cas and hint_cas != cand_cas:
            return True
        # Formula conflict
        if hint_formula and cand_formula and hint_formula != cand_formula:
            return True
        # Medium conflict (only check core medium)
        if hint_medium and cand_medium and hint_medium != cand_medium:
            return True
        return False

    if llm is not None:
        try:
            context = {
                "hint": {
                    "name": hint.get("name"),
                    "category": hint.get("category"),
                    "flowType": hint.get("flowType"),
                    "cas": hint.get("cas"),
                    "formula": hint.get("formula"),
                    "synonyms": hint.get("synonyms"),
                },
                "candidates": [
                    {
                        "index": idx,
                        "uuid": cand.get("uuid"),
                        "name": cand.get("base_name"),
                        "cas": cand.get("cas"),
                        "flow_type": cand.get("flow_type"),
                        "category": cand.get("category"),
                        "category_path": cand.get("category_path"),
                        "synonyms": cand.get("synonyms"),
                        "classification": cand.get("classification"),
                        "version": cand.get("version"),
                    }
                    for idx, cand in enumerate(candidates[:10])
                ],
            }
            prompt = (
                "You are an LCA Data Reconciliation Expert matching an input flow to catalog candidates.\n"
                "Cognitive process:\n"
                "- Chemical fingerprint: CAS/formula are decisive. A match means same substance even if names differ. CAS/formula conflict (ion vs element) = reject.\n"
                "- Semantic compartment: infer core medium (air, water, soil, resource); ignore prefixes/suffixes/adjectives like 'unspecified'.\n"
                "  If media align, wording/path differences are acceptable; if media differ, it's a conflict.\n"
                "- Specificity: respect 'unspecified'. Input unspecified + candidate unspecified or generic water = good; input unspecified + candidate sea water = downgrade or reject.\n"
                'Decide if a candidate refers to the same physical entity in the same compartment. Return JSON {"best_index": int or null} (0-based).'
            )
            response = llm.invoke({"prompt": prompt, "context": context, "response_format": {"type": "json_object"}})
            if isinstance(response, dict):
                best_index = response.get("best_index")
                if isinstance(best_index, int) and 0 <= best_index < len(candidates):
                    return candidates[best_index]
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("jsonld_stage3.elementary_flow_selection_llm_failed", error=str(exc))

    def _score(cand: Any) -> float:
        score = 0.0
        if _is_conflict(cand):
            return -1.0
        cand_name = _coerce_text(cand.get("base_name")).lower()
        cand_cas = _coerce_text(cand.get("cas")).lower()
        cand_category_path = _normalize_category_path(cand.get("category_path") or cand.get("category") or "")
        cand_medium, cand_kind = _compartment_info(cand_category_path) if cand_category_path else (None, None)
        hint_medium_local, hint_kind_local = hint_medium, hint_kind
        hint_unspecified = "unspecified" in hint_category_path.lower()
        cand_unspecified = "unspecified" in cand_category_path.lower()
        if hint_cas and cand_cas and cand_cas == hint_cas:
            score += 5
        # If CAS is missing but media align, still consider.
        if hint_name and cand_name and cand_name == hint_name:
            score += 4
        elif hint_name and cand_name:
            from difflib import SequenceMatcher

            score += SequenceMatcher(None, hint_name, cand_name).ratio()
        for syn in hint_synonyms:
            if syn and syn == cand_name:
                score += 3
        if hint_category_path and cand_category_path:
            if hint_medium_local and cand_medium and hint_medium_local == cand_medium:
                score += 1.0
            if hint_kind_local and cand_kind and hint_kind_local == cand_kind:
                score += 1.0
            if hint_unspecified:
                if cand_unspecified:
                    score += 0.5
                else:
                    score -= 0.2
            elif cand_unspecified:
                score -= 0.2
        return score

    ranked = sorted(((_score(c), c) for c in candidates), key=lambda item: item[0], reverse=True)
    debug_dump = []
    for cand in candidates:
        debug_dump.append(
            {
                "uuid": cand.get("uuid"),
                "name": cand.get("base_name"),
                "flow_type": cand.get("flow_type"),
                "cas": cand.get("cas"),
                "category": cand.get("category"),
                "category_path": cand.get("category_path"),
                "classification": cand.get("classification"),
                "score": _score(cand),
                "conflict": _is_conflict(cand),
            }
        )
    if debug_dump:
        LOGGER.info("jsonld_stage3.elementary_candidates_debug", hint=hint, candidates=debug_dump)
    if ranked and ranked[0][0] > 0.0:
        return ranked[0][1]
    return None


def _resolve_elementary_flow_replacements(
    hints: list[dict[str, Any]],
    llm: Any | None,
    client: FlowSearchClient,
) -> dict[str, dict[str, str]]:
    replacements: dict[str, dict[str, str]] = {}
    for entry in hints:
        hint = entry.get("hint") if isinstance(entry, dict) else None
        original_uuid = entry.get("original_uuid") if isinstance(entry, dict) else None
        if not isinstance(hint, Mapping) or not original_uuid:
            continue
        name = _coerce_text(hint.get("name"))
        if not name:
            continue
        description = _build_hint_description(hint)
        query = FlowQuery(exchange_name=name, description=description)
        try:
            matches = _flow_search_with_cas(client, query)
        except FlowSearchError as exc:
            LOGGER.warning("jsonld_stage3.elementary_flow_search_failed", flow=name, error=str(exc))
            continue
        candidate = _select_elementary_candidate(hint, matches, llm)
        if candidate and candidate.get("uuid"):
            replacements[original_uuid] = {
                "uuid": candidate.get("uuid"),
                "version": candidate.get("version") or "01.01.000",
            }
    return replacements


def _rewrite_elementary_flow_references(
    process_dir: Path,
    replacements: Mapping[str, Mapping[str, str]],
    hint_lookup: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    mapping_records: list[dict[str, Any]] = []
    if not process_dir.exists() or not replacements:
        return mapping_records
    for path, payload in _iterate_datasets(process_dir):
        dataset = payload.get("processDataSet") if isinstance(payload, dict) else None
        if not isinstance(dataset, dict):
            dataset = payload
        if not isinstance(dataset, dict):
            continue
        data_info = dataset.get("processInformation", {}).get("dataSetInformation", {}) if isinstance(dataset.get("processInformation"), Mapping) else {}
        process_id = _coerce_text(data_info.get("common:UUID"))
        process_name = _compose_process_display_name(dataset)
        exchanges_container = dataset.get("exchanges") or {}
        exchanges = exchanges_container.get("exchange") if isinstance(exchanges_container, Mapping) else None
        changed = False
        if isinstance(exchanges, list):
            for exchange in exchanges:
                if not isinstance(exchange, dict):
                    continue
                reference = exchange.get("referenceToFlowDataSet")
                if not isinstance(reference, dict):
                    continue
                ref_uuid = _coerce_text(reference.get("@refObjectId"))
                if not ref_uuid:
                    continue
                if ref_uuid not in hint_lookup:
                    continue
                replacement = replacements.get(ref_uuid)
                flow_name = _coerce_text(hint_lookup[ref_uuid].get("name")) or ref_uuid
                record = {
                    "process_id": process_id,
                    "process_name": process_name,
                    "flow_name": flow_name,
                    "original_flow_id": ref_uuid,
                    "tiangong_flow_id": None,
                    "status": "FAILED",
                }
                if replacement and replacement.get("uuid"):
                    new_uuid = replacement["uuid"]
                    version = replacement.get("version") or "01.01.000"
                    reference["@refObjectId"] = new_uuid
                    reference["@uri"] = build_portal_uri("flow", new_uuid, version)
                    reference["@version"] = version
                    record["tiangong_flow_id"] = new_uuid
                    record["status"] = "SUCCESS"
                    changed = True
                mapping_records.append(record)
        if changed:
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return mapping_records


def _populate_global_mapping_from_uuid_log(logs_dir: Path, global_mapping: dict[str, dict[str, str]]) -> None:
    """
    Populate global id mapping directly from Stage 1 uuid_mapping_log.json.
    This avoids depending on export_process_map/export_source_map files that are not generated.
    """
    log_path = logs_dir / "uuid_mapping_log.json"
    if not log_path.exists():
        return
    try:
        entries = json.loads(log_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("jsonld_stage3.global_mapping_uuid_log_parse_failed", error=str(exc))
        return
    if not isinstance(entries, list):
        return
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        dataset_type = _coerce_text(entry.get("type")).lower()
        src = _coerce_text(entry.get("original_uuid"))
        dst = _coerce_text(entry.get("new_uuid"))
        if not src or not dst:
            continue
        if src in global_mapping.get("processes", {}) or src in global_mapping.get("sources", {}) or src in global_mapping.get("product_flows", {}):
            continue
        if dataset_type == "process":
            global_mapping["processes"][src] = dst
        elif dataset_type == "source":
            global_mapping["sources"][src] = dst
        elif dataset_type == "flow":
            global_mapping["product_flows"][src] = dst


def _collapse_global_mapping(global_mapping: dict[str, dict[str, str]]) -> None:
    """Resolve chained mappings (e.g., original -> stage1 -> export) into direct original -> export."""

    def _resolve(mapping: dict[str, str], key: str, seen: set[str]) -> str:
        if key in seen:
            return mapping.get(key, "")
        seen.add(key)
        value = mapping.get(key)
        if value and value in mapping:
            mapping[key] = _resolve(mapping, value, seen)
        return mapping.get(key, "")

    for table in ("processes", "sources", "product_flows"):
        mapping = global_mapping.get(table, {})
        for src in list(mapping.keys()):
            _resolve(mapping, src, set())


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

    hints = _load_elementary_flow_hints(exports_dir)
    logs_dir = exports_dir.parent / "logs"
    substitution_log_path = logs_dir / "elementary_flow_substitution_log.json"
    existing_substitution_records = _load_substitution_log(substitution_log_path)
    success_flow_ids = {
        record.get("original_flow_id")
        for record in existing_substitution_records
        if isinstance(record, dict) and record.get("status") == "SUCCESS"
    }
    if success_flow_ids and hints:
        before = len(hints)
        hints = [item for item in hints if item.get("original_uuid") not in success_flow_ids]
        LOGGER.info(
            "jsonld_stage3.resume_skip_completed_hints",
            skipped=len(success_flow_ids),
            remaining=len(hints),
            total_before=before,
        )
    hint_lookup = {entry.get("original_uuid"): entry.get("hint") for entry in hints if isinstance(entry, Mapping) and entry.get("original_uuid") and isinstance(entry.get("hint"), Mapping)}
    replacements: dict[str, dict[str, str]] = {}
    mapping_records: list[dict[str, Any]] = []
    llm = None
    flow_search_client: FlowSearchClient | None = None
    if hints and not args.skip_processes:
        api_key, model, base_url = load_secrets(args.secrets)
        cache_dir = None if args.disable_cache else (args.llm_cache or run_cache_path(run_id, Path("openai/stage3_jsonld")))
        if cache_dir and not args.disable_cache:
            cache_dir.parent.mkdir(parents=True, exist_ok=True)
        llm = OpenAIResponsesLLM(
            api_key=api_key,
            model=model,
            cache_dir=cache_dir,
            use_cache=not args.disable_cache,
            base_url=base_url,
        )
        try:
            flow_search_client = FlowSearchClient()
            replacements = _resolve_elementary_flow_replacements(hints, llm, flow_search_client)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("jsonld_stage3.elementary_flow_alignment_init_failed", error=str(exc))

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
                try:
                    _upsert_dataset(client, "flowproperties", payload, dry_run)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("jsonld_stage3.flow_property_publish_failed", path=str(path), error=str(exc))

        if not args.skip_flows:
            datasets = _iterate_datasets(exports_dir / "flows")
            for path, payload in datasets:
                LOGGER.info("jsonld_stage3.publish_flow", path=str(path), dry_run=dry_run)
                try:
                    result = _upsert_dataset(client, "flows", payload, dry_run)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("jsonld_stage3.flow_publish_failed", path=str(path), error=str(exc))
                    continue
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
                try:
                    result = _upsert_dataset(client, "sources", payload, dry_run)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("jsonld_stage3.source_publish_failed", path=str(path), error=str(exc))
                    continue
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

        if replacements and not args.skip_processes:
            mapping_records = _rewrite_elementary_flow_references(exports_dir / "processes", replacements, hint_lookup)
            if mapping_records:
                logs_dir.mkdir(parents=True, exist_ok=True)
                combined = _merge_substitution_logs(existing_substitution_records, mapping_records)
                substitution_log_path.write_text(json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8")

        # Build global ID mapping (original @id -> final UUID) for processes/sources/product flows
        try:
            logs_dir.mkdir(parents=True, exist_ok=True)
            mapping_path = logs_dir / "global_id_mapping.json"
            if mapping_path.exists():
                LOGGER.info("jsonld_stage3.global_mapping_exists_skip_rebuild", path=str(mapping_path))
            else:
                global_mapping = {"processes": {}, "sources": {}, "product_flows": {}}
                process_map = logs_dir / "export_process_map.json"
                source_map = logs_dir / "export_source_map.json"
                # process map: original @id (source_uuid/stage1_uuid) -> final export_uuid
                if process_map.exists():
                    try:
                        items = json.loads(process_map.read_text(encoding="utf-8"))
                        if isinstance(items, list):
                            for entry in items:
                                if isinstance(entry, dict):
                                    src = _coerce_text(entry.get("source_uuid") or entry.get("stage1_uuid"))
                                    dst = _coerce_text(entry.get("export_uuid"))
                                    if src and dst:
                                        global_mapping["processes"][src] = dst
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.warning("jsonld_stage3.global_mapping_process_parse_failed", error=str(exc))
                # source/flow map: only final export_uuid mappings; flows here are product/waste flows
                if source_map.exists():
                    try:
                        items = json.loads(source_map.read_text(encoding="utf-8"))
                        if isinstance(items, list):
                            for entry in items:
                                if not isinstance(entry, dict):
                                    continue
                                entry_type = _coerce_text(entry.get("type")).lower()
                                src = _coerce_text(entry.get("source_uuid") or entry.get("stage1_uuid"))
                                dst = _coerce_text(entry.get("export_uuid"))
                                if src and dst:
                                    if entry_type == "source":
                                        global_mapping["sources"][src] = dst
                                    elif entry_type == "flow":
                                        global_mapping["product_flows"][src] = dst
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.warning("jsonld_stage3.global_mapping_source_parse_failed", error=str(exc))
                # Fall back to Stage 1 mapping log to populate mappings when export maps are absent
                _populate_global_mapping_from_uuid_log(logs_dir, global_mapping)
                _collapse_global_mapping(global_mapping)
                mapping_path.write_text(json.dumps(global_mapping, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("jsonld_stage3.global_mapping_write_failed", error=str(exc))

        if not args.skip_processes:
            datasets = _iterate_datasets(exports_dir / "processes")
            for path, payload in datasets:
                LOGGER.info("jsonld_stage3.publish_process", path=str(path), dry_run=dry_run)
                try:
                    _upsert_dataset(client, "processes", payload, dry_run)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("jsonld_stage3.process_publish_failed", path=str(path), error=str(exc))

    finally:
        client.close()
        if flow_search_client:
            flow_search_client.close()

    status = "COMMITTED" if args.commit else "DRY-RUN"
    print(f"[jsonld-stage3] Publish complete ({status}) for run {run_id}")


if __name__ == "__main__":
    main()
