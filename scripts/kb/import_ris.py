"""Import RIS entries (and their attachments) into the configured knowledge base dataset."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import rispy

from tiangong_lca_spec.kb import (
    KnowledgeBaseClient,
    KnowledgeBaseConfig,
    build_metadata_entries,
    format_citation,
    load_kb_config,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload RIS entries and attachments to the knowledge base.")
    parser.add_argument("--secrets", type=Path, default=Path(".secrets/secrets.toml"), help="Secrets file containing [kb] credentials.")
    parser.add_argument("--ris-path", type=Path, help="Direct path to the RIS file. Overrides --ris-dir/--ris-file when provided.")
    parser.add_argument("--ris-dir", type=Path, help="Directory containing the RIS file and related attachments.")
    parser.add_argument("--ris-file", default="battery.ris", help="RIS filename when --ris-path is not provided. Defaults to battery.ris.")
    parser.add_argument(
        "--attachments-root",
        type=Path,
        help="Root directory for attachment paths. Defaults to the RIS directory.",
    )
    parser.add_argument("--category", help="Optional override for the metadata category. Defaults to the input_data subdirectory name.")
    parser.add_argument("--pipeline-inputs", help="Inline JSON string for pipeline inputs (defaults to config `[kb.pipeline.inputs]`).")
    parser.add_argument("--pipeline-inputs-file", type=Path, help="Path to a JSON file containing pipeline inputs.")
    parser.add_argument("--limit", type=int, help="Optionally limit the number of references ingested.")
    parser.add_argument("--dry-run", action="store_true", help="Only print the planned operations without contacting the API.")
    return parser.parse_args()


def load_ris_entries(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"RIS file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        return list(rispy.load(handle))


def resolve_attachment_path(record: dict[str, Any], root: Path) -> Path | None:
    attachment_keys = [key for key in record.keys() if key.startswith("file_attachments")]
    for key in sorted(attachment_keys):
        candidate = record.get(key)
        if not candidate:
            continue
        attachment_path = Path(candidate)
        if attachment_path.is_absolute():
            return attachment_path if attachment_path.exists() else None
        resolved = (root / attachment_path).resolve()
        if resolved.exists():
            return resolved
    return None


def main() -> None:
    args = parse_args()
    ris_path = _resolve_ris_path(args)
    attachments_root = _resolve_attachments_root(args, ris_path)
    category_value = _resolve_category(args, ris_path)
    config: KnowledgeBaseConfig = load_kb_config(args.secrets)
    pipeline_inputs = _resolve_pipeline_inputs(args, config)
    entries = load_ris_entries(ris_path)
    if not entries:
        print(f"[kb] No entries found in {ris_path}")
        return

    if args.limit is not None:
        entries = entries[: args.limit]

    if args.dry_run:
        run_dry(entries, attachments_root, category_value, config, pipeline_inputs)
        return

    with KnowledgeBaseClient(config) as client:
        metadata_ids = client.ensure_metadata_fields(config.metadata_fields)
        start_node_id = config.pipeline_start_node_id or client.resolve_pipeline_start_node_id(
            config.pipeline_datasource_type, is_published=config.pipeline_is_published
        )
        if not start_node_id:
            raise SystemExit(
                "Unable to resolve pipeline start node id. Provide [kb.pipeline].start_node_id or ensure "
                "the datasource_type matches a published pipeline datasource block."
            )

        total = len(entries)
        uploaded = 0

        for idx, record in enumerate(entries, start=1):
            attachment = resolve_attachment_path(record, attachments_root)
            title = record.get("title") or record.get("primary_title") or f"record_{idx}"
            if not attachment:
                print(f"[skip] No attachment for '{title}' (record #{idx}).")
                continue
            enriched_record = dict(record)
            citation = format_citation(record)
            if citation:
                enriched_record["meta"] = citation
            if category_value:
                enriched_record["category"] = category_value
            metadata_entries = build_metadata_entries(enriched_record, metadata_ids, config.metadata_fields)
            meta_value = _get_metadata_value(metadata_entries, "meta")
            run_inputs = _build_pipeline_inputs(pipeline_inputs, meta_value)

            uploaded_file = client.upload_pipeline_file(attachment)
            datasource_entry = _build_datasource_entry(
                uploaded_file, config.pipeline_datasource_type, meta_value=meta_value
            )
            pipeline_payload = {
                "inputs": run_inputs,
                "datasource_type": config.pipeline_datasource_type,
                "datasource_info_list": [datasource_entry],
                "start_node_id": start_node_id,
                "is_published": config.pipeline_is_published,
                "response_mode": config.pipeline_response_mode,
                "meta": meta_value or "",
            }
            response = client.run_pipeline(pipeline_payload)
            document_ids = _extract_document_ids(response)
            if not document_ids:
                print(f"[warn] Pipeline run completed but no document IDs were returned for '{title}'. Response: {response}")
                continue
            for document_id in document_ids:
                client.attach_metadata(document_id, metadata_entries)
                uploaded += 1
                print(f"[ok] Uploaded '{title}' through pipeline as document {document_id}.")

        print(f"[kb] Completed uploads: {uploaded}/{total} (category='{category_value}')")


def run_dry(
    entries: list[dict[str, Any]],
    attachments_root: Path,
    category_value: str | None,
    config: KnowledgeBaseConfig,
    pipeline_inputs: dict[str, Any],
) -> None:
    """Print the planned operations without invoking the remote API."""
    metadata_ids = {definition.name: f"dry_{idx}" for idx, definition in enumerate(config.metadata_fields, start=1)}
    for idx, record in enumerate(entries, start=1):
        title = record.get("title") or record.get("primary_title") or f"record_{idx}"
        attachment = resolve_attachment_path(record, attachments_root)
        if not attachment:
            print(f"[dry-run][skip] Missing attachment for '{title}'.")
            continue
        enriched_record = dict(record)
        citation = format_citation(record)
        if citation:
            enriched_record["meta"] = citation
        if category_value:
            enriched_record["category"] = category_value
        metadata_entries = build_metadata_entries(enriched_record, metadata_ids, config.metadata_fields)
        meta_value = _get_metadata_value(metadata_entries, "meta")
        meta_entry = meta_value or "<empty>"
        category_entry = _get_metadata_value(metadata_entries, "category") or "<empty>"
        run_inputs = _build_pipeline_inputs(pipeline_inputs, meta_value)
        print(
            f"[dry-run][ok] Would upload '{attachment.name}' from '{attachment}' with meta='{meta_entry}', "
            f"category='{category_entry}', pipeline_inputs={json.dumps(run_inputs, ensure_ascii=False)}"
        )


def _get_metadata_value(entries: list[dict[str, Any]], name: str) -> str | None:
    for entry in entries:
        if entry.get("name") == name:
            value = entry.get("value")
            return str(value) if value is not None else None
    return None


def _build_pipeline_inputs(base_inputs: dict[str, Any], meta_value: str | None) -> dict[str, Any]:
    """Return the pipeline inputs payload with the required meta field."""
    merged = dict(base_inputs)
    merged["meta"] = meta_value or ""
    return merged


def _resolve_ris_path(args: argparse.Namespace) -> Path:
    if args.ris_path:
        return args.ris_path
    if args.ris_dir:
        return args.ris_dir / args.ris_file
    raise SystemExit("Provide either --ris-path or --ris-dir.")


def _resolve_attachments_root(args: argparse.Namespace, ris_path: Path) -> Path:
    if args.attachments_root:
        return args.attachments_root
    if args.ris_dir:
        return args.ris_dir
    return ris_path.parent


def _resolve_category(args: argparse.Namespace, ris_path: Path) -> str | None:
    if args.category:
        return args.category
    source_path = args.ris_dir or ris_path.parent
    return _derive_category_from_path(source_path)


def _derive_category_from_path(path: Path) -> str | None:
    try:
        normalized = path.resolve()
    except FileNotFoundError:
        normalized = path
    parts = normalized.parts
    if "input_data" in parts:
        idx = parts.index("input_data")
        if idx + 1 < len(parts):
            candidate = parts[idx + 1]
            if candidate:
                return candidate
    name = normalized.name
    return name or None


def _resolve_pipeline_inputs(args: argparse.Namespace, config: KnowledgeBaseConfig) -> dict[str, Any]:
    if args.pipeline_inputs and args.pipeline_inputs_file:
        raise SystemExit("Use either --pipeline-inputs or --pipeline-inputs-file (not both).")
    if args.pipeline_inputs:
        return _load_payload_from_string(args.pipeline_inputs)
    if args.pipeline_inputs_file:
        return _load_payload_from_file(args.pipeline_inputs_file)
    return dict(config.pipeline_inputs)


def _load_payload_from_string(payload_text: str) -> dict[str, Any]:
    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON provided via --payload: {exc}") from exc
    return _ensure_payload_object(payload)


def _load_payload_from_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Payload file not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON in payload file {path}: {exc}") from exc
    return _ensure_payload_object(payload)


def _ensure_payload_object(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise SystemExit("Pipeline inputs must be a JSON object.")
    return payload


def _build_datasource_entry(
    uploaded_file: dict[str, Any],
    datasource_type: str,
    *,
    meta_value: str | None = None,
) -> dict[str, Any]:
    entry = {
        "related_id": uploaded_file.get("id"),
        "name": uploaded_file.get("name"),
        "type": uploaded_file.get("mime_type") or uploaded_file.get("type") or "application/octet-stream",
        "size": uploaded_file.get("size"),
        "extension": uploaded_file.get("extension") or Path(uploaded_file.get("name", "")).suffix.lstrip("."),
        "mime_type": uploaded_file.get("mime_type") or "application/octet-stream",
        "url": uploaded_file.get("url") or "",
        "transfer_method": datasource_type,
        "credential_id": uploaded_file.get("credential_id"),
    }
    if meta_value:
        entry["meta"] = meta_value
    return entry


def _extract_document_ids(response: dict[str, Any]) -> list[str]:
    candidates = []
    if isinstance(response, dict):
        documents = response.get("documents") or response.get("document_list") or response.get("data")
        if isinstance(documents, list):
            for item in documents:
                if isinstance(item, dict):
                    doc_id = item.get("id") or item.get("document_id")
                    if doc_id:
                        candidates.append(str(doc_id))
        elif isinstance(documents, dict):
            doc_id = documents.get("id") or documents.get("document_id")
            if doc_id:
                candidates.append(str(doc_id))
        result = response.get("result")
        if isinstance(result, dict):
            candidates.extend(_extract_document_ids(result))
    return candidates


if __name__ == "__main__":
    main()
