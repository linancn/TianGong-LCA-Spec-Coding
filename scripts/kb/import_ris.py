"""Import RIS entries (and their attachments) into the configured knowledge base dataset."""

from __future__ import annotations

import argparse
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
    parser.add_argument(
        "--ris-dir",
        type=Path,
        default=Path("input_data/battery"),
        help="Directory containing the RIS file and related attachments.",
    )
    parser.add_argument("--ris-file", default="battery.ris", help="RIS filename when --ris-path is not provided. Defaults to battery.ris.")
    parser.add_argument(
        "--attachments-root",
        type=Path,
        help="Root directory for attachment paths. Defaults to the RIS directory.",
    )
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


def build_process_rule(doc_form: str) -> dict[str, Any]:
    if doc_form == "hierarchical_model":
        return {
            "mode": "hierarchical",
            "rules": {
                "pre_processing_rules": [
                    {"id": "remove_extra_spaces", "enabled": True},
                    {"id": "remove_urls_emails", "enabled": False},
                ],
                "segmentation": {"separator": "\n\n", "max_tokens": 600, "chunk_overlap": 120},
                "parent_mode": "paragraph",
                "subchunk_segmentation": {"separator": "\n", "max_tokens": 200, "chunk_overlap": 40},
            },
        }
    return {
        "mode": "custom",
        "rules": {
            "pre_processing_rules": [
                {"id": "remove_extra_spaces", "enabled": True},
                {"id": "remove_urls_emails", "enabled": False},
            ],
            "segmentation": {"separator": "\n\n", "max_tokens": 500, "chunk_overlap": 100},
        },
    }


def main() -> None:
    args = parse_args()
    ris_path = args.ris_path or (args.ris_dir / args.ris_file)
    attachments_root = args.attachments_root or args.ris_dir
    entries = load_ris_entries(ris_path)
    if not entries:
        print(f"[kb] No entries found in {ris_path}")
        return

    if args.limit is not None:
        entries = entries[: args.limit]

    config: KnowledgeBaseConfig = load_kb_config(args.secrets)

    if args.dry_run:
        run_dry(entries, attachments_root, config)
        return

    with KnowledgeBaseClient(config) as client:
        dataset_info = client.fetch_dataset_info()
        doc_form = dataset_info.get("doc_form") or dataset_info.get("chunk_structure") or "text_model"
        indexing = dataset_info.get("indexing_technique") or "high_quality"
        process_rule = build_process_rule(doc_form)
        metadata_ids = client.ensure_metadata_fields(config.metadata_fields)

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
            metadata_entries = build_metadata_entries(enriched_record, metadata_ids, config.metadata_fields)
            payload_dict: dict[str, Any] = {"indexing_technique": indexing, "doc_form": doc_form, "process_rule": process_rule}

            response = client.upload_document(attachment, payload_dict)
            document_id = response.get("document", {}).get("id")
            if not document_id:
                print(f"[warn] Upload succeeded without document ID for '{title}'. Raw response: {response}")
                continue
            client.attach_metadata(document_id, metadata_entries)
            uploaded += 1
            print(f"[ok] Uploaded '{title}' as document {document_id}.")

        print(f"[kb] Completed uploads: {uploaded}/{total}")


def run_dry(entries: list[dict[str, Any]], attachments_root: Path, config: KnowledgeBaseConfig) -> None:
    """Print the planned operations without invoking the remote API."""
    doc_form = "text_model"
    process_rule = build_process_rule(doc_form)
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
        metadata_entries = build_metadata_entries(enriched_record, metadata_ids, config.metadata_fields)
        meta_value = metadata_entries[0]["value"] if metadata_entries else "<empty>"
        print(f"[dry-run][ok] Would upload '{attachment.name}' from '{attachment}' with meta='{meta_value}'")


if __name__ == "__main__":
    main()
