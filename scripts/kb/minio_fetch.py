"""List and download KB artifacts stored in MinIO."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Iterable

from minio.error import S3Error

from tiangong_lca_spec.kb import MinioConfig, create_minio_client, load_minio_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect or download KB artifacts stored in MinIO.")
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".secrets/secrets.toml"),
        help="Path to the secrets file containing the [minio] block.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List objects under the configured prefix.")
    list_parser.add_argument(
        "--path",
        default="",
        help="Remote path relative to the configured prefix (default: root of the prefix).",
    )
    list_parser.add_argument("--recursive", action="store_true", help="List contents recursively.")
    list_parser.add_argument("--limit", type=int, help="Optionally limit the number of results.")

    download_parser = subparsers.add_parser("download", help="Download a paper bundle from MinIO.")
    download_parser.add_argument(
        "--path",
        required=True,
        help="Remote folder relative to the configured prefix (e.g., 'Wang_et_al_...').",
    )
    download_parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/minio"),
        help="Local directory to store the downloaded files (default: artifacts/minio).",
    )
    download_parser.add_argument(
        "--include-source",
        action="store_true",
        help="Download source.pdf along with the parsed assets. Disabled by default.",
    )
    download_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print the planned downloads without fetching objects.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_minio_config(args.secrets)
    client = create_minio_client(config)

    try:
        if args.command == "list":
            handle_list(client, config, args.path, recursive=args.recursive, limit=args.limit)
        elif args.command == "download":
            handle_download(
                client,
                config,
                remote_path=args.path,
                output_dir=args.output,
                include_source=args.include_source,
                dry_run=args.dry_run,
            )
    except S3Error as exc:
        raise SystemExit(f"[minio] Request failed: {exc}") from exc


def handle_list(
    client,
    config: MinioConfig,
    remote_path: str,
    *,
    recursive: bool,
    limit: int | None,
) -> None:
    listing_prefix = _build_listing_prefix(config, remote_path)
    display_prefix = config.build_prefix(remote_path or None)
    display_root = display_prefix if remote_path else config.normalized_prefix()
    location = display_prefix or "(bucket root)"
    print(f"[minio] Listing bucket='{config.bucket_name}', path='{location or '/'}', recursive={recursive}")

    objects = client.list_objects(config.bucket_name, prefix=listing_prefix, recursive=recursive)
    total = 0
    for total, item in enumerate(_limit(objects, limit), start=1):
        relative = _relative_key(item.object_name, display_prefix or config.normalized_prefix())
        if not relative:
            relative = item.object_name
        kind = "dir " if item.is_dir else "file"
        size = "-" if item.is_dir else _format_size(item.size)
        timestamp = _format_timestamp(item.last_modified)
        name = relative.rstrip("/") + ("/" if item.is_dir and not relative.endswith("/") else "")
        print(f"{kind:>4}  {size:>10}  {timestamp:<20}  {name}")
    if total == 0:
        scope = display_root or "/"
        print(f"[minio] No objects found under '{scope}'.")
    else:
        print(f"[minio] Displayed {total} object(s).")


def handle_download(
    client,
    config: MinioConfig,
    *,
    remote_path: str,
    output_dir: Path,
    include_source: bool,
    dry_run: bool,
) -> None:
    if not remote_path:
        raise SystemExit("Provide --path pointing to the paper folder under the configured prefix.")

    listing_prefix = _build_listing_prefix(config, remote_path)
    display_prefix = config.build_prefix(remote_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[minio] Downloading from '{display_prefix}' into '{output_dir}' (include_source={include_source}, dry_run={dry_run})")
    objects = client.list_objects(config.bucket_name, prefix=listing_prefix, recursive=True)
    downloaded = 0
    skipped = 0

    for item in objects:
        if item.is_dir:
            continue
        relative = _relative_key(item.object_name, display_prefix)
        if not relative:
            relative = Path(item.object_name).name

        if not include_source and relative.lower().endswith("source.pdf"):
            print(f"[skip] source.pdf excluded: {relative}")
            skipped += 1
            continue

        destination = output_dir / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if dry_run:
            print(f"[dry-run] Would download '{item.object_name}' -> '{destination}' ({_format_size(item.size)})")
            downloaded += 1
            continue

        client.fget_object(config.bucket_name, item.object_name, str(destination))
        print(f"[ok] Downloaded '{relative}' -> '{destination}' ({_format_size(item.size)})")
        downloaded += 1

    if downloaded == 0:
        raise SystemExit(f"[minio] No files matched under '{display_prefix}'.")
    print(f"[minio] Completed downloads: {downloaded} file(s), skipped {skipped} file(s).")


def _build_listing_prefix(config: MinioConfig, remote_path: str) -> str:
    prefix = config.build_prefix(remote_path or None)
    if not prefix:
        return ""
    normalized = prefix.rstrip("/")
    return f"{normalized}/"


def _relative_key(full_key: str, base_prefix: str | None) -> str:
    normalized_key = full_key.strip("/")
    if not base_prefix:
        return normalized_key
    normalized_base = base_prefix.strip("/")
    if not normalized_base:
        return normalized_key
    if normalized_key == normalized_base:
        return normalized_key.split("/")[-1]
    anchor = f"{normalized_base}/"
    if normalized_key.startswith(anchor):
        return normalized_key[len(anchor) :]
    if normalized_key.startswith(normalized_base):
        return normalized_key[len(normalized_base) :].lstrip("/")
    return normalized_key


def _limit(items: Iterable, limit: int | None):
    if limit is None or limit <= 0:
        yield from items
        return
    for idx, item in enumerate(items, start=1):
        if idx > limit:
            break
        yield item


def _format_size(size: int | None) -> str:
    if size is None:
        return "-"
    size = int(size)
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{size}B"


def _format_timestamp(ts: datetime | None) -> str:
    if not ts:
        return "-"
    return ts.strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    main()
