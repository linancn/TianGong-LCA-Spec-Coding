#!/usr/bin/env python
"""Stage 2: run the process extraction pipeline to produce process blocks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from _workflow_common import OpenAIResponsesLLM, dump_json, load_secrets

from tiangong_lca_spec.process_extraction import ProcessExtractionService


def _read_clean_text(path: Path) -> str:
    raw = path.read_text(encoding="utf-8")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if isinstance(payload, dict) and "clean_text" in payload:
        value = payload["clean_text"]
        if not isinstance(value, str):
            raise SystemExit(f"'clean_text' must be a string in {path}")
        return value
    raise SystemExit(
        (
            f"Unexpected clean text format in {path}; expected plain markdown or JSON "
            "with 'clean_text'."
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--clean-text",
        type=Path,
        default=Path("artifacts/stage1_clean_text.md"),
        help="Clean markdown emitted by stage1_preprocess.",
    )
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".secrets/secrets.toml"),
        help="Secrets file containing OpenAI credentials.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/stage2_process_blocks.json"),
        help="Where to write the extracted process blocks JSON.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip extraction if the output file already exists and appears valid.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(".cache/openai/stage2"),
        help="Directory used to cache OpenAI responses for resumable runs.",
    )
    parser.add_argument(
        "--disable-cache",
        action="store_true",
        help="Disable disk caching even if a cache directory is provided.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.resume and args.output.exists():
        try:
            existing = json.loads(args.output.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = None
        if isinstance(existing, dict) and existing.get("process_blocks"):
            print(f"Stage 2 output already present at {args.output}; skipping due to --resume.")
            return
        print("Existing output is missing or invalid; continuing extraction.")

    clean_text = _read_clean_text(args.clean_text)
    api_key, model = load_secrets(args.secrets)
    llm = OpenAIResponsesLLM(
        api_key=api_key,
        model=model,
        cache_dir=args.cache_dir if not args.disable_cache else None,
        use_cache=not args.disable_cache,
    )
    service = ProcessExtractionService(llm)
    process_blocks = service.extract(clean_text)
    dump_json({"process_blocks": process_blocks}, args.output)
    print(f"Extracted {len(process_blocks)} process blocks -> {args.output}")


if __name__ == "__main__":
    main()
