#!/usr/bin/env python
"""Stage 2: run the process extraction pipeline to produce process blocks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from scripts.md._workflow_common import (  # type: ignore
        OpenAIResponsesLLM,
        dump_json,
        ensure_run_cache_dir,
        load_secrets,
        resolve_run_id,
        run_cache_path,
        save_latest_run_id,
    )
except ModuleNotFoundError:  # pragma: no cover - allows direct CLI execution
    from _workflow_common import (
        OpenAIResponsesLLM,
        dump_json,
        ensure_run_cache_dir,
        load_secrets,
        resolve_run_id,
        run_cache_path,
        save_latest_run_id,
    )

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
    raise SystemExit((f"Unexpected clean text format in {path}; expected plain markdown or JSON " "with 'clean_text'."))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-id",
        help=("Identifier used to locate run artifacts under artifacts/<run_id>/. " "Defaults to the most recent run recorded by stage1_preprocess."),
    )
    parser.add_argument(
        "--clean-text",
        type=Path,
        help=("Optional override for the Stage 1 output path. " "Defaults to artifacts/<run_id>/cache/stage1_clean_text.md."),
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
        help=("Optional override for the Stage 2 process blocks JSON path. " "Defaults to artifacts/<run_id>/cache/stage2_process_blocks.json."),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip extraction if the output file already exists and appears valid.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        help=("Directory used to cache OpenAI responses. " "Defaults to artifacts/<run_id>/cache/openai/stage2."),
    )
    parser.add_argument(
        "--disable-cache",
        action="store_true",
        help="Disable disk caching even if a cache directory is provided.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_id = resolve_run_id(args.run_id)
    ensure_run_cache_dir(run_id)
    save_latest_run_id(run_id)

    clean_text_path = args.clean_text or run_cache_path(run_id, "stage1_clean_text.md")
    output_path = args.output or run_cache_path(run_id, "stage2_process_blocks.json")
    openai_cache_dir = args.cache_dir or run_cache_path(run_id, Path("openai/stage2"))

    if args.resume and output_path.exists():
        try:
            existing = json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = None
        if isinstance(existing, dict) and existing.get("process_blocks"):
            print(f"Stage 2 output already present at {output_path}; skipping due to --resume.")
            return
        print("Existing output is missing or invalid; continuing extraction.")

    if not clean_text_path.exists():
        raise SystemExit(f"Clean text file not found: {clean_text_path}")

    clean_text = _read_clean_text(clean_text_path)
    api_key, model, base_url = load_secrets(args.secrets)
    if not args.disable_cache:
        openai_cache_dir.parent.mkdir(parents=True, exist_ok=True)
        openai_cache = openai_cache_dir
    else:
        openai_cache = None

    llm = OpenAIResponsesLLM(
        api_key=api_key,
        model=model,
        cache_dir=openai_cache,
        use_cache=not args.disable_cache,
        base_url=base_url,
    )
    service = ProcessExtractionService(llm)
    process_blocks = service.extract(clean_text)
    dump_json({"process_blocks": process_blocks}, output_path)
    print(f"[{run_id}] Extracted {len(process_blocks)} process blocks -> {output_path}")


if __name__ == "__main__":
    main()
