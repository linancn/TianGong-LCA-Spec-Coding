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
    raise SystemExit(f"Unexpected clean text format in {path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--clean-text",
        type=Path,
        default=Path("artifacts/stage1_clean_text.json"),
        help="Clean text JSON emitted by stage1_preprocess.",
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    clean_text = _read_clean_text(args.clean_text)
    api_key, model = load_secrets(args.secrets)
    llm = OpenAIResponsesLLM(api_key=api_key, model=model)
    service = ProcessExtractionService(llm)
    process_blocks = service.extract(clean_text)
    dump_json({"process_blocks": process_blocks}, args.output)
    print(f"Extracted {len(process_blocks)} process blocks -> {args.output}")


if __name__ == "__main__":
    main()
