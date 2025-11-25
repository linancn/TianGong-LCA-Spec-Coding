#!/usr/bin/env python
"""Convert JSON-LD conversion prompts into inline text for Codex executions."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PREFIX = "Follow the staged workflow strictly:"
DEFAULT_PROMPT_PATH = Path(".github/prompts/convert_json.prompt.md")


def markdown_to_inline(md: str) -> str:
    """Convert markdown content to a single inline string."""
    text = md
    text = re.sub(r"```.*?```", " ", text, flags=re.DOTALL)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = re.sub(r"\*(.*?)\*", r"\1", text)
    text = re.sub(r"__(.*?)__", r"\1", text)
    text = re.sub(r"_([^_]+)_", r"\1", text)
    text = re.sub(r"!\[[^\]]*\]\([^)]+\)", " ", text)
    text = re.sub(r"\[[^\]]*\]\(([^)]+)\)", r"\1", text)
    text = re.sub(r"^\s{0,3}[-*+]\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s{0,3}>\s?", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s{0,3}#{1,6}\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def build_inline_prompt(prompt_path: Path, source_json: str) -> str:
    """Generate the inline prompt string for Codex executions."""
    try:
        content = prompt_path.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - delegated to caller
        raise SystemExit(f"Failed to read {prompt_path}: {exc}") from exc

    inline_text = markdown_to_inline(content)
    source_path = Path(source_json)
    if not source_path.exists():
        raise SystemExit(f"Source path does not exist: {source_path}")
    if source_path.is_dir():
        suffix = "The source text consists of JSON-LD payloads under " f"{{{source_json}}}; iterate over every *.json file recursively."
    else:
        suffix = f"The source text is located at {{{source_json}}}."
    return " ".join(part for part in (PREFIX, inline_text, suffix) if part).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prompt-path",
        type=Path,
        default=DEFAULT_PROMPT_PATH,
        help="Path to the JSON-LD conversion prompt (default: .github/prompts/convert_json.prompt.md).",
    )
    parser.add_argument(
        "--source-json",
        required=True,
        help="Path to the JSON-LD source directory (processes/flows) that the LLM should iterate.",
    )
    parser.add_argument(
        "--output",
        default="inline_prompt_jsonld.txt",
        help="Output file for inline prompt text (use '-' for stdout).",
    )
    args = parser.parse_args()

    result = build_inline_prompt(args.prompt_path, args.source_json)

    if args.output == "-":
        sys.stdout.write(result)
        return

    output_path = Path(args.output)
    try:
        output_path.write_text(result, encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"Failed to write to {output_path}: {exc}") from exc


if __name__ == "__main__":
    main()
