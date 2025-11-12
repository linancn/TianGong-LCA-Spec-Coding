from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


PREFIX = "Follow the staged workflow strictly:"
DEFAULT_PROMPT_PATH = Path(".github/prompts/extract-process-workflow.prompt.md")


def markdown_to_inline(md: str) -> str:
    """Convert markdown content to a single inline text string."""
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert a markdown prompt file into inline text.")
    parser.add_argument(
        "--prompt-path",
        type=Path,
        default=DEFAULT_PROMPT_PATH,
        help=("Path to the markdown file to insert between the prefix and suffix. " "Defaults to .github/prompts/extract-process-workflow.prompt.md."),
    )
    parser.add_argument(
        "--source-json",
        required=True,
        help="Required path to the source JSON file inserted in the closing sentence.",
    )
    parser.add_argument(
        "--output",
        default="inline_prompt.txt",
        help=("Optional path for the generated inline text. Defaults to " "inline_prompt.txt in the current working directory. Use '-' to write to stdout."),
    )
    args = parser.parse_args()

    try:
        content = args.prompt_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"Failed to read {args.prompt_path}: {exc}") from exc

    inline_text = markdown_to_inline(content)
    suffix = f"The source text is located at {{{args.source_json}}}."

    parts: list[str] = [PREFIX]
    if inline_text:
        parts.append(inline_text)
    parts.append(suffix)

    result = " ".join(parts)

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
