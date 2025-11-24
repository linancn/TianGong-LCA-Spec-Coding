#!/usr/bin/env python
"""Generate inline JSON-LD prompts and execute Stage 1→3 in a single command."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from scripts_jsonld.convert_prompt_to_inline import DEFAULT_PROMPT_PATH, build_inline_prompt

try:
    from scripts._workflow_common import generate_run_id  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    from _workflow_common import generate_run_id  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--process-dir",
        type=Path,
        required=True,
        help="Directory (or file) containing OpenLCA JSON-LD process datasets.",
    )
    parser.add_argument("--flows-dir", type=Path, help="Directory containing OpenLCA JSON-LD flow datasets.")
    parser.add_argument("--sources-dir", type=Path, help="Directory containing OpenLCA JSON-LD source datasets.")
    parser.add_argument(
        "--prompt-path",
        type=Path,
        default=DEFAULT_PROMPT_PATH,
        help="Markdown prompt that will be flattened into inline text.",
    )
    parser.add_argument(
        "--inline-output",
        type=Path,
        default=Path("inline_prompt_jsonld.txt"),
        help="Destination file for the generated inline text.",
    )
    parser.add_argument("--run-id", help="Optional run identifier shared across JSON-LD stages.")
    parser.add_argument(
        "--clean-exports",
        action="store_true",
        help="Clean artifacts/<run>/exports before Stage 2 writes new files.",
    )
    parser.add_argument(
        "--dry-run-publish",
        action="store_true",
        help="Keep Stage 3 in dry-run mode (no Database_CRUD_Tool commit).",
    )
    parser.add_argument("--skip-stage3", action="store_true", help="Stop after Stage 2 (no publish).")
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".secrets/secrets.toml"),
        help="Secrets file containing OpenAI credentials.",
    )
    parser.add_argument("--llm-cache", type=Path, help="Optional override for the Stage 1 JSON-LD LLM cache directory.")
    parser.add_argument("--disable-cache", action="store_true", help="Disable LLM response caching during Stage 1.")
    parser.add_argument(
        "--stage2-extra-args",
        nargs=argparse.REMAINDER,
        help="Additional arguments appended to the Stage 2 command (after orchestrator-provided args).",
    )
    parser.add_argument(
        "--inline-only",
        action="store_true",
        help="Generate the inline prompt and exit without executing Stage 1→Stage 3.",
    )
    parser.add_argument(
        "--print-inline",
        action="store_true",
        help="Echo the inline prompt to stdout after writing it to disk.",
    )
    return parser.parse_args()


def _write_inline(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _masked_command(cmd: list[str]) -> str:
    masked: list[str] = []
    skip_next = False
    for idx, token in enumerate(cmd):
        if skip_next:
            skip_next = False
            continue
        if token == "--prompt-inline" and idx + 1 < len(cmd):
            masked.append("--prompt-inline")
            masked.append("<inline-prompt>")
            skip_next = True
        else:
            masked.append(token)
    return " ".join(shlex.quote(item) for item in masked)


def _run_command(cmd: list[str]) -> None:
    print(f"[jsonld-inline] Executing: {_masked_command(cmd)}")
    subprocess.run(cmd, check=True)


def main() -> None:
    args = parse_args()
    inline_text = build_inline_prompt(args.prompt_path, str(args.process_dir))
    _write_inline(inline_text, args.inline_output)
    print(f"[jsonld-inline] Inline prompt written to {args.inline_output}")
    if args.print_inline:
        print()
        print(inline_text)
    if args.inline_only:
        return

    run_id = args.run_id or generate_run_id()
    run_pipeline = Path("scripts_jsonld") / "run_pipeline.py"
    if not run_pipeline.exists():
        raise SystemExit("scripts_jsonld/run_pipeline.py not found; ensure you are in the repository root.")

    cmd: list[str] = [
        sys.executable,
        str(run_pipeline),
        "--process-dir",
        str(args.process_dir),
        "--run-id",
        run_id,
        "--prompt-inline",
        inline_text,
        "--secrets",
        str(args.secrets),
    ]
    if args.flows_dir:
        cmd.extend(["--flows-dir", str(args.flows_dir)])
    if args.sources_dir:
        cmd.extend(["--sources-dir", str(args.sources_dir)])
    if args.clean_exports:
        cmd.append("--clean-exports")
    if args.dry_run_publish:
        cmd.append("--dry-run-publish")
    if args.skip_stage3:
        cmd.append("--skip-stage3")
    if args.llm_cache:
        cmd.extend(["--llm-cache", str(args.llm_cache)])
    if args.disable_cache:
        cmd.append("--disable-cache")
    if args.stage2_extra_args:
        cmd.append("--stage2-extra-args")
        cmd.extend(args.stage2_extra_args)

    print(f"[jsonld-inline] Run ID: {run_id}")
    _run_command(cmd)


if __name__ == "__main__":
    main()
