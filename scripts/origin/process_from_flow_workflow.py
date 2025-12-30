#!/usr/bin/env python
"""Run the process_from_flow workflow with SI integration before Step 1.

Usage:
  uv run python scripts/origin/process_from_flow_workflow.py --flow <path> [options]

This script orchestrates:
  - Step 1a/1b/1c via process_from_flow_langgraph.py --stop-after references
  - reference usability screening (1b optional)
  - SI download + MinerU parsing (1d)
  - reference usage tagging (1e)
  - resume full pipeline so SI affects Step 1/2/3
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPT_DIR.parent
REPO_ROOT = SCRIPTS_DIR.parent
for path in (SCRIPTS_DIR, REPO_ROOT):
    if str(path) not in sys.path:
        sys.path.append(str(path))

try:
    from scripts.md._workflow_common import generate_run_id  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    from _workflow_common import generate_run_id  # type: ignore

try:
    from scripts.origin.process_from_flow_langgraph import DEFAULT_FLOW_PATH  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    from process_from_flow_langgraph import DEFAULT_FLOW_PATH  # type: ignore

PROCESS_FROM_FLOW_ARTIFACTS_ROOT = Path("artifacts/process_from_flow")
DEFAULT_SI_SUBDIR = Path("input/si")
MINERU_SUFFIXES = {".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp"}
TEXT_SUFFIXES = {".txt", ".md", ".markdown", ".csv", ".tsv", ".xlsx", ".docx"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flow", type=Path, default=DEFAULT_FLOW_PATH, help="Path to the reference flow JSON.")
    parser.add_argument("--operation", choices=("produce", "treat"), default="produce", help="Whether the process produces or treats the reference flow.")
    parser.add_argument("--run-id", help="Run identifier under artifacts/process_from_flow/<run_id>.")
    parser.add_argument("--secrets", type=Path, default=Path(".secrets/secrets.toml"), help="Secrets file for LLM/SI tools.")
    parser.add_argument("--no-llm", action="store_true", help="Run without LLM (deterministic fallback).")
    parser.add_argument("--no-translate-zh", action="store_true", help="Skip adding Chinese translations.")
    parser.add_argument("--min-si-hint", default="possible", help="Min si_hint to download (none|possible|likely).")
    parser.add_argument("--si-max-links", type=int, help="Max SI links per DOI.")
    parser.add_argument("--si-timeout", type=float, help="HTTP timeout for SI download.")
    parser.add_argument("--publish", action="store_true", help="Publish generated process datasets after completion.")
    parser.add_argument("--publish-flows", action="store_true", help="Also publish placeholder flow datasets.")
    parser.add_argument("--commit", action="store_true", help="Actually invoke Database_CRUD_Tool (default: dry-run).")
    parser.add_argument(
        "--stop-after",
        choices=("references", "tech", "processes", "exchanges", "matches", "sources", "datasets"),
        help="Stop after a stage (debug only).",
    )
    return parser.parse_args()


def _run_python(script: Path, args: list[str]) -> None:
    cmd = [sys.executable, str(script), *args]
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def _run_reference_stage(args: argparse.Namespace, run_id: str) -> None:
    script = SCRIPT_DIR / "process_from_flow_langgraph.py"
    cmd = [
        "--flow",
        str(args.flow),
        "--operation",
        args.operation,
        "--run-id",
        run_id,
        "--stop-after",
        "references",
        "--secrets",
        str(args.secrets),
    ]
    if args.no_llm:
        cmd.append("--no-llm")
    if args.no_translate_zh:
        cmd.append("--no-translate-zh")
    _run_python(script, cmd)


def _run_usability(args: argparse.Namespace, run_id: str) -> None:
    script = SCRIPT_DIR / "process_from_flow_reference_usability.py"
    cmd = [
        "--run-id",
        run_id,
        "--secrets",
        str(args.secrets),
    ]
    _run_python(script, cmd)


def _run_si_download(args: argparse.Namespace, run_id: str) -> None:
    script = SCRIPT_DIR / "process_from_flow_download_si.py"
    cmd = [
        "--run-id",
        run_id,
        "--min-si-hint",
        args.min_si_hint,
    ]
    if args.si_max_links is not None:
        cmd.extend(["--max-links", str(args.si_max_links)])
    if args.si_timeout is not None:
        cmd.extend(["--timeout", str(args.si_timeout)])
    _run_python(script, cmd)


def _run_usage_tagging(args: argparse.Namespace, run_id: str) -> None:
    script = SCRIPT_DIR / "process_from_flow_reference_usage_tagging.py"
    cmd = [
        "--run-id",
        run_id,
        "--secrets",
        str(args.secrets),
    ]
    _run_python(script, cmd)


def _iter_si_files(run_id: str) -> list[Path]:
    si_root = PROCESS_FROM_FLOW_ARTIFACTS_ROOT / run_id / DEFAULT_SI_SUBDIR
    if not si_root.exists():
        return []
    files: list[Path] = []
    for path in sorted(si_root.rglob("*")):
        if not path.is_file():
            continue
        files.append(path)
    return files


def _run_mineru_for_si(args: argparse.Namespace, run_id: str) -> None:
    script = SCRIPT_DIR / "mineru_for_process_si.py"
    failures: list[Path] = []
    for path in _iter_si_files(run_id):
        suffix = path.suffix.lower()
        if suffix in TEXT_SUFFIXES:
            continue
        if suffix not in MINERU_SUFFIXES:
            print(f"[warn] Skip unsupported SI file: {path}", file=sys.stderr)
            continue
        cmd = [
            str(path),
            "--run-id",
            run_id,
            "--secrets-path",
            str(args.secrets),
        ]
        try:
            _run_python(script, cmd)
        except subprocess.CalledProcessError:
            failures.append(path)
    if failures:
        print(f"[warn] MinerU failed for {len(failures)} SI file(s).", file=sys.stderr)


def _run_main_pipeline(args: argparse.Namespace, run_id: str) -> None:
    script = SCRIPT_DIR / "process_from_flow_langgraph.py"
    cmd = [
        "--flow",
        str(args.flow),
        "--operation",
        args.operation,
        "--run-id",
        run_id,
        "--resume",
        "--secrets",
        str(args.secrets),
    ]
    if args.no_llm:
        cmd.append("--no-llm")
    if args.no_translate_zh:
        cmd.append("--no-translate-zh")
    if args.stop_after:
        cmd.extend(["--stop-after", args.stop_after])
    if args.publish:
        cmd.append("--publish")
    if args.publish_flows:
        cmd.append("--publish-flows")
    if args.commit:
        cmd.append("--commit")
    _run_python(script, cmd)


def main() -> None:
    args = parse_args()
    run_id = args.run_id or generate_run_id()

    if args.no_llm:
        raise SystemExit("--no-llm is not supported in this workflow (Step 1b/1e require LLM).")

    _run_reference_stage(args, run_id)
    _run_usability(args, run_id)
    _run_si_download(args, run_id)
    _run_mineru_for_si(args, run_id)
    _run_usage_tagging(args, run_id)
    _run_main_pipeline(args, run_id)


if __name__ == "__main__":  # pragma: no cover
    main()
