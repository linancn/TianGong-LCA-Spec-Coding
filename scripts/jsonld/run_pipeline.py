#!/usr/bin/env python
"""One-click orchestrator for the JSON-LD Stage 1 → Stage 3 pipeline."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

JSONLD_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = JSONLD_DIR.parent
REPO_ROOT = SCRIPTS_DIR.parent
for path in (SCRIPTS_DIR, REPO_ROOT):
    if str(path) not in sys.path:
        sys.path.append(str(path))

try:
    from scripts.md._workflow_common import generate_run_id, save_latest_run_id  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    from _workflow_common import generate_run_id, save_latest_run_id  # type: ignore


def _as_path(value: str | None) -> Path | None:
    return Path(value) if value else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--process-dir",
        type=Path,
        required=True,
        help="Directory (or file) containing OpenLCA JSON-LD process datasets.",
    )
    parser.add_argument("--flows-dir", type=Path, help="Directory containing JSON-LD flow datasets.")
    parser.add_argument("--sources-dir", type=Path, help="Directory containing JSON-LD source datasets.")
    parser.add_argument("--run-id", help="Optional run identifier shared across all stages.")
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
    parser.add_argument("--secrets", type=Path, default=Path(".secrets/secrets.toml"), help="Secrets file containing OpenAI credentials.")
    parser.add_argument("--llm-cache", type=Path, help="Override for Stage 1 JSON-LD LLM cache directory.")
    parser.add_argument("--disable-cache", action="store_true", help="Disable LLM response caching during Stage 1.")
    parser.add_argument(
        "--stage2-extra-args",
        nargs=argparse.REMAINDER,
        help="Additional arguments appended to the Stage 2 command (after orchestrator-provided args).",
    )
    parser.add_argument(
        "--new-run",
        action="store_true",
        help=(
            "Generate a fresh run id (default behavior). This flag is retained for backward compatibility; "
            "omit it and the orchestrator will still start a new run unless --run-id is provided."
        ),
    )
    return parser.parse_args()


def _run(cmd: list[str]) -> None:
    print(f"[jsonld-run] Executing: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def main() -> None:
    args = parse_args()
    if args.run_id:
        if args.new_run:
            print("[jsonld-run] --new-run ignored because --run-id was provided; using supplied value.")
        run_id = args.run_id
        print(f"[jsonld-run] Using provided run id: {run_id}")
    else:
        run_id = generate_run_id()
        if args.new_run:
            print(f"[jsonld-run] Generated fresh run id via --new-run: {run_id}")
        else:
            print(f"[jsonld-run] Generated fresh run id (default behavior): {run_id}")
    save_latest_run_id(run_id, pipeline="jsonld")
    stage1_script = JSONLD_DIR / "stage1_jsonld_extract.py"
    stage2_script = JSONLD_DIR / "stage2_jsonld_validate.py"
    stage3_script = JSONLD_DIR / "stage3_jsonld_publish.py"

    if not stage1_script.exists() or not stage2_script.exists():
        raise SystemExit("JSON-LD stage scripts not found under scripts/jsonld/. Ensure the repo is up to date.")

    stage1_cmd = [
        sys.executable,
        str(stage1_script),
        "--process-dir",
        str(args.process_dir),
        "--run-id",
        run_id,
    ]
    if args.flows_dir:
        stage1_cmd.extend(["--flow-dir", str(args.flows_dir)])
    if args.sources_dir:
        stage1_cmd.extend(["--source-dir", str(args.sources_dir)])
    stage1_cmd.extend(["--secrets", str(args.secrets)])
    if args.llm_cache:
        stage1_cmd.extend(["--llm-cache", str(args.llm_cache)])
    if args.disable_cache:
        stage1_cmd.append("--disable-cache")
    print(f"[jsonld-run] Stage 1 (extract) -> {stage1_script}")
    _run(stage1_cmd)

    stage2_cmd = [
        sys.executable,
        str(stage2_script),
        "--run-id",
        run_id,
        "--skip-auto-publish",
    ]
    if args.clean_exports:
        stage2_cmd.append("--clean-exports")
    if args.stage2_extra_args:
        stage2_cmd.extend(args.stage2_extra_args)
    print(f"[jsonld-run] Stage 2 (validate) -> {stage2_script}")
    _run(stage2_cmd)

    if args.skip_stage3:
        print("[jsonld-run] Stage 3 skipped via --skip-stage3.")
        return

    if not stage3_script.exists():
        raise SystemExit("Stage 3 JSON-LD script missing; cannot publish.")

    stage3_cmd = [
        sys.executable,
        str(stage3_script),
        "--run-id",
        run_id,
    ]
    if not args.dry_run_publish:
        stage3_cmd.append("--commit")
    print(f"[jsonld-run] Stage 3 (publish) -> {stage3_script}")
    _run(stage3_cmd)


if __name__ == "__main__":
    main()
