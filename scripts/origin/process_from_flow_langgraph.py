#!/usr/bin/env python
"""Build ILCD process dataset(s) from a reference flow using LangGraph.

This command is a human-in-the-loop helper:
- Step 1: list plausible technology/process routes from the reference flow.
- Step 2: for each route, split into 1..N unit processes (ordered; last process produces/treats the reference flow;
  structured fields include inputs/outputs, exchange keywords, and standardized name_parts with quantitative_reference).
- Step 3: derive per-process input/output exchanges.
- Step 4: match exchanges to Tiangong flows via MCP flow_search.
- Step 5: generate TIDAS/ILCD process datasets via tidas-sdk.

Typical usage (demo flow):
  uv run python scripts/origin/process_from_flow_langgraph.py \\
    --flow artifacts/cache/manual_flows/01132_bdbb913b-620c-42a0-baf6-c5802a2b6c4b_01.01.000.json

Outputs (by default):
  - artifacts/process_from_flow/<run_id>/exports/processes

Manual cleanup (keep latest 3 runs):
  uv run python scripts/origin/process_from_flow_langgraph.py --cleanup-only --retain-runs 3

Publish latest run (commit to DB):
  uv run python scripts/origin/process_from_flow_langgraph.py --publish-only --commit

Checkpoint flow (edit cache JSON, then resume):
  uv run python scripts/origin/process_from_flow_langgraph.py --stop-after exchanges
  # edit artifacts/process_from_flow/<run_id>/cache/process_from_flow_state.json
  uv run python scripts/origin/process_from_flow_langgraph.py --resume
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPT_DIR.parent
REPO_ROOT = SCRIPTS_DIR.parent
for path in (SCRIPTS_DIR, REPO_ROOT):
    if str(path) not in sys.path:
        sys.path.append(str(path))

try:
    from scripts.md._workflow_common import (  # type: ignore
        OpenAIResponsesLLM,
        dump_json,
        generate_run_id,
        load_secrets,
    )
except ModuleNotFoundError:  # pragma: no cover
    from _workflow_common import (  # type: ignore
        OpenAIResponsesLLM,
        dump_json,
        generate_run_id,
        load_secrets,
    )

DEFAULT_FLOW_PATH = Path("artifacts/cache/manual_flows/01132_bdbb913b-620c-42a0-baf6-c5802a2b6c4b_01.01.000.json")
PROCESS_FROM_FLOW_ARTIFACTS_ROOT = Path("artifacts/process_from_flow")
LATEST_RUN_ID_PATH = PROCESS_FROM_FLOW_ARTIFACTS_ROOT / ".latest_run_id"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flow", type=Path, default=DEFAULT_FLOW_PATH, help="Path to the reference flow JSON (ILCD flowDataSet wrapper).")
    parser.add_argument("--operation", choices=("produce", "treat"), default="produce", help="Whether the process produces or treats/disposes the reference flow.")
    parser.add_argument(
        "--run-id",
        help="Run identifier under artifacts/process_from_flow/<run_id>. Defaults to a new id when not resuming.",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from artifacts/process_from_flow/<run_id>/cache/process_from_flow_state.json.")
    parser.add_argument(
        "--stop-after",
        choices=("references", "tech", "processes", "exchanges", "matches", "datasets"),
        help="Stop after a stage, writing state to cache for manual editing.",
    )
    parser.add_argument("--secrets", type=Path, default=Path(".secrets/secrets.toml"), help="Secrets file containing OpenAI credentials.")
    parser.add_argument("--no-llm", action="store_true", help="Run without an LLM (uses minimal deterministic fallbacks).")
    parser.add_argument("--no-translate-zh", action="store_true", help="Skip adding Chinese translations to multi-language fields.")
    parser.add_argument(
        "--retain-runs",
        type=int,
        help="Manually clean process_from_flow run directories, keeping only the most recent N runs under artifacts/process_from_flow/.",
    )
    parser.add_argument(
        "--cleanup-only",
        action="store_true",
        help="Only perform cleanup (requires --retain-runs), skip running the pipeline.",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Publish generated process datasets via Database_CRUD_Tool after the pipeline completes.",
    )
    parser.add_argument(
        "--publish-only",
        action="store_true",
        help="Publish process datasets from an existing run and skip the pipeline.",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="Actually invoke Database_CRUD_Tool (default: dry-run).",
    )
    return parser.parse_args()


def _load_state(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"State file must contain an object: {path}")
    return payload


def _extract_process_uuid(process_payload: dict[str, Any]) -> str:
    dataset = process_payload.get("processDataSet") if isinstance(process_payload.get("processDataSet"), dict) else {}
    info = dataset.get("processInformation") if isinstance(dataset.get("processInformation"), dict) else {}
    data_info = info.get("dataSetInformation") if isinstance(info.get("dataSetInformation"), dict) else {}
    uuid_value = data_info.get("common:UUID")
    if isinstance(uuid_value, str) and uuid_value.strip():
        return uuid_value.strip()
    raise SystemExit("Generated process payload missing processInformation.dataSetInformation.common:UUID")


def _extract_process_version(process_payload: dict[str, Any]) -> str:
    dataset = process_payload.get("processDataSet") if isinstance(process_payload.get("processDataSet"), dict) else {}
    admin = dataset.get("administrativeInformation") if isinstance(dataset.get("administrativeInformation"), dict) else {}
    pub = admin.get("publicationAndOwnership") if isinstance(admin.get("publicationAndOwnership"), dict) else {}
    version = pub.get("common:dataSetVersion")
    if isinstance(version, str) and version.strip():
        return version.strip()
    return "01.01.000"


def _ensure_run_root(run_id: str) -> Path:
    run_root = PROCESS_FROM_FLOW_ARTIFACTS_ROOT / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    return run_root


def ensure_run_cache_dir(run_id: str) -> Path:
    run_root = _ensure_run_root(run_id)
    cache_dir = run_root / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def ensure_run_exports_dir(run_id: str, *, clean: bool = False) -> Path:
    run_root = _ensure_run_root(run_id)
    export_root = run_root / "exports"
    if clean and export_root.exists():
        shutil.rmtree(export_root)
    for name in ("processes", "flows", "sources"):
        (export_root / name).mkdir(parents=True, exist_ok=True)
    return export_root


def _ensure_run_input_dir(run_id: str) -> Path:
    run_root = _ensure_run_root(run_id)
    input_dir = run_root / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    return input_dir


def _find_runs(base_dir: Path, marker: Path) -> list[Path]:
    runs: list[Path] = []
    if not base_dir.exists():
        return runs
    for entry in base_dir.iterdir():
        if not entry.is_dir():
            continue
        if (entry / marker).exists():
            runs.append(entry)
    return runs


def _parse_run_id(run_id: str) -> datetime | None:
    try:
        return datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _cleanup_runs(*, retain: int, current_run_id: str | None = None) -> None:
    if retain <= 0:
        raise SystemExit("--retain-runs must be >= 1")

    artifacts_root = PROCESS_FROM_FLOW_ARTIFACTS_ROOT
    artifacts_marker = Path("cache/process_from_flow_state.json")

    artifacts_runs = _find_runs(artifacts_root, artifacts_marker)
    artifacts_index = {path.name: path for path in artifacts_runs}
    all_run_ids = set(artifacts_index)
    if not all_run_ids:
        print("No process_from_flow runs found for cleanup.", file=sys.stderr)
        return

    def _sort_key(run_id: str) -> tuple[int, datetime]:
        parsed = _parse_run_id(run_id)
        if parsed is not None:
            return (0, parsed)
        entry = artifacts_index.get(run_id)
        if entry is None:
            return (2, datetime.min.replace(tzinfo=timezone.utc))
        try:
            ts = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)
        except OSError:
            ts = datetime.min.replace(tzinfo=timezone.utc)
        return (1, ts)

    sorted_run_ids = sorted(all_run_ids, key=_sort_key, reverse=True)
    keep = set(sorted_run_ids[:retain])
    if current_run_id:
        keep.add(current_run_id)

    def _safe_remove(base_dir: Path, run_id: str) -> bool:
        target = base_dir / run_id
        try:
            if target.exists() and target.is_dir() and target.resolve().parent == base_dir.resolve():
                shutil.rmtree(target)
                return True
        except OSError as exc:
            print(f"Failed to remove {target}: {exc}", file=sys.stderr)
        return False

    removed = 0
    for run_id in sorted_run_ids:
        if run_id in keep:
            continue
        if run_id in artifacts_index and _safe_remove(artifacts_root, run_id):
            removed += 1

    print(
        f"Cleanup complete: kept {len(keep)} run(s), removed {removed} directory(s).",
        file=sys.stderr,
    )


def _resolve_run_id(run_id: str | None) -> str:
    if run_id:
        return run_id
    if LATEST_RUN_ID_PATH.exists():
        latest = LATEST_RUN_ID_PATH.read_text(encoding="utf-8").strip()
        if latest:
            return latest
    raise SystemExit("Missing --run-id and no latest run marker found in artifacts/process_from_flow.")


def _load_process_datasets(run_id: str) -> list[dict[str, Any]]:
    process_dir = PROCESS_FROM_FLOW_ARTIFACTS_ROOT / run_id / "exports" / "processes"
    if not process_dir.exists():
        raise SystemExit(f"Process output directory not found: {process_dir}")
    datasets: list[dict[str, Any]] = []
    for path in sorted(process_dir.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            datasets.append(payload)
    if not datasets:
        raise SystemExit(f"No process datasets found under {process_dir}")
    return datasets


def _publish_processes(datasets: list[dict[str, Any]], *, commit: bool) -> None:
    from tiangong_lca_spec.publishing import ProcessPublisher

    publishable = [item for item in datasets if isinstance(item, dict)]
    if not publishable:
        raise SystemExit("No valid process datasets found for publishing.")
    publisher = ProcessPublisher(dry_run=not commit)
    try:
        results = publisher.publish(publishable)
        if commit:
            print(f"Published {len(results)} process dataset(s) via Database_CRUD_Tool.", file=sys.stderr)
        else:
            print(f"Dry-run: prepared {len(publishable)} process dataset(s) for publish.", file=sys.stderr)
    finally:
        publisher.close()


def main() -> None:
    from tiangong_lca_spec.process_from_flow import ProcessFromFlowService
    from tiangong_lca_spec.utils.translate import Translator

    args = parse_args()
    if args.cleanup_only:
        if args.retain_runs is None:
            raise SystemExit("--cleanup-only requires --retain-runs")
        _cleanup_runs(retain=args.retain_runs)
        return
    if args.publish_only:
        run_id = _resolve_run_id(args.run_id)
        datasets = _load_process_datasets(run_id)
        _publish_processes(datasets, commit=args.commit)
        return

    if args.resume:
        if args.run_id:
            run_id = args.run_id
        elif LATEST_RUN_ID_PATH.exists():
            run_id = LATEST_RUN_ID_PATH.read_text(encoding="utf-8").strip()
        else:
            raise SystemExit("Missing --run-id and no cached run marker found for --resume.")
    else:
        run_id = args.run_id or generate_run_id()

    cache_dir = ensure_run_cache_dir(run_id)
    exports_dir = ensure_run_exports_dir(run_id)
    state_path = cache_dir / "process_from_flow_state.json"
    input_dir = _ensure_run_input_dir(run_id)
    try:
        shutil.copy2(args.flow, input_dir / args.flow.name)
    except FileNotFoundError:
        raise SystemExit(f"Reference flow file not found: {args.flow}")
    dump_json(
        {
            "run_id": run_id,
            "flow_path": str(args.flow),
            "operation": args.operation,
        },
        input_dir / "input_manifest.json",
    )

    initial_state: dict[str, Any] | None = None
    if args.resume:
        if not state_path.exists():
            raise SystemExit(f"Missing cached state file for --resume: {state_path}")
        initial_state = _load_state(state_path)

    llm = None
    if not args.no_llm:
        if not args.secrets.exists():
            raise SystemExit(f"Secrets file not found: {args.secrets} (or pass --no-llm)")
        api_key, model, base_url = load_secrets(args.secrets)
        llm = OpenAIResponsesLLM(api_key=api_key, model=model, base_url=base_url)

    translator = None
    if llm is not None and not args.no_translate_zh:
        translator = Translator(llm=llm)

    service = ProcessFromFlowService(llm=llm, translator=translator)
    stop_after = None if args.stop_after == "datasets" else args.stop_after
    result_state = service.run(
        flow_path=args.flow,
        operation=args.operation,
        initial_state=initial_state,
        stop_after=stop_after,
    )

    dump_json(result_state, state_path)

    if args.stop_after and args.stop_after != "datasets":
        print(f"Stopped after stage '{args.stop_after}'. Edit state and resume with: --resume --run-id {run_id}", file=sys.stderr)
        LATEST_RUN_ID_PATH.write_text(run_id, encoding="utf-8")
        return

    datasets = result_state.get("process_datasets") or []
    if not isinstance(datasets, list) or not datasets:
        print("No process datasets generated.", file=sys.stderr)
        LATEST_RUN_ID_PATH.write_text(run_id, encoding="utf-8")
        return

    written: list[Path] = []
    for payload in datasets:
        if not isinstance(payload, dict):
            continue
        uuid_value = _extract_process_uuid(payload)
        version = _extract_process_version(payload)
        filename = f"{uuid_value}_{version}.json"
        target = exports_dir / "processes" / filename
        dump_json(payload, target)
        written.append(target)

    LATEST_RUN_ID_PATH.write_text(run_id, encoding="utf-8")
    print(f"Wrote {len(written)} process dataset(s) to {exports_dir / 'processes'}", file=sys.stderr)
    if args.publish:
        _publish_processes(datasets, commit=args.commit)
    if args.retain_runs:
        _cleanup_runs(retain=args.retain_runs, current_run_id=run_id)


if __name__ == "__main__":  # pragma: no cover
    main()
