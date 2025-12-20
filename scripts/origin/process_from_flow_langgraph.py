#!/usr/bin/env python
"""Build ILCD process dataset(s) from a reference flow using LangGraph.

This command is a human-in-the-loop helper:
- Step 1: derive a technical description from the reference flow.
- Step 2: split into 1..N processes.
- Step 3: derive per-process input/output exchanges.
- Step 4: match exchanges to Tiangong flows via MCP flow_search.
- Step 5: generate TIDAS/ILCD process datasets via tidas-sdk.

Typical usage (demo flow):
  uv run python scripts/origin/process_from_flow_langgraph.py \\
    --flow artifacts/cache/manual_flows/01132_bdbb913b-620c-42a0-baf6-c5802a2b6c4b_01.01.000.json

Checkpoint flow (edit cache JSON, then resume):
  uv run python scripts/origin/process_from_flow_langgraph.py --stop-after exchanges
  # edit artifacts/<run_id>/cache/process_from_flow_state.json
  uv run python scripts/origin/process_from_flow_langgraph.py --resume
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    from scripts.md._workflow_common import (  # type: ignore
        OpenAIResponsesLLM,
        dump_json,
        ensure_run_cache_dir,
        ensure_run_exports_dir,
        generate_run_id,
        load_secrets,
    )
except ModuleNotFoundError:  # pragma: no cover
    from _workflow_common import (  # type: ignore
        OpenAIResponsesLLM,
        dump_json,
        ensure_run_cache_dir,
        ensure_run_exports_dir,
        generate_run_id,
        load_secrets,
    )

from tiangong_lca_spec.process_from_flow import ProcessFromFlowService

DEFAULT_FLOW_PATH = Path("artifacts/cache/manual_flows/01132_bdbb913b-620c-42a0-baf6-c5802a2b6c4b_01.01.000.json")
LATEST_RUN_ID_PATH = Path("artifacts/.latest_process_from_flow_run_id")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--flow", type=Path, default=DEFAULT_FLOW_PATH, help="Path to the reference flow JSON (ILCD flowDataSet wrapper).")
    parser.add_argument("--operation", choices=("produce", "treat"), default="produce", help="Whether the process produces or treats/disposes the reference flow.")
    parser.add_argument(
        "--run-id",
        help="Run identifier for artifacts/<run_id> output. Defaults to a new id when not resuming.",
    )
    parser.add_argument("--resume", action="store_true", help="Resume from artifacts/<run_id>/cache/process_from_flow_state.json.")
    parser.add_argument(
        "--stop-after",
        choices=("tech", "processes", "exchanges", "matches", "datasets"),
        help="Stop after a stage, writing state to cache for manual editing.",
    )
    parser.add_argument("--secrets", type=Path, default=Path(".secrets/secrets.toml"), help="Secrets file containing OpenAI credentials.")
    parser.add_argument("--no-llm", action="store_true", help="Run without an LLM (uses minimal deterministic fallbacks).")
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


def main() -> None:
    args = parse_args()

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

    service = ProcessFromFlowService(llm=llm)
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


if __name__ == "__main__":  # pragma: no cover
    main()
