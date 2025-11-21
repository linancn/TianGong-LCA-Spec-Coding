"""Utility to exercise the Tiangong LCA workflow against the sample paper."""

from __future__ import annotations

import argparse
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from _workflow_common import OpenAIResponsesLLM, dump_json, load_paper, load_secrets

from tiangong_lca_spec.orchestrator import WorkflowOrchestrator


class _NoOpTidas:
    """Optional stub that bypasses the remote TIDAS validation step."""

    def validate(self, datasets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        print(f"[noop] Skipping TIDAS validation for {len(datasets)} datasets")
        return []

    def close(self) -> None:  # pragma: no cover - nothing to clean up
        pass


def _to_serializable(obj: Any) -> Any:
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, list):
        return [_to_serializable(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _to_serializable(value) for key, value in obj.items()}
    return obj


def run_workflow(paper_path: Path, output_path: Path, skip_tidas: bool) -> None:
    api_key, model, base_url = load_secrets(Path(".secrets/secrets.toml"))
    paper_md_json = load_paper(paper_path)
    llm = OpenAIResponsesLLM(api_key=api_key, model=model, base_url=base_url)

    orchestrator = WorkflowOrchestrator(llm)
    if skip_tidas:
        setattr(orchestrator, "_tidas", _NoOpTidas())  # type: ignore[attr-defined]
    try:
        result = orchestrator.run(paper_md_json)
    finally:
        orchestrator.close()

    alignment_serializable = []
    for entry in result.alignment:
        alignment_serializable.append(
            {
                "process_name": entry.get("process_name"),
                "matched_flows": [_to_serializable(flow) for flow in entry.get("matched_flows", [])],
                "unmatched_flows": [_to_serializable(flow) for flow in entry.get("unmatched_flows", [])],
                "origin_exchanges": _to_serializable(entry.get("origin_exchanges", {})),
            }
        )

    payload = {
        "process_datasets": [dataset.as_dict() for dataset in result.process_datasets],
        "alignment": alignment_serializable,
        "validation_report": [_to_serializable(item) for item in result.validation_report],
    }
    dump_json(payload, output_path)
    print(f"Workflow completed. Datasets={len(result.process_datasets)} " f"alignment_entries={len(result.alignment)} findings={len(result.validation_report)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--paper",
        type=Path,
        default=Path("test/data/test-paper.json"),
        help="Path to the paper markdown JSON payload.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/test_workflow_output.json"),
        help="Target path for the workflow result JSON.",
    )
    parser.add_argument(
        "--skip-tidas",
        action="store_true",
        help="Bypass the local TIDAS validation CLI (useful if validation should be skipped).",
    )
    return parser.parse_args()


def main() -> None:  # pragma: no cover - manual utility
    args = parse_args()
    run_workflow(args.paper, args.output, skip_tidas=args.skip_tidas)


if __name__ == "__main__":
    main()
