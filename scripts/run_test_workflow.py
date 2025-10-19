"""Utility to exercise the Tiangong LCA workflow against the sample paper."""

from __future__ import annotations

import argparse
import json
import time
import tomllib
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from openai import APIConnectionError, APIStatusError, OpenAI

from tiangong_lca_spec.orchestrator import WorkflowOrchestrator


class OpenAIResponsesLLM:
    """Minimal wrapper around the OpenAI Responses API."""

    def __init__(self, api_key: str, model: str, timeout: int = 1200) -> None:
        self._client = OpenAI(api_key=api_key, timeout=timeout)
        self._model = model

    def invoke(self, input_data: dict[str, Any]) -> str:
        prompt = input_data.get("prompt") or ""
        context = input_data.get("context")
        if isinstance(context, (dict, list)):
            user_content = json.dumps(context, ensure_ascii=False)
        else:
            user_content = str(context) if context is not None else ""
        payload = [
            {"role": "system", "content": [{"type": "input_text", "text": str(prompt)}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_content}]},
        ]

        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = self._client.responses.create(model=self._model, input=payload)
                if getattr(response, "output_text", None):
                    return response.output_text
                parts: list[str] = []
                for item in getattr(response, "output", []) or []:
                    if item.get("type") == "message":
                        for content in item["content"]:
                            if content.get("type") == "output_text":
                                parts.append(content.get("text", ""))
                return "\n".join(parts)
            except (APIConnectionError, APIStatusError) as exc:
                last_error = exc
                if attempt == 2:
                    raise
                time.sleep(5 * (attempt + 1))
        if last_error:
            raise last_error
        raise RuntimeError("OpenAI invocation failed without response")


class _NoOpTidas:
    """Optional stub that bypasses the remote TIDAS validation step."""

    def validate(self, datasets: list[dict[str, Any]]) -> list[dict[str, Any]]:
        print(f"[noop] Skipping TIDAS validation for {len(datasets)} datasets")
        return []

    def close(self) -> None:  # pragma: no cover - nothing to clean up
        pass


def _load_secrets(path: Path) -> tuple[str, str]:
    secrets = tomllib.loads(path.read_text(encoding="utf-8"))
    openai_cfg = secrets.get("OPENAI", {})
    api_key = openai_cfg.get("API_KEY") or openai_cfg.get("api_key")
    model = openai_cfg.get("MODEL") or openai_cfg.get("model") or "gpt-5"
    if not api_key:
        raise SystemExit("OpenAI API key missing in .secrets/secrets.toml")
    return api_key, model


def _load_paper(path: Path) -> str:
    raw = path.read_text(encoding="utf-8")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return raw
    if isinstance(parsed, dict) and "result" in parsed:
        fragments = [
            item.get("text", "")
            for item in parsed["result"]
            if isinstance(item, dict) and item.get("text")
        ]
        return json.dumps(fragments, ensure_ascii=False)
    return raw


def _to_serializable(obj: Any) -> Any:
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, list):
        return [_to_serializable(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _to_serializable(value) for key, value in obj.items()}
    return obj


def run_workflow(paper_path: Path, output_path: Path, skip_tidas: bool) -> None:
    api_key, model = _load_secrets(Path(".secrets/secrets.toml"))
    paper_md_json = _load_paper(paper_path)
    llm = OpenAIResponsesLLM(api_key=api_key, model=model)

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
                "matched_flows": [
                    _to_serializable(flow) for flow in entry.get("matched_flows", [])
                ],
                "unmatched_flows": [
                    _to_serializable(flow) for flow in entry.get("unmatched_flows", [])
                ],
                "origin_exchanges": _to_serializable(entry.get("origin_exchanges", {})),
            }
        )

    payload = {
        "process_datasets": [dataset.as_dict() for dataset in result.process_datasets],
        "alignment": alignment_serializable,
        "validation_report": [_to_serializable(item) for item in result.validation_report],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"Workflow completed. Datasets={len(result.process_datasets)} "
        f"alignment_entries={len(result.alignment)} findings={len(result.validation_report)}"
    )


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
        help="Bypass the TIDAS validation MCP call (useful if the remote tool errors).",
    )
    return parser.parse_args()


def main() -> None:  # pragma: no cover - manual utility
    args = parse_args()
    run_workflow(args.paper, args.output, skip_tidas=args.skip_tidas)


if __name__ == "__main__":
    main()
