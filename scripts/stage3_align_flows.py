#!/usr/bin/env python
"""Stage 3: align extracted exchanges against TianGong flow datasets."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

from _workflow_common import OpenAIResponsesLLM, dump_json, load_secrets

from tiangong_lca_spec.flow_alignment import FlowAlignmentService


def _read_process_blocks(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "process_blocks" not in payload:
        raise SystemExit(f"Process blocks JSON must contain 'process_blocks': {path}")
    blocks = payload["process_blocks"]
    if not isinstance(blocks, list):
        raise SystemExit(f"'process_blocks' must be a list in {path}")
    for index, block in enumerate(blocks):
        if not isinstance(block, dict):
            raise SystemExit(f"Process block #{index} must be an object: {path}")
        if "processDataSet" not in block:
            raise SystemExit(
                "Each process block must contain 'processDataSet'. Stage 2 now writes "
                "normalised exchanges directly inside the dataset; legacy 'exchange_list' "
                "is no longer emitted."
            )
        if "exchange_list" in block and block["exchange_list"]:
            print(
                "stage3_align_flows: ignoring legacy 'exchange_list' data; use "
                "'processDataSet.exchanges' instead.",
                file=sys.stderr,
            )
    return blocks


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
    raise SystemExit(
        (
            f"Unexpected clean text format in {path}; expected plain markdown or JSON "
            "with 'clean_text'."
        )
    )


def _maybe_create_llm(path: Path | None) -> OpenAIResponsesLLM | None:
    if path is None or not path.exists():
        return None
    api_key, model = load_secrets(path)
    return OpenAIResponsesLLM(api_key=api_key, model=model)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--process-blocks",
        type=Path,
        default=Path("artifacts/stage2_process_blocks.json"),
        help="Process blocks JSON emitted by stage2_extract_processes.",
    )
    parser.add_argument(
        "--clean-text",
        type=Path,
        default=Path("artifacts/stage1_clean_text.md"),
        help="Clean markdown emitted by stage1_preprocess.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/stage3_alignment.json"),
        help="Where to store the alignment results.",
    )
    parser.add_argument(
        "--unmatched-output",
        type=Path,
        default=Path("artifacts/stage3_unmatched_flows.json"),
        help="Deprecated. Unmatched exchanges are no longer written to disk.",
    )
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".secrets/secrets.toml"),
        help="Secrets file containing OpenAI credentials for LLM-based alignment.",
    )
    parser.add_argument(
        "--allow-missing-hints",
        action="store_true",
        help=(
            "Proceed even when exchanges are missing 'FlowSearch hints' metadata. "
            "By default the command aborts so that Stage 2 outputs can be fixed first."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    process_blocks = _read_process_blocks(args.process_blocks)
    clean_text = _read_clean_text(args.clean_text)

    llm = _maybe_create_llm(args.secrets)
    service = FlowAlignmentService(llm=llm)
    alignment_entries: list[dict[str, Any]] = []
    process_summaries: list[tuple[str, int]] = []
    try:
        for block in process_blocks:
            dataset = block.get("processDataSet")
            if not isinstance(dataset, dict):
                raise SystemExit("Each process block must contain 'processDataSet'")

            process_id = _resolve_process_id(block, dataset)
            process_name = _resolve_process_name(dataset, block.get("process_name"))
            exchanges = list(_ensure_exchange_list(dataset))
            _validate_flow_hints(
                exchanges,
                process_id,
                process_name,
                allow_missing=args.allow_missing_hints,
            )

            result = service.align_exchanges(dataset, clean_text)
            alignment_entries.append(_serialise_alignment(result, process_id))
            result_name = result.get("process_name") or process_name
            process_label = _format_process_label(result_name, process_id)
            process_summaries.append((process_label, len(exchanges)))
    finally:
        service.close()

    dump_json({"alignment": alignment_entries}, args.output)
    print(f"Aligned flows for {len(alignment_entries)} processes -> {args.output}")
    for label, total in process_summaries:
        print(f" - {label}: processed {total} exchanges")


def _resolve_process_id(block: dict[str, Any], dataset: dict[str, Any]) -> str | None:
    candidate = block.get("process_id")
    if isinstance(candidate, str) and candidate.strip():
        return candidate.strip()
    process_info = dataset.get("processInformation", {})
    data_info = process_info.get("dataSetInformation", {})
    uuid = data_info.get("common:UUID") or data_info.get("uuid")
    if isinstance(uuid, str) and uuid.strip():
        return uuid.strip()
    return None


def _ensure_exchange_list(dataset: dict[str, Any]) -> Iterable[dict[str, Any]]:
    exchanges_block = dataset.get("exchanges") or {}
    exchanges = exchanges_block.get("exchange")
    if exchanges is None:
        return []
    if isinstance(exchanges, list):
        return exchanges
    if isinstance(exchanges, dict):
        return [exchanges]
    raise SystemExit("Unexpected exchanges structure; expected list or dict.")


def _validate_flow_hints(
    exchanges: Iterable[dict[str, Any]],
    process_id: str | None,
    process_name: str | None,
    *,
    allow_missing: bool,
) -> None:
    missing_hints: list[str] = []
    process_label = _format_process_label(process_name, process_id)
    for index, exchange in enumerate(exchanges, start=1):
        name = _ensure_exchange_name(exchange, index, process_label)
        comment_text = _extract_comment_text(exchange)
        if not comment_text or not comment_text.lstrip().startswith("FlowSearch hints:"):
            descriptor = f"{name} (#{index})" if name else _describe_exchange(exchange, index)
            missing_hints.append(descriptor)
    if not missing_hints:
        return
    message = (
        f"{process_label} is missing FlowSearch hints for "
        f"{len(missing_hints)} exchange(s): {', '.join(missing_hints)}"
    )
    if allow_missing:
        print(f"Warning: {message}", file=sys.stderr)
        return
    raise SystemExit(message)


def _extract_comment_text(exchange: dict[str, Any]) -> str:
    comment = exchange.get("generalComment") or exchange.get("comment")
    if comment is None:
        return ""
    if isinstance(comment, str):
        return comment.strip()
    if isinstance(comment, dict):
        for key in ("#text", "text", "@value"):
            value = comment.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for value in comment.values():
            if isinstance(value, str) and value.strip():
                return value.strip()
    if isinstance(comment, list):
        parts = [part for part in (_coerce_str(item) for item in comment) if part]
        return "; ".join(parts)
    return _coerce_str(comment)


def _extract_exchange_label(exchange: dict[str, Any]) -> str:
    for key in ("exchangeName", "name", "flowName"):
        label = _coerce_str(exchange.get(key))
        if label:
            return label
    reference = exchange.get("referenceToFlowDataSet")
    if isinstance(reference, dict):
        short_desc = reference.get("common:shortDescription")
        text = _coerce_str(short_desc)
        if text:
            parts = [part.strip() for part in text.split(";") if part.strip()]
            if parts:
                return parts[0]
            return text
    return "unknown_exchange"


def _describe_exchange(exchange: dict[str, Any], index: int) -> str:
    label = _extract_exchange_label(exchange)
    if label and label != "unknown_exchange":
        return label
    amount = _coerce_str(
        exchange.get("meanAmount") or exchange.get("resultingAmount") or exchange.get("amount")
    )
    unit = _coerce_str(exchange.get("unit") or exchange.get("resultingAmountUnit"))
    if amount and unit:
        return f"{amount} {unit} (#{index})"
    return f"exchange #{index}"


def _ensure_exchange_name(exchange: dict[str, Any], index: int, process_label: str) -> str:
    name = _extract_exchange_label(exchange)
    if name != "unknown_exchange":
        return name
    comment_text = _extract_comment_text(exchange)
    inferred = _infer_name_from_comment(comment_text)
    if inferred:
        exchange["exchangeName"] = inferred
        return inferred
    raise SystemExit(
        f"{process_label} exchange #{index} is missing `exchangeName` and could not be "
        "inferred from FlowSearch hints. Please revise Stage 2 outputs."
    )


def _infer_name_from_comment(comment: str) -> str:
    if not comment:
        return ""
    text = comment.strip()
    prefix = "FlowSearch hints:"
    if text.startswith(prefix):
        text = text[len(prefix) :].strip()
    fields = [segment.strip() for segment in text.split("|") if segment.strip()]

    def _extract_values(key: str) -> list[str]:
        for field in fields:
            label, _, value = field.partition("=")
            if label.strip() == key and value:
                return [candidate.strip() for candidate in value.split(";") if candidate.strip()]
        return []

    for key in ("en_synonyms", "zh_synonyms"):
        candidates = _extract_values(key)
        if candidates:
            return candidates[0]
    for field in fields:
        _, _, value = field.partition("=")
        if value.strip():
            return value.strip()
    return ""


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("#text", "text", "@value"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        for candidate in value.values():
            coerced = _coerce_str(candidate)
            if coerced:
                return coerced
        return ""
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
        parts = [part.strip() for part in (_coerce_str(item) for item in value) if part]
        return "; ".join(parts)
    return str(value).strip()


def _format_process_label(process_name: str | None, process_id: str | None) -> str:
    if process_id and process_name:
        return f"{process_name} [{process_id}]"
    if process_name:
        return process_name
    if process_id:
        return process_id
    return "unknown_process"


def _serialise_alignment(entry: dict[str, Any], process_id: str | None = None) -> dict[str, Any]:
    origin = entry.get("origin_exchanges") or {}
    payload = {
        "process_name": entry.get("process_name"),
        "origin_exchanges": origin,
    }
    if process_id:
        payload["process_id"] = process_id
    return payload


def _resolve_process_name(dataset: dict[str, Any], fallback: str | None = None) -> str | None:
    process_info = dataset.get("processInformation", {})
    data_info = process_info.get("dataSetInformation", {})
    name_block = data_info.get("name")
    name = _coerce_str(name_block)
    if name:
        return name
    return fallback or dataset.get("process_name")


if __name__ == "__main__":
    main()
