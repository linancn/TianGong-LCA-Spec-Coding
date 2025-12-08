"""Query the configured knowledge base dataset for relevant chunks."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

import httpx

from tiangong_lca_spec.kb import KnowledgeBaseClient, load_kb_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Retrieve the most relevant KB chunks by issuing a dataset query.")
    parser.add_argument(
        "--secrets",
        type=Path,
        default=Path(".secrets/secrets.toml"),
        help="Path to the secrets file containing the [kb] configuration.",
    )
    parser.add_argument("--query", help="Inline query text. Required unless --query-file or the payload already provides it.")
    parser.add_argument(
        "--query-file",
        type=Path,
        help="Text file containing the query snippet. Use '-' to read from stdin.",
    )
    parser.add_argument("--top-k", type=int, help="Maximum number of records returned by the API.")
    parser.add_argument("--score-threshold", type=float, help="Minimum score accepted by the server.")
    score_group = parser.add_mutually_exclusive_group()
    score_group.add_argument(
        "--score-threshold-enabled",
        action="store_const",
        const=True,
        dest="score_threshold_enabled",
        help="Enable server-side score filtering (default: enabled).",
    )
    score_group.add_argument(
        "--score-threshold-disabled",
        action="store_const",
        const=False,
        dest="score_threshold_enabled",
        help="Disable server-side score filtering.",
    )
    parser.add_argument(
        "--retrieval-model",
        help="JSON object describing retrieval_model overrides (merge friendly with convenience flags).",
    )
    parser.add_argument("--retrieval-model-file", type=Path, help="Path to a JSON file describing retrieval_model overrides.")
    parser.add_argument(
        "--search-method",
        choices=["semantic_search", "keyword_search", "hybrid_search"],
        help="Set retrieval_model.search_method without writing JSON.",
    )
    rerank_group = parser.add_mutually_exclusive_group()
    rerank_group.add_argument(
        "--reranking-enable",
        action="store_const",
        const=True,
        dest="reranking_enable",
        help="Enable reranking in retrieval_model (default: disabled).",
    )
    rerank_group.add_argument(
        "--reranking-disable",
        action="store_const",
        const=False,
        dest="reranking_enable",
        help="Disable reranking in retrieval_model.",
    )
    parser.add_argument(
        "--reranking-provider",
        help="Convenience flag to set retrieval_model.reranking_mode.reranking_provider_name.",
    )
    parser.add_argument(
        "--reranking-model",
        help="Convenience flag to set retrieval_model.reranking_mode.reranking_model_name.",
    )
    parser.add_argument(
        "--metadata-filters",
        help="JSON array applied to metadata_filtering_conditions.",
    )
    parser.add_argument(
        "--metadata-filters-file",
        type=Path,
        help="Path to a JSON file for metadata_filtering_conditions.",
    )
    parser.add_argument(
        "--filter",
        dest="filter_expressions",
        action="append",
        help="Metadata filter expression in the form field[:operator]=value (default operator: eq).",
    )
    parser.add_argument(
        "--filter-operator",
        choices=["and", "or"],
        default="and",
        help="Logical operator applied when combining multiple --filter expressions (default: and).",
    )
    parser.add_argument("--weights", help="JSON value for the `weights` field.")
    parser.add_argument("--weights-file", type=Path, help="Path to a JSON file for the `weights` field.")
    parser.add_argument("--payload", help="JSON object merged into the request body before applying other overrides.")
    parser.add_argument("--payload-file", type=Path, help="Path to a JSON file merged into the request body.")
    parser.add_argument(
        "--format",
        choices=["pretty", "json"],
        default="pretty",
        help="Display mode for the response payload (default: pretty).",
    )
    parser.add_argument("--display-limit", type=int, help="Only print the first N records when using pretty output.")
    parser.add_argument(
        "--max-snippet-chars",
        type=int,
        default=240,
        help="Maximum characters displayed per chunk in pretty mode (0 means no truncation).",
    )
    # The KB API expects explicit flags for reranking and score threshold filtering.
    parser.set_defaults(score_threshold_enabled=True, reranking_enable=False)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload(args)
    config = load_kb_config(args.secrets)

    try:
        with KnowledgeBaseClient(config) as client:
            response = client.retrieve_chunks(payload=payload)
    except httpx.HTTPError as exc:
        detail = _format_http_error(exc)
        raise SystemExit(f"[kb] Retrieve request failed: {detail}") from exc

    if args.format == "json":
        print(json.dumps(response, ensure_ascii=False, indent=2))
    else:
        print_pretty_records(
            response,
            limit=args.display_limit,
            max_chars=args.max_snippet_chars,
        )


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload = _load_payload_dict(args.payload, args.payload_file)
    try:
        query_text = _resolve_query(args)
    except SystemExit:
        query_text = None
    else:
        _override_query(payload, query_text)
    if not _has_query(payload.get("query")):
        raise SystemExit("Provide --query/--query-file or ensure the payload includes a non-empty 'query'.")

    metadata_filters_raw = _load_json_value(args.metadata_filters, args.metadata_filters_file, label="metadata-filters")
    metadata_filters = _normalize_metadata_filters(metadata_filters_raw, default_operator=args.filter_operator)
    cli_filters = _build_metadata_filters(args.filter_expressions or [], args.filter_operator)
    filters_payload = metadata_filters or cli_filters

    weights_raw = _load_json_value(args.weights, args.weights_file, label="weights")
    weights_value = _coerce_float_value(weights_raw, label="weights") if weights_raw is not None else None

    retrieval_components: list[dict[str, Any]] = []
    base_model = _compose_retrieval_model(args)
    if base_model:
        retrieval_components.append(base_model)

    field_overrides: dict[str, Any] = {}
    if args.top_k is not None:
        field_overrides["top_k"] = int(args.top_k)
    if args.score_threshold is not None:
        field_overrides["score_threshold"] = float(args.score_threshold)
    if args.score_threshold_enabled is not None:
        field_overrides["score_threshold_enabled"] = args.score_threshold_enabled
    if filters_payload is not None:
        field_overrides["metadata_filtering_conditions"] = filters_payload
    if weights_value is not None:
        field_overrides["weights"] = weights_value
    if field_overrides:
        retrieval_components.append(field_overrides)

    existing_model = payload.get("retrieval_model")
    merged_model = _merge_retrieval_model(existing_model, *retrieval_components)
    if merged_model is not None:
        payload["retrieval_model"] = merged_model
    elif existing_model is not None and not isinstance(existing_model, dict):
        raise SystemExit("Existing payload retrieval_model must be a JSON object when applying overrides.")

    return payload


def _compose_retrieval_model(args: argparse.Namespace) -> dict[str, Any] | None:
    base_model = _load_json_dict(args.retrieval_model, args.retrieval_model_file, label="retrieval-model")
    search_method = args.search_method
    reranking_enable = args.reranking_enable
    rerank_provider = args.reranking_provider
    rerank_model = args.reranking_model

    if not any([base_model, search_method, reranking_enable is not None, rerank_provider, rerank_model]):
        return None

    model: dict[str, Any] = dict(base_model or {})
    if search_method:
        model["search_method"] = search_method
    if reranking_enable is not None:
        model["reranking_enable"] = reranking_enable
    if rerank_provider or rerank_model:
        mode = dict(model.get("reranking_mode") or {})
        if rerank_provider:
            mode["reranking_provider_name"] = rerank_provider
        if rerank_model:
            mode["reranking_model_name"] = rerank_model
        model["reranking_mode"] = mode
    return model


def _merge_retrieval_model(existing: Any, *components: dict[str, Any]) -> dict[str, Any] | None:
    model: dict[str, Any] = {}
    if existing is not None:
        if not isinstance(existing, dict):
            raise SystemExit("Existing retrieval_model in the payload must be a JSON object.")
        model.update(existing)
    for component in components:
        if not component:
            continue
        for key, value in component.items():
            if key == "reranking_mode" and isinstance(value, dict):
                merged_mode = dict(model.get("reranking_mode") or {})
                merged_mode.update(value)
                model["reranking_mode"] = merged_mode
            else:
                model[key] = value
    return model or None


def _resolve_query(args: argparse.Namespace) -> str:
    if args.query is not None:
        normalized = args.query.strip()
        if not normalized:
            raise SystemExit("Query text is empty.")
        return normalized

    if args.query_file:
        if str(args.query_file) == "-":
            text = sys.stdin.read()
        else:
            if not args.query_file.exists():
                raise SystemExit(f"Query file not found: {args.query_file}")
            text = args.query_file.read_text(encoding="utf-8")
        normalized = text.strip()
        if not normalized:
            raise SystemExit("Query text is empty.")
        return normalized

    raise SystemExit("Provide --query or --query-file.")


def _load_payload_dict(inline: str | None, path: Path | None) -> dict[str, Any]:
    payload = _load_json_value(inline, path, label="payload")
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise SystemExit("--payload/--payload-file must define a JSON object.")
    return dict(payload)


def _load_json_dict(inline: str | None, path: Path | None, *, label: str) -> dict[str, Any] | None:
    value = _load_json_value(inline, path, label=label)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise SystemExit(f"{label} JSON must be an object.")
    return dict(value)


def _load_json_value(inline: str | None, path: Path | None, *, label: str) -> Any | None:
    if inline and path:
        raise SystemExit(f"Use either --{label} or --{label}-file (not both).")
    if inline is not None:
        return _decode_json(inline, label)
    if path is not None:
        if not path.exists():
            raise SystemExit(f"{label} file not found: {path}")
        return _decode_json(path.read_text(encoding="utf-8"), label)
    return None


def _decode_json(text: str, label: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON for {label}: {exc}") from exc


def _has_query(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        if "content" in value:
            content = value.get("content")
            return isinstance(content, str) and bool(content.strip())
        return bool(value)
    return True


def _override_query(payload: dict[str, Any], query_text: str) -> None:
    existing_query = payload.get("query")
    if isinstance(existing_query, dict):
        merged_query = dict(existing_query)
        merged_query["content"] = query_text
        payload["query"] = merged_query
    else:
        payload["query"] = query_text


def print_pretty_records(response: dict[str, Any], *, limit: int | None, max_chars: int) -> None:
    records = _extract_chunks(response)
    if not records:
        print("[kb] No records returned.")
        return

    cap = limit if limit and limit > 0 else None
    for idx, record in enumerate(records, start=1):
        if cap and idx > cap:
            break
        segment = record.get("segment")
        if not isinstance(segment, dict):
            segment = {}
        score = _extract_score(record, segment)
        doc_id = _coerce_str(segment.get("document_id") or record.get("document_id"))
        segment_id = _coerce_str(segment.get("id") or record.get("segment_id"))
        position = segment.get("position") or record.get("position")
        snippet = _build_snippet(segment, record, max_chars)
        print(f"[{idx}] score={_format_score(score)} " f"doc={doc_id or '-'} segment={segment_id or '-'} " f"pos={position if position is not None else '-'}")
        if snippet:
            print(f"    {snippet}")
        metadata_line = _format_metadata(record, segment)
        if metadata_line:
            print(f"    metadata: {metadata_line}")


def _extract_chunks(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("records", "chunks", "data", "documents", "hits"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _extract_chunks(value)
            if nested:
                return nested
    for key in ("result", "response", "retrieval", "payload"):
        value = payload.get(key)
        if isinstance(value, (dict, list)):
            nested = _extract_chunks(value)
            if nested:
                return nested
    return []


def _build_snippet(segment: dict[str, Any], record: dict[str, Any], max_chars: int) -> str:
    primary = _coerce_str(segment.get("content") or record.get("content"))
    fallback = _coerce_str(segment.get("sign_content") or record.get("sign_content"))
    text = primary or fallback
    if not text:
        return ""
    normalized = " ".join(text.split())
    if max_chars and max_chars > 0 and len(normalized) > max_chars:
        cutoff = max(max_chars - 3, 1)
        normalized = normalized[:cutoff].rstrip() + "..."
    return normalized


def _format_metadata(record: dict[str, Any], segment: dict[str, Any]) -> str:
    metadata = record.get("metadata") or segment.get("metadata")
    if not metadata:
        return ""
    items: Iterable[tuple[str, Any]]
    if isinstance(metadata, dict):
        items = metadata.items()
    elif isinstance(metadata, list):
        pairs: list[tuple[str, Any]] = []
        for entry in metadata:
            if isinstance(entry, dict):
                name = entry.get("name") or entry.get("key")
                value = entry.get("value")
                if name:
                    pairs.append((str(name), value))
        items = pairs
    else:
        return str(metadata)
    pieces = [f"{key}={value}" for key, value in items if value not in (None, "")]
    return ", ".join(pieces)


def _build_metadata_filters(expressions: Iterable[str], logical_operator: str) -> dict[str, Any] | None:
    normalized = [expr.strip() for expr in expressions if expr and expr.strip()]
    if not normalized:
        return None
    conditions = [_parse_filter_expression(expr) for expr in normalized]
    operator = (logical_operator or "and").lower()
    return {"logical_operator": operator, "conditions": conditions}


def _normalize_metadata_filters(value: Any, *, default_operator: str) -> dict[str, Any] | None:
    if value is None:
        return None
    operator = (default_operator or "and").lower()
    if isinstance(value, dict):
        logical_operator = str(value.get("logical_operator") or operator).lower()
        raw_conditions = value.get("conditions")
        if not isinstance(raw_conditions, list):
            raise SystemExit("metadata_filtering_conditions.conditions must be an array.")
        conditions = [_normalize_filter_condition(item) for item in raw_conditions]
        return {"logical_operator": logical_operator, "conditions": conditions}
    if isinstance(value, list):
        conditions = [_normalize_filter_condition(item) for item in value]
        return {"logical_operator": operator, "conditions": conditions}
    raise SystemExit("metadata-filters JSON must be either an object or an array.")


def _normalize_filter_condition(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SystemExit("metadata_filtering_conditions entries must be objects containing name/comparison_operator/value.")
    name = str(value.get("name") or "").strip()
    if not name:
        raise SystemExit("metadata_filtering_conditions entries must include 'name'.")
    operator = str(value.get("comparison_operator") or value.get("operator") or "eq").strip() or "eq"
    condition = {"name": name, "comparison_operator": operator}
    if "value" in value:
        condition["value"] = value.get("value")
    return condition


def _parse_filter_expression(expression: str) -> dict[str, str]:
    text = expression.strip()
    if not text:
        raise SystemExit("Metadata filter expression cannot be blank.")
    if "=" not in text:
        raise SystemExit(f"Metadata filter expression must contain '=': {expression}")
    key_part, value = text.split("=", 1)
    value = value.strip()
    if not value:
        raise SystemExit(f"Metadata filter expression missing value: {expression}")
    if ":" in key_part:
        name, operator = key_part.split(":", 1)
    else:
        name, operator = key_part, "eq"
    name = name.strip()
    operator = (operator or "eq").strip() or "eq"
    if not name:
        raise SystemExit(f"Metadata filter expression missing field name: {expression}")
    return {"name": name, "comparison_operator": operator, "value": value}


def _extract_score(record: dict[str, Any], segment: dict[str, Any]) -> Any:
    if "score" in record:
        return record.get("score")
    if "score" in segment:
        return segment.get("score")
    return None


def _format_score(score: Any) -> str:
    if score is None:
        return "-"
    if isinstance(score, (int, float)):
        return f"{float(score):.3f}"
    return str(score)


def _coerce_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_float_value(value: Any, *, label: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise SystemExit(f"{label} must be a numeric value.") from None


def _format_http_error(exc: httpx.HTTPError) -> str:
    if isinstance(exc, httpx.HTTPStatusError) and exc.response is not None:
        body = exc.response.text.strip()
        status = f"{exc.response.status_code} {exc.response.reason_phrase}"
        preview = body[:200] + ("..." if len(body) > 200 else "")
        return f"{status}: {preview}"
    return str(exc)


if __name__ == "__main__":
    main()
