"""Tests covering the KB retrieval CLI helpers."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

kb_retrieve = importlib.import_module("scripts.kb.retrieve")


def test_resolve_query_prefers_inline_value() -> None:
    args = SimpleNamespace(query="battery recycling", query_file=None)
    assert kb_retrieve._resolve_query(args) == "battery recycling"


def test_resolve_query_reads_from_file(tmp_path: Path) -> None:
    query_file = tmp_path / "query.txt"
    query_file.write_text("critical materials", encoding="utf-8")
    args = SimpleNamespace(query=None, query_file=query_file)
    assert kb_retrieve._resolve_query(args) == "critical materials"


def test_resolve_query_requires_source() -> None:
    args = SimpleNamespace(query=None, query_file=None)
    with pytest.raises(SystemExit):
        kb_retrieve._resolve_query(args)


def test_build_metadata_filters_parses_expressions() -> None:
    filters = kb_retrieve._build_metadata_filters(["category:eq=battery", "language=zh"], "and")
    assert filters == {
        "logical_operator": "and",
        "conditions": [
            {"name": "category", "comparison_operator": "eq", "value": "battery"},
            {"name": "language", "comparison_operator": "eq", "value": "zh"},
        ],
    }


def test_parse_filter_expression_requires_value() -> None:
    with pytest.raises(SystemExit):
        kb_retrieve._parse_filter_expression("category:")


def test_normalize_metadata_filters_accepts_object() -> None:
    raw = {
        "logical_operator": "or",
        "conditions": [
            {"name": "category", "comparison_operator": "eq", "value": "battery"},
        ],
    }
    normalized = kb_retrieve._normalize_metadata_filters(raw, default_operator="and")
    assert normalized["logical_operator"] == "or"
    assert normalized["conditions"][0]["name"] == "category"


def test_normalize_metadata_filters_wraps_array() -> None:
    raw = [{"name": "category", "comparison_operator": "eq", "value": "battery"}]
    normalized = kb_retrieve._normalize_metadata_filters(raw, default_operator="and")
    assert normalized == {
        "logical_operator": "and",
        "conditions": [{"name": "category", "comparison_operator": "eq", "value": "battery"}],
    }


def test_merge_retrieval_model_combines_components() -> None:
    existing = {"search_method": "semantic_search", "reranking_mode": {"reranking_provider_name": "foo"}}
    overrides = {"top_k": 10, "reranking_mode": {"reranking_model_name": "bar"}}
    merged = kb_retrieve._merge_retrieval_model(existing, overrides)
    assert merged["top_k"] == 10
    assert merged["reranking_mode"]["reranking_provider_name"] == "foo"
    assert merged["reranking_mode"]["reranking_model_name"] == "bar"


def test_extract_chunks_prefers_chunks_key() -> None:
    payload = {"chunks": [{"content": "chunk text"}]}
    extracted = kb_retrieve._extract_chunks(payload)
    assert extracted == [{"content": "chunk text"}]


def test_extract_chunks_walks_nested_payload() -> None:
    payload = {"result": {"data": [{"text": "nested"}]}}
    extracted = kb_retrieve._extract_chunks(payload)
    assert extracted == [{"text": "nested"}]
