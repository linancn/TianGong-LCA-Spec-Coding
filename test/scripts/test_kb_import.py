"""Tests covering the RIS knowledge-base ingestion utilities."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tiangong_lca_spec.kb.config import MetadataFieldDefinition
from tiangong_lca_spec.kb.metadata import build_metadata_entries, format_citation

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

kb_import = importlib.import_module("scripts.kb.import_ris")


def test_format_citation_includes_core_fields() -> None:
    record = {
        "authors": ["Chen, Quanwei", "Lai, Xin"],
        "year": "2023",
        "title": "Comparative environmental impacts ...",
        "secondary_title": "Separation and Purification Technology",
        "volume": "324",
        "start_page": "124642",
        "doi": "10.1016/j.seppur.2023.124642",
        "urls": ["https://example.com/paper"],
    }
    citation = format_citation(record)
    assert "Chen, Quanwei; Lai, Xin (2023)" in citation
    assert "Separation and Purification Technology" in citation
    assert "vol. 324" in citation
    assert "p. 124642" in citation
    assert "DOI: 10.1016/j.seppur.2023.124642" in citation
    assert "URL: https://example.com/paper" in citation


def test_build_metadata_entries_handles_meta_and_category() -> None:
    record = {"meta": "citation text", "category": "battery"}
    definitions = [
        MetadataFieldDefinition(name="meta", source="meta"),
        MetadataFieldDefinition(name="category", source="category"),
    ]
    metadata_ids = {"meta": "meta-id", "category": "cat-id"}
    entries = build_metadata_entries(record, metadata_ids, definitions)
    assert entries == [
        {"id": "meta-id", "name": "meta", "value": "citation text"},
        {"id": "cat-id", "name": "category", "value": "battery"},
    ]


def test_derive_category_from_input_data_subdirectory(tmp_path: Path) -> None:
    input_dir = tmp_path / "input_data" / "battery"
    input_dir.mkdir(parents=True)
    category = kb_import._derive_category_from_path(input_dir)
    assert category == "battery"


def test_derive_category_falls_back_to_last_segment(tmp_path: Path) -> None:
    custom_dir = tmp_path / "papers" / "nickel"
    custom_dir.mkdir(parents=True)
    category = kb_import._derive_category_from_path(custom_dir)
    assert category == "nickel"


def test_resolve_category_prefers_cli_override(tmp_path: Path) -> None:
    ris_dir = tmp_path / "input_data" / "battery"
    ris_dir.mkdir(parents=True)
    ris_path = ris_dir / "battery.ris"
    ris_path.write_text("TY  - JOUR\nER  -\n", encoding="utf-8")
    args = SimpleNamespace(category="custom", ris_dir=ris_dir)
    assert kb_import._resolve_category(args, ris_path) == "custom"


def test_resolve_ris_path_requires_source(tmp_path: Path) -> None:
    ris_path = tmp_path / "custom.ris"
    ris_path.write_text("TY  - JOUR\nER  -\n", encoding="utf-8")
    args = SimpleNamespace(ris_path=ris_path, ris_dir=None, ris_file="ignored")
    assert kb_import._resolve_ris_path(args) == ris_path

    missing_args = SimpleNamespace(ris_path=None, ris_dir=None, ris_file="missing.ris")
    with pytest.raises(SystemExit):
        kb_import._resolve_ris_path(missing_args)


def test_resolve_pipeline_inputs_defaults_to_config() -> None:
    args = SimpleNamespace(pipeline_inputs=None, pipeline_inputs_file=None)
    config = SimpleNamespace(pipeline_inputs={"mode": "auto"})
    assert kb_import._resolve_pipeline_inputs(args, config) == {"mode": "auto"}


def test_resolve_pipeline_inputs_inline_json() -> None:
    args = SimpleNamespace(pipeline_inputs='{"foo": "bar"}', pipeline_inputs_file=None)
    config = SimpleNamespace(pipeline_inputs={})
    assert kb_import._resolve_pipeline_inputs(args, config) == {"foo": "bar"}


def test_resolve_pipeline_inputs_from_file(tmp_path: Path) -> None:
    payload_file = tmp_path / "inputs.json"
    payload_file.write_text('{"alpha": 1}', encoding="utf-8")
    args = SimpleNamespace(pipeline_inputs=None, pipeline_inputs_file=payload_file)
    config = SimpleNamespace(pipeline_inputs={})
    assert kb_import._resolve_pipeline_inputs(args, config) == {"alpha": 1}


def test_resolve_pipeline_inputs_requires_object(tmp_path: Path) -> None:
    payload_file = tmp_path / "inputs.json"
    payload_file.write_text('["a", "b"]', encoding="utf-8")
    args = SimpleNamespace(pipeline_inputs=None, pipeline_inputs_file=payload_file)
    config = SimpleNamespace(pipeline_inputs={})
    with pytest.raises(SystemExit):
        kb_import._resolve_pipeline_inputs(args, config)


def test_resolve_pipeline_inputs_conflict(tmp_path: Path) -> None:
    payload_file = tmp_path / "inputs.json"
    payload_file.write_text("{}", encoding="utf-8")
    args = SimpleNamespace(pipeline_inputs="{}", pipeline_inputs_file=payload_file)
    config = SimpleNamespace(pipeline_inputs={})
    with pytest.raises(SystemExit):
        kb_import._resolve_pipeline_inputs(args, config)


def test_build_datasource_entry_populates_expected_fields() -> None:
    file_meta = {
        "id": "file-123",
        "name": "sample.pdf",
        "mime_type": "application/pdf",
        "size": 1024,
        "extension": "pdf",
    }
    entry = kb_import._build_datasource_entry(file_meta, "local_file", meta_value="citation text")
    assert entry["related_id"] == "file-123"
    assert entry["name"] == "sample.pdf"
    assert entry["transfer_method"] == "local_file"
    assert entry["mime_type"] == "application/pdf"
    assert entry["meta"] == "citation text"


def test_extract_document_ids_handles_nested_payloads() -> None:
    response = {
        "documents": [{"id": "doc-1"}],
        "result": {"documents": [{"document_id": "doc-2"}]},
    }
    assert set(kb_import._extract_document_ids(response)) == {"doc-1", "doc-2"}


def test_build_pipeline_inputs_injects_meta_without_mutating_base() -> None:
    base_inputs = {"foo": "bar"}
    merged = kb_import._build_pipeline_inputs(base_inputs, "citation text")
    assert merged["meta"] == "citation text"
    assert merged["foo"] == "bar"
    assert "meta" not in base_inputs


def test_build_pipeline_inputs_defaults_meta_to_empty_string() -> None:
    merged = kb_import._build_pipeline_inputs({}, None)
    assert merged["meta"] == ""
