# Tiangong LCA Spec Coding Development Guidelines

This guide focuses on general conventions for engineering collaboration, helping the team understand repository organization, development environment, credential management, and quality self-check procedures. Detailed responsibilities for process extraction and the staged scripts are described in `.github/prompts/extract-process-workflow.prompt.md`.

## 1. Project Overview
- **Objective**: Deliver end-to-end automation of the Tiangong LCA Spec Coding workflow, covering paper cleaning, process extraction, exchange alignment, data merging, TIDAS validation, and final delivery.
- **Core directories**:
  - `src/tiangong_lca_spec/`: Workflow services, MCP clients, data models, and logging utilities.
  - `scripts/md/`: Staged CLIs from `stage1_preprocess.py` through `stage4_publish.py`, plus the regression entry point `run_test_workflow.py`.
  - `scripts/jsonld/`: JSON-LD extraction, validation, and publishing helpers.
  - `.github/prompts/`: Prompt specifications for Codex, with `extract-process-workflow.prompt.md` dedicated to the process extraction task.
  - `scripts/kb/`: Knowledge base tooling (e.g., `import_ris.py` for ingestion, `minio_fetch.py` for downloading parsed bundles from storage) for pushing bibliographic PDFs into Tiangong datasets.
- **Collaboration interfaces**: The standard workflow depends on `.secrets/secrets.toml` where OpenAI, Tiangong LCA Remote, and TIDAS validation services are configured. Validate credentials before running Stage 3 or later in batch during your first integration.
- **Further references**: Requirements, alignment strategies, and exception handling for each stage are documented in `.github/prompts/extract-process-workflow.prompt.md`. For supplemental classification or geographic information, use the helper CLIs provided by `scripts/md/list_*_children.py`.
- **Stage 4 flow publishing**: When filling in missing flow definitions, the publisher now leans on the configured LLM to infer both the flow type and the most specific product classification. Follow the credential setup above so the scripts can call `scripts/md/list_product_flow_category_children.py` via the LLM-assisted selector.

## 2. Development Environment and Dependencies
- **Python version**: ≥ 3.12. Manage it with `uv toolchain`; the default virtual environment lives in `.venv/`.
- **Command conventions**: Workstations do not expose a system-level `python`. Use `uv run python …` or `uv run -- python script.py`. For one-liners, use `uv run python - <<'PY'`.
- **Dependency installation**:
  ```bash
  uv sync                # Install runtime dependencies
  uv sync --upgrade  # Upgrade all dependencies to the latest allowed versions
  uv sync --group dev    # Install development dependencies including black/ruff
  ```
  Set `UV_PYPI_URL=https://pypi.tuna.tsinghua.edu.cn/simple` temporarily if you need a mirror.
- **Key runtime libraries**: `anyio`, `httpx`, `mcp`, `pydantic`, `pydantic-settings`, `tenacity`, `structlog`, `python-dotenv`, `jsonschema`, `openai`.
- **Build system**: The project uses `hatchling`. In `pyproject.toml`, `[tool.hatch.build.targets.wheel]` declares `src/tiangong_lca_spec` as the build target.

## 3. Credentials and Remote Services
1. Copy the template to create a local configuration: `cp .secrets/secrets.example.toml .secrets/secrets.toml`.
2. Edit `.secrets/secrets.toml`:
   - `[openai]`: `api_key`, `model` (default `gpt-5`, override as needed).
   - `[tiangong_lca_remote]`: `url`, `service_name`, `tool_name`, `api_key`.
   - `[kb]`: `base_url`, `dataset_id`, `api_key`, optional `timeout`, and `metadata_fields` (defaults already set to the `meta` and `category` fields).
   - `[kb.pipeline]`: `datasource_type`, `start_node_id`, `is_published`, `response_mode`, and optional `inputs` for the RAG pipeline runner. The pipeline node ID is available from the dataset’s pipeline designer.
   - `[minio]`: `endpoint`, `access_key`, `secret_key`, `bucket_name`, and `prefix` for the KB bundle bucket; optional `secure` (defaults to `https` when omitted) and `session_token` are supported for custom deployments.
3. Write plaintext tokens directly into `api_key`; the framework automatically prepends `Bearer`.
4. Before running Stage 3, call `FlowSearchService` with one or two sample exchanges to perform a connectivity self-test (see the workflow prompt document for Python snippets).
- If operations has already provisioned `.secrets/secrets.toml`, Codex uses it as-is. Only revisit the local configuration when scripts raise missing-credential errors or connection failures.

Local TIDAS validation now relies on the CLI command `uv run tidas-validate -i artifacts`, which Stage 3 executes automatically. No additional MCP credentials are required for this step.

**Knowledge base ingestion**
- Populate the `[kb]` section in `.secrets/secrets.toml` with the real host (e.g., `https://<kb-host>/v1`), dataset ID, and API key.
- Configure `[kb.pipeline]` so the importer can trigger the published RAG pipeline: `datasource_type` should match your FILE block (typically `local_file`), `start_node_id` must be copied from the pipeline designer, and `inputs` carries any required input-field values. If omitted, pipeline defaults apply.
- Default metadata includes `meta` (auto-generated citation text) and `category` (taken from the first subdirectory under `input_data/`, e.g., `battery`). Override via `--category` if needed.
- Use `uv run python scripts/kb/import_ris.py --ris-dir input_data/<dir>` (or `--ris-path ...`) to ingest RIS files; add `--dry-run` for previews. Attachments must live under the same `input_data/<dir>` root.
- The importer now uploads files through the dataset pipeline (`/pipeline/file-upload` + `/pipeline/run`) so the configured pipeline stages run exactly as the UI workflow. Metadata is attached after the pipeline reports the generated document IDs.
- When you need to pull parsed artifacts from MinIO, populate `[minio]` as described above and run `uv run python scripts/kb/minio_fetch.py list --path <remote_subdir>` to inspect available bundles or `uv run python scripts/kb/minio_fetch.py download --path <remote_subdir> --output input_data/<dir> --include-source` to materialize the `meta.txt`, `parsed.json`, `pages/`, and optional `source.pdf` files locally. Omit `--include-source` to skip the PDF.

## 4. Quality Assurance and Self-Checks
- After modifying Python source code, run:
  ```bash
  uv run black .
  uv run ruff check
  uv run python -m compileall src scripts
  uv run pytest
  ```
- When changes involve process extraction or alignment logic, prioritize a minimal Stage 1→Stage 6 end-to-end run. Command examples and stage requirements are in `.github/prompts/extract-process-workflow.prompt.md`.
- Structured logging defaults to `structlog`. While running CLIs, monitor `flow_alignment.*`, `process_extraction.*`, and similar events to quickly localize issues.
- Before committing, ensure intermediate files under `artifacts/` are not accidentally staged. Clean them locally or add them to `.gitignore` if needed.

## 5. Support and Communication
- Consult the docstrings and type definitions in the relevant modules under `src/tiangong_lca_spec/` to keep terminology consistent when questions arise.
- If external services are unavailable or time out for extended periods, capture reproduction steps and log excerpts, then escalate to operations or the workflow owner promptly.
- When expanding prompts or reorganizing workflow responsibilities, update the corresponding documents in `.github/prompts/` first to keep roles aligned with this guide.
