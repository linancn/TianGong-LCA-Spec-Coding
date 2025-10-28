# Tiangong LCA Spec Coding Development Guidelines

This guide focuses on general conventions for engineering collaboration, helping the team understand repository organization, development environment, credential management, and quality self-check procedures. Detailed responsibilities for process extraction and the staged scripts are described in `.github/prompts/extract-process-workflow.prompt.md`.

## 1. Project Overview
- **Objective**: Deliver end-to-end automation of the Tiangong LCA Spec Coding workflow, covering paper cleaning, process extraction, exchange alignment, data merging, TIDAS validation, and final delivery.
- **Core directories**:
  - `src/tiangong_lca_spec/`: Workflow services, MCP clients, data models, and logging utilities.
  - `scripts/`: Staged CLIs from `stage1_preprocess.py` through `stage7_publish.py`, plus the regression entry point `run_test_workflow.py`.
  - `.github/prompts/`: Prompt specifications for Codex, with `extract-process-workflow.prompt.md` dedicated to the process extraction task.
- **Collaboration interfaces**: The standard workflow depends on `.secrets/secrets.toml` where OpenAI, Tiangong LCA Remote, and TIDAS validation services are configured. Validate credentials before running Stage 3 or later in batch during your first integration.
- **Further references**: Requirements, alignment strategies, and exception handling for each stage are documented in `.github/prompts/extract-process-workflow.prompt.md`. For supplemental classification or geographic information, use the helper CLIs provided by `scripts/list_*_children.py`.

## 2. Development Environment and Dependencies
- **Python version**: ≥ 3.12. Manage it with `uv toolchain`; the default virtual environment lives in `.venv/`.
- **Command conventions**: Workstations do not expose a system-level `python`. Use `uv run python …` or `uv run -- python script.py`. For one-liners, use `uv run python - <<'PY'`.
- **Dependency installation**:
  ```bash
  uv sync                # Install runtime dependencies
  uv sync --group dev    # Install development dependencies including black/ruff
  ```
  Set `UV_PYPI_URL=https://pypi.tuna.tsinghua.edu.cn/simple` temporarily if you need a mirror.
- **Key runtime libraries**: `anyio`, `httpx`, `mcp`, `pydantic`, `pydantic-settings`, `tenacity`, `structlog`, `python-dotenv`, `jsonschema`, `openai`.
- **Build system**: The project uses `hatchling`. In `pyproject.toml`, `[tool.hatch.build.targets.wheel]` declares `src/tiangong_lca_spec` as the build target.

## 3. Credentials and Remote Services
1. Copy the template to create a local configuration: `cp .secrets/secrets.example.toml .secrets/secrets.toml`.
2. Edit `.secrets/secrets.toml`:
   - `[OPENAI]`: `API_KEY`, `MODEL` (default `gpt-5`, override as needed).
   - `[tiangong_lca_remote]`: `url`, `service_name`, `tool_name`, `api_key`.
   - `[tidas_data_validate]`: `url`, `tool_name`, optional `api_key`.
3. Write plaintext tokens directly into `api_key`; the framework automatically prepends `Bearer`.
4. Before running Stage 3, call `FlowSearchService` with one or two sample exchanges to perform a connectivity self-test (see the workflow prompt document for Python snippets).
- If operations has already provisioned `.secrets/secrets.toml`, Codex uses it as-is. Only revisit the local configuration when scripts raise missing-credential errors or connection failures.

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
