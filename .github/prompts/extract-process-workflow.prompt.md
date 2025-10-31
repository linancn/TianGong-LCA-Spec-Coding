# Tiangong LCA Data Extraction Workflow Guide

This document focuses on data extraction and workflow orchestration: it outlines the staged scripts, core modules, data structures, and critical validation checkpoints so Codex can follow a unified strategy during execution. For development environment details and general collaboration conventions, see `AGENTS.md` in the repository root.

The process extraction workflow splits responsibilities as follows:
- **Stage 1 (Preprocessing)**: Parse the source paper/material and output structured `clean_text` for downstream steps.
- **Stage 2 (Process generation)**: Codex drives the LLM to extract process blocks, add `FlowSearch hints`, and document conversion assumptions—the foundational input for alignment.
- **Stage 3 (Flow alignment & artifact export)**: Combine MCP search results and operator context to confirm the standard flow for each exchange, then merge the results, generate ILCD artifacts under `artifacts/<run_id>/exports/`, run local validation, and assemble `workflow_result.json` for downstream tasks.

Every run stores its artifacts under `artifacts/<run_id>/`, where `run_id` is a UTC timestamp (Stage 1 prints the value and records it in `artifacts/.latest_run_id`). The directory layout is:
- `cache/`: Stage outputs (`stage1_clean_text.md`, `stage2_process_blocks.json`, alignment summaries, validation reports) and on-disk LLM caches.
- `exports/`: Contains only `processes/`, `flows/`, and `sources/`; TIDAS validation reads from this directory.
Subsequent stages fall back to the most recent `run_id` when `--run-id` is omitted, so you can run Stage 2 and Stage 3 immediately after Stage 1 without retyping the identifier.
## 0. Execution Ground Rules (Avoid Wasted Iterations)
- **Read the source material first**: Before you start, quickly review the original paper or `clean_text` to confirm chapter layout, data tables, and the functional unit.
- **Run the standard commands directly**: By default, follow the Stage 1-3 CLI templates listed below (input/output paths follow repository conventions) without repeatedly calling `--help`. Only open the help text when you truly need custom parameters.
- **Always run the staged scripts**: Unless the user explicitly instructs otherwise, invoke `stage1` → `stage3`. Do not handcraft large JSON payloads or skip stages to fabricate intermediate files. If credentials (OpenAI, MCP, TIDAS) are missing, notify the user immediately and wait for guidance. You can execute `stage4_publish` after Stage 3 when publication is required.
- **Assume credentials are provisioned**: In standard environments `.secrets/secrets.toml` is preconfigured by operations, so Codex can start from any stage. Only revisit credential settings when scripts actually raise missing-credential errors or connection failures.
- **Validate inputs before calling the LLM/MCP**: Check whether `clean_text` is non-empty and contains tables and units as expected; prompt the user to supplement the data when necessary.
- **Final JSON requirements**: The delivered `workflow_result.json` must be generated from data that has already passed the Stage 3 artifact validation step. Remove debugging fields, empty structures, and temporary notes so every process dataset strictly follows the schema and is “clean” enough for direct ingestion.
- **Smoke test MCP once**: Write a quick five-line Python snippet (import `FlowSearchService` and construct a `FlowQuery`) to test a single exchange, confirm credentials and network connectivity, and only then launch Stage 3. This avoids learning about configuration errors after long timeouts.
- **Control the number of exchanges**: Stage 3 issues MCP requests sequentially (`flow_search_max_parallel=1`); each `exchange` triggers an independent call. Stage 2 must reproduce the literature table row by row—every original line becomes its own `exchange` (no merging, averaging, or omission). Capture scenario notes and footnotes in `generalComment` so Stage 3 can align each row to its original context.
- **Enrich the search hints**: For every `exchange.generalComment`, add common synonyms (semantic equivalents such as “electric power supply”), aliases or abbreviations (“COG”, “DAC”), chemical formulas/CAS numbers, and key parameters with Chinese-English pairs. Doing so lets FlowSearchService exploit multilingual synonym expansion and richer context to improve recall. For high-frequency base flows (`Electricity, medium voltage`, `Water, process`, `Steam, low pressure`, `Oxygen`, `Hydrogen`, `Natural gas, processed`, etc.), list at least two or three Chinese/English aliases or typical descriptions (e.g., “grid electricity 10–30 kV”, “中压电”, “technological water”, “饱和蒸汽 0.4 MPa”, “O₂, CAS 7782-44-7”) and describe state/purity/source. `generalComment` must begin with `FlowSearch hints:` and follow the structure `en_synonyms=... | zh_synonyms=... | abbreviation=... | formula_or_CAS=... | state_purity=... | source_or_pathway=... | usage_context=...`. Use `NA` for missing fields to keep placeholders, then append table references or conversion assumptions at the end. Without these clues, MCP often returns short Chinese names or low-similarity candidates, forcing Stage 3 to fall back to placeholders.
- **Self-check Stage 2 outputs**: Before kicking off Stage 3, sample `artifacts/<run_id>/cache/stage2_process_blocks.json` to confirm that every `exchange.generalComment` includes the `FlowSearch hints` structure, critical synonyms, and bilingual parameters. If you still see brief “Table X” descriptions, redo Stage 2 or manually enrich the context; otherwise Stage 3 will miss frequently because it lacks semantic signals and you will end up with numerous `unmatched:placeholder` entries.
- **Normalize flow names**: Prefer Tiangong/ILCD canonical flow names rather than keeping the paper’s parentheses or process-specific qualifiers (for example, avoid `Electricity for electrolysis (PV)`). Standard names dramatically raise Stage 3 hit rates and reduce duplicate searches and timeouts.
- **Tune long-running commands in advance**: Stage 2/3 may exceed 15 minutes. In constrained environments, increase outer CLI timeouts or bump the `.secrets` `timeout` field to prevent forced termination and reruns.
- **Cap retry attempts**: Do not retry the same LLM/MCP call more than twice. Explain the reason for every prompt or context adjustment; if the issue persists, switch to manual analysis and escalate to the user.
- **Record key assumptions**: Document any inference (unit completion, default geography, classification path) in `generalComment` so downstream reviewers do not have to confirm them repeatedly.

## 1. Module Overview (`src/tiangong_lca_spec`)
- `core/`
  - `config.py`: Centralizes MCP/TIDAS endpoints, retry and concurrency policies, and artifact directories.
  - `exceptions.py`: Defines `SpecCodingError` and specialized exceptions to unify error semantics.
  - `models.py`: Declares data structures such as `FlowQuery`, `FlowCandidate`, `ProcessDataset`, and `WorkflowResult`.
  - `logging.py`: Emits JSON logs via `structlog`.
  - `json_utils.py`: Cleans LLM outputs and fixes malformed JSON or unbalanced brackets.
  - `mcp_client.py`: Builds persistent sessions with the official `mcp` SDK (streamable HTTP + `ClientSession`) and provides synchronous `invoke_tool`/`invoke_json_tool`.
- `flow_search/`: Wraps MCP flow retrieval with retry logic, candidate filtering, and matched/unmatched assembly.
- `flow_alignment/`: Aligns exchanges in parallel, supports LLM-based candidate screening (falls back to similarity scoring), and outputs `matched` results along with placeholder `origin_exchanges`.
- `process_extraction/`: Handles preprocessing, parent splitting, classification, location normalization, and `processDataSet` consolidation.
- `tidas_validation/`: Invokes the TIDAS MCP tool and converts responses into `TidasValidationFinding`.
- `orchestrator/`: Provides a sequential orchestrator that links all stages into a single entry point.
- `scripts/`: Staged CLIs (`stage1`–`stage4`) and the regression entry point `run_test_workflow.py`.

## 2. Staged Scripts
Scripts read and write intermediate files under `artifacts/<run_id>/cache/` by default; override the paths with CLI arguments when needed.

| Stage | Script | Artifacts | Description |
| ---- | ---- | ---- | ---- |
| 1 | `stage1_preprocess.py` | `artifacts/<run_id>/cache/stage1_clean_text.md` | Parse Markdown/JSON papers and output `clean_text`. |
| 2 | `stage2_extract_processes.py` | `artifacts/<run_id>/cache/stage2_process_blocks.json` | Use OpenAI Responses to generate process blocks. |
| 3 | `stage3_align_flows.py` | `artifacts/<run_id>/cache/stage3_alignment.json`, `.../cache/process_datasets.json`, `.../cache/tidas_validation.json`, `.../cache/workflow_result.json`, plus ILCD JSON archives under `artifacts/<run_id>/exports/processes|flows|sources/` | Invoke `FlowAlignmentService`, enforce hint quality, merge aligned results, materialise ILCD artifacts, and run `tidas_tools.validate`. |
| 4 (optional) | `stage4_publish.py` | `artifacts/<run_id>/cache/stage4_publish_preview.json` | Read Stage 3 outputs and build the `Database_CRUD_Tool` payload; runs as a dry run by default, add `--commit` to publish flows and process datasets. |

Recommended execution sequence (from the repository root):
```bash
RUN_ID=$(date -u +"%Y%m%dT%H%M%SZ")  # optional helper; Stage 1 prints the generated run_id
uv run python scripts/stage1_preprocess.py --paper path/to/paper.json --run-id "$RUN_ID"
uv run python scripts/stage2_extract_processes.py --run-id "$RUN_ID"
uv run python scripts/stage3_align_flows.py --run-id "$RUN_ID"
uv run python scripts/stage4_publish.py --run-id "$RUN_ID" \
  --publish-flows --publish-processes \
  --update-alignment --update-datasets \
  --commit  # omit --commit to preview only
```

- If `stage3_align_flows.py` detects OpenAI credentials in `.secrets/secrets.toml`, it automatically enables LLM scoring to evaluate the ten MCP candidates; otherwise it falls back to local similarity matching. Before alignment, the script verifies every exchange has both `exchangeName` and `FlowSearch hints` (field requirements are listed in §0). When hints are missing the script stops by default; use `--allow-missing-hints` only to bypass explicit warning handling. If `exchangeName` is absent, it first tries to auto-fill it from the multilingual synonyms in `FlowSearch hints`. The resulting `artifacts/<run_id>/cache/stage3_alignment.json` always includes `process_id`, `matched_flows`, `unmatched_flows`, and `origin_exchanges`, and the CLI prints match statistics for each process.

## 3. Core Data Structures
```python
from dataclasses import dataclass, field
from typing import Any, Mapping, Literal

@dataclass(slots=True, frozen=True)
class FlowQuery:
    exchange_name: str
    description: str | None = None
    process_name: str | None = None
    paper_md: str | None = None

@dataclass(slots=True)
class FlowCandidate:
    uuid: str | None
    base_name: str
    treatment_standards_routes: str | None = None
    mix_and_location_types: str | None = None
    flow_properties: str | None = None
    version: str | None = None
    general_comment: str | None = None
    geography: Mapping[str, Any] | None = None
    classification: list[Mapping[str, Any]] | None = None
    reasoning: str = ""

@dataclass(slots=True)
class UnmatchedFlow:
    base_name: str
    general_comment: str | None = None
    status: Literal["requires_creation"] = "requires_creation"
    process_name: str | None = None

@dataclass(slots=True)
class ProcessDataset:
    process_information: dict[str, Any]
    modelling_and_validation: dict[str, Any]
    administrative_information: dict[str, Any]
    exchanges: list[dict[str, Any]] = field(default_factory=list)
    process_data_set: dict[str, Any] | None = None

@dataclass(slots=True)
class WorkflowResult:
    process_datasets: list[ProcessDataset]
    alignment: list[dict[str, Any]]
    validation_report: list[TidasValidationFinding]
```

## 4. Flow Search
- `FlowSearchClient` uses `MCPToolClient.invoke_json_tool` to access the remote `Search_flows_Tool` and automatically builds the search context from a `FlowQuery`.
- `FlowQuery.description` comes directly from the Stage 2 `exchange.generalComment` `FlowSearch hints` string; keep the field order and separators intact so QueryFlow Service can extract multilingual synonyms and physical properties.
- The remote `tiangong_lca_remote` tool already embeds an LLM, which expands synonyms from the full `generalComment` and performs hybrid full-text + semantic search, so Stage 3 does not need to craft extra prompts manually.
- When `generalComment` is complete and concise, you can trust the candidates returned by `tiangong_lca_remote`; focus on selecting the best-fitting flow and writing any necessary notes.
- `stage3_align_flows.py` is the only entry point: do not stitch `referenceToFlowDataSet` during Stage 2. Let Stage 3 read the Stage 2 `process_blocks` and trigger the lookup.
- Before running Stage 3, sanity-check one or two exchanges by building a `FlowQuery` manually to confirm the service returns candidates (avoid an empty batch run).
- Issue at least one MCP lookup per exchange; only mark it as `UnmatchedFlow` and record the reason after up to three failures.
- Use exponential backoff for retries; catch `httpx.HTTPStatusError` and `McpError`, and strip context when necessary to avoid 413/5xx errors.
- `FlowSearchService` handles similarity filtering, cache hits, and `UnmatchedFlow` assembly. After Stage 3, review the logs to confirm hit rates and list exchanges that still did not match.
- If the logs show many `flow_search.filtered_out` events without matches, start by checking (i) whether `exchangeName`/`unit` is missing or misspelled, (ii) whether `clean_text` includes overly long context that introduces noise, and (iii) whether `.secrets` sets a larger `timeout` to accommodate slow responses.
- `mcp_tool_client.close_failed` warnings typically occur when cleanup coroutines run after a request and are considered normal; if timeouts persist, lower `flow_search_max_parallel` or split Stage 3 into batches.

## 5. Flow Alignment
- Each process block submits exchange lookups on independent threads, aggregating `matched_flows` and `origin_exchanges`; unmatched items are only tallied in logs as reminders.
- The Stage 3 script validates Stage 2 outputs against the §0 `FlowSearch hints` spec and infers missing names from the synonym fields when possible, preventing hint-free exchanges from reaching MCP.
- Successful matches write back `referenceToFlowDataSet`; failures keep the original exchange and record the reason.
- The script emits structured logs such as `flow_alignment.start` and `flow_alignment.exchange_failed` for diagnostics.

## 6. Process Extraction
- Execute `extract_sections` → `classify_process` → `normalize_location` → `finalize` in order.
- Processing notes:
  - `extract_sections` splits content by parent section or aliases; if nothing matches, fall back to the full document.
  - If the LLM does not return `processDataSets`/`processDataSet`, raise `ProcessExtractionError`.
  - Normalize table fields to alignment-friendly base units (e.g., convert t→kg, keep Nm³ as cubic meters, and describe density-based assumptions for volume) and document the conversion logic in `generalComment`.
  - `finalize` calls `build_tidas_process_dataset` to populate required ILCD/TIDAS fields, producing process blocks that contain only `processDataSet` (with meta fields such as `process_id`); Stage 2 no longer returns the legacy `exchange_list` cache.
- LLM output validation checklist:
  1. The top level must be a `processDataSets` array.
  2. Each process needs the four subfields under `processInformation.dataSetInformation.name`: `baseName`, `treatmentStandardsRoutes`, `mixAndLocationTypes`, `functionalUnitFlowProperties`.
  3. Every `exchanges.exchange` entry must include `exchangeDirection`, `meanAmount`, and `unit`.
- Prompt the LLM not to emit `referenceToFlowDataSet` placeholders—Stage 3 adds them after alignment. Keep `@dataSetInternalID` so the artifact builder and validation logic in Stage 3 can use it.
- When table values need cleaning (filling units, merging duplicates), prototype the logic in plain Python first and then integrate it into `ProcessExtractionService`; avoid editing rows manually in a response.
- `merge_results` incorporates alignment candidates and builds the functional unit string.
- When Stage 3 logic deserializes `process_blocks`, always pull exchanges from `processDataSet.exchanges` instead of the deprecated `exchange_list`.

## 7. TIDAS Validation
- The Stage 3 export step runs `uv run python -m tidas_tools.validate -i artifacts/<run_id>/exports` to confirm ILCD compliance and records the findings in `artifacts/<run_id>/cache/tidas_validation.json`.
- If the local validator is temporarily unavailable, rerun Stage 3 with `--skip-artifact-validation` and document the reason; treat Stage 7 as preview-only until validation passes.

## 8. Workflow Orchestration
- `WorkflowOrchestrator` runs the stages sequentially: `preprocess` → `extract_processes` → `align_flows` → `merge_datasets` → `validate` → `finalize`.
- It returns a `WorkflowResult` that Stage 3 now writes to `workflow_result.json`, or that external integrations can consume directly.

## 9. Validation Recommendations
- Prioritize unit tests for JSON cleaning (`json_utils`), FlowSearchService filtering/caching, FlowAlignmentService fallback handling, error branches across process extraction stages, and `merge_results` resilience.
- For integration checks, run `stage1` → `stage3` on a minimal paper sample, then verify schema compliance, match statistics, and validation reports; add a dry-run `stage4_publish` if you need to validate the publication flow.
- Monitoring: enable `configure_logging` JSON output and filter for events such as `flow_alignment.exchange_failed` and `process_extraction.parents_uncovered` to quickly pinpoint failing stages.

## 10. Classification and Location Reference Resources
- `tidas_processes_category.json` (`src/tidas/schemas/tidas_processes_category.json`) is the authoritative source for process classifications, covering every level of the ISIC tree. When Codex needs the classification path, use `uv run python scripts/list_process_category_children.py <code>` to expand level by level (`<code>` empty returns the top level, e.g., `uv run python scripts/list_process_category_children.py 01`). You can also load specific branches with `tiangong_lca_spec.tidas.get_schema_repository().resolve_with_references("tidas_processes_category.json")` and paste the relevant snippets into Codex prompts so it can choose the correct `@classId`/`#text` within limited context.
- Geographic codes follow `tidas_locations_category.json` (`src/tidas/schemas/tidas_locations_category.json`). Use `uv run python scripts/list_location_children.py <code>` (e.g., `uv run python scripts/list_location_children.py CN` to see China's hierarchy). When briefing Codex, share only the subtree relevant to the current process to avoid sending the entire taxonomy.
- When a process involves flow classifications, call `uv run python scripts/list_product_flow_category_children.py <code>` for product flows (data source `tidas_flows_product_category.json`) or `uv run python scripts/list_elementary_flow_category_children.py <code>` for elementary flows (data source `tidas_flows_elementary_category.json`).

## 11. Stage 4 Publish & Database CRUD
- `stage4_publish.py` calls the `Database_CRUD_Tool` from `tiangong_lca_remote` to persist `flows`, `processes`, and `sources`. The `insert` payload must set the tool-level `id` to the dataset UUID already present in the export: use `flowInformation.dataSetInformation.common:UUID`, `processInformation.dataSetInformation.common:UUID`, or `sourceInformation.dataSetInformation.common:UUID` directly. Do not generate alternate identifiers and do not reuse previous run IDs.
- `Database_CRUD_Tool` payload fields:
  - `operation`: `"select"`, `"insert"`, `"update"`, or `"delete"`.
  - `table`: `"flows"`, `"processes"`, `"sources"`, `"contacts"`, or `"lifecyclemodels"`.
  - `jsonOrdered`: required for insert/update; pass the canonical ILCD document (e.g., `{"processDataSet": {...}}`) with namespace declarations, timestamps, and reference stubs intact.
  - `id`: required for insert/update/delete; match the UUID from the dataset’s `dataSetInformation`.
  - `version`: required for update/delete and stored alongside `json_ordered`.
  - Optional `filters` and `limit` cover equality queries during select operations.
- Successful responses echo the record `id`, `version`, and a `data` array. Validation failures raise `SpecCodingError`; log the payload path from the error and fix the dataset before retrying.
- Minimum insert checklist:
  - Preserve ILCD root attributes (`@xmlns`, `@xmlns:common`, `@xmlns:xsi`, schema location) and include `administrativeInformation.dataEntryBy.common:timeStamp`, `common:referenceToDataSetFormat`, and `common:referenceToPersonOrEntityEnteringTheData`.
  - Keep compliance references (e.g., ILCD format UUID `a97a0155-0234-4b87-b4ce-a45da52f2a40`, ownership UUID `f4b4c314-8c4c-4c83-968f-5b3c7724f6a8`, contact UUID `1f8176e3-86ba-49d1-bab7-4eca2741cdc1`) and declare `modellingAndValidation.LCIMethod.typeOfDataSet`.
  - Flows additionally require `flowProperties.flowProperty` (mass property UUID `93a60a56-a3c8-11da-a746-0800200b9a66`), `quantitativeReference.referenceToReferenceFlowProperty`, and `classificationInformation`/`elementaryFlowCategorization` as applicable.
  - Processes must retain the Stage 3 functional unit, exchange list, and `modellingAndValidation` blocks. Sources must keep bibliographic metadata and publication timestamps.
- For batch publication, queue inserts per dataset type (`flows` → `processes` → `sources`) so references resolve immediately, and record each returned `id`/`version` pair for audit.
- **Elementary flows**: Treat emission/resource exchanges as lookup-only. Resolve them to existing flow datasets before Stage 4; if Stage 3 still returns `unmatched:placeholder`, fix the hints or manually map the flow instead of publishing. Stage 4 only auto-generates new product (or waste) flow datasets for true products when the catalogue lacks a record.
