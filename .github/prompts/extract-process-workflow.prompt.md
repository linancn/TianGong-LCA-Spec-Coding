# Tiangong LCA Data Extraction Workflow Guide

This document focuses on data extraction and workflow orchestration: it outlines the staged scripts, core modules, data structures, and critical validation checkpoints so Codex can follow a unified strategy during execution. For development environment details and general collaboration conventions, see `AGENTS.md` in the repository root.

**Always execute Python with `uv`: the workstation does not provide a bare `python` binary.** Use `uv run python …` (or `uv run -- python script.py`) for scripts, one-liners via `uv run python - <<'PY'`, and `uv run python -m module` when invoking modules.

The process extraction workflow splits responsibilities as follows:
- **Stage 1 (Preprocessing)**: Parse the source paper/material and output structured `clean_text` for downstream steps.
- **Stage 2 (Process generation)**: Codex drives the LLM in **two passes**. First, enumerate the complete process list (every table row / unit operation) with stable `processId`s. Second, loop over that list and generate each `processDataSet` individually, ensuring every `exchange` carries a complete `flowHints` object. Stage 2 is the single source of truth for exchange metadata; downstream stages will not auto-fill or repair missing hints.
- **Stage 3 (Flow alignment & artifact export)**: Combine MCP search results and operator context to confirm the standard flow for each exchange, then merge the results, generate ILCD artifacts under `artifacts/<run_id>/exports/`, run local validation, and assemble `workflow_result.json` for downstream tasks.

Every run stores its artifacts under `artifacts/<run_id>/`, where `run_id` is a UTC timestamp (Stage 1 prints the value and records it in `artifacts/.latest_run_id`). The directory layout is:
- `cache/`: Stage outputs (`stage1_clean_text.md`, `stage2_process_blocks.json`, alignment summaries, validation reports) and on-disk LLM caches.
- `exports/`: Contains only `processes/`, `flows/`, and `sources/`; TIDAS validation reads from this directory.
Subsequent stages fall back to the most recent `run_id` when `--run-id` is omitted, so you can run Stage 2 and Stage 3 immediately after Stage 1 without retyping the identifier.
## 0. Execution Ground Rules (Avoid Wasted Iterations)
- **Read the source material first**: Before you start, quickly review the original paper or `clean_text` to confirm chapter layout, data tables, and the functional unit.
- **Run the standard commands directly**: By default, follow the Stage 1-3 CLI templates listed below (input/output paths follow repository conventions) without repeatedly calling `--help`. Only open the help text when you truly need custom parameters.
- **Always run the staged scripts**: Unless the user explicitly instructs otherwise, invoke `stage1` → `stage3`. Do not handcraft large JSON payloads or skip stages to fabricate intermediate files. If credentials (OpenAI, MCP, TIDAS) are missing, notify the user immediately and wait for guidance. Stage 3 now attempts publication automatically after validation succeeds—reusing any path overrides—and writes a live insert (no dry run). The auto publish is skipped when `artifacts/<run_id>/cache/published.json` already exists, unless you pass `--force-publish` or delete the flag. Run `stage4_publish.py` manually only when you need to re-publish or inspect the payload; committed runs append the same `published.json` summary for traceability.
- **Assume credentials are provisioned**: In standard environments `.secrets/secrets.toml` is preconfigured by operations, so Codex can start from any stage. Only revisit credential settings when scripts actually raise missing-credential errors or connection failures.
- **Validate inputs before calling the LLM/MCP**: Check whether `clean_text` is non-empty and contains tables and units as expected; prompt the user to supplement the data when necessary.
- **Final JSON requirements**: The delivered `workflow_result.json` must be generated from data that has already passed the Stage 3 artifact validation step. Remove debugging fields, empty structures, and temporary notes so every process dataset strictly follows the schema and is “clean” enough for direct ingestion.
- **Smoke test MCP once**: Write a quick five-line Python snippet (import `FlowSearchService` and construct a `FlowQuery`) to test a single exchange, confirm credentials and network connectivity, and only then launch Stage 3. This avoids learning about configuration errors after long timeouts.
- **Control the number of exchanges**: Stage 3 issues MCP requests sequentially (`flow_search_max_parallel=1`); each `exchange` triggers an independent call. Stage 2 must reproduce the literature table row by row—every original line becomes its own `exchange` (no merging, averaging, or omission). Capture scenario notes and footnotes in `generalComment` so Stage 3 can align each row to its original context.
- **Enrich the search hints**: For every `exchange`, populate a structured `flowHints` object (serialized inside `generalComment`) with descriptive values—synonyms (semantic equivalents such as “electric power supply”), aliases or abbreviations (“COG”, “DAC”), chemical formulas/CAS numbers, and Chinese-English parameter pairs. Doing so lets FlowSearchService exploit multilingual synonym expansion and richer context to improve recall. High-frequency utilities (`Electricity, medium voltage`, `Water, process`, `Steam, low pressure`, `Oxygen`, `Hydrogen`, `Natural gas, processed`, etc.) must list at least two or three bilingual aliases or usage scenarios (e.g., “grid electricity 10–30 kV”, “中压电”, “technological water”, “饱和蒸汽 0.4 MPa”) plus state/purity/source cues. `flowHints` / `generalComment` must begin with `FlowSearch hints:` and follow this ordered template (all fields except `zh_synonyms` must be written in English first; if you need to add a Chinese gloss, append it in parentheses after the English phrase rather than interleaving tokens). **Flatten every list into a semicolon-delimited string when writing `generalComment`; never emit Python-style list literals such as `['Electricity', ...]`.**
  - `basename=<...>` – English-only technical name mirroring authoritative datasets. State the substance/product/waste exactly as industry or customers describe it, separate descriptors with commas (never semicolons), avoid geography/quantities, spell out acronyms on first mention, and note states/grades (“gaseous”, “granulate”, “recycled”) when needed for identification (e.g., “Polypropylene, PP, granulate”; “Sulfur dioxide, gaseous”; “Waste glass cullet, mixed colors”).
  - `treatment=<...>` – comma-separated qualitative qualifiers written in English and ordered from intrinsic modifications to contextual uses: surface/material treatments (“Hot rolled”, “Sterilised”), referenced standards or grades (“EN 10025 S355”, “ASTM D4806”), key performance attributes (“UV-resistant”, “food-grade”), intended uses (“for wafer production”, “medical packaging”), and production or recycling routes (“primary production route”, “secondary feedstock, steam cracking route”). Each qualifier should be concise, technical, and free of marketing adjectives.
  - `mix_location=<...>` – comma-separated mix and delivery descriptors written in English (“Production mix, at plant”; “Consumption mix, to consumer”; “Technology-specific, to wholesale”; “Production mix, to waste incineration plant”). Use “at” for handover nodes and “to” when transport burdens up to that node are included; provide a descriptor whenever either mix type or delivery point is known rather than leaving the field blank.
  - `flow_properties=<...>` – comma-separated quantitative properties with explicit measurement bases (“45 % Fe mass/mass”, “9.6 MJ/kg net calorific value”, “90.5 % methane by volume”, “750 g/L total dissolved solids”). Use SI or widely accepted industrial units, specify the basis when it deviates from mass fraction (by volume, molar, dry basis, etc.), avoid redundancy with other fields, and order entries by relevance.
  - `en_synonyms=<...>` – semicolon-separated English alternative names/trade names/abbreviations (“Electric power supply; Grid electricity; Utility electricity”). Prevent duplicates with `basename` and never wrap the list in brackets or quotes.
  - `zh_synonyms=<...>` – semicolon-separated Chinese equivalents（如“电力；电网供电”），与 `en_synonyms` 含义对应且不重复 `basename`，同样禁止输出 `['…']` 这类列表字面量。
  - `abbreviation=<...>` – canonical abbreviations or short codes (“MV electricity”, “NCM622-SiGr”); include only validated labels.
  - `state_purity=<...>` – physical state, purity or grade, and key operating conditions (“AC 10–30 kV, 50 Hz”, “Liquid, battery grade, 31–37 wt% HCl”).
  - `source_or_pathway=<...>` – English description of supply routes, origin, or geography (“Regional grid, CN; Secondary aluminium route; Steam reforming feed”). When Chinese context is helpful, append it in parentheses after the English phrase (e.g., “Regional grid, CN（中国电网）”) rather than interleaving tokens.
  - `usage_context=<...>` – scenario references (table IDs, process step, functional role) such as “Input to cathode coating line, Table 3”.
  - `formula_or_CAS=<...>` *(optional)* – molecular formulas, CAS numbers。If no identifier exists and the literature provides no defensible description, omit this field entirely rather than inserting a placeholder.

Populate every required slot with substantive bilingual descriptors—never write placeholders such as “NA”/“N/A”, and never move critical content into the free-text notes while leaving the structured field empty. When the literature omits a detail, infer it from context (e.g., typical purity, supply pathway) or write the best supported industry description before appending table references or calculation notes. Without these clues, MCP often returns low-similarity candidates, forcing Stage 3 to fall back to placeholders.

**Process metadata checklist (required in Stage 2 output):**

- `processInformation.dataSetInformation.common:generalComment` – 2–4 full sentences on scope, boundary, sources, and assumptions. **All narrative descriptions (methodology, caveats, citation notes) must live here or in other `common:generalComment` multi-language fields. Do not stuff prose into structural slots such as `functionalUnitFlowProperties`, `referenceTo*`, or administrative reference blocks.**
- `processInformation.dataSetInformation.identifierOfSubDataSet` – stable ID for the dataset (e.g., “P001”, “Production stage”).
- `processInformation.dataSetInformation.name.functionalUnitFlowProperties` – describe the functional-unit qualifiers (per-unit basis, dry mass, etc.).
- `processInformation.quantitativeReference.referenceToReferenceFlow` plus `functionalUnitOrOther` text matching the reference amount/unit.
- `processInformation.time` – fill `referenceYear` (and, when available, `dataSetValidUntil` / `timeRepresentativeness`).
- `processInformation.geography.locationOfOperationSupplyOrProduction` – include `@location` ISO code and optional comment.
- `modellingAndValidation.LCIMethodAndAllocation` – specify `typeOfDataSet`, `LCIMethodPrinciple`, allocation approach, and related notes.
- `modellingAndValidation.validation.review` – always provide a review block (use “Not reviewed” when appropriate).
- `modellingAndValidation.complianceDeclarations` – populate the ILCD compliance entries (nomenclature, methodological, documentation, quality).
- `modellingAndValidation.dataSourcesTreatmentAndRepresentativeness` – provide narrative text for `dataCutOffAndCompletenessPrinciples`, `dataSelectionAndCombinationPrinciples`, `dataTreatmentAndExtrapolationsPrinciples`, plus `referenceToDataSource`. Only emit `referenceToDataHandlingPrinciples` when you have a verifiable document/contact/UUID to cite; otherwise omit the field entirely.
- `administrativeInformation.common:commissionerAndGoal.common:intendedApplications` – explain the intended analytical use.
- `administrativeInformation.dataEntryBy` / `publicationAndOwnership` – timestamps, format references, ownership, dataset version, and (if known) preceding-version references.
- **Do not fabricate optional external references**: fields such as `common:referenceToPrecedingDataSetVersion`, `common:referenceToUnchangedRepublication`, `common:referenceToRegistrationAuthority`, `common:referenceToEntitiesWithExclusiveAccess`, `referenceToDataHandlingPrinciples`, `referenceToLCAMethodDetails`, and `referenceToSupportedImpactAssessmentMethods` are optional and must only be emitted when the source provides a concrete contact/data-set/document ID. If no trustworthy reference exists, *omit the field completely* (never drop in prose or “N/A”). Example of a valid reference:
  ```json
  {
    "modellingAndValidation": {
      "dataSourcesTreatmentAndRepresentativeness": {
        "referenceToDataHandlingPrinciples": {
          "referenceToDocument": {
            "common:UUID": "c1b2d3e4-f567-4890-9123-abcdef456789"
          }
        }
      }
    }
  }
  ```
  If that UUID or contact is unavailable, remove the entire `referenceToDataHandlingPrinciples` block rather than inserting free text.
- **Self-check Stage 2 outputs**: Before kicking off Stage 3, sample `artifacts/<run_id>/cache/stage2_process_blocks.json` to confirm that every `exchange.generalComment` includes the `FlowSearch hints` structure, critical synonyms, and bilingual parameters. If you still see brief “Table X” descriptions, redo Stage 2 or manually enrich the context; otherwise Stage 3 will miss frequently because it lacks semantic signals and you will end up with numerous `unmatched:placeholder` entries.
- **Normalize flow names**: Prefer Tiangong/ILCD canonical flow names rather than keeping the paper’s parentheses or process-specific qualifiers (for example, avoid `Electricity for electrolysis (PV)`). Standard names dramatically raise Stage 3 hit rates and reduce duplicate searches and timeouts.
- **Tune long-running commands in advance**: Stage 2/3 may exceed 15 minutes. In constrained environments, increase outer CLI timeouts or bump the `.secrets` `timeout` field to prevent forced termination and reruns.
- **Cap retry attempts**: Do not retry the same LLM/MCP call more than twice. Explain the reason for every prompt or context adjustment; if the issue persists, switch to manual analysis and escalate to the user.
- **Record key assumptions**: Document any inference (unit completion, default geography, classification path) in `generalComment` so downstream reviewers do not have to confirm them repeatedly.
- **Stage 2 output self-check**: Before kicking off Stage 3, sample `artifacts/<run_id>/cache/stage2_process_blocks.json` to ensure every `exchange` carries a non-placeholder `exchangeName` plus a fully filled `flowHints` object with the ordered fields above. Any missing field or value such as `GLO`, `CN`, `NA`, `LN2`, etc., must be corrected at Stage 2—do not rely on Stage 3 to fix it.

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
| 3 | `stage3_align_flows.py` | `artifacts/<run_id>/cache/stage3_alignment.json`, `.../cache/process_datasets.json`, `.../cache/tidas_validation.json`, `.../cache/workflow_result.json`, plus ILCD JSON archives under `artifacts/<run_id>/exports/processes|flows|sources/` | Invoke `FlowAlignmentService`, enforce hint quality, merge aligned results, materialise ILCD artifacts, run `tidas_tools.validate`, and on success trigger publication to persist flows, processes, and sources (skips when `cache/published.json` already exists unless `--force-publish`; live commit, no dry run). |
| 4 (optional) | `stage4_publish.py` | `artifacts/<run_id>/cache/stage4_publish_preview.json` | Read Stage 3 outputs and build the `Database_CRUD_Tool` payload; after successful Stage 3 validation it runs automatically (subject to the published flag) to commit flows, processes, and sources. Invoke it manually only when you need to re-publish or debug. |

Recommended execution sequence (from the repository root):
```bash
RUN_ID=$(date -u +"%Y%m%dT%H%M%SZ")  # optional helper; Stage 1 prints the generated run_id
uv run python scripts/stage1_preprocess.py --paper path/to/paper.json --run-id "$RUN_ID"
uv run python scripts/stage2_extract_processes.py --run-id "$RUN_ID"
uv run python scripts/stage3_align_flows.py --run-id "$RUN_ID"
uv run python scripts/stage4_publish.py --run-id "$RUN_ID" \
  --publish-flows --publish-processes \
  --update-alignment --update-datasets
```
Stage 3 invokes the Stage 4 publisher automatically after validation to commit data with no dry run. If `artifacts/<run_id>/cache/published.json` already exists, the auto publish is skipped—you can remove the flag or rerun Stage 3 with `--force-publish` when a re-publication is intentional.

- If `stage3_align_flows.py` detects OpenAI credentials in `.secrets/secrets.toml`, it automatically enables LLM scoring to evaluate the ten MCP candidates; otherwise it falls back to local similarity matching. Before alignment, the script verifies every exchange has both `exchangeName` and a fully populated `FlowSearch hints` string (see §0). Missing or placeholder fields now halt the run; Stage 3 no longer attempts to infer names or hints from synonyms. The resulting `artifacts/<run_id>/cache/stage3_alignment.json` always includes `process_id`, `matched_flows`, `unmatched_flows`, and `origin_exchanges`, and the CLI prints match statistics for each process.
- Stage 3 reuses the existing ILCD format source (`a97a0155-0234-4b87-b4ce-a45da52f2a40`) and the shared ILCD entry-level compliance UUID (`d92a1a12-2545-49e2-a585-55c259997756`). The exported process and flow files keep these reference blocks, but no local `exports/sources/*.json` stubs are generated for those shared datasets.
- Relative `@uri` pointers inside ILCD files now follow the pattern `../processes/{uuid}_{version}.xml`, `../flows/{uuid}_{version}.xml`, and `../sources/{uuid}_{version}.xml` so downstream loaders can resolve a specific dataset revision.

## 3. Core Data Structures
```python
from dataclasses import dataclass, field
from typing import Any, Mapping, Literal

@dataclass(slots=True, frozen=True)
class FlowQuery:
    exchange_name: str
    description: str | None = None

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
- `FlowSearchClient` uses `MCPToolClient.invoke_json_tool` to access the remote `Search_flows_Tool` and automatically builds the search context from a `FlowQuery` (now limited to `exchange_name` and `FlowSearch hints`).
- `FlowQuery.description` comes directly from the Stage 2 `exchange.generalComment` `FlowSearch hints` string; keep the field order and separators intact so QueryFlow Service can extract multilingual synonyms and physical properties.
- The remote `tiangong_lca_remote` tool already embeds an LLM, which expands synonyms from the full `generalComment` and performs hybrid full-text + semantic search, so Stage 3 does not need to craft extra prompts manually.
- When `generalComment` is complete and concise, you can trust the candidates returned by `tiangong_lca_remote`; focus on selecting the best-fitting flow and writing any necessary notes.
- `stage3_align_flows.py` is the only entry point: do not stitch `referenceToFlowDataSet` during Stage 2. Let Stage 3 read the Stage 2 `process_blocks` and trigger the lookup.
- Before running Stage 3, sanity-check one or two exchanges by building a `FlowQuery` manually to confirm the service returns candidates (avoid an empty batch run). If no candidates are returned, inspect the `FlowSearch hints` because Stage 3 now relies solely on `exchange_name` plus that string; process-level clean text is no longer used.
- Issue at least one MCP lookup per exchange; only mark it as `UnmatchedFlow` and record the reason after up to three failures.
- Use exponential backoff for retries; catch `httpx.HTTPStatusError` and `McpError`, and strip context when necessary to avoid 413/5xx errors.
- `FlowSearchService` handles similarity filtering, cache hits, and `UnmatchedFlow` assembly. After Stage 3, review the logs to confirm hit rates and list exchanges that still did not match.
- If the logs show many `flow_search.filtered_out` events without matches, start by checking (i) whether `exchangeName`/`unit` is missing or misspelled, and (ii) whether the `FlowSearch hints` string contains sufficient synonyms/usage context. (Stage 3 no longer injects clean-text context into flow search.)
- `mcp_tool_client.close_failed` warnings typically occur when cleanup coroutines run after a request and are considered normal; if timeouts persist, lower `flow_search_max_parallel` or split Stage 3 into batches.

## 5. Flow Alignment
- Each process block submits exchange lookups on independent threads, aggregating `matched_flows` and `origin_exchanges`; unmatched items are only tallied in logs as reminders.
- The Stage 3 script validates Stage 2 outputs against the §0 `FlowSearch hints` spec and will **fail fast** if an exchange lacks `exchangeName` or any hint fields—no inference or auto-fill is performed at this stage.
- Successful matches write back `referenceToFlowDataSet`; failures keep the original exchange and record the reason.
- When a flow lookup succeeds, Stage 3 must populate `referenceToFlowDataSet.common:shortDescription` with the concatenated string `baseName; treatmentStandardsRoutes; mixAndLocationTypes; flowProperties` (substitute `-` for missing segments) so the process exchange retains the matched flow’s name, treatment, location, and quantity in a single field.
- The script emits structured logs such as `flow_alignment.start` and `flow_alignment.exchange_failed` for diagnostics.

## 6. Process Extraction
- Execute `extract_sections` → `classify_process` → `normalize_location` → `finalize` in order.
- Processing notes:
  - `extract_sections` splits content by parent section or aliases; if nothing matches, fall back to the full document.
  - If the LLM does not return `processDataSets`/`processDataSet`, raise `ProcessExtractionError`.
  - Normalize table fields to alignment-friendly base units (e.g., convert t→kg, keep Nm³ as cubic meters, and describe density-based assumptions for volume) and document the conversion logic in `generalComment`.
  - `finalize` calls `build_tidas_process_dataset` to populate required ILCD/TIDAS fields, producing process blocks that contain only `processDataSet` (with meta fields such as `process_id`); Stage 2 no longer returns the legacy `exchange_list` cache.
  - Do **not** emit ILCD root metadata (`@xmlns`, `@xmlns:common`, `@xmlns:xsi`, `@xsi:schemaLocation`, `@version`, `@locations`, etc.) in Stage 2 outputs; the Stage 3/TIDAS normaliser injects the canonical values and will overwrite any upstream attempts.
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
- For integration checks, run `stage1` → `stage3` on a minimal paper sample, then verify schema compliance, match statistics, and validation reports; run `stage4_publish` manually only if you need to replay or inspect the publication payload now that the workflow commits automatically after validation.
- Monitoring: enable `configure_logging` JSON output and filter for events such as `flow_alignment.exchange_failed` and `process_extraction.parents_uncovered` to quickly pinpoint failing stages.

## 10. Classification and Location Reference Resources
- `tidas_processes_category.json` (`src/tidas/schemas/tidas_processes_category.json`) is the authoritative source for process classifications, covering every level of the ISIC tree. When Codex needs the classification path, use `uv run python scripts/list_process_category_children.py <code>` to expand level by level (`<code>` empty returns the top level, e.g., `uv run python scripts/list_process_category_children.py 01`). You can also load specific branches with `tiangong_lca_spec.tidas.get_schema_repository().resolve_with_references("tidas_processes_category.json")` and paste the relevant snippets into Codex prompts so it can choose the correct `@classId`/`#text` within limited context.
- Stage 2 now queries the LLM one level at a time: each prompt (`You are selecting level {level}...`) lists the candidate `code`/`description` pairs under the current parent, and the model must return exactly one object `{"@level": "...", "@classId": "...", "#text": "..."}`. When providing manual assistance, follow the same approach—only share the candidates for the current layer, confirm the choice, then move to the next layer.
- Geographic codes follow `tidas_locations_category.json` (`src/tidas/schemas/tidas_locations_category.json`). Use `uv run python scripts/list_location_children.py <code>` (e.g., `uv run python scripts/list_location_children.py CN` to see China's hierarchy). When briefing Codex, share only the subtree relevant to the current process to avoid sending the entire taxonomy.
- When a process involves flow classifications, call `uv run python scripts/list_product_flow_category_children.py <code>` for product flows (data source `tidas_flows_product_category.json`) or `uv run python scripts/list_elementary_flow_category_children.py <code>` for elementary flows (data source `tidas_flows_elementary_category.json`).

## 11. Stage 4 Publish & Database CRUD
- `stage4_publish.py` calls the `Database_CRUD_Tool` from `tiangong_lca_remote` to persist `flows`, `processes`, and `sources`. After Stage 3 validation succeeds, this publisher runs automatically (unless a prior `cache/published.json` flag suppresses it) to commit the datasets. When rerunning it manually, the `insert` payload must set the tool-level `id` to the dataset UUID already present in the export: use `flowInformation.dataSetInformation.common:UUID`, `processInformation.dataSetInformation.common:UUID`, or `sourceInformation.dataSetInformation.common:UUID` directly. Do not generate alternate identifiers and do not reuse previous run IDs.
- Committed runs write `artifacts/<run_id>/cache/published.json`, capturing the timestamp and counts of prepared/committed datasets. Remove that file or pass `--force-publish` to Stage 3 if you explicitly need to re-publish the same run.
- `Database_CRUD_Tool` payload fields:
  - `operation`: `"select"`, `"insert"`, `"update"`, or `"delete"`.
  - `table`: `"flows"`, `"processes"`, `"sources"`, `"contacts"`, or `"lifecyclemodels"`.
  - `jsonOrdered`: required for insert/update; pass the canonical ILCD document (e.g., `{"processDataSet": {...}}`) with namespace declarations, timestamps, and reference stubs intact.
  - `id`: required for insert/update/delete; match the UUID from the dataset’s `dataSetInformation`.
  - `version`: required for update/delete and stored alongside `json_ordered`.
  - Optional `filters` and `limit` cover equality queries during select operations.
- During live runs the publisher automatically retries with an `update` if an `insert` collides with an existing UUID; keep `administrativeInformation.publicationAndOwnership.common:dataSetVersion` aligned with the record you intend to overwrite.
- Flow properties are now resolved through `src/tiangong_lca_spec/tidas/flow_property_registry.py`. Use `uv run python scripts/flow_property_cli.py list` to inspect available mappings, `emit-block` to print an ILCD-compatible `flowProperties` fragment, and `match-unit --unit <name>` when you only know the unit. The CLI reads `flowproperty_unitgroup_mapping.json`, so updates propagate automatically to the scripted stages.
- Stage 3 and Stage 4 enforce the following publication rules:
  - When alignment finds a matching catalogue flow **without** a declared flow property, Stage 4 rebuilds the dataset with the correct property block, increments `common:dataSetVersion` (patch component `+1`), and commits it via `update` while preserving the original UUID.
  - When no catalogue flow matches, Stage 4 continues to insert a new dataset (UUID generated at publish time) using the registry to attach the correct flow property and unit group.
  - Elementary flows remain lookup-only—you must supply an existing dataset manually before Stage 4 runs.
- Stage 4 exposes two flags to control the registry:
  - `--default-flow-property <uuid>` overrides the fallback property used when neither the hints nor overrides resolve a specific UUID (defaults to Mass `93a60a56-a3c8-11da-a746-0800200b9a66`).
  - `--flow-property-overrides overrides.json` loads a list of objects (`{"exchange": "Copper content", "flow_property_uuid": "<uuid>", "process": "Battery pack …", "mean_value": "1.0"}`) that force specific process/exchange pairs—or all processes when `process` is omitted—to adopt the given property and optional mean value before publication.
- Successful responses echo the record `id`, `version`, and a `data` array. Validation failures raise `SpecCodingError`; log the payload path from the error and fix the dataset before retrying.
- Minimum insert checklist:
  - Preserve ILCD root attributes (`@xmlns`, `@xmlns:common`, `@xmlns:xsi`, schema location) and include `administrativeInformation.dataEntryBy.common:timeStamp`, `common:referenceToDataSetFormat`, and `common:referenceToPersonOrEntityEnteringTheData`.
  - Keep compliance references (e.g., ILCD format UUID `a97a0155-0234-4b87-b4ce-a45da52f2a40`, ownership UUID `f4b4c314-8c4c-4c83-968f-5b3c7724f6a8`, data entry person UUID `f4b4c314-8c4c-4c83-968f-5b3c7724f6a8`) and declare `modellingAndValidation.LCIMethod.typeOfDataSet`.
  - Flows additionally require `flowProperties.flowProperty` (mass property UUID `93a60a56-a3c8-11da-a746-0800200b9a66`), `quantitativeReference.referenceToReferenceFlowProperty`, and `classificationInformation`/`elementaryFlowCategorization` as applicable.
  - Processes must retain the Stage 3 functional unit, exchange list, and `modellingAndValidation` blocks. Sources must keep bibliographic metadata and publication timestamps.
- For batch publication, queue inserts per dataset type (`flows` → `processes` → `sources`) so references resolve immediately, and record each returned `id`/`version` pair for audit.
