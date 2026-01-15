# Process From Flow Workflow Guide (LangGraph Core + Origin Orchestration)

## Overview
- Goal: Derive ILCD process datasets from a reference flow dataset (ILCD JSON). Exchange flow uuid/shortDescription must come from `search_flows` candidates; use placeholders only when no match exists.
- Scope: This guide covers the LangGraph core flow in `src/tiangong_lca_spec/process_from_flow/service.py` and the orchestration layer under `scripts/origin/`.
- Outputs: `process_datasets` / `source_datasets`, plus artifacts under `artifacts/process_from_flow/<run_id>/`.
- References: `PROCESS_FROM_FLOW_FLOWCHART.zh.md` / `PROCESS_FROM_FLOW_FLOWCHART.zh.svg` for flowcharts.

## Cross References
- CLI entrypoints: `scripts/origin/process_from_flow_langgraph.py` and `scripts/origin/process_from_flow_workflow.py` (see module docstrings and `--help`).

## Architecture and Main Flow
### Layer Responsibilities
- LangGraph core layer: `ProcessFromFlowService` runs the main inference chain (references -> routes -> processes -> exchanges -> matching -> datasets).
- Origin orchestration layer: `scripts/origin` handles SI download/parse, usability tagging, run/resume, publishing, and cleanup.

### Main Flow Outline
- Step 0 load_flow: Parse reference flow and build summary.
- Step 1 references + tech routes: 1a search -> 1b fulltext -> 1c clustering -> technology routes.
- Step 2 split processes: Split unit processes into an ordered chain.
- Step 3 generate exchanges: Create per-process input/output exchanges.
- Step 3b enrich exchange amounts: Extract or estimate amounts/units from text and SI.
- Step 4 match flows: Search flows and select candidates to fill uuid/shortDescription.
- Step 1f build sources: Generate ILCD source datasets and references.
- Step 5 build process datasets: Emit final ILCD process datasets.

## LangGraph Core Workflow (ProcessFromFlowService)
### Entry and Dependencies
- Entry: `ProcessFromFlowService.run(flow_path, operation="produce", initial_state=None, stop_after=None)`.
- Dependencies: LLM (routes/split/exchanges/selection), flow search `search_flows`, candidate selector (LLM selector recommended), optional Translator/MCP client.
- `stop_after`: `references`/`tech`/`processes`/`exchanges`/`matches`/`sources` (CLI also supports `datasets`, see Origin orchestration).

### Node Details (from coarse to fine)
0) load_flow
- Read `flow_path`, build `flow_dataset` and `flow_summary` (multi-lang names, classification, comments, UUID, version).

1) references + tech routes
- 1a reference_search: search technical route literature -> `scientific_references.step_1a_reference_search` (default topK=10).
- 1b reference_fulltext: dedupe DOIs and fetch fulltext (`filter: {"doi": [...]}` + `topK=1` + `extK`) -> `scientific_references.step_1b_reference_fulltext`.
- 1c reference_clusters: cluster by system boundary/main chain/intermediate flows -> `scientific_references.step_1c_reference_clusters`.
- Step 1 route output: produce `technology_routes` with route_summary, key inputs/outputs, key unit processes, assumptions/scope, and attach `supported_dois` + `route_evidence`.
- If Step 1a/1b/1c has no usable references, Steps 1-3 fall back to common sense and must mark `expert_judgement` with reasons.

2) split_processes
- Split each route into ordered unit processes; chain intermediates must match and the last process produces/treats `load_flow`.
- Required fields: `technology`/`inputs`/`outputs`/`boundary`/`assumptions` + `exchange_keywords`.
- `name_parts` must include `base_name`/`treatment_and_route`/`mix_and_location`/`quantitative_reference`, where `quantitative_reference` is numeric.
- Provide a geography decision per process (ILCD location code) and document any representativeness limits in `descriptionOfRestrictions` (e.g., non-local input datasets).
- When evidence is aggregated, mark `aggregation_scope`/`allocation_strategy` in `assumptions`.
- If references are usable, extra split evidence can be retrieved and stored in `scientific_references.step2`.

3) generate_exchanges
- Use `EXCHANGES_PROMPT` to generate exchanges; `is_reference_flow` aligns with `reference_flow_name` (Output for production, Input for treatment).
- Exchange names must be searchable and not composite; fill unit/amount (placeholders if unknown).
- Emissions add media suffix (`to air`/`to water`/`to soil`), plus `flow_type` and `search_hints`.
- Assign `material_role` for each exchange (`raw_material|auxiliary|catalyst|energy|emission|product|waste|service|unknown`); set `balance_exclude=true` for auxiliary/catalyst inputs not embodied in the product.
- Every exchange records `data_source`/`evidence`; inferred items must mark `source_type=expert_judgement`.
- If references are usable, extra exchange evidence can be retrieved and stored in `scientific_references.step3`.

3b) enrich_exchange_amounts
- Use `EXCHANGE_VALUE_PROMPT` with fulltext and SI to extract verifiable values, writing `value_citations`/`value_evidence` and filling amount/unit.
- Missing values remain placeholders; if boundary + quantitative reference exist, `INDUSTRY_AVERAGE_PROMPT` may estimate and store `scientific_references.industry_average`.
- Scalable exchanges use `basis_*` for conversion and add scaling notes.

4) match_flows
- Search flows for each exchange (top 10 candidates), then select with LLM selector (no similarity fallback when LLM is enabled).
- Record `flow_search.query/candidates/selected_uuid/selected_reason/selector/unmatched` and fill uuid/shortDescription.
- Only add matching info; do not overwrite `data_source`/`evidence`.

1f) build_sources
- Generate ILCD source datasets from references (`tidas_sdk.create_source`), writing `source_datasets` and `source_references`.
- Infer usage from `usage_tagging`/Step 1c summaries/Step 1b usability/industry_average and filter out `background_only`.

5) build_process_datasets
- Build ILCD process datasets (reference direction follows `operation`; optional Translator adds Chinese fields).
- `ProcessClassifier` falls back to Manufacturing on failure; missing flows use placeholders only.
- Try `DatabaseCrudClient.select_flow` to fill flow version/shortDescription and flowProperty/unit group.
- Ensure reference flow exchange; empty amounts fall back to `"1.0"`; validate via `tidas_sdk.create_process` (warnings only).
- Exchange `referencesToDataSource` prefer `value_citations`/`value_evidence`; remaining evidence is rolled up to process level.

## Origin Orchestration Workflow (scripts/origin)
### Goal and Order
- Goal: Write SI and usage tagging back before Steps 1-3 so prompts can read SI evidence.
- Orchestration order:
  Step 0 -> Step 1a -> Step 1b -> 1b-usability -> Step 1c -> Step 1d -> Step 1e -> Step 1 -> Step 2 -> Step 3 -> Step 3b -> Step 4 -> Step 1f -> Step 5

### Key Scripts and Tools
- `process_from_flow_workflow.py`: main orchestrator, runs 1b-usability/1d/1e before resuming the main flow.
- `process_from_flow_langgraph.py`: LangGraph CLI (run/resume/cleanup/publish), supports `--stop-after` and `--publish/--commit`.
- `process_from_flow_reference_usability.py`: Step 1b usability screening (LCIA vs LCI).
- `process_from_flow_download_si.py`: download SI originals and write SI metadata.
- `mineru_for_process_si.py`: parse PDF/image SI into JSON structure.
- `process_from_flow_reference_usage_tagging.py`: tag reference usage.
- `process_from_flow_build_sources.py`: backfill source datasets from cached state.

### Run Notes
- `process_from_flow_workflow.py` does not support `--no-llm` (Step 1b/1e require LLM).
- `--min-si-hint` controls SI download threshold (none|possible|likely), with `--si-max-links`/`--si-timeout`.
- `process_from_flow_langgraph.py --stop-after datasets` means run through dataset writeout; other values stop early and save state.

## State Fields (state)
- Input/context: `flow_path`, `flow_dataset`, `flow_summary`, `operation`, `scientific_references`.
- Routes/processes: `technology_routes`, `process_routes`, `selected_route_id`, `technical_description`, `assumptions`, `scope`, `processes`.
- Exchanges/matching: `process_exchanges`, `exchange_value_candidates`, `exchange_values_applied`, `matched_process_exchanges`.
- Outputs: `process_datasets`, `source_datasets`, `source_references`.
- Evaluation/markers: `coverage_metrics`, `coverage_history`, `stop_rule_decision`, `step_markers`, `stop_after`.

## SI Injection Points (Actual Behavior)
- Step 1: `TECH_DESCRIPTION_PROMPT` reads `si_snippets`.
- Step 2: `PROCESS_SPLIT_PROMPT` reads `si_snippets`.
- Step 3: `EXCHANGES_PROMPT` reads `si_snippets`.
- Step 3b: `EXCHANGE_VALUE_PROMPT` reads `fulltext_references` + `si_snippets`.
- Step 4/Step 5 do not read SI directly.
- SI must be written back to `process_from_flow_state.json` before Step 1; otherwise rerun Step 1-3.

## Outputs and Debugging
- Output root: `artifacts/process_from_flow/<run_id>/` with `input/`, `cache/`, and `exports/`.
- State file: `cache/process_from_flow_state.json`.
- Resume: `uv run python scripts/origin/process_from_flow_langgraph.py --resume --run-id <run_id>`.
- Backfill sources: `uv run python scripts/origin/process_from_flow_build_sources.py --run-id <run_id>`.
- Publish existing run: `uv run python scripts/origin/process_from_flow_langgraph.py --publish-only --run-id <run_id> [--publish-flows] [--commit]`.
- Cleanup old runs: `uv run python scripts/origin/process_from_flow_langgraph.py --cleanup-only --retain-runs 3`.

## Publishing Flow (Flow/Source/Process)
Recommended order: flows -> sources -> processes to avoid missing references.

### Dependencies and Configuration
- Entrypoints: `FlowPublisher` / `ProcessPublisher` / `DatabaseCrudClient`.
- MCP service: configure `tiangong_lca_remote` in `.secrets/secrets.toml` (`Database_CRUD_Tool`).
- LLM optional: used for flow type and product category inference.

### Step 0: Publish sources (optional but recommended)
- `--publish/--publish-only` publishes sources before processes.
- Only publish sources referenced by process/exchange `referenceToDataSource`.

### Step 1: Prepare alignment structure (for FlowPublisher)
- Structure: `[{ "process_name": "...", "origin_exchanges": { "<exchangeName>": [<exchange dict>, ...] } }]`.
- Each exchange dict must include: `exchangeName`, `exchangeDirection`, `unit`, `meanAmount|resultingAmount|amount`, `generalComment`, `referenceToFlowDataSet`.
- Optionally add `matchingDetail.selectedCandidate` mapped from `flow_search` for better classification/property selection.

### Step 2: Publish/update flows
- `FlowPublisher.prepare_from_alignment()` builds `FlowPublishPlan`:
  - Placeholder `referenceToFlowDataSet` -> insert.
  - Matched but missing flow property -> update (version +1).
  - Elementary flows are not created; Product/Waste flows generate ILCD flow datasets.
- Auto inference:
  - `FlowTypeClassifier`: LLM first, fallback rules.
  - `FlowProductCategorySelector`: pick product category level by level.
  - `FlowPropertyRegistry`: defaults to Mass (override per exchange if needed).
- After publish, use `FlowPublishPlan.exchange_ref` to replace placeholders in process datasets.

### Step 3: Publish processes
- `ProcessPublisher.publish(process_datasets)` defaults to dry-run; `--commit` writes.
- Always `close()` MCP clients after publishing.

## Literature Service Configuration and Operation
### Retrieval Strategy
- Build queries from flow name, operation, and technical description.
- Step 2/Step 3 can add retrievals, stored in `scientific_references.step2/step3`.
- Step 1b uses `filter: {"doi": [...]}` + `topK=1` + `extK` (default `extK=200`).

### Configuration
Configure `tiangong_kb_remote` in `.secrets/secrets.toml`:

```toml
[tiangong_kb_remote]
transport = "streamable_http"
service_name = "TianGong_KB_Remote"
url = "https://mcp.tiangong.earth/mcp"
api_key = "<YOUR_TG_KB_REMOTE_API_KEY>"
timeout = 180
```

If not configured or invalid, the workflow falls back to LLM common sense only.

### Logs
- `process_from_flow.mcp_client_created`: MCP client created
- `process_from_flow.search_references`: literature search succeeded (query + count)
- `process_from_flow.search_references_failed`: literature search failed (non-blocking)
- `process_from_flow.mcp_client_closed`: MCP client closed

### Performance
- Each literature search takes ~1-2 seconds.
- Step 1b fulltext time depends on DOI count and extK.
- Full workflow adds ~3-6 seconds (excluding extra fulltext retrieval).

### Testing
```bash
uv run python test/test_scientific_references.py
```

### Reference Usability Screening
- Optional step: check if Step 1b fulltext supports route/process/exchange needs.
- Mark `unusable` if the text only reports LCIA impact indicators (e.g., ADP/AP/GWP/EP/PED/RI) or impact units like `kg CO2 eq`, `kg SO2 eq`, `kg Sb eq`, `kg PO4 eq`, with no LCI inventory rows (kg, g, t, m2, m3, pcs, kWh, MJ).
- If the paper hints Supporting Information/Appendix for inventory tables, record `si_hint` (`likely|possible|none`) and `si_reason`; still keep `decision=unusable` if the main text has no LCI tables.
- Prompt: `src/tiangong_lca_spec/process_from_flow/prompts.py` `REFERENCE_USABILITY_PROMPT`.
- Script: `uv run python scripts/origin/process_from_flow_reference_usability.py --run-id <run_id>`.
- Output: `scientific_references.usability` in `process_from_flow_state.json`.

## Usage Notes
- Ensure LLM is configured; `process_from_flow_workflow.py` does not allow `--no-llm`.
- Keep flow search/selector interfaces consistent (`FlowQuery` -> `(candidates, unmatched)`).
- CLI adds Chinese translations by default; disable with `--no-translate-zh`.

## Stop Rules
- Stop rules rely on coverage, not retrieval count; thresholds can evolve without changing the node order.
- Coverage definitions:
  - `process_coverage` = processes with evidence / total planned processes.
  - `exchange_value_coverage` = key exchanges with evidence / total key exchanges.
- `stop_rule_decision` records `should_stop/action/reason/coverage_delta`; `coverage_history` stores each evaluation time.
- Default thresholds (adjustable):
  - Stop when `process_coverage >= 0.5` and `exchange_value_coverage >= 0.6`.
  - Stop when coverage delta vs previous evaluation is < 0.1.
- If below thresholds and usability shows `unusable` with `si_hint=none`, switch to `expert_judgement` and log reasons.
- Key exchanges: explicit `is_key_exchange`/`isKeyExchange`, `is_reference_flow`, `flow_type=elementary`, or input-side energy (electricity/diesel/gasoline/heat). If none, treat all exchanges as key exchanges.
