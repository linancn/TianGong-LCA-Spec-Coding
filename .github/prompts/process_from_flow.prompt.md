# Process From Flow Workflow Guide

This document describes the LangGraph workflow in `src/tiangong_lca_spec/process_from_flow/service.py`, focusing on step-by-step output structure and constraints.

## Goals and Inputs
- Goal: Derive an ILCD process dataset from a reference flow dataset (ILCD JSON). Flow uuid/shortDescription in exchanges must come from `search_flows` results; use placeholders only when no match is found.
- Entry point: `ProcessFromFlowService.run(flow_path, operation="produce", initial_state=None, stop_after=None)`.
- Dependencies: LLM is required for technology routes, unit process split, exchange generation, and candidate selection; also relies on flow search function `search_flows` (injectable) and a candidate selector (LLM selector recommended).
- `stop_after` supports `"references"|"tech"|"processes"|"exchanges"|"matches"` for early termination in debugging.

## State Fields
The workflow passes a state dict with key fields:
- `flow_path`: input file path.
- `flow_dataset` / `flow_summary`: parsed flow and summary (names, classification, comments, UUID, version).
- `technical_description` / `assumptions` / `scope`: technology route and constraints (from the selected route summary and assumptions).
- `technology_routes`: Step 1 output routes (route_id/route_name/route_summary/key inputs/outputs, etc.).
- `process_routes` / `selected_route_id`: Step 2 route split and chosen route.
- `processes`: unit process plan (ordered list with `reference_flow_name`, `name_parts`, structured fields, exchange keywords).
- `process_exchanges`: per-process exchange list (structure only, no matching info).
- `matched_process_exchanges`: exchanges with flow search results and selected candidates (uuid/shortDescription filled).
- `process_datasets`: final ILCD process datasets.
- `step_markers`: stage flags (step1/step2/step3) for inspection.

## Node Order and Behavior
Each node checks if its target fields already exist to avoid rework.
- 0) load_flow: read `flow_path` JSON and build `flow_summary` (multi-language names, classification, general comment); this flow is the reference flow.
- 1a) reference_search: search technology-route literature (topK=10), write to `scientific_references.step_1a_reference_search`.
- 1b) reference_fulltext: dedupe DOIs from Step 1a and fetch full text via DOI filter, write to `scientific_references.step_1b_reference_fulltext` (`filter: {"doi": [...]}` + `topK=1` + `extK`).
- 1b-optional) reference_usability: optional screening step to determine whether Step 1b full text is sufficient to support process split and exchange generation; output to `scientific_references.usability`.
- 1c) reference_clusters: cluster DOIs by boundary, main chain, and key intermediate flows using Step 1b full text and usability, write to `scientific_references.step_1c_reference_clusters`.
- If any of Step 1a/1b/1c lacks usable references (including usability results all marked unusable), Steps 1-3 fall back to common sense: do not use literature evidence, and Steps 2/3 do not issue retrievals.
- 1) Describe technology (Step 1): output plausible technology/process routes (route1/route2...), each with route_summary, key inputs/outputs, key unit processes, assumptions, and scope; if references exist, prefer the Step 1c primary cluster.
- 2) Split into unit processes (Step 2): output ordered unit processes per route; the reference flow of process i must appear as an input of process i+1, and the last process produces/treats `load_flow`. Each process outputs:
  - Structured fields: `technology` / `inputs` / `outputs` / `boundary` / `assumptions`.
  - `inputs`/`outputs` labeled `f1:`/`f2:` per flow (chain intermediates must match).
  - Exchange keywords: `exchange_keywords.inputs` / `exchange_keywords.outputs`.
  - Name parts: `name_parts` with `base_name` / `treatment_and_route` / `mix_and_location` / `quantitative_reference`.
  - Quantitative reference: numeric expression like `1 kg of <reference_flow_name>` or `1 unit of <reference_flow_name>`.
  - Explicit main output: `reference_flow_name` for the process, consistent with chain inputs.
- 3) generate_exchanges: use `EXCHANGES_PROMPT` to generate exchanges per process (each must mark `is_reference_flow` matching `reference_flow_name`; production uses Output, treatment uses Input). Exchange names must be searchable, no composite flows; add unit and amount (placeholder if unknown); evidence selection follows the Step 1c primary cluster.
  - Emission exchanges add media suffix (`to air` / `to water` / `to soil`) to reduce ambiguity.
  - Exchanges include `flow_type` (product/elementary/waste/service) and `search_hints` aliases.
- 4) match_flows: search flows for each exchange (keep top 10 candidates), select with LLM selector (no similarity fallback); record reasoning and unmatched items; exchange uuid/shortDescription must come from selected candidates.
- 5) build_process_datasets: assemble ILCD process datasets (reference direction depends on operation; if Translator provided, add Chinese fields):
  - Use `ProcessClassifier`; fall back to Manufacturing on failure.
  - Use matched flows; missing matches use placeholders (no invented uuid/shortDescription).
  - Ensure a reference exchange; empty amounts fall back to `"1.0"`.
  - Fill functional unit, time/region, compliance, data entry, copyright; validate with `tidas_sdk.create_process` (log warning on failure).

## Outputs and Debugging
- Normal runs return full state; `process_datasets` is the final output list.
- CLI writes only under `artifacts/process_from_flow/<run_id>/` with `input/`, `cache/`, and `exports/`; state file is `cache/process_from_flow_state.json`.
- Use `stop_after` for debugging (e.g., `"matches"` to stop after flow matching).

## Literature Service Configuration and Operation

### Retrieval Strategy
- Build queries from flow name, operation (produce/treat), and technical description.
- Step 1b uses `filter: {"doi": [...]}` + `topK=1` + `extK` (default `extK=200`) to fetch full text; `query` must be non-empty and can use merged content or a short summary.
- Step 1c outputs `clusters` + `primary_cluster_id` + `selection_guidance` for evidence selection and merging in Step 2/Step 3.

**Resource management:**
- MCP client auto-created when LLM is available.
- Connection closes at workflow end.
- Retrieval failures log warnings and do not block execution.

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

If not configured or API key is invalid, the workflow falls back to common sense without literature.

### Logs

- `process_from_flow.mcp_client_created`: MCP client created
- `process_from_flow.search_references`: literature search succeeded (query + count)
- `process_from_flow.search_references_failed`: literature search failed (error, non-blocking)
- `process_from_flow.mcp_client_closed`: MCP client closed

### Performance

- Each literature search takes ~1-2 seconds
- Step 1b fulltext retrieval time depends on DOI count and extK
- Workflow adds ~3-6 seconds (excluding extra fulltext fetch time)
- Reliability unaffected

### Testing

```bash
uv run python test/test_scientific_references.py
```

### Reference Usability Screening

- Optional step: evaluate whether Step 1b fulltext is sufficient to support Step 1c process split/exchange generation.
- Prompt template: `src/tiangong_lca_spec/process_from_flow/prompts.py` `REFERENCE_USABILITY_PROMPT`.
- Script: `uv run python scripts/origin/process_from_flow_reference_usability.py --run-id <run_id>`.
- Output: `scientific_references.usability` in `process_from_flow_state.json`.

## Usage Notes
- Ensure LLM is configured; do not run without it.
- Configure `tiangong_kb_remote` to enable literature integration (optional but recommended).
- Keep flow search/selector interface consistent (`FlowQuery` -> `(candidates, unmatched)`; candidates include uuid/base_name, etc.).
- CLI adds Chinese translations by default (disable with `--no-translate-zh`).
