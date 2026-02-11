# Product Flow Manual Insert Guide (Manual Adapter)

This document is the runbook for creating/publishing **new product flows** manually or semi-automatically.  
It maps to the unified architecture as `manual_insert_adapter`, not the `process_from_flow` main pipeline.

## Scope
- In scope: manual new-flow creation and controlled update/insert operations.
- Out of scope: `process_from_flow` orchestration, Stage3 alignment flow, JSON-LD extraction pipeline.

## Relation to other files
- This file: rule source (what must be true before publish).
- `scripts/md/bulk_insert_product_flows.py`: current batch execution adapter.
- `scripts/product_flow/product_flow_sdk_insert.py`: classification-driven adapter.
- `src/tiangong_lca_spec/product_flow_creation/service.py`: shared `ProductFlowCreationService` (single builder for ILCD assembly + `tidas_sdk` validation).
- `src/tiangong_lca_spec/product_flow_creation/dedup.py`: shared `FlowDedupService` for pre-publish action decisions (`insert`/`update`/`reuse`), currently used by the `FlowPublisher` publish path.
- `src/tiangong_lca_spec/publishing/crud.py` and `src/tiangong_lca_spec/workflow/artifacts.py`: wired to the same builder for publish/export chains; publish path uses dedup decisions.
- `scripts/origin/process_from_flow_langgraph.py`: process-from-flow mainline (different task).

## Hard constraints
1. Classification path must come from `tidas_flows_product_category.json` (no guessing).
2. `classificationInformation` is mandatory.
3. `common:synonyms` must include both EN and ZH entries; fallback to `baseName` if missing.
4. Default flow property is Mass (`93a60a56-a3c8-11da-a746-0800200b9a66`, `meanValue=1.0`).
5. Current `bulk_insert_product_flows.py --commit` behavior is direct `insert`; if you need `reuse/update`, run `--select-id` first or publish through the `FlowPublisher` path.
6. One publish action per record; fix payload before retrying. No blind retry loops.

## Current execution entry
`scripts/md/bulk_insert_product_flows.py`

- Technical route: convert input into `ProductFlowCreateRequest`, then call `ProductFlowCreationService` for ILCD assembly and `tidas_sdk.create_flow(validate=True)` normalization before CRUD publish (no unvalidated raw JSON direct-post).

- Input:
  - Required: `class_id`, `leaf_name`
  - Recommended: `leaf_name_zh`, `desc`
  - Optional overrides: `base_en`, `base_zh`, `en_synonyms`, `zh_synonyms`, `treatment`, `mix`, `comment`
- Run:
  ```bash
  uv run python scripts/md/bulk_insert_product_flows.py --input <json_or_jsonl>
  uv run python scripts/md/bulk_insert_product_flows.py --input <json_or_jsonl> --commit
  ```
- Query:
  ```bash
  uv run python scripts/md/bulk_insert_product_flows.py --select-id <uuid>
  ```

## Pre-publish checklist
- Classification path is complete and level-ordered.
- `baseName` / `treatment` / `mix` are clear and semicolon-free.
- EN/ZH synonyms are both present and non-empty.
- `common:generalComment` is traceable to evidence.
- Action is explicit (`insert` vs `update`) to avoid duplicate UUID misuse.

## Migration note
This adapter remains a task entry, while core build logic is centralized in `ProductFlowCreationService` and reused by `FlowPublisher`/`artifacts`; dedup/action decisions are currently enforced in `FlowPublisher` and can be adopted by manual adapters in a follow-up step.
