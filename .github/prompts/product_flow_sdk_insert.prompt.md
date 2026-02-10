# Product Flow Insert (tidas_sdk + Database_CRUD_Tool, LLM-constrained treatment/mix)

End-to-end checklist and a constrained LLM prompt to build/insert a product flow via `tidas_sdk`:

- Run all Python with `uv run python ...`.
- Classification: use SDK product flow category schema (`tidas_flows_product_category.json` via navigator)；不再支持传入其他分类文件。
- Input data: `input_data/origin/manual_flows/flow_class_with_desc.json` (class_id, leaf_name, desc, leaf_name_zh); keep `input_data/origin/*` aligned with `scripts/origin/*` tooling.
- Names/comments: bilingual `baseName`; `common:generalComment` has EN and ZH, with `--translate-desc` auto-filling the missing side (EN→ZH if source is English, ZH→EN if source contains Chinese; otherwise reuse the same text); `common:synonyms` omitted; `quantitativeReference.referenceToReferenceFlowProperty` = `"0"`.
- Flow property: Mass UUID `93a60a56-a3c8-11da-a746-0800200b9a66`, version `03.00.003`, meanValue `"1.0"`.
- Governance defaults: compliance `ILCD Data Network - Entry-level`; contact/ownership `Tiangong LCA Data Working Group`; dataset version `01.01.000`.
- Timestamp string format: `YYYY-MM-DDTHH:MM:SSZ` (no offsets, no datetime objects).
- `common:shortDescription` blocks must be lists of language dicts.
- Insert once; if it fails, fix payload then retry (no blind retries).
- Treatment/Mix: LLM-only selection from fixed options; if LLM fails, the entry errors out. Semicolons (full/half width) are replaced with commas.

## Constrained LLM prompt for treatment/mix
Use this prompt inside automation to choose **only** from fixed options. It aligns with ILCD definitions:
- `treatmentStandardsRoutes`: technical qualifiers (treatment received, standard fulfilled, product quality, use info, production route name), comma-separated.
- `mixAndLocationTypes`: production/consumption mix + delivery point (e.g., at plant / at farm gate / at forest roadside / at landing site / to consumer), comma-separated.

**Model**: from `.secrets/secrets.toml` `[openai].model` (fallback `gpt-4o-mini`).

**Options** (English → Chinese mapped in code):
- treatment: `Seed-grade, cleaned for sowing` | `Harvested grain, unprocessed` | `Fresh, unprocessed produce` | `Raw milk, chilled` | `Eggs, shell-on` | `Greasy wool, unscoured` | `Raw honey` | `Unprocessed roundwood` | `Unprocessed catch, landing quality` | `Live animal, unprocessed` | `Finished product, manufactured` | `Unspecified treatment`
- mix: `Production mix, at farm gate` | `Production mix, at forest roadside` | `Production mix, at landing site` | `Production mix, at plant` | `Consumption mix, at plant` | `Production mix, to consumer` | `Consumption mix, to consumer`

**Prompt**:
```
You provide two ILCD fields for a product flow:
- treatmentStandardsRoutes: technical qualifiers (treatment received, standard fulfilled, product quality, use info, production route name), comma-separated.
- mixAndLocationTypes: production/consumption mix and delivery point (e.g., at plant / at farm gate / at forest roadside / at landing site / to consumer), comma-separated.
Select ONLY from the given options; do not invent new text. If the flow is a finished manufactured product, prefer 'Finished product, manufactured' + 'Production mix, at plant'.
If the flow is clearly agricultural/livestock/forestry/fish, pick the matching farm gate / forest roadside / landing site + corresponding treatment. Otherwise keep plant.
Respond strict JSON: {"treatment_en": <option>, "mix_en": <option>} with no extra keys.
class_id: <...>
leaf_name: <...>
description: <... or N/A>
treatment_options: [...]
mix_options: [...]
```

## CLI usage (script already wired to LLM; no rule toggles)
- Dry-run, specific IDs:  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --class-id 23161 44428`
- Override LLM model (else uses .secrets [openai].model or gpt-4o-mini):  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --class-id 23161 --llm-model gpt-4o`
- Insert to remote DB (single MCP session reused):  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --class-id 23161 44428 --commit`

Outputs: `artifacts/cache/manual_flows/{classid}_{uuid}_{version}.json|.xml`, summary at `artifacts/cache/manual_flows/product_flow_sdk_insert_summary.json`, LLM choices logged to `llm_mix_rules.jsonl`.
