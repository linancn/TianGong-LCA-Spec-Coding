# JSON-LD Extraction Prompt (Stage 1)

You are the Stage 1 LLM for the JSON-LD pipeline. For each OpenLCA JSON-LD file (process, flow, or source, including embedded references) you must generate ILCD-compliant datasets that Stage 2/Stage 3 can consume without any structural fixes. Stage 2 will remap UUIDs, export ILCD files, and run validation; Stage 3 will publish to the database. That means every semantic field, constant reference, classification, and geography value must already be correct when you respond.

## Execution rules

1. **Pipeline**:  
   - Stage 1 → `scripts_jsonld/stage1_jsonld_extract.py` (this prompt)  
   - Stage 2 → `scripts_jsonld/stage2_jsonld_validate.py` (UUID remap + JSON Schema + `uv run tidas-validate`)  
   - Stage 3 → `scripts_jsonld/stage3_jsonld_publish.py` (Database CRUD, dry-run optional)  
   `scripts_jsonld/run_pipeline.py` chains these steps automatically.
2. **Keep original IDs**: Use the JSON-LD `@id` (or blank) for `common:UUID`; Stage 2 will replace them and rewrite references.
3. **Schema fidelity**: Your output must already satisfy `tidas_processes.json`, `tidas_flows.json`, and `tidas_sources.json`. No TODOs, no placeholders, no FlowSearch hints.
4. **Dataset envelope only**: Each element in `datasets[]` may contain exactly one key (`processDataSet`, `flowDataSet`, or `sourceDataSet`). Do not emit sibling keys (for example `":{": "…"`) or helper fragments.
4. **Language handling**: Any ILCD `StringMultiLang` / `FTMultiLang` field must be `{"@xml:lang": "en", "#text": "…"}`. Add extra languages if supplied.
5. **Numbers**: Copy numeric values exactly and render them as strings where ILCD expects strings (`meanAmount`, `meanValue`, etc.). Never fabricate numbers—omit the field or leave an empty string if absent.
6. **Drop unsupported metadata**: If OpenLCA fields have no ILCD equivalent, omit them instead of squeezing them into unrelated sections.
7. **Multiple datasets**: Some JSON-LD files embed flows or sources; emit each ILCD dataset as a separate object inside `datasets`.
8. **Classification & geography**: Determine them in Stage 1 using the Tiangong schemas (see section 4). Stage 2 will not reclassify or reassign locations.
9. **Flow properties & unit groups**: Reuse the JSON-LD flow property references whenever they exist so downstream modules can map them directly. Do not create new flow property/unit group files.
10. **Schema pruning**: Stage 1 enforces `tidas_*` schemas with `additionalProperties=false`. Any ad-hoc keys you add outside the spec will be removed before export, so keep outputs limited to documented fields.

## Stage overview

| Stage | Script | Responsibilities | Outputs |
| --- | --- | --- | --- |
| Stage 1 – JSON-LD extraction | `scripts_jsonld/stage1_jsonld_extract.py` | Iterate `--process-dir`, `--flow-dir`, `--sources-dir`; call this prompt to build ILCD data for processes/flows/sources. | `artifacts/<run>/cache/stage1_process_blocks.json`, `stage1_flow_blocks.json`, `stage1_source_blocks.json` |
| Stage 2 – Export & validate | `scripts_jsonld/stage2_jsonld_validate.py` | Remap UUIDs via `UUIDMapper`, write `exports/processes|flows|sources/`, run JSON Schema (planned) + `uv run tidas-validate`, attempt auto-fixes, emit `workflow_result.json`, `tidas_validation.json`. | `exports/` ILCD JSON + validation artifacts |
| Stage 3 – Publish | `scripts_jsonld/stage3_jsonld_publish.py` | Read `exports/`, perform Database CRUD (flows → processes → sources), support dry-run, write `cache/published.json`. | Database inserts/updates + publish summary |

- Stage 1 (`scripts/jsonld/stage1_jsonld_extract.py`) now ingests `--process-dir`, `--flows-dir`, and `--source-dir` together, runs the same LocationNormalizer/ProcessClassifier stack as the markdown workflow, and finishes by feeding every process through `build_tidas_process_dataset` so cached blocks already satisfy the ILCD/TIDAS schema. If a JSON-LD flow lacks geography we now leave `flowInformation.geography.locationOfSupply` empty instead of defaulting to `GLO`; only resolvable location codes are persisted.
- Stage 2 (`scripts/jsonld/stage2_jsonld_validate.py`) remaps UUIDs and then rebuilds each exchange’s `referenceToFlowDataSet`/`referenceToFlowPropertyDataSet` via `workflow.flow_references`; it aborts early whenever a process references a flow UUID that is missing from the current run—there is no automatic fallback, so Stage 1 must supply matching flows.
- Stage 3 keeps the same “validate → auto publish” contract as the markdown pipeline (`--dry-run-publish` disables only the final CRUD call). The publish order is now **flows → sources → processes**; as each flow/source is inserted, the returned remote ID/version is written back into the staged process exports so that `referenceToFlowDataSet` and `referenceToDataSource` point to real portal records before the processes themselves are uploaded.

## Input / output contract

- **Input**: One JSON-LD payload (process/flow/source). Must keep original `@type`, `@id`, nested references.
- **Output**: JSON only (no prose, no Markdown). Use this envelope:

```json
{
  "datasets": [
    { "processDataSet": { ... } },
    { "flowDataSet": { ... } },
    { "sourceDataSet": { ... } }
  ]
}
```

If the payload only describes one dataset, `datasets` contains a single object. Only include `flowPropertyDataSet` / `unitGroupDataSet` objects if the JSON-LD file truly defines new ones (rare); otherwise reference the standard UUIDs.

## 1. ProcessDataSet requirements

- **Headers**: Include full ILCD root attributes (`@xmlns`, `@xmlns:common`, `@xmlns:ecn`, `@xmlns:xsi`, `@version`, `@locations`, `@xsi:schemaLocation`).
- **Root layout**: `processDataSet` must contain the canonical ILCD sections as direct children (`processInformation`, `modellingAndValidation`, `administrativeInformation`, `exchanges`, optional `LCIAResults`). Never nest these sections under `processInformation` or any other node. When a field is missing in the JSON-LD source, leave it empty/omitted; do **not** invent placeholders such as “Converted from JSON-LD”, “Production mix, at plant”, or generic timestamps.
- **processInformation.dataSetInformation**:
  - `common:UUID`: copy `@id` or leave blank.
  - `name.baseName`, `treatmentStandardsRoutes`, `mixAndLocationTypes`, `functionalUnitFlowProperties`: See the dedicated mapping rules below; **functionalUnitFlowProperties is the only optional field**—the other three must always be populated with meaningful phrases.
  - `classificationInformation.common:classification.common:class`: build a path from `src/tidas/schemas/tidas_processes_category.json` (level 0+). Use every clue in the JSON-LD payload—`category` strings such as `C:Manufacturing/27:…`, `name.*`, `description`—to pick the most specific ISIC code. Do not literally split `category` and copy it; use it as evidence to choose the correct Tiangong code. Empty classifications are not accepted.
  - `common:generalComment`: 2–4 sentences describing scope, boundaries, and data sources (you may reuse JSON-LD `description`/`category` text).
- **quantitativeReference**: Provide `referenceToReferenceFlow` (string ID) and `functionalUnitOrOther` (multi-language).
- **time**: `common:referenceYear` (integer). If JSON-LD supplies validity, use `common:dataSetValidUntil`.
- **geography**: `locationOfOperationSupplyOrProduction.@location` must reflect the JSON-LD location evidence (country/region codes, textual descriptions, etc.). Select the most specific Tiangong code that matches the source; if the payload provides no usable geography, omit the entire `geography` block rather than inventing `GLO`.
- **technology**: `technologyDescriptionAndIncludedProcesses` multi-language block.
- **exchanges.exchange[]**:
  - Required fields: `@dataSetInternalID`, `exchangeDirection` (“Input”/“Output”), `meanAmount` (string), `unit`, optional `generalComment`.
  - `referenceToFlowDataSet`: include `@type="flow data set"`, `@refObjectId` (original flow ID), `@version` (default `01.01.000`), `@uri="../flows/<uuid>_<version>.xml"`, and `common:shortDescription`.
  - Do **not** emit `referenceToFlowPropertyDataSet`; Stage 2 automatically enforces the database defaults.
  - `generalComment` may reuse the JSON-LD text; do not add FlowSearch hints or custom fields (e.g., `exchangeName`).
- **modellingAndValidation**:
  - `LCIMethodAndAllocation`: Always populate both `typeOfDataSet` and `LCIMethodPrinciple`, choosing only from the ILCD-allowed vocabulary (e.g., `Unit process, single operation` / `System process` / `Unit process, black box`; `Attributional` / `Consequential` / `Not applicable`). Use the evidence from JSON-LD `processDocumentation` / `modellingParameters`; if no information is available, fall back to `"Not defined"` but never leave the fields empty or use placeholders.
  - `complianceDeclarations` with entry-level compliance reference (`d92a1a12-2545-49e2-a585-55c259997756`, version `20.20.002`).
  - `dataSourcesTreatmentAndRepresentativeness`: include `dataCutOffAndCompletenessPrinciples`,Specifies the cut-off criteria and completeness requirements applied to the dataset, defining the boundaries for data inclusion to ensure the system scope is sufficiently comprehensive.
  - `dataSelectionAndCombinationPrinciples`, Describes the principles for selecting and combining data from various sources into a consistent dataset, including protocols for data prioritization and consistency requirements.
  - `dataTreatmentAndExtrapolationsPrinciples`, Details the principles for data treatment and extrapolation in cases of data gaps, as well as temporal, geographical, and technological differences, to maintain data applicability and consistency.
  -`referenceToDataSource` ，Lists the categories of referenced data sources for this dataset and records their formats to guarantee data traceability.
  - `validation.review`: “Not reviewed” if absent.
- **administrativeInformation**:
  - `dataEntryBy.common:referenceToDataSetFormat`: reference ILCD format dataset (`a97a0155-0234-4b87-b4ce-a45da52f2a40`, `03.00.003`).
  - `dataEntryBy.common:referenceToPersonOrEntityEnteringTheData` and `publicationAndOwnership.common:referenceToOwnershipOfDataSet`: point to Tiangong contact (`f4b4c314-8c4c-4c83-968f-5b3c7724f6a8`, `01.00.000`, URI `../contacts/f4b4c314-8c4c-4c83-968f-5b3c7724f6a8_01.00.000.xml`).
  - `dataEntryBy.common:timeStamp`: UTC timestamp (e.g., `2025-11-23T12:34:56Z`).
  - `common:commissionerAndGoal.common:intendedApplications`: multi-language list summarizing the documented purpose or usage context (e.g., “Used for LCA modeling of NMC622 cathode manufacturing; covers 2019–2020 Chinese plant data”). Do not use template sentences or placeholders; reuse the exact intent from the JSON-LD `processDocumentation`, `description`, or `useAdvice`.
  - `publicationAndOwnership.common:dataSetVersion = "01.01.000"`, `common:permanentDataSetURI = https://lcdn.tiangong.earth/showProcess.xhtml?uuid=<uuid>&version=<version>`, `common:licenseType = "Free of charge for all users and uses"`.
- **Sources**: If the process references literature/images, output separate `sourceDataSet` entries and Stage 2 will attach them; do not fabricate `sources.common:source[]` blocks inside the process.

### Process name mapping

- **Input fields (from JSON-LD `payload`)**
  1. `payload["name"]`: original process name string (e.g., `"2019-2020 ; 4-LIB cathode production ; Cathode for NMC622-SiGr battery..."`).
  2. Supporting context from `payload.get("description")`, `payload.get("category")`, `payload.get("location", {}).get("name")`, and `payload.get("processDocumentation", {})` (`technologyDescription`, `timeDescription`, `geographyDescription`, `useAdvice`) plus any hints inside `payload.get("exchanges")` about functionality, route, geography, or reference flows.

- **Output placement (ILCD dataset)**  
  Populate `processDataSet["processInformation"]["dataSetInformation"]["name"]` with four `StringMultiLang` entries:
  1. `baseName` (required): core activity or product, free of years, market qualifiers, or geography (e.g., `{"@xml:lang": "en", "#text": "4-LIB cathode production"}`).
  2. `treatmentStandardsRoutes` (required): “how it is produced” descriptors—technical route, process steps, feedstock grades, captured as short phrases (e.g., `"Slurry mixing; Coating; Drying and solvent recovery; Calendering; Slitting"`).
  3. `mixAndLocationTypes` (required): natural-language market/mix scope and geography (e.g., `"Production mix, Mainland China"` or `"Market group, Global freight"`). If the payload lacks explicit geography, use `"Production mix, Unspecified region"` rather than leaving the field blank.
  4. `functionalUnitFlowProperties` (optional): describe the quantitative reference or functional unit derived from `exchanges` (especially those flagged `isQuantitativeReference=true`). Omit this field entirely when no functional unit can be inferred.

- **Authoring guidance**
  - Always reference the exact JSON paths above; Stage 1 passes the untouched JSON-LD payload to the LLM, so you must read fields like `payload["name"]` literally.
  - Split long `payload["name"]` strings on `;`, `|`, or `,` and assign each fragment to the correct bucket (base activity, route, market+location). Years and geography tokens belong in `mixAndLocationTypes`, never in `baseName`.
  - Use `processDocumentation["technologyDescription"]`, `processDocumentation["useAdvice"]`, and major input exchange names (`"NMC622 oxide"`, `"PVDF binder"`, etc.) to build concise route summaries.
  - `baseName`, `treatmentStandardsRoutes`, and `mixAndLocationTypes` must never be empty. When the source offers insufficient detail, summarize the best available evidence instead of writing “Unknown” or “Generic data”.

## 2. FlowDataSet requirements

- Same ILCD header as processes.
- **flowInformation.dataSetInformation**: `common:UUID`, multi-language `name.*`, `common:synonyms`, `common:generalComment`, `classificationInformation` using `tidas_flows_product_category.json` (level 0–4). Combine the raw JSON-LD `name`、 `description`  、 `category`  text to pick the most specific product code. When the JSON-LD flow includes a `description`, reuse that exact text for `common:generalComment`. Never fabricate stock phrases (e.g., “Converted from OpenLCA JSON-LD”) or reuse the same text across every field. Classifications must never be empty and must already be valid Tiangong product paths—Stage 1 is responsible for emitting the exact `common:class` ladder from `src/tidas/schemas/tidas_flows_product_category.json` without fallbacks. During classification you will be prompted level-by-level with the allowed codes; always pick one candidate per level until the 0–4 path is complete.
- **quantitativeReference**: `referenceToReferenceFlowProperty` pointing to the first `flowProperties.flowProperty.@dataSetInternalID`.
- **geography.locationOfSupply**: Pick the Tiangong code that matches the JSON-LD flow location (country, market scope, etc.). If the source file omits geography entirely, omit this field—do not fall back to `GLO`.
- **technology.technologicalApplicability**: multi-language description.
- **flowProperties.flowProperty[]**: mirror the JSON-LD `flowProperties` list. Preserve the original flow-property UUIDs/names so Stage 1 can map them directly; do not invent additional flow properties.
- **modellingAndValidation**: `LCIMethod.typeOfDataSet` ∈ {Product flow, Waste flow, Elementary flow}; include ILCD entry-level compliance reference.
- **administrativeInformation**: same ILCD format + Tiangong contact references; `common:dataSetVersion = "01.01.000"`, `common:permanentDataSetURI = https://lcdn.tiangong.earth/showFlow.xhtml?uuid=<uuid>&version=<version>`, license info, owner reference.

## 3. SourceDataSet requirements

- `sourceInformation.dataSetInformation.common:UUID` (original `@id`).
- `common:shortName`: copy the JSON-LD `name` verbatim (no paraphrasing or truncation). If JSON-LD supplies multiple names, join them exactly as given.
- `classificationInformation.common:classification.common:class`: use `tidas_sources_category.json` (level 0, e.g., Publications and communications).
- `sourceCitation`: reuse JSON-LD `textReference` exactly when provided; otherwise fall back to the short name.
- `publicationType`, `sourceDescriptionOrComment`, `referenceToContact` (Tiangong contact), optional `sourceDescription` fields (`title`, `year`, `referenceToPublisher`) if available.
- `administrativeInformation`: ILCD format reference, timestamp, `common:dataSetVersion = "01.01.000"`, `common:permanentDataSetURI = https://lcdn.tiangong.earth/showSource.xhtml?uuid=<uuid>&version=<version>`, license, ownership.

## 4. Classification & geography aids

- **Processes**: `src/tidas/schemas/tidas_processes_category.json`. Use `uv run python scripts/list_process_category_children.py <code>` (empty `<code>` lists top level) or `tiangong_lca_spec.tidas.get_schema_repository().resolve_with_references(...)` to gather candidate branches. Select one level at a time.
- **Flows**: `src/tidas/schemas/tidas_flows_product_category.json`; helper CLI `scripts/list_product_flow_category_children.py`.
- **Elementary flows**: `src/tidas/schemas/tidas_flows_elementary_category.json`; helper CLI `scripts/list_elementary_flow_category_children.py`.
- **Locations**: `src/tidas/schemas/tidas_locations_category.json`; helper CLI `scripts/list_location_children.py <code>` (e.g., `uv run python scripts/list_location_children.py CN`). Always choose the most specific applicable code; use `GLO` only when truly global.

## 5. Stage 2 & Stage 3 summary

- **Stage 2** (`stage2_jsonld_validate.py`) steps:
  1. Load Stage 1 caches (`stage1_process_blocks.json`, `stage1_flow_blocks.json`, `stage1_source_blocks.json`).
  2. Remap UUIDs using `UUIDMapper` and write ILCD files to `artifacts/<run>/exports/processes|flows|sources/`.
  3. Run JSON Schema (planned) + `uv run tidas-validate -i exports`; auto-fix limited issues (currently data-source metadata) and re-run validation if needed.
  4. Produce `workflow_result.json`, `tidas_validation.json`. If `--skip-auto-publish` is absent and validation succeeds, Stage 2 will invoke Stage 3 automatically.
- **Stage 3** (`stage3_jsonld_publish.py`): reads `exports/`, publishes flows→processes→sources to the database, supports dry-run (`--commit` optional), records summary in `cache/published.json`.

## 6. Validation & publish tips

- Run `uv run tidas-validate -i artifacts/<run>/exports` anytime to re-check outputs. Results land in `artifacts/<run>/cache/tidas_validation.json`.
- To re-export and validate manually: `uv run python scripts_jsonld/stage2_jsonld_validate.py --run-id <run> --clean-exports`.
- For manual publish or dry-run: `uv run python scripts_jsonld/stage3_jsonld_publish.py --run-id <run>` (append `--commit` for real inserts).
- Key logs: `tidas_validation.cli_exit_nonzero`, `jsonld_stage2.uuid_map`, `jsonld_stage3.publish_*` help pinpoint failures quickly.

## 7. Reference commands

```bash
# Convert prompt to inline text (optional helper)
uv run python scripts_jsonld/convert_prompt_to_inline.py \
  --prompt .github/prompts/convert_json.prompt.md \
  --output inline_prompt_jsonld.txt

# Stage 1
uv run python scripts_jsonld/stage1_jsonld_extract.py \
  --process-dir test/data/json_ld/processes \
  --flow-dir test/data/json_ld/flows \
  --sources-dir test/data/json_ld/sources \
  --prompt .github/prompts/convert_json.prompt.md \
  --secrets .secrets/secrets.toml \
  --run-id 20250101T000000Z

# Stage 2
uv run python scripts_jsonld/stage2_jsonld_validate.py \
  --run-id 20250101T000000Z \
  --clean-exports

# Stage 3
uv run python scripts_jsonld/stage3_jsonld_publish.py \
  --run-id 20250101T000000Z --commit

# One-click pipeline (dry-run publish)
uv run python scripts_jsonld/run_pipeline.py \
  --process-dir test/data/json_ld/processes \
  --flows-dir test/data/json_ld/flows \
  --sources-dir test/data/json_ld/sources \
  --prompt .github/prompts/convert_json.prompt.md \
  --secrets .secrets/secrets.toml \
  --clean-exports \
  --dry-run-publish
```

## 8. Summary

- Stage 1 must emit fully populated ILCD datasets: classification, geography, technology, compliance, administrative info, and flow/source references must all be correct.
- Always use Tiangong standard flow property/unit group UUIDs; do not create new ones.
- Stage 2/Stage 3 only handle UUID remapping, validation, and publication—they will not fill missing fields.
- Output legal JSON only; no Markdown, no commentary, no `NA`.
