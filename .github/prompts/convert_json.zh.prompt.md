# JSON-LD 数据抽取工作流指引

本提示用于 **JSON-LD → ILCD** 的 Stage 1。你要读取单个 JSON-LD 文件（process / flow / source，含嵌套引用）并直接生成符合 Tiangong/TIDAS schema 的结构。Stage 2/Stage 3 会自动重映射 UUID、写入 `artifacts/<run>/exports/`、执行校验并发布，所以在本阶段务必把所有语义字段、模板常量、分类/地理信息一次写全。

**执行约定**

- 所有脚本通过 `uv run python …` 执行（工作站无系统级 `python`）。
- JSON-LD 管线严格遵循 Stage 1→Stage 2→Stage 3：  
  1. `scripts_jsonld/stage1_jsonld_extract.py`（LLM 抽取）  
  2. `scripts_jsonld/stage2_jsonld_validate.py`（导出 + JSON Schema + `uv run tidas-validate` + 自动修复 + 可选自动发布）  
  3. `scripts_jsonld/stage3_jsonld_publish.py`（发布到 `Database_CRUD_Tool`，可 dry-run）  
  `scripts_jsonld/run_pipeline.py` 会自动串联这三步。
- Stage 1 负责所有“需要理解语义”的字段（分类、地理、技术、合规、行政引用等），以及 ILCD 头部/常量引用、多语言字段格式规范化。**不要在后续阶段期望再补数据**。
- Flow property / unit group 引用统一指向 Tiangong 官方常量（如 Mass 属性 `93a60a56-a3c8-11da-a746-0800200b9a66`、其 unit group）。当引用该属性时，`@refObjectId` 必须保持纯 UUID（不带 `_03.00.003` 后缀），版本号写在 `@version`，并仅在 `@uri` 中追加 `_03.00.003`。不再额外导出这些数据集。
- Stage 2 的 `UUIDMapper` 会为 flows/sources/processes 生成新 UUID，并自动将流程内的 `referenceToFlowDataSet` / `referenceToSourceDataSet` 等引用改成新值。Stage 1 只需沿用 JSON-LD 原始 `@id` 或留空即可。
- 输出中不得含 TODO、空字符串、`NA/N/A` 等占位符；缺信息时要么回退到合规默认值（`Z/Unspecified`、`GLO`、Mass 属性），要么在 `common:generalComment` 说明“原始 JSON-LD 未提供”。

---

## 1. 工作流概览

| Stage | 脚本 | 主要职责 | 产物 |
|-------|------|----------|------|
| **Stage 1：JSON-LD 抽取** | `scripts_jsonld/stage1_jsonld_extract.py` | 遍历 `--process-dir`、`--flow-dir`、`--sources-dir`，调用本提示生成符合 schema 的 ILCD 数据集（process / flow / source）。 | `artifacts/<run>/cache/stage1_process_blocks.json`、`stage1_flow_blocks.json`、`stage1_source_blocks.json` |
| **Stage 2：导出 + 校验** | `scripts_jsonld/stage2_jsonld_validate.py` | 使用 `UUIDMapper` 重写 UUID/引用 → 写入 `artifacts/<run>/exports/processes|flows|sources/` → 运行本地 JSON Schema（计划中）+ `uv run tidas-validate` → 尝试自动修复可处理字段 → 输出 `workflow_result.json` / `tidas_validation.json`。可选 `--skip-auto-publish`。 | `exports/` 下的 ILCD JSON、`cache/tidas_validation.json`、`cache/workflow_result.json` |
| **Stage 3：发布** | `scripts_jsonld/stage3_jsonld_publish.py` | 读取 `exports/`，调用 `Database_CRUD_Tool` 提交 flows→processes→sources；支持 dry-run，产出 `cache/published.json`。 | 数据库提交 + 发布摘要 |

---

## 2. Stage 1 输入 / 输出契约

- **输入**：单个JSON-LD 对象。必须包含原始 `@type`、`@id`、`name`、`category`、嵌套 flow / source / unitGroup / flowProperty 等结构。
- **输出**：仅返回合法 JSON（禁止 Markdown、禁止额外文字）。统一格式：

```json
{
  "datasets": [
    { "processDataSet": { ... } },
    { "flowDataSet": { ... } },
    { "sourceDataSet": { ... } }
  ]
}
```

数组可包含 1 个或多个数据集；若输入只描述流程，就只输出一个 `processDataSet`。

---

## 3. 数据集必填要素

### 3.1 ProcessDataSet

- `@xmlns`、`@xmlns:common`、`@xmlns:xsi`、`@version`、`@locations`、`@xsi:schemaLocation`：沿用 ILCD 标准。
- `processInformation.dataSetInformation`
  - `common:UUID`：沿用 JSON-LD `@id` ，Stage 2 会重写。  
  - `name.baseName / treatmentStandardsRoutes / mixAndLocationTypes / functionalUnitFlowProperties`：全部使用 `{"@xml:lang": "en", "#text": "…"}` 结构，可追加其他语言。  
  - `classificationInformation.common:classification.common:class`：按照 `tidas_processes_category.json` 生成完整路径（0 层起）。要结合 JSON-LD 的 `category`（如 `C:Manufacturing/27:…`）、`name.*`、`description`、`generalComment` 等信息推断最贴切的分类；不要直接把 `category` 字符串拆分后照抄。缺少分类会导致 Stage 1 直接失败。
  - `common:generalComment`：2–4 句描述系统边界、假设、来源。可以直接将json_ld数据中的"category"字段以及"description"字段填入其中。  
  - 若 JSON-LD 没有 `identifierOfSubDataSet` 可留空。
- `processInformation.quantitativeReference`：`referenceToReferenceFlow`（字符串 ID）+ `functionalUnitOrOther`（多语言）。  
- `time`：`common:referenceYear`（整数），如有 `dataSetValidUntil` 写到 `common:dataSetValidUntil`。  
- `geography.locationOfOperationSupplyOrProduction.@location`：严格遵守`scripts/list_location_children.py`及`src/tidas/schemas/tidas_locations_category.json`文档要求，填入标准的代码（若地理位置不确定则填上 `GLO`）。  
- `technology.technologyDescriptionAndIncludedProcesses`：多语言文本，必要时说明边界/工艺。  
- `exchanges.exchange[]`：  
  - 必含 `@dataSetInternalID`、`exchangeDirection`（Input/Output）、`meanAmount`（字符串）、`unit`。  
  - `referenceToFlowDataSet`：沿用 JSON-LD 原 `@id`，并补齐 `@type="flow data set"`, `@version`（缺省 `01.01.000`）, `@uri="../flows/<uuid>_<version>.xml"`, `common:shortDescription`。  
  - `referenceToFlowPropertyDataSet`：若 JSON-LD 指向官方属性（如 Mass）直接写常量；若缺失则默认 Mass（UUID `93a60a56-a3c8-11da-a746-0800200b9a66`, 版本 `03.00.003`, URI `../flowproperties/93a60a56-a3c8-11da-a746-0800200b9a66_03.00.003.xml`）。  
  - `exchangeName` / `generalComment` 可直接沿用 JSON-LD，禁止添加 FlowSearch hints（JSON-LD 流程不再走 MCP 对齐）。
- `modellingAndValidation`  
  - `LCIMethodAndAllocation.typeOfDataSet`（通常 “Unit process, single operation”）+ `LCIMethodPrinciple`（如 “Attributional”）。  
  解决 `complianceDeclarations`（完全填写 ILCD 合规字段）。  
  - `dataSourcesTreatmentAndRepresentativeness`：写入 `dataCutOffAndCompletenessPrinciples`、`dataSelectionAndCombinationPrinciples`、`dataTreatmentAndExtrapolationsPrinciples`、`referenceToDataSource`（默认可引用 ILCD format 数据源 + “Converted from OpenLCA JSON-LD…” 描述）。  
  - `validation.review`：若无信息，写 “Not reviewed”。  
- `administrativeInformation`  
  - `dataEntryBy.common:referenceToDataSetFormat`：引用 ILCD format 源（UUID `a97a0155-0234-4b87-b4ce-a45da52f2a40`, 版本 `03.00.003`, URI `../sources/a97a0155-0234-4b87-b4ce-a45da52f2a40_03.00.003.xml`）。  
  - `dataEntryBy.common:referenceToPersonOrEntityEnteringTheData` + `common:referenceToOwnershipOfDataSet`：均引用 Tiangong 联系人（UUID `f4b4c314-8c4c-4c83-968f-5b3c7724f6a8`, 版本 `01.00.000`, URI `../contacts/f4b4c314-8c4c-4c83-968f-5b3c7724f6a8_01.00.000.xml`）。  
  - `dataEntryBy.common:timeStamp`：UTC 时间（`YYYY-MM-DDTHH:MM:SSZ`）。  
  - `common:commissionerAndGoal.common:intendedApplications`：写明用途，多语言。  
  - `publicationAndOwnership.common:dataSetVersion` 固定 `01.01.000`；`common:permanentDataSetURI = https://lcdn.tiangong.earth/showProcess.xhtml?uuid=<uuid>&version=<version>`；`common:licenseType = "Free of charge for all users and uses"`。

### 3.2 FlowDataSet

- 结构和命名空间同上。
- `flowInformation.dataSetInformation.common:UUID` ：沿用 JSON-LD `@id`。  
- `name.baseName / treatmentStandardsRoutes / mixAndLocationTypes / flowProperties`：全部多语言。  
- `common:synonyms`:若json_ld数据中有别名信息，可填入此字段，使用多语言格式。若没有可以生成几个常见别名（例如英文名的小写版本，或去掉特殊字符的版本）。
- `common:generalComment`: 2–4 句描述流的性质、用途、来源。可以直接将json_ld数据中"description"字段填入其中。
- `technology.technologicalApplicability`：多语言列表，可复用 JSON-LD 文本。  
- `classificationInformation`：使用 `tidas_flows_product_category.json` 的编码路径（0→4 层）。必须在 Stage 1 直接输出 Tiangong 官方分类路径，不允许交给后续阶段再补。结合 `name.*`、`treatmentStandardsRoutes`、`mixAndLocationTypes`、`flowProperties`、`common:generalComment` 以及 JSON-LD `category` 文字，选择最贴切的产品分类。流程会逐层向你提供候选代码，请在每一层只选一个候选，直到补全 0–4 层路径。
  - 分类不得留空，也不得使用 schema 外的临时代码；一旦 LLM 输出未知的 `@classId`，Stage 1 会立即报错。
- `quantitativeReference.referenceToReferenceFlowProperty`：指向 `flowProperties.flowProperty` 的首个 `@dataSetInternalID`。  
- `geography.locationOfSupply`：严格遵守`scripts/list_location_children.py`及`src/tidas/schemas/tidas_locations_category.json`文档要求，填入标准的代码（若地理位置不确定则填上 `GLO`）。   
- `flowProperties.flowProperty[]`：  
  - `@dataSetInternalID`、`meanValue`（字符串）必填。  
  - `referenceToFlowPropertyDataSet`：若 JSON-LD 提供 `flowProperty.@id`，直接引用；否则默认 Mass 属性常量。引用该常量时，`@refObjectId` 只能写 `93a60a56-a3c8-11da-a746-0800200b9a66`，`@version` 写 `03.00.003`，`@uri` 才追加 `_03.00.003`。  
- `modellingAndValidation.LCIMethod.typeOfDataSet` ∈ {Product flow, Waste flow, Elementary flow}。  
  - `complianceDeclarations.compliance.common:referenceToComplianceSystem`：引用 ILCD entry-level（UUID `d92a1a12-2545-49e2-a585-55c259997756`, 版本 `20.20.002`, URI `../sources/d92a1a12-2545-49e2-a585-55c259997756_20.20.002.xml`）。  
- `administrativeInformation`：与流程相同的 ILCD format 引用、Tiangong contact、timeStamp、license、owner。`common:dataSetVersion` 固定 `01.01.000`，`common:permanentDataSetURI = https://lcdn.tiangong.earth/showFlow.xhtml?uuid=<uuid>&version=<version>`。

### 3.3 SourceDataSet

- `sourceInformation.dataSetInformation.common:UUID`：沿用 JSON-LD `@id`。  
- `common:shortName`：直接填资料题名/图片描述（根据来源类型）。  
- `classificationInformation`：引用 `tidas_sources_category.json` level 0（例如 Publications and communications）。  
- `sourceCitation`：完整引用文本；`publicationType` 取 ILCD 枚举（文章、章节、软件等）。  
- `sourceDescriptionOrComment`：多语言摘要。  
- `referenceToContact`：引用 Tiangong 联系人（同上）。  
- `sourceDescription`（如有：`title`、`year`、`referenceToPublisher` 等）可按 JSON-LD 原值补充。  
- `administrativeInformation`：ILCD format 引用 + timeStamp；`publicationAndOwnership` 固定版本/URI/owner/license，与流程一致。

---

## 4. 分类与地理辅助资源

- **流程分类**：权威来源为 `src/tidas/schemas/tidas_processes_category.json`。需要逐级选择分类时，可先运行 `uv run python scripts/list_process_category_children.py <code>`（`<code>` 为空输出顶层，例如 `uv run python scripts/list_process_category_children.py 01``），或使用 `tiangong_lca_spec.tidas.get_schema_repository().resolve_with_references("tidas_processes_category.json")` 读入局部节点，再把候选列表（code + description）贴给 LLM。无论自动还是人工辅助，必须逐层确认（`You are selecting level {level}...`），单次只决定一层，避免一次性输出整条路径。
- **产品流 / 初级流分类**：分别参考 `src/tidas/schemas/tidas_flows_product_category.json` 与 `src/tidas/schemas/tidas_flows_elementary_category.json`。可通过 `uv run python scripts/list_product_flow_category_children.py <code>` 或 `uv run python scripts/list_elementary_flow_category_children.py <code>` 查询候选。
- **地理编码**：使用 `src/tidas/schemas/tidas_locations_category.json`，命令 `uv run python scripts/list_location_children.py <code>`（如 `uv run python scripts/list_location_children.py CN`）按需展开。向 LLM 提供选项时，只粘贴与当前流程相关的分支，避免整棵树。
- **Stage 1/2 协作**：Stage 1 LLM 根据 `name.*`、`common:generalComment` 等上下文决定流程/流的分类与地理；Stage 2 仅做格式校验，不会重新分类。因此分类和位置判断必须在 Stage 1 就完成。

---

## 5. 校验与发布（Stage 2 & Stage 3 摘要）

- Stage 2 (`stage2_jsonld_validate.py`) 会：
  1. 读取 Stage 1 缓存 → 使用 `UUIDMapper` 为 flow/source/process 生成新 UUID，并自动重写引用。  
  2. 将结果写入 `artifacts/<run>/exports/processes|flows|sources/`。  
  3. 运行本地 JSON Schema（计划中）+ `uv run tidas-validate -i exports`；若发现 `dataSourcesTreatmentAndRepresentativeness` 等可自动修复的缺项，会调用 `auto_fix_from_validation`。  
  4. 产出 `workflow_result.json`、`tidas_validation.json`、`stage3_alignment.json`（目前为空占位）。  
  5. 如未传 `--skip-auto-publish`，会在校验通过后调用 Stage 3。
- Stage 3 (`stage3_jsonld_publish.py`) 负责 Database CRUD：读取 `exports/`，按 flows→processes→sources 顺序执行 insert/update；成功后写入 `cache/published.json`。`run_pipeline.py` 默认在 Stage 2 结束且未指定 `--dry-run-publish` 时自动触发。

---

## 6. 验证与发布注意事项

- **本地校验**：Stage 2 会运行 `uv run tidas-validate -i artifacts/<run>/exports`。如需单独复现，可直接在命令行执行该指令并查看 `artifacts/<run>/cache/tidas_validation.json`。若验证工具不可用，可在 Stage 2 添加 `--validation-only` 手动检查导出结果，待工具恢复后再运行完整 Stage 2。
- **自动发布 vs dry-run**：默认 `run_pipeline.py` 在 Stage 2 成功后会调用 `stage3_jsonld_publish.py`。若希望全面 dry-run，可传入 `--dry-run-publish` 或手动执行 `uv run python scripts_jsonld/stage3_jsonld_publish.py --run-id <run>` 而不加 `--commit`。
- **手动复查**：在提交前可以重新运行 `uv run python scripts_jsonld/stage2_jsonld_validate.py --run-id <run> --clean-exports` 确认导出与校验通过，再执行 `uv run python scripts_jsonld/stage3_jsonld_publish.py --run-id <run> --commit`。
- **日志观测**：运行 Stage 2/Stage 3 时，可关注 `tidas_validation.cli_exit_nonzero`、`jsonld_stage2.uuid_map`、`jsonld_stage3.publish_*` 等结构化日志，快速定位验证或发布失败原因。

---

## 7. 常用命令

```bash
# 生成单行 prompt（可与 LLM 会话结合）
uv run python scripts_jsonld/convert_prompt_to_inline.py \
  --prompt .github/prompts/convert_json.prompt.md \
  --output inline_prompt_jsonld.txt

# Stage 1：LLM 抽取
uv run python scripts_jsonld/stage1_jsonld_extract.py \
  --process-dir test/data/json_ld/processes \
  --flow-dir test/data/json_ld/flows \
  --sources-dir test/data/json_ld/sources \
  --prompt .github/prompts/convert_json.prompt.md \
  --secrets .secrets/secrets.toml \
  --run-id 20250101T000000Z

# Stage 2：导出 + 校验
uv run python scripts_jsonld/stage2_jsonld_validate.py \
  --run-id 20250101T000000Z \
  --clean-exports

# Stage 3：发布（可 dry run）
uv run python scripts_jsonld/stage3_jsonld_publish.py \
  --run-id 20250101T000000Z --commit

# 一键执行（含 dry-run 发布）
uv run python scripts_jsonld/run_pipeline.py \
  --process-dir test/data/json_ld/processes \
  --flows-dir test/data/json_ld/flows \
  --sources-dir test/data/json_ld/sources \
  --prompt .github/prompts/convert_json.prompt.md \
  --secrets .secrets/secrets.toml \
  --clean-exports \
  --dry-run-publish
```

---

## 8. 总结

- Stage 1（本提示）要生成 **完整、合规、无需后修** 的 ILCD 结构：分类、地理、技术、合规、行政信息、来源引用、默认常量全部就位。  
- Flow property / unit group 统一引用 Tiangong 官方常量；引用 Mass 属性时仅在 `@uri` 保留 `_03.00.003`，`@refObjectId` 必须是纯 UUID。  
- Stage 2/Stage 3 只负责 UUID 重写 + 校验 + 发布，不会再填充语义字段。  
- 输出必须是合法 JSON，禁止 Markdown / 注释 / `NA`。
