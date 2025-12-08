# JSON-LD 数据抽取工作流指引

本提示用于 **JSON-LD → ILCD** 的 Stage 1。你要读取单个 JSON-LD 文件（process / flow / source，含嵌套引用）并直接生成符合 Tiangong/TIDAS schema 的结构。Stage 2/Stage 3 会自动重映射 UUID、写入 `artifacts/<run>/exports/`、执行校验并发布，所以在本阶段务必把所有语义字段、模板常量、分类/地理信息一次写全。

##  执行约定

1. **流水线**：  
   - Stage 1 → `scripts_jsonld/stage1_jsonld_extract.py`（本提示）  
   - Stage 2 → `scripts_jsonld/stage2_jsonld_validate.py`（UUID 重映射 + JSON Schema + `uv run tidas-validate`）  
   - Stage 3 → `scripts_jsonld/stage3_jsonld_publish.py`（数据库 CRUD，可 dry-run）  
   `scripts_jsonld/run_pipeline.py` 会自动串联这三步。
2. **保留原始 ID**：`common:UUID` 必须沿用 JSON-LD `@id`（或留空）；Stage 2 会统一替换并更新引用。
3. **严格匹配 schema**：输出需直接满足 `tidas_processes.json`、`tidas_flows.json`、`tidas_sources.json`，不得留下 TODO、占位符或 FlowSearch 提示。
4. **仅输出标准封装**：`datasets[]` 中的每个元素只能包含一个键（`processDataSet` / `flowDataSet` / `sourceDataSet`）；禁止添加兄弟键或辅助片段。
5. **多语言字段**：所有 ILCD `StringMultiLang` / `FTMultiLang` 字段写成 `{"@xml:lang": "en", "#text": "…"}`；若输入自带其他语言可追加。
6. **数值**：按原值抄写；凡 ILCD 要求字符串的字段（如 `meanAmount`、`meanValue`）必须以字符串输出。缺失时可省略或留空，严禁杜撰。
7. **删除无对应元数据**：OpenLCA 字段若无 ILCD 对应项，直接丢弃，别硬塞进其他章节。
8. **多数据集**：某些 JSON-LD 会嵌套 flows/sources，需在 `datasets` 数组中分别输出对应的数据集。
9. **分类与地理**：按照 Tiangong schema（见第 4 节）在 Stage 1 完成分类/地理判断；Stage 2 不会再重分。
10. **Flow property / unit group**：尽量复用 JSON-LD 原有的 `flowProperties` 引用，勿自创新的 flow property / unit group 数据集。
11. **Schema 裁剪**：Stage 1 会按 `tidas_*` schema（`additionalProperties=false`）自动删掉未定义字段，因此输出仅限文档列出的键。

---

## 工作流概览

| Stage | 脚本 | 主要职责 | 产物 |
|-------|------|----------|------|
| **Stage 1：JSON-LD 抽取** | `scripts_jsonld/stage1_jsonld_extract.py` | 遍历 `--process-dir`、`--flow-dir`、`--sources-dir`，调用本提示生成符合 schema 的 ILCD 数据集（process / flow / source）。 | `artifacts/<run>/cache/stage1_process_blocks.json`、`stage1_flow_blocks.json`、`stage1_source_blocks.json` |
| **Stage 2：导出 + 校验** | `scripts_jsonld/stage2_jsonld_validate.py` | 使用 `UUIDMapper` 重写 UUID/引用 → 写入 `artifacts/<run>/exports/processes|flows|sources/` → 运行本地 JSON Schema（计划中）+ `uv run tidas-validate` → 尝试自动修复可处理字段 → 输出 `workflow_result.json` / `tidas_validation.json`。可选 `--skip-auto-publish`。 | `exports/` 下的 ILCD JSON、`cache/tidas_validation.json`、`cache/workflow_result.json` |
| **Stage 3：发布** | `scripts_jsonld/stage3_jsonld_publish.py` | 读取 `exports/`，调用 `Database_CRUD_Tool` 提交 flows→processes→sources；支持 dry-run，产出 `cache/published.json`。 | 数据库提交 + 发布摘要 |

- Stage 1（`scripts/jsonld/stage1_jsonld_extract.py`）需要同时传入 `--process-dir`/`--flows-dir`/`--source-dir`，对每个 JSON-LD 文件套用与文献流程一致的 LocationNormalizer、ProcessClassifier，并在缓存前执行 `build_tidas_process_dataset`，确保 `stage1_*_blocks.json` 已满足 ILCD/TIDAS 结构。若 Flow 无法解析出可信的地域代码，现在不会再默认写入 `GLO`，而是保留为空，只有在成功匹配地区时才设置 `flowInformation.geography.locationOfSupply`。
- Stage 2（`scripts/jsonld/stage2_jsonld_validate.py`）在 UUID 重映射后，会借助 `workflow.flow_references` 统一重建 `referenceToFlowDataSet`/`referenceToFlowPropertyDataSet`，若发现某流程引用的 Flow UUID 本轮并未导出，会立即报错；Stage 2 不再提供 `--json-ld-flows` 兜底，必须确保 Stage 1 已产出对应的 flow 数据集。
- Stage 3 仍沿用“校验通过即自动发布”的约定（除非显式添加 `--dry-run-publish`），与文献流程共用 TIDAS 校验与 Database CRUD，发布顺序调整为 **flow → source → process**。每批 flow/source 成功写入远端后，Stage 3 会将 CRUD 返回的远端 ID/版本写回流程缓存，再提交最终 process，确保门户界面上的 `referenceToFlowDataSet`/`referenceToDataSource` 可以直接打开对应的远端条目。

---

## 2. Stage 1 输入 / 输出规约

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
仅当 JSON-LD 的确定义了新的 flow property / unit group 时才额外输出 `flowPropertyDataSet` / `unitGroupDataSet`，否则直接引用标准 UUID。

---

## 1. ProcessDataSet 要求

- `@xmlns`、`@xmlns:common`、`@xmlns:xsi`、`@version`、`@locations`、`@xsi:schemaLocation`：沿用 ILCD 标准。
- **根结构**：`processDataSet` 必须直接包含标准 ILCD 模块（`processInformation`、`modellingAndValidation`、`administrativeInformation`、`exchanges`，以及可选的 `LCIAResults`），不得将这些模块嵌套在 `processInformation` 或其他节点下。若 JSON-LD 某字段缺失，就省略或留空，**禁止**用“Converted from JSON-LD.”、“Production mix, at plant”等占位文本凑数。
- `processInformation.dataSetInformation`
  - `common:UUID`：沿用 JSON-LD `@id` ，Stage 2 会重写。  
  - `name.baseName / treatmentStandardsRoutes / mixAndLocationTypes / functionalUnitFlowProperties`：详见后附“工艺名称拆分规则”章节，只有 `functionalUnitFlowProperties` 可以在确无信息时省略，其余三个字段必须填充真实内容。
  - `classificationInformation.common:classification.common:class`：应基于 `src/tidas/schemas/tidas_processes_category.json`（level 0 起）逐级构建路径，并结合 JSON-LD 中的全部线索（如 `category` 文本 `C:Manufacturing/27:…`、`name.*`、`description` 等）选出最具体的 ISIC/Tiangong 代码；切勿直接拆分 `category` 字符串照抄。分类不得留空。
  - `common:generalComment`：2–4 句描述系统边界、假设、来源。可以直接将json_ld数据中的"category"字段以及"description"字段填入其中。  
- `processInformation.quantitativeReference`：`referenceToReferenceFlow`（字符串 ID）+ `functionalUnitOrOther`（多语言）。  
- `time`：`common:referenceYear`（整数），如有 `dataSetValidUntil` 写到 `common:dataSetValidUntil`。  
- `geography.locationOfOperationSupplyOrProduction.@location`：严格遵守`scripts/list_location_children.py`及`src/tidas/schemas/tidas_locations_category.json`，根据 JSON-LD 的地理证据选择最精确的代码。若源文件完全缺失地理信息，就省略整个 `geography`，不要自动写 `GLO`。  
- `technology.technologyDescriptionAndIncludedProcesses`：多语言文本，必要时说明边界/工艺。  
- `exchanges.exchange[]`：  
  - 必含 `@dataSetInternalID`、`exchangeDirection`（Input/Output）、`meanAmount`（字符串）、`unit`。  
  - `referenceToFlowDataSet`：沿用 JSON-LD 原 `@id`，并补齐 `@type="flow data set"`, `@version`（缺省 `01.01.000`）, `@uri="../flows/<uuid>_<version>.xml"`, `common:shortDescription`。  
  - 不再输出 `referenceToFlowPropertyDataSet`；Stage 2 会按数据库默认值补齐。  
  - `generalComment` 可直接沿用 JSON-LD，禁止额外增加 FlowSearch hints 或自定义字段（如 `exchangeName`）。
- `modellingAndValidation`  
  - `LCIMethodAndAllocation.typeOfDataSet` 与 `LCIMethodPrinciple` 必须从 ILCD 允许的取值聚集中选择（例如 `Unit process, single operation` / `System process` / `Unit process, black box`；`Attributional` / `Consequential` / `Not applicable` / `Not defined` 等），并依据 JSON-LD 的 `processDocumentation`、`modellingParameters`、`inventoryMethodDescription` 等文本判断，严禁使用占位句或随意兜底。  
  解决 `complianceDeclarations`（完全填写 ILCD 合规字段）。  
  - `dataSourcesTreatmentAndRepresentativeness`：写入 `dataCutOffAndCompletenessPrinciples`, 用于说明数据集采用的切除规则和完整性要求，以界定哪些数据应被纳入并确保系统范围足够完整。
  - `dataSelectionAndCombinationPrinciples`, 用于说明从不同来源选择数据并将其组合为一致数据集的原则，包括数据优先级与一致性要求。
  - `dataTreatmentAndExtrapolationsPrinciples`, 用于说明针对缺失数据、时间与地域差异、技术差异等情形的数据处理与外推原则，以保持数据的可用性和一致性。
  - `referenceToDataSource`用于列出本数据集引用的数据源类别，并记录其来源格式,以确保数据可追溯性。 
  - `validation.review`：若无信息，写 “Not reviewed”。  
- `administrativeInformation`  
  - `dataEntryBy.common:referenceToDataSetFormat`：引用 ILCD format 源（UUID `a97a0155-0234-4b87-b4ce-a45da52f2a40`, 版本 `03.00.003`, URI `../sources/a97a0155-0234-4b87-b4ce-a45da52f2a40_03.00.003.xml`）。  
  - `dataEntryBy.common:referenceToPersonOrEntityEnteringTheData` + `common:referenceToOwnershipOfDataSet`：均引用 Tiangong 联系人（UUID `f4b4c314-8c4c-4c83-968f-5b3c7724f6a8`, 版本 `01.00.000`, URI `../contacts/f4b4c314-8c4c-4c83-968f-5b3c7724f6a8_01.00.000.xml`）。  
  - `dataEntryBy.common:timeStamp`：UTC 时间（`YYYY-MM-DDTHH:MM:SSZ`）。  
  - `common:commissionerAndGoal.common:intendedApplications`：多语言描述数据集的目标用途，内容需基于 JSON-LD `processDocumentation`/`useAdvice` 等信息总结（如“用于中国 NMC622 正极制造 LCA 数据建模；覆盖 2019–2020 年工厂数据”），严禁使用“Life cycle data…”之类模板句。  
  - `publicationAndOwnership.common:dataSetVersion` 固定 `01.01.000`；`common:permanentDataSetURI = https://lcdn.tiangong.earth/showProcess.xhtml?uuid=<uuid>&version=<version>`；`common:licenseType = "Free of charge for all users and uses"`。

### 工艺名称拆分规则 · Process name mapping

- **输入字段（来自 JSON-LD `payload`）**  
  1. `payload["name"]`：原始工艺名称字符串（如 `"2019-2020 ; 4-LIB cathode production ; Cathode for NMC622-SiGr battery..."`）。  
  2. `payload.get("description")`、`payload.get("category")`、`payload.get("location", {}).get("name")`、`payload.get("processDocumentation", {}).get("technologyDescription")`、`payload.get("processDocumentation", {}).get("timeDescription")`、`payload.get("processDocumentation", {}).get("geographyDescription")`、`payload.get("processDocumentation", {}).get("useAdvice")` 及 `payload.get("exchanges")` 中关于配方/路线/地域/功能单位的描述。

- **输出位置（ILCD 数据集中）**  
  `processDataSet["processInformation"]["dataSetInformation"]["name"]` 必须含下列四个 `StringMultiLang` 字段：
  1. `baseName`（必填）：描述核心活动或产品，不含年份、市场、地域限定。  
  2. `treatmentStandardsRoutes`（必填）：提炼技术路线、工艺步骤、材料等级等“如何生产”的信息，可从 `payload["name"]`、`description`、`processDocumentation["technologyDescription"]`、关键投料 `exchanges` 推断，多个短语用逗号连接。  
  3. `mixAndLocationTypes`（必填）：用自然语言说明市场/混合类型与地域，例如 `"Production mix, Mainland China"` 或 `"Market group, Global freight"`，可结合 `payload.get("location", {}).get("name")`地理描述等信息，字段仍需存在。  
  4. `functionalUnitFlowProperties`（可选）：若能从 `payload["exchanges"]`（特别是 `isQuantitativeReference=true` 的条目）推导出功能单位/参照流，则写出值、单位及对象（如 `"1 kg cathode, mass basis"`）；否则删除该字段。

- **写作提示**  
  - 搜索字段时严格使用以上 JSON 路径（如 `payload["name"]`、`payload.get("location", {}).get("name")`），因为 Stage 1 直接把原始 JSON-LD 传给 LLM，不会改名。  
  - 将复杂的 `payload["name"]` 先按 `;`、`|`、`,` 分割，再把不同片段归类到“主体活动 / 技术路线 / 市场+地域”三类。年份（`2019-2020`）、“Global”“China”等地理词、`market group` 等短语应落在 `mixAndLocationTypes`，不要写进 `baseName`。  
  - 技术路线可综合 `processDocumentation["technologyDescription"]`、`processDocumentation["useAdvice"]`、关键输入流名称（如 `NMC622 oxide`、`PVDF binder`）等线索，形成简洁的多段句。  
  - `baseName`、`treatmentStandardsRoutes`、`mixAndLocationTypes` 绝不能留空；若源数据确实缺失相关信息，需要根据上下文合理总结，不可复制 “Unknown”“Generic data”。  

## 2. FlowDataSet 要求

- 结构和命名空间同上。
- `flowInformation.dataSetInformation.common:UUID` ：沿用 JSON-LD `@id`。  
- `name.baseName / treatmentStandardsRoutes / mixAndLocationTypes / flowProperties`：全部多语言。  
- `common:synonyms`:若 JSON-LD 数据中有别名信息，可填入此字段，使用多语言格式；若没有则留空，**不要**随意生成。  
- `common:generalComment`: 2–4 句描述流的性质、用途、来源，可直接引用 JSON-LD `description`，但禁止写“Converted from OpenLCA JSON-LD.” 等模板句。
- `technology.technologicalApplicability`：多语言列表，可复用 JSON-LD 文本。  
- `classificationInformation`：使用 `tidas_flows_product_category.json` 的编码路径（0→4 层）。必须在 Stage 1 直接输出 Tiangong 官方分类路径，不允许交给后续阶段再补。结合 JSON-LD 的 `name`、 `description`  、 `category` 字段，选择最贴切的产品分类。流程会逐层向你提供候选代码，请在每一层只选一个候选，直到补全 0–4 层路径。
  - 分类不得留空，也不得使用 schema 外的临时代码；一旦 LLM 输出未知的 `@classId`，Stage 1 会立即报错。
- `quantitativeReference.referenceToReferenceFlowProperty`：指向 `flowProperties.flowProperty` 的首个 `@dataSetInternalID`。  
- `geography.locationOfSupply`：严格遵守`scripts/list_location_children.py`及`src/tidas/schemas/tidas_locations_category.json`文档要求，根据 JSON-LD 的地理线索选择代码；若源数据没有任何地理信息，则省略此字段，禁止填 `GLO`。   
- `flowProperties.flowProperty[]`：逐条镜像 JSON-LD `flowProperties` 列表，保留原有 flow property UUID / 名称，勿创造额外属性。若源文件完全缺失该列表，则视为输入不合规，Stage 1 应返回错误而不是兜底。  
- `modellingAndValidation.LCIMethod.typeOfDataSet` ∈ {Product flow, Waste flow, Elementary flow}。  
  - `complianceDeclarations.compliance.common:referenceToComplianceSystem`：引用 ILCD entry-level（UUID `d92a1a12-2545-49e2-a585-55c259997756`, 版本 `20.20.002`, URI `../sources/d92a1a12-2545-49e2-a585-55c259997756_20.20.002.xml`）。  
- `administrativeInformation`：与流程相同的 ILCD format 引用、Tiangong contact、timeStamp、license、owner。`common:dataSetVersion` 固定 `01.01.000`，`common:permanentDataSetURI = https://lcdn.tiangong.earth/showFlow.xhtml?uuid=<uuid>&version=<version>`。

## 3. SourceDataSet 要求

- `sourceInformation.dataSetInformation.common:UUID`：沿用 JSON-LD `@id`。  
- `common:shortName`：逐字复制 JSON-LD `name`（如果提供多个名称，按原顺序连接）。  
- `classificationInformation`：引用 `tidas_sources_category.json` level 0（例如 Publications and communications）。  
- `sourceCitation`：完整引用文本；`publicationType` 取 ILCD 枚举（文章、章节、软件等）。  
- `sourceDescriptionOrComment`：多语言摘要。  
- `referenceToContact`：引用 Tiangong 联系人（同上）。  
- `sourceDescription`（如有：`title`、`year`、`referenceToPublisher` 等）可按 JSON-LD 原值补充。  
- `administrativeInformation`：ILCD format 引用 + timeStamp；`publicationAndOwnership` 固定版本/URI/owner/license，与流程一致。

---

## 4. 分类与地理辅助资源

- **流程分类**：权威来源为 `src/tidas/schemas/tidas_processes_category.json`。需要逐级选择分类时，可先运行 `uv run python scripts/list_process_category_children.py <code>`（`<code>` 为空输出顶层，例如 `uv run python scripts/list_process_category_children.py 01``），或使用 `tiangong_lca_spec.tidas.get_schema_repository().resolve_with_references("tidas_processes_category.json")` 读入局部节点，再把候选列表（code + description）贴给 LLM。无论自动还是人工辅助，必须逐层确认（`You are selecting level {level}...`），单次只决定一层，避免一次性输出整条路径。
- **产品流分类**：参考 `src/tidas/schemas/tidas_flows_product_category.json`，可通过 `uv run python scripts/list_product_flow_category_children.py <code>` 获取候选。
- **初级流分类**：参考 `src/tidas/schemas/tidas_flows_elementary_category.json`，可运行 `uv run python scripts/list_elementary_flow_category_children.py <code>`。
- **地理编码**：使用 `src/tidas/schemas/tidas_locations_category.json`，命令 `uv run python scripts/list_location_children.py <code>`（如 `uv run python scripts/list_location_children.py CN`）按需展开。向 LLM 提供选项时，只粘贴与当前流程相关的分支，避免整棵树。
- **Stage 1/2 协作**：Stage 1 LLM 根据 `name.*`、`common:generalComment` 等上下文决定流程/流的分类与地理；Stage 2 仅做格式校验，不会重新分类。因此分类和位置判断必须在 Stage 1 就完成。

---

## 5. Stage 2 & Stage 3 summary

- **Stage 2**（`stage2_jsonld_validate.py`）：
  1. 读取 Stage 1 缓存（`stage1_process_blocks.json`、`stage1_flow_blocks.json`、`stage1_source_blocks.json`）。  
  2. 使用 `UUIDMapper` 重映射 UUID，写入 `artifacts/<run>/exports/processes|flows|sources/`。  
  3. 运行 JSON Schema（规划中）+ `uv run tidas-validate -i exports`，对少量可修复问题尝试自动修复后重跑验证。  
  4. 生成 `workflow_result.json`、`tidas_validation.json`；若未传 `--skip-auto-publish` 且验证通过，则自动触发 Stage 3。
- **Stage 3**（`stage3_jsonld_publish.py`）：读取 `exports/`，按 flows→processes→sources 顺序发布到数据库，支持 dry-run（`--commit` 控制），并将摘要写入 `cache/published.json`。

---

## 6. Validation & publish tips

- 随时运行 `uv run tidas-validate -i artifacts/<run>/exports` 复查；结果位于 `artifacts/<run>/cache/tidas_validation.json`。
- 若需重新导出并校验：执行 `uv run python scripts_jsonld/stage2_jsonld_validate.py --run-id <run> --clean-exports`。
- 手动发布或 dry-run：`uv run python scripts_jsonld/stage3_jsonld_publish.py --run-id <run>`（正式提交需附加 `--commit`）。
- 关键日志：`tidas_validation.cli_exit_nonzero`、`jsonld_stage2.uuid_map`、`jsonld_stage3.publish_*` 有助于快速定位问题。

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
- Flow property / unit group 必须使用 Tiangong 官方 UUID，不要自造数据集。  
- Stage 2/Stage 3 仅做 UUID 重映射、验证与发布，不会补齐缺失字段。  
- 输出必须是合法 JSON，禁止 Markdown、注释或 `NA`。
