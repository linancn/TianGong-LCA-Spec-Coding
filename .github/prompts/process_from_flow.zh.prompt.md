# Process From Flow 工作流说明

本文件说明 `src/tiangong_lca_spec/process_from_flow/service.py` 中的 LangGraph 工作流，聚焦按步骤优化输出结构与约束。

## 目标与输入
- 目标：从参考 flow 数据集（ILCD JSON）推导对应的 process 数据集（ILCD 格式），exchange 里的 flow uuid/shortDescription 必须来自 `search_flows` 结果，未命中时才用占位符。
- 核心入口：`ProcessFromFlowService.run(flow_path, operation="produce", initial_state=None, stop_after=None)`。
- 依赖：LLM 为必选，用于技术路径识别、单元过程拆分、交换生成与候选选择；同时依赖 flow 搜索函数 `search_flows`（可注入自定义）与候选选择器（建议 LLM 版本）。
- `stop_after` 支持 `"references"|"tech"|"processes"|"exchanges"|"matches"|"sources"`，用于调试时提前终止。

## 状态字段
工作流以状态字典传递数据，关键字段：
- `flow_path`：输入文件路径。
- `flow_dataset` / `flow_summary`：解析后的原始 flow 与摘要（名称、分类、注释、UUID、版本）。
- `technical_description` / `assumptions` / `scope`：技术路径与约束（来自选定路径的摘要与假设）。
- `technology_routes`：第一步输出的多条技术路径（route_id/route_name/route_summary/关键输入输出等）。
- `process_routes` / `selected_route_id`：第二步按路径拆分的单元过程列表与选定路径。
- `processes`：单元过程计划（来自选定路径；有序列表，含 `reference_flow_name`、`name_parts`、结构化字段与 exchange 关键词）。
- `process_exchanges`：每个过程的交换清单（仅结构，无匹配信息）。
- `matched_process_exchanges`：为每个交换附上 flow 搜索结果与已选候选，并回填 uuid/shortDescription。
- `process_datasets`：最终生成的 ILCD process 数据集。
- `source_datasets`：由检索文献生成的 ILCD source 数据集。
- `source_references`：可直接挂载到 process / exchange `referenceToDataSource` 的 source 引用列表。
- `step_markers`：阶段标记（step1/step2/step3），用于人工查阅。

## 节点顺序与行为
各节点会首先检查相应字段是否已存在，避免重复工作。
- 0) load_flow：读取 `flow_path` JSON，生成 `flow_summary`（多语言名称、分类、通用注释等）；该 flow 作为 reference flow。
- 1a) reference_search：检索技术工艺路径相关文献（topK=10），写入 `scientific_references.step_1a_reference_search`。
- 1b) reference_fulltext：基于 Step 1a 返回的 DOI 合并去重，使用 DOI 过滤检索全文并写入 `scientific_references.step_1b_reference_fulltext`（`filter: {"doi": [...]}` + `topK=1` + `extK`）。
- 1b-optional) reference_usability：可选筛选步骤，判断 Step 1b 的 DOI 全文是否足以支撑后续过程拆分与交换生成；若仅有 LCIA 影响指标或缺少任何 LCI 定量表格行，则判为不可用并输出 `scientific_references.usability`；同时记录 `si_hint` 用于标记全文是否提示 Supporting Information/Appendix 可能包含清单表。
- 1c) reference_clusters：基于 Step 1b 全文与可用性筛选结果，按系统边界/主流程/中间流一致性聚类 DOI，输出主干候选与可补充集合（写入 `scientific_references.step_1c_reference_clusters`，并保留 `reference_summaries` 的 `si_hint`/`si_reason` 以便后续 SI 筛选）。
- 1d) reference_si_download_and_parse：若 `si_hint` 为 `likely/possible` 或正文给出明确 SI 链接，则下载 SI 原件并登记元数据；原件保存于 `artifacts/process_from_flow/<run_id>/input/si/`，解析结果保存于 `input/si_mineru/`。
  - PDF/图像：调用 `scripts/origin/mineru_for_process_si.py` 拆分并输出 JSON（保留页码与表格块）。
  - 表格/文本（xls/xlsx/csv/doc/docx/txt/md）：保留原件并记录可读提取文本/表格快照（可用 mineru 或直接读取文本）。
  - 元数据建议包含 `doi`/`si_url`/`file_type`/`local_path`/`mineru_output_path`/`status`/`error`，便于溯源与重跑。
- 1e) reference_usage_tagging：综合正文与 SI，标注文献用途（`tech_route`/`process_split`/`exchange_values`/`background_only`），写入 `reference_summaries[*].usage_tags` 或独立索引表。
- 1f) reference_sources：基于 Step 1a/1b/Step 2/Step 3 的检索结果，使用 `tidas_sdk.create_source` 生成 ILCD source 数据集，写入 `source_datasets`，并生成 `source_references` 以便后续 process 引用。
- 斩杀线判定：按“斩杀线规则”评估覆盖率与检索收益，决定是否继续检索或转入 `expert_judgement`。
- 若 Step 1a/1b/1c 任一没有可用参考文献（包含可用性筛选结果全部为不可用的情况），则 Step 1-3 进入 common sense 模式：不再使用文献证据，Step 2/Step 3 不再发起检索；但仍需在过程与 exchange 中标注数据来源为 `expert_judgement` 并写明理由。
- 1) 识别技术路径（Step 1）：基于 reference flow + Step 1c 主干候选（必要时结合 si_snippets）输出所有可能的技术/工艺路径（route1/route2...），每条路径给出 route_summary、关键输入/输出、关键单元过程、假设与范围；必须附 `supported_dois` 与 `route_evidence`，仅做结构化路线归纳，不替代证据明细。
- 2) 路径内拆分单元过程（Step 2）：针对每条路径输出单元过程列表，并保证链式顺序（第 i 个过程的 `reference_flow_name` 必须作为第 i+1 个过程的 exchange input，最后一个过程直接生产/处置 `load_flow`）。每个过程输出结构化字段：
  - 结构化字段：`technology` / `inputs` / `outputs` / `boundary` / `assumptions`。
  - `inputs`/`outputs` 每行以 `f1:`/`f2:` 标记独立 flow（链式中间流在相邻过程输入输出中应一致）。
  - 交换关键词：`exchange_keywords.inputs` / `exchange_keywords.outputs`（用于 flow 搜索）。
  - 名称模块：`name_parts` 包含 `base_name` / `treatment_and_route` / `mix_and_location` / `quantitative_reference`。
  - 量纲表达：`quantitative_reference` 必须为数值表达（如 `1 kg of <reference_flow_name>` / `1 unit of <reference_flow_name>`）。
  - 显式主输出：`reference_flow_name` 为该过程主输出流名称，并与链式输入严格一致。
  - `processes` 先作为可迭代的过程计划，记录结构化事实、来源与假设；Step 5 再统一落到 ILCD 结构，便于后续新增文献时增量完善。
  - 若 exchange 数值仅覆盖多个过程的聚合层级，需在 `assumptions` 中标记 `aggregation_scope`/`allocation_strategy`，必要时调整过程粒度或补充中间推导步骤。
  - 在 `technology`/`boundary`/`assumptions` 中记录引用来源（DOI + SI 文件/表格/页码），用于后续 exchange 溯源。
- 3) generate_exchanges：调用 `EXCHANGES_PROMPT` 产出各过程的输入/输出交换（每个过程必须标记 `is_reference_flow` 对应 `reference_flow_name`；生产用 Output，处置/处理用 Input 作为参考流）。exchangeName 需可搜索，禁止复合流（能量/排放/人工/辅料需拆分为具体项）；补充 unit 与 amount（未知时用占位符）；证据筛选以 Step 1c 的主干候选为准。
  - 对排放类 exchange 自动补充介质标签（`to air` / `to water` / `to soil`），降低检索歧义。
  - 为 exchange 增加 `flow_type`（product/elementary/waste/service）与 `search_hints` 别名。
  - 每条 exchange 必须附 `data_source`/`evidence` 字段，记录 DOI/正文/ SI 文件与表格位置；若为推断或补全，标记 `source_type=expert_judgement` 并说明依据。
- 3b) exchange_amounts：调用 `EXCHANGE_VALUE_PROMPT` 基于正文与 SI 抽取可核查的 exchange 数值与单位；仅使用显式证据，无法定位时保持占位符并保留 `expert_judgement`。抽取结果会回填 `process_exchanges`，用于后续 `meanAmount/resultingAmount`，并据 evidence 把 exchange 的 `referencesToDataSource` 关联到 `source_references`。
- 4) match_flows：对每个交换执行 flow 搜索（最多保留前 10 个候选并列为 list），用 LLM 选择器挑选最合适的候选，不使用相似度兜底；必须记录决策理由与未匹配项；exchange 的 flow uuid/shortDescription 必须来自已选候选。
  - match_flows 仅补充流匹配信息，不得覆盖/丢失 `data_source`/`evidence`。
- 5) build_process_datasets：组合前述信息生成 ILCD process 数据集（参考流方向随 operation 调整，若提供 Translator 则补充中文多语字段）：
  - 使用 `ProcessClassifier` 进行分类，失败时落到默认 Manufacturing。
  - 根据 `match_flows` 结果引用真实 flow；缺失时创建占位 flow 引用，禁止凭空生成 uuid/shortDescription。
  - 强制存在参考流交换；空量值回退为 `"1.0"`。
  - 自动填充功能单位、时间/地域、合规声明、数据录入与版权块；使用 `tidas_sdk.create_process` 进行模型校验（失败仅记录警告）。
  - `process_datasets` 为最终落库结构，`processes`/`process_exchanges` 作为可迭代中间文档；新增文献时优先更新中间文档并重建数据集。

## 产出与调试
- 正常运行返回完整状态，其中 `process_datasets` 为生成结果（可直接写出或继续处理），`source_datasets` 可写出到 `exports/sources/`。
- 如需从缓存 state 补写 source 文件：`uv run python scripts/origin/process_from_flow_build_sources.py --run-id <run_id>`。
- CLI 仅写入 `artifacts/process_from_flow/<run_id>/`，包含 `input/`、`cache/` 与 `exports/`，其中状态文件保存在 `cache/process_from_flow_state.json`。
- 调试时可配合 `stop_after` 查看中间态，例如设置为 `"matches"` 只跑到流匹配阶段。

## 发布流程（Flow/Process 入库）
发布建议顺序：先补齐/更新 flows，再发布 processes，避免 process 引用的 flow 不存在。

### 依赖与配置
- 入口类：`src/tiangong_lca_spec/publishing` 内的 `FlowPublisher` / `ProcessPublisher` / `DatabaseCrudClient`。
- MCP 服务：需在 `.secrets/secrets.toml` 配置 `tiangong_lca_remote`（`Database_CRUD_Tool` 使用该服务名）。
- LLM：可选；用于 flow 类型与产品分类的推断（无 LLM 时走规则/默认兜底）。

### Step 1：准备对齐结构（供 FlowPublisher 使用）
- `FlowPublisher.prepare_from_alignment()` 需要 alignment 风格输入（与 Stage 3 对齐结果一致）：
  - 结构：`[{ "process_name": "...", "origin_exchanges": { "<exchangeName>": [<exchange dict>, ...] } }]`。
  - 每个 exchange dict 至少包含：`exchangeName`、`exchangeDirection`、`unit`、`meanAmount|resultingAmount|amount`、`generalComment`（可包含 `FlowSearch hints:`）、`referenceToFlowDataSet`（占位符需含 `unmatched:placeholder`）。
  - 若已有匹配候选，建议补齐 `matchingDetail.selectedCandidate`（可由 `flow_search.selected_uuid/selected_reason/candidates` 映射），便于流属性与分类选择。

### Step 2：发布/更新 flows
- 调用 `FlowPublisher.prepare_from_alignment()` 生成 `FlowPublishPlan`：
  - `referenceToFlowDataSet` 为占位符 → 生成 **insert** 计划。
  - 已匹配但缺少 flow property → 生成 **update** 计划（版本自动 +1）。
  - Elementary flow 不新建（强制复用已存在流）；Product/Waste flow 会生成 ILCD flow。
- 自动推断逻辑（来自 `publishing/crud.py`）：
  - `FlowTypeClassifier`：LLM 优先，失败回退规则；允许值为 Product/Elementary/Waste flow。
  - `FlowProductCategorySelector`：调用 `scripts/md/list_product_flow_category_children.py` 逐层挑选产品分类；失败则回退默认分类。
  - `FlowPropertyRegistry`：默认用 Mass（可通过 `FlowPropertyOverride` 按 process/exchange 覆盖）。
- `FlowPublisher.publish()` 默认 dry-run；正式提交需设置 `dry_run=False`。
- 发布后使用 `FlowPublishPlan.exchange_ref` 替换 process 数据集中的占位 `referenceToFlowDataSet`（移除 `unmatched:placeholder`），确保引用指向真实 UUID/版本。

### Step 3：发布 processes
- 调用 `ProcessPublisher.publish(process_datasets)`，默认 dry-run；提交时设置 `dry_run=False`。
- 发布逻辑：先尝试 insert，失败时自动 fallback 到 update。
- 发布完毕务必 `close()` 释放 MCP 连接。

## 文献服务配置与运行

### 检索策略
- 基于 flow 名称、操作类型（produce/treat）和技术描述构建搜索查询。
- Step 1b 使用 `filter: {"doi": [...]}` + `topK=1` + `extK` 拉取全文（默认 `extK=200`）；`query` 不能为空，可用合并后的 content 或简短 summary。
- Step 1c 输出 `clusters` + `primary_cluster_id` + `selection_guidance`，供 Step 2/Step 3 做证据选择与融合。

**资源管理：**
- MCP 客户端在 LLM 可用时自动创建
- 工作流结束时自动关闭连接
- 检索失败不会阻塞工作流执行（记录警告并继续）

### 配置要求

需要在 `.secrets/secrets.toml` 中配置 `tiangong_kb_remote` 服务：

```toml
[tiangong_kb_remote]
transport = "streamable_http"
service_name = "TianGong_KB_Remote"
url = "https://mcp.tiangong.earth/mcp"
api_key = "<YOUR_TG_KB_REMOTE_API_KEY>"
timeout = 180
```

如果不配置此服务或 API key 无效，工作流将回退到仅使用 LLM common sense。

### 日志标识

- `process_from_flow.mcp_client_created`：MCP 客户端创建成功
- `process_from_flow.search_references`：文献检索成功（记录查询和结果数量）
- `process_from_flow.search_references_failed`：文献检索失败（记录错误但不中断）
- `process_from_flow.mcp_client_closed`：MCP 客户端正常关闭

### 性能影响

- 每次文献检索约 1-2 秒
- Step 1b 的 DOI 全文拉取耗时与 DOI 数量和 extK 相关
- 完整工作流增加约 3-6 秒（不含额外全文抓取时长）
- 不影响工作流可靠性

### 测试

运行测试脚本验证功能：

```bash
uv run python test/test_scientific_references.py
```

### 文献可用性筛选（Reference Usability Screening）

- 可选步骤：针对 Step 1b 的 DOI 全文结果，判断是否足以支撑 Step 1c 的技术路径/过程拆分/交换清单需求。
- 若全文仅包含 LCIA 影响指标（如 ADP/AP/GWP/EP/PED/RI）或单位为 `kg CO2 eq`/`kg SO2 eq`/`kg Sb eq`/`kg PO4 eq` 等，而没有任何 LCI 物理清单行（kg、g、t、m2、m3、pcs、kWh、MJ 作为清单单位），一律标记为 `unusable`。
- 当正文提示 Supporting Information/补充材料/Appendix 可能包含清单表时，记录 `si_hint`（`likely|possible|none`）与 `si_reason` 以便后续决定是否下载 SI；若正文本身没有 LCI 表，仍保持 `decision=unusable`。
- Prompt 模板：`src/tiangong_lca_spec/process_from_flow/prompts.py` 中的 `REFERENCE_USABILITY_PROMPT`。
- 脚本：`uv run python scripts/origin/process_from_flow_reference_usability.py --run-id <run_id>`。
- 输出位置：`process_from_flow_state.json` 的 `scientific_references.usability`。

## 使用建议
- 确保 LLM 配置正确；未配置 LLM 时不应运行该流程。
- 配置 `tiangong_kb_remote` 服务以启用科学文献集成（可选但推荐）。
- 在自定义 `flow_search_fn` 或选择器时保持返回/入参协议一致（`FlowQuery` → `(candidates, unmatched)`，候选含 uuid/base_name 等字段）。
- CLI 默认会补充中文翻译（可用 `--no-translate-zh` 跳过）。

## 斩杀线规则（Stop Rules）
- 斩杀线优先依据覆盖率，而非检索次数；流程仅在节点中调用本规则，具体阈值可在此处独立更新。
- 覆盖率定义：
  - `process_coverage` = 已有明确证据的过程数 / 计划过程总数。
  - `exchange_value_coverage` = 关键 exchange 中有量值证据的条目数 / 关键 exchange 总数。
- 默认阈值（可调整）：
  - `process_coverage >= 0.5` 且 `exchange_value_coverage >= 0.6` 时，停止继续扩检。
  - 连续两次新增文献带来的覆盖率提升 < 0.1，停止继续扩检。
- 若未达阈值但已出现 `unusable` 结论且 `si_hint=none`，转入 `expert_judgement` 补齐并记录原因。
- 关键 exchange 建议包含：参考流、主能耗、主原料、主要排放（正文或 SI 明确点名的前 3-5 项）。
