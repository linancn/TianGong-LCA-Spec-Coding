# Process From Flow 工作流说明（LangGraph 核心 + Origin 编排）

## 总览
- 目标：以 reference flow（ILCD JSON）推导 process 数据集（ILCD 格式）；exchange 的 flow uuid/shortDescription 必须来自 `search_flows` 候选，未命中时才使用占位符。
- 范围：本文同时说明 `src/tiangong_lca_spec/process_from_flow/service.py` 的 LangGraph 核心流程，以及 `scripts/origin/` 的编排流程和相关工具。
- 主要产出：`process_datasets` / `source_datasets`，以及 `artifacts/process_from_flow/<run_id>/` 下的输入、缓存与导出文件。
- 参考图示：`PROCESS_FROM_FLOW_FLOWCHART.zh.md` / `PROCESS_FROM_FLOW_FLOWCHART.zh.svg`。

## 交叉参考
- CLI 入口：`scripts/origin/process_from_flow_langgraph.py` 与 `scripts/origin/process_from_flow_workflow.py`（详见模块文档与 `--help`）。

## 分层架构与主干流程
### 分层职责
- LangGraph 核心层：`ProcessFromFlowService` 负责“检索 → 路径 → 拆分 → 交换 → 匹配 → 数据集 → 占位符补全”的主干推理与结构化输出。
- Origin 编排层：`scripts/origin` 负责 SI 获取/解析、可用性/用途标注、运行与恢复、发布与清理。

### 主干流程（一步概览）
- Step 0 load_flow：解析 reference flow 并生成摘要。
- Step 1 references + tech routes：1a 检索 → 1b 全文 → 1c 聚类 → 技术路径输出。
- Step 2 split processes：路径内拆分单元过程，形成链式过程计划。
- Step 3 generate exchanges：生成每个过程的输入/输出 exchange。
- Step 3b enrich exchange amounts：从正文/SI 抽取或估算量值。
- Step 4 match flows：flow 搜索 + 候选选择，回填 uuid/shortDescription。
- Step 1f build sources：生成 source 数据集与引用。
- Step 5 build process datasets：输出最终 ILCD process 数据集。
- Step 6 resolve placeholders：对占位符 exchange 进行二次检索与筛选，回填匹配结果并输出占位符报告。

## LangGraph 核心工作流（ProcessFromFlowService）
### 入口与依赖
- 入口：`ProcessFromFlowService.run(flow_path, operation="produce", initial_state=None, stop_after=None)`。
- 依赖：LLM（路线/拆分/交换/候选选择）、flow 搜索 `search_flows`、候选选择器（推荐 LLM 版本），可选 Translator/MCP 客户端。
- `stop_after`：`references`/`tech`/`processes`/`exchanges`/`matches`/`sources`（CLI 额外支持 `datasets`；占位符补全在 datasets 之后执行，无单独 stop_after）。

### 节点细节（由粗到细）
0) load_flow
- 读取 `flow_path`，生成 `flow_dataset` 与 `flow_summary`（多语言名称、分类、注释、UUID、版本）。

1) references + tech routes
- 1a reference_search：检索技术路径文献 → `scientific_references.step_1a_reference_search`（默认 topK=10）。
- 1b reference_fulltext：DOI 去重后拉全文（`filter: {"doi": [...]}` + `topK=1` + `extK`）→ `scientific_references.step_1b_reference_fulltext`。
- 1c reference_clusters：按系统边界/主流程/中间流一致性聚类 → `scientific_references.step_1c_reference_clusters`。
- Step 1 技术路径输出：生成 `technology_routes`，包含 route_summary、关键输入输出、关键过程、假设/范围，且必须附 `supported_dois` 与 `route_evidence`。
- 若 Step 1a/1b/1c 无可用文献，则 Steps 1-3 转为 common sense，必须标注 `expert_judgement` 理由。

2) split_processes
- 每条路线拆分为有序单元过程，链式中间流必须一致，最后一个过程直接生产/处置 `load_flow`。
- 过程字段：`technology`/`inputs`/`outputs`/`boundary`/`assumptions` + `exchange_keywords`。
- `name_parts` 必含 `base_name`/`treatment_and_route`/`mix_and_location`/`quantitative_reference`，`quantitative_reference` 为数值表达。
- 证据为聚合层级时，在 `assumptions` 标记 `aggregation_scope`/`allocation_strategy`。
- 可用文献时会额外检索拆分证据，写入 `scientific_references.step2`。

3) generate_exchanges
- `EXCHANGES_PROMPT` 生成交换清单，`is_reference_flow` 与 `reference_flow_name` 对齐；生产用 Output，处置/处理用 Input。
- Exchange 名称必须可搜索且不复合；补全 unit/amount（缺失用占位符）。
- 对排放类自动补充介质标签（`to air`/`to water`/`to soil`），并标注 `flow_type` 与 `search_hints`。
- 每条 exchange 写入 `data_source`/`evidence`，推断项标记 `source_type=expert_judgement`。
- 可用文献时会额外检索交换证据，写入 `scientific_references.step3`。

3b) enrich_exchange_amounts
- `EXCHANGE_VALUE_PROMPT` 从正文与 SI 抽取可核查量值，写入 `value_citations`/`value_evidence` 并回填 amount/unit。
- 无证据时保留占位符；若具备边界/量纲信息可调用 `INDUSTRY_AVERAGE_PROMPT` 估算并记录 `scientific_references.industry_average`。
- 可缩放 exchange 依据 `basis_*` 进行换算并追加说明。

4) match_flows
- 对每个 exchange 执行 flow 搜索（候选最多 10 条），LLM 选择器挑选，禁止相似度兜底。
- 写入 `flow_search.query/candidates/selected_uuid/selected_reason/selector/unmatched`，并回填 uuid/shortDescription。
- 仅补充匹配信息，不覆盖 `data_source`/`evidence`。

1f) build_sources
- 基于检索结果生成 ILCD source 数据集（`tidas_sdk.create_source`），写入 `source_datasets` 与 `source_references`。
- 按 `usage_tagging`/Step 1c summaries/Step 1b usability/industry_average 推断用途，过滤保留被使用文献（非 `background_only`）。

5) build_process_datasets
- 生成 ILCD process 数据集（`operation` 决定参考流方向；可选 Translator 补充中文多语字段）。
- `ProcessClassifier` 分类失败回 Manufacturing；缺失 flow 用占位符，禁止凭空生成 uuid/shortDescription。
- 尝试 `DatabaseCrudClient.select_flow` 补齐 flow 版本/shortDescription 与 flowProperty/unit group。
- 强制参考流交换；空量值回退 `"1.0"`；`tidas_sdk.create_process` 做校验（失败仅记录警告）。
- exchange 的 `referencesToDataSource` 优先使用 `value_citations`/`value_evidence` 匹配，其余 evidence 聚合到 process 层。

6) resolve_placeholders（占位符补全，后处理）
- 仅在 `build_process_datasets` 之后运行：扫描 `referenceToFlowDataSet.unmatched:placeholder=true` 的 exchange。
- 对应回溯 `matched_process_exchanges`，取 exchangeName/Direction/unit/flow_type/search_hints/generalComment 构造二次 `flow_search` 查询。
- 过滤候选：优先匹配 flow_type；若为 elementary，则按介质（air/water/soil）过滤候选。
- 通过候选选择器（LLM/规则）二次选择；写回 `flow_search.secondary_query/resolution_*`，并更新 process_datasets 中占位符引用。
- 若仍未命中则保留占位符，并记录 `resolution_status/reason` 供人工复核。

## Origin 编排工作流（scripts/origin）
### 目标与顺序
- 目标：在 Step 1-3 前写回 SI/用途标注，保证提示词能读取 SI 证据。
- 编排顺序：
  Step 0 → Step 1a → Step 1b → 1b-usability → Step 1c → Step 1d → Step 1e → Step 1 → Step 2 → Step 3 → Step 3b → Step 4 → Step 1f → Step 5 → Step 6

### 核心脚本与工具
- `process_from_flow_workflow.py`：主编排脚本，负责前置 1b-usability/1d/1e 并 resume 主流程。
- `process_from_flow_langgraph.py`：LangGraph CLI（run/resume/cleanup/publish），支持 `--stop-after` 与 `--publish/--commit`。
- `process_from_flow_reference_usability.py`：Step 1b 可用性筛选（LCIA vs LCI）。
- `process_from_flow_download_si.py`：下载 SI 原件并写回元数据。
- `mineru_for_process_si.py`：解析 PDF/图像 SI 为 JSON 结构。
- `process_from_flow_reference_usage_tagging.py`：标注文献用途 `usage_tagging`。
- `process_from_flow_build_sources.py`：从缓存 state 补写 source 数据集。
- `process_from_flow_placeholder_report.py`：生成占位符补全报告（默认写入 `cache/placeholder_report.json` 并更新 state）。

### 运行要点
- `process_from_flow_workflow.py` 不支持 `--no-llm`（Step 1b/1e 需要 LLM）。
- `--min-si-hint` 控制 SI 下载阈值（none|possible|likely），可配 `--si-max-links`/`--si-timeout`。
- `process_from_flow_langgraph.py` 的 `--stop-after datasets` 等价完整跑完（含占位符补全）后写出 datasets；其他值会提前停并保存 state。
- 占位符补全默认仅执行一次；需重跑时手动清空 `placeholder_resolution_applied`/`placeholder_resolutions` 后再 `--resume`。

## 状态字段（state）
- 输入与上下文：`flow_path`、`flow_dataset`、`flow_summary`、`operation`、`scientific_references`。
- 路线与过程：`technology_routes`、`process_routes`、`selected_route_id`、`technical_description`、`assumptions`、`scope`、`processes`。
- 交换与匹配：`process_exchanges`、`exchange_value_candidates`、`exchange_values_applied`、`matched_process_exchanges`。
- 产出：`process_datasets`、`source_datasets`、`source_references`。
- 评估与标记：`coverage_metrics`、`coverage_history`、`stop_rule_decision`、`step_markers`、`stop_after`。
- 占位符补全：`placeholder_report`、`placeholder_resolutions`、`placeholder_resolution_applied`。

## SI 注入点（实际行为）
- Step 1：`TECH_DESCRIPTION_PROMPT` 读取 `si_snippets`。
- Step 2：`PROCESS_SPLIT_PROMPT` 读取 `si_snippets`。
- Step 3：`EXCHANGES_PROMPT` 读取 `si_snippets`。
- Step 3b：`EXCHANGE_VALUE_PROMPT` 读取 `fulltext_references` + `si_snippets`。
- Step 4/Step 5 不直接读取 SI。
- SI 必须在 Step 1 前写回 `process_from_flow_state.json`，否则需回跑 Step 1-3。

## 产出与调试
- 运行输出目录：`artifacts/process_from_flow/<run_id>/`（`input/`、`cache/`、`exports/`）。
- 状态文件：`cache/process_from_flow_state.json`。
- 占位符报告：`cache/placeholder_report.json`（来自 `resolve_placeholders` 或 `process_from_flow_placeholder_report.py`）。
- 恢复/补写：`uv run python scripts/origin/process_from_flow_langgraph.py --resume --run-id <run_id>`。
- 补写 source：`uv run python scripts/origin/process_from_flow_build_sources.py --run-id <run_id>`。
- 生成占位符报告：`uv run python scripts/origin/process_from_flow_placeholder_report.py --run-id <run_id>`（`--no-update-state` 可避免写回 state）。
- 仅发布已有 run：`uv run python scripts/origin/process_from_flow_langgraph.py --publish-only --run-id <run_id> [--publish-flows] [--commit]`。
- 清理旧 run：`uv run python scripts/origin/process_from_flow_langgraph.py --cleanup-only --retain-runs 3`。

## 发布流程（Flow/Source/Process）
发布顺序建议：flows → sources → processes，避免引用缺失。

### 依赖与配置
- 入口类：`FlowPublisher` / `ProcessPublisher` / `DatabaseCrudClient`。
- MCP 服务：`.secrets/secrets.toml` 配置 `tiangong_lca_remote`（`Database_CRUD_Tool`）。
- LLM 可选：用于 flow 类型与产品分类推断。

### Step 0：发布 sources（可选但推荐）
- `--publish/--publish-only` 会在发布 process 前先发布 sources。
- 仅发布在 process/exchange `referenceToDataSource` 中出现的 source UUID。

### Step 1：准备对齐结构（供 FlowPublisher 使用）
- 结构：`[{ "process_name": "...", "origin_exchanges": { "<exchangeName>": [<exchange dict>, ...] } }]`。
- 每个 exchange dict 至少包含：`exchangeName`、`exchangeDirection`、`unit`、`meanAmount|resultingAmount|amount`、`generalComment`、`referenceToFlowDataSet`。
- 可补充 `matchingDetail.selectedCandidate`（由 `flow_search` 结果映射）以便分类与流属性选择。

### Step 2：发布/更新 flows
- `FlowPublisher.prepare_from_alignment()` 生成 `FlowPublishPlan`：
  - 占位 `referenceToFlowDataSet` → insert。
  - 已匹配但缺少 flow property → update（版本自动 +1）。
  - Elementary flow 不新建；Product/Waste flow 生成 ILCD flow。
- 自动推断逻辑：
  - `FlowTypeClassifier`：LLM 优先，失败回退规则。
  - `FlowProductCategorySelector`：逐层选择产品分类。
  - `FlowPropertyRegistry`：默认 Mass（可按 exchange 覆盖）。
- 发布后使用 `FlowPublishPlan.exchange_ref` 替换 process 数据集中的占位引用。

### Step 3：发布 processes
- `ProcessPublisher.publish(process_datasets)` 默认 dry-run；`--commit` 实际写入。
- 发布完毕务必 `close()` 释放 MCP 连接。

## 文献服务配置与运行
### 检索策略
- 基于 flow 名称、operation、技术描述构建 query。
- Step 2/Step 3 可追加检索，写入 `scientific_references.step2/step3`。
- Step 1b 使用 `filter: {"doi": [...]}` + `topK=1` + `extK` 拉取全文（默认 `extK=200`）。

### 配置要求
需要在 `.secrets/secrets.toml` 配置 `tiangong_kb_remote`：

```toml
[tiangong_kb_remote]
transport = "streamable_http"
service_name = "TianGong_KB_Remote"
url = "https://mcp.tiangong.earth/mcp"
api_key = "<YOUR_TG_KB_REMOTE_API_KEY>"
timeout = 180
```

若未配置或 API key 无效，工作流将回退到仅使用 LLM common sense。

### 日志标识
- `process_from_flow.mcp_client_created`：MCP 客户端创建成功。
- `process_from_flow.search_references`：文献检索成功（记录查询与结果数）。
- `process_from_flow.search_references_failed`：文献检索失败（记录错误但不中断）。
- `process_from_flow.mcp_client_closed`：MCP 客户端正常关闭。

### 性能影响
- 每次文献检索约 1-2 秒。
- Step 1b 全文拉取耗时与 DOI 数量、extK 相关。
- 完整工作流增加约 3-6 秒（不含额外全文抓取时长）。

### 测试
```bash
uv run python test/test_scientific_references.py
```

### 文献可用性筛选（Reference Usability Screening）
- 可选步骤：判断 Step 1b 全文是否足以支撑路径拆分/交换生成。
- 若全文仅包含 LCIA 影响指标（如 ADP/AP/GWP/EP/PED/RI）或单位为 `kg CO2 eq`/`kg SO2 eq`/`kg Sb eq`/`kg PO4 eq` 等，而没有任何 LCI 物理清单行（kg、g、t、m2、m3、pcs、kWh、MJ），一律标记为 `unusable`。
- 当正文提示 Supporting Information/Appendix 可能包含清单表时，记录 `si_hint`（`likely|possible|none`）与 `si_reason`；若正文本身无 LCI 表，仍保持 `decision=unusable`。
- Prompt 模板：`src/tiangong_lca_spec/process_from_flow/prompts.py` 中的 `REFERENCE_USABILITY_PROMPT`。
- 脚本：`uv run python scripts/origin/process_from_flow_reference_usability.py --run-id <run_id>`。
- 输出：`process_from_flow_state.json` 的 `scientific_references.usability`。

## 使用建议
- 确保 LLM 配置有效；`process_from_flow_workflow.py` 不支持 `--no-llm`。
- 自定义 `flow_search_fn` 或选择器时保持协议一致（`FlowQuery` → `(candidates, unmatched)`）。
- CLI 默认补充中文翻译，可用 `--no-translate-zh` 跳过。

## 斩杀线规则（Stop Rules）
- 斩杀线优先依据覆盖率，而非检索次数；流程仅在节点中调用本规则，阈值可独立更新。
- 覆盖率定义：
  - `process_coverage` = 已有明确证据的过程数 / 计划过程总数。
  - `exchange_value_coverage` = 关键 exchange 中有量值证据的条目数 / 关键 exchange 总数。
- `stop_rule_decision` 记录 `should_stop/action/reason/coverage_delta`，`coverage_history` 记录每次评估时间点。
- 默认阈值（可调整）：
  - `process_coverage >= 0.5` 且 `exchange_value_coverage >= 0.6` 时停止扩检。
  - 本次评估相对上次覆盖率提升 < 0.1，停止扩检。
- 若未达阈值但已出现 `unusable` 且 `si_hint=none`，转入 `expert_judgement` 并记录原因。
- key exchange 判定：显式 `is_key_exchange`/`isKeyExchange`、`is_reference_flow`、`flow_type=elementary`，或输入侧能耗（electricity/diesel/gasoline/heat 等）；若无 key exchange，则用全部 exchange 作为 key exchange 计算覆盖率。
