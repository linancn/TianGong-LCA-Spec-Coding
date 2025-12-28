# Process From Flow 工作流说明

本文件说明 `src/tiangong_lca_spec/process_from_flow/service.py` 中的 LangGraph 工作流，聚焦按步骤优化输出结构与约束。

## 目标与输入
- 目标：从参考 flow 数据集（ILCD JSON）推导对应的 process 数据集（ILCD 格式），exchange 里的 flow uuid/shortDescription 必须来自 `search_flows` 结果，未命中时才用占位符。
- 核心入口：`ProcessFromFlowService.run(flow_path, operation="produce", initial_state=None, stop_after=None)`。
- 依赖：LLM 为必选，用于技术路径识别、单元过程拆分、交换生成与候选选择；同时依赖 flow 搜索函数 `search_flows`（可注入自定义）与候选选择器（建议 LLM 版本）。
- `stop_after` 支持 `"references"|"tech"|"processes"|"exchanges"|"matches"`，用于调试时提前终止。

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
- `step_markers`：阶段标记（step1/step2/step3），用于人工查阅。

## 节点顺序与行为
各节点会首先检查相应字段是否已存在，避免重复工作。
- 0) load_flow：读取 `flow_path` JSON，生成 `flow_summary`（多语言名称、分类、通用注释等）；该 flow 作为 reference flow。
- 1) 识别技术路径（Step 1）：基于 reference flow 输出所有可能的技术/工艺路径（route1/route2...），每条路径给出 route_summary、关键输入/输出、关键单元过程、假设与范围。
- 2) 路径内拆分单元过程（Step 2）：针对每条路径输出单元过程列表，并保证链式顺序（第 i 个过程的 `reference_flow_name` 必须作为第 i+1 个过程的 exchange input，最后一个过程直接生产/处置 `load_flow`）。每个过程输出结构化字段：
  - 结构化字段：`technology` / `inputs` / `outputs` / `boundary` / `assumptions`。
  - `inputs`/`outputs` 每行以 `f1:`/`f2:` 标记独立 flow（链式中间流在相邻过程输入输出中应一致）。
  - 交换关键词：`exchange_keywords.inputs` / `exchange_keywords.outputs`（用于 flow 搜索）。
  - 名称模块：`name_parts` 包含 `base_name` / `treatment_and_route` / `mix_and_location` / `quantitative_reference`。
  - 量纲表达：`quantitative_reference` 必须为数值表达（如 `1 kg of <reference_flow_name>` / `1 unit of <reference_flow_name>`）。
  - 显式主输出：`reference_flow_name` 为该过程主输出流名称，并与链式输入严格一致。
- 3) generate_exchanges：调用 `EXCHANGES_PROMPT` 产出各过程的输入/输出交换（每个过程必须标记 `is_reference_flow` 对应 `reference_flow_name`；生产用 Output，处置/处理用 Input 作为参考流）。exchangeName 需可搜索，禁止复合流（能量/排放/人工/辅料需拆分为具体项）；补充 unit 与 amount（未知时用占位符）。
  - 对排放类 exchange 自动补充介质标签（`to air` / `to water` / `to soil`），降低检索歧义。
  - 为 exchange 增加 `flow_type`（product/elementary/waste/service）与 `search_hints` 别名。
- 4) match_flows：对每个交换执行 flow 搜索（最多保留前 10 个候选并列为 list），用 LLM 选择器挑选最合适的候选，不使用相似度兜底；必须记录决策理由与未匹配项；exchange 的 flow uuid/shortDescription 必须来自已选候选。
- 5) build_process_datasets：组合前述信息生成 ILCD process 数据集（参考流方向随 operation 调整，若提供 Translator 则补充中文多语字段）：
  - 使用 `ProcessClassifier` 进行分类，失败时落到默认 Manufacturing。
  - 根据 `match_flows` 结果引用真实 flow；缺失时创建占位 flow 引用，禁止凭空生成 uuid/shortDescription。
  - 强制存在参考流交换；空量值回退为 `"1.0"`。
  - 自动填充功能单位、时间/地域、合规声明、数据录入与版权块；使用 `tidas_sdk.create_process` 进行模型校验（失败仅记录警告）。

## 产出与调试
- 正常运行返回完整状态，其中 `process_datasets` 为生成结果（可直接写出或继续处理）。
- CLI 仅写入 `artifacts/process_from_flow/<run_id>/`，包含 `input/`、`cache/` 与 `exports/`，其中状态文件保存在 `cache/process_from_flow_state.json`。
- 调试时可配合 `stop_after` 查看中间态，例如设置为 `"matches"` 只跑到流匹配阶段。

## 科学文献集成（Scientific References Integration）

从 2025-12-26 版本开始，工作流集成了 `tiangong_kb_remote` 的 `Search_Sci_Tool`，在 Step 1a/1b/1c 阶段自动检索相关科学文献，让 LLM 基于真实的科学参考资料而非仅凭 common sense 做出决策。

### 功能特点

**自动检索时机：**
- **Step 1a (reference search)**：检索技术工艺路径相关文献（topK=10），写入 `scientific_references.step_1a_reference_search`
- **Step 1b (fulltext fetch)**：基于 Step 1a 返回的 `source` 里的 DOI 合并去重，使用 DOI 过滤检索全文并写入 `scientific_references.step_1b_reference_fulltext`
- **Step 1c (reference clustering)**：基于 Step 1b 的全文与可用性筛选结果，按系统边界/主流程/中间流一致性聚类 DOI，输出主干候选与可补充集合（写入 `scientific_references.step_1c_reference_clusters`）
- **Step 2 (split_processes)**：检索单元过程分解与清单文献，用于识别/拆分单元过程
- **Step 3 (generate_exchanges)**：检索库存数据和排放因子文献，用于确认 exchange 的 flow 名称与数值

**查询构建策略：**
- 基于 flow 名称、操作类型（produce/treat）和技术描述构建搜索查询
- 每次检索默认返回 top 10 篇最相关文献
- Step 1b 通过 `filter: {"doi": [...]}` + `topK=1` + `extK` 拉取全文（默认 `extK=200`）
- `query` 不能为空，可使用合并后的 content 或简短 summary；无可用内容时可退回到基础查询或 `doi <id>`
- Step 1c 使用 LLM 输出 `clusters` + `primary_cluster_id` + `selection_guidance`，用于 Step 2/Step 3 的证据选择与融合

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
- Prompt 模板：`src/tiangong_lca_spec/process_from_flow/prompts.py` 中的 `REFERENCE_USABILITY_PROMPT`。
- 脚本：`uv run python scripts/origin/process_from_flow_reference_usability.py --run-id <run_id>`。
- 输出位置：`process_from_flow_state.json` 的 `scientific_references.usability`。

## 使用建议
- 确保 LLM 配置正确；未配置 LLM 时不应运行该流程。
- 配置 `tiangong_kb_remote` 服务以启用科学文献集成（可选但推荐）。
- 在自定义 `flow_search_fn` 或选择器时保持返回/入参协议一致（`FlowQuery` → `(candidates, unmatched)`，候选含 uuid/base_name 等字段）。
- CLI 默认会补充中文翻译（可用 `--no-translate-zh` 跳过；`--io-root` 可指定 I/O 根目录）。
