# 天工 LCA 工作流（Codex 分阶段指引）

本说明汇总工作流相关信息：模块职责、分阶段脚本、核心数据结构与关键校验要点，帮助 Codex 在多阶段协作中保持一致行为。

## 0. 执行约定（避免无效迭代）
- **先读原始资料与脚本参数**：开始编写或调用脚本前，快速确认 `scripts/stage*.py --help`，避免遗漏必需参数或产物路径。
- **必须走标准阶段脚本**：除非用户特别说明，优先调用 `stage1`→`stage6`，不要手写长 JSON 或跳步生成中间文件。若缺少凭据（OpenAI、MCP、TIDAS），需第一时间告知用户并等待指示。
- **在调用 LLM/MCP 前做输入校验**：例如检查 `clean_text` 是否非空、是否含有表格与单位，必要时提示用户补充。
- **终态 JSON 要求**：最终交付的 `workflow_result.json` 必须基于已通过 Stage 5 校验的数据生成，去除调试字段、空结构或临时备注，确保各流程数据集严格符合 schema、内容“干干净净”可直接入库。
- **MCP 预检一次**：随手写个 5 行 Python（导入 `FlowSearchService` + 构造 `FlowQuery`）测试单个交换量，确认凭据与网络正常，再启动 Stage 3，避免长时间超时才发现配置错误。
- **控制交换数量**：Stage 3 的流检索串行执行（`flow_search_max_parallel=1`），每个 `exchange` 都会独立调用 MCP。Stage 2 汇总表格时，应将同类资源/排放合并成 8~12 个代表性交换（例如统一为 `Electricity, medium voltage`、`Carbon dioxide, fossil`），但要在 `generalComment` 或 `notes` 中保留原始表格的细分条目、单位、上下游去向等信息，保障信息增益充足的同时避免长时间串行检索。
- **补充检索线索**：为每个 `exchange` 的 `generalComment1` 写入常见同义词（语义近似描述，如“electric power supply”）、别名或缩写（如“COG”“DAC”）、化学式/CAS 号，以及中英文对照的关键参数。这样 FlowSearchService 的多语言同义词扩展能利用更丰富的上下文提升召回率。
- **规范流名称**：优先采用 Tiangong/ILCD 常用流名，不保留论文里的括号或工艺限定（如 `Electricity for electrolysis (PV)`）。规范名称能显著提高 Stage 3 命中率，减少重复检索与超时。
- **长耗时命令提前调参**：Stage 2/3 可能超过 15 分钟；在受限环境下先提升命令超时（如外层 CLI 15min 限制）或增加 `.secrets` 中的 `timeout` 字段，避免半途被杀导致反复重跑。
- **限定重试次数**：对同一 LLM/MCP 调用的重试不超过 2 次，且每次调整 prompt 或上下文都要说明理由；若问题持续，转为人工分析并同步用户。
- **记录关键假设**：任何推断（单位补全、地理默认值、分类路径）都要写入 `notes` / `generalComment`，避免后续比对时反复确认。

## 1. 模块概览（`src/tiangong_lca_spec`）
- `core/`
  - `config.py`：集中管理 MCP/TIDAS 端点、重试与并发策略及产物目录。
  - `exceptions.py`：定义 `SpecCodingError` 及细分异常，统一错误语义。
  - `models.py`：声明 `FlowQuery`, `FlowCandidate`, `ProcessDataset`, `WorkflowResult` 等数据结构。
  - `logging.py`：基于 `structlog` 输出 JSON 日志。
  - `json_utils.py`：清洗 LLM 输出，修正 JSON 与括号不平衡。
  - `mcp_client.py`：使用官方 `mcp` SDK（Streamable HTTP + `ClientSession`）建立持久会话，提供同步 `invoke_tool` / `invoke_json_tool`。
- `flow_search/`：封装 MCP 流检索（重试、候选过滤、命中/未命中组装）。
- `flow_alignment/`：并行对齐交换量，支持基于 LLM 的候选筛选（回退至相似度评分），输出 `matched` 结果和带占位符的 `origin_exchanges`。
- `process_extraction/`：完成预处理、父级拆分、分类、地理标准化与 `processDataSet` 归并。
- `tidas_validation/`：调用 TIDAS MCP 工具并转化为 `TidasValidationFinding`。
- `orchestrator/`：顺序式 orchestrator，将各阶段串联成单一入口。
- `scripts/`：阶段化 CLI（`stage1`~`stage6`）和回归入口 `run_test_workflow.py`。

## 2. 分阶段脚本
脚本默认读写 `artifacts/` 下的中间文件，可通过参数重定向。

| 阶段 | 脚本 | 产物 | 说明 |
| ---- | ---- | ---- | ---- |
| 1 | `stage1_preprocess.py` | `stage1_clean_text.json` | 解析论文 Markdown/JSON，输出 `clean_text`。 |
| 2 | `stage2_extract_processes.py` | `stage2_process_blocks.json` | 使用 OpenAI Responses 生成流程块。 |
| 3 | `stage3_align_flows.py` | `stage3_alignment.json` | 调用 `FlowAlignmentService` 对齐交换量，仅保留匹配结果。 |
| 4 | `stage4_merge_datasets.py` | `stage4_process_datasets.json` | 合并流程块、候选流与功能单位。 |
| 5 | `stage5_validate.py` | `stage5_validation.json` | 调用 TIDAS MCP 工具（支持 `--skip`）。 |
| 6 | `stage6_finalize.py` | `workflow_result.json` | 汇总流程数据集、对齐信息与校验报告。 |

推荐执行序列（仓库根目录）：
```bash
uv run python scripts/stage1_preprocess.py --paper path/to/paper.json
uv run python scripts/stage2_extract_processes.py --clean-text artifacts/stage1_clean_text.json
uv run python scripts/stage3_align_flows.py \
  --process-blocks artifacts/stage2_process_blocks.json \
  --clean-text artifacts/stage1_clean_text.json
uv run python scripts/stage4_merge_datasets.py \
  --process-blocks artifacts/stage2_process_blocks.json \
  --alignment artifacts/stage3_alignment.json
uv run python scripts/stage5_validate.py --process-datasets artifacts/stage4_process_datasets.json
uv run python scripts/stage6_finalize.py \
  --process-datasets artifacts/stage4_process_datasets.json \
  --alignment artifacts/stage3_alignment.json \
  --validation artifacts/stage5_validation.json
```

- `stage3_align_flows.py` 若检测到 `.secrets/secrets.toml` 中的 OpenAI 凭据，会自动启用 LLM 评分评估 MCP 返回的 10 个候选；否则退回本地相似度匹配。脚本现仅写出 `stage3_alignment.json`，未命中的交换不会落盘，CLI 日志会提示跳过的数量。

## 3. 核心数据结构
```python
from dataclasses import dataclass, field
from typing import Any, Mapping, Literal

@dataclass(slots=True, frozen=True)
class FlowQuery:
    exchange_name: str
    description: str | None = None
    process_name: str | None = None
    paper_md: str | None = None

@dataclass(slots=True)
class FlowCandidate:
    uuid: str | None
    base_name: str
    treatment_standards_routes: str | None = None
    mix_and_location_types: str | None = None
    flow_properties: str | None = None
    version: str | None = None
    general_comment: str | None = None
    geography: Mapping[str, Any] | None = None
    classification: list[Mapping[str, Any]] | None = None
    reasoning: str = ""

@dataclass(slots=True)
class UnmatchedFlow:
    base_name: str
    general_comment: str | None = None
    status: Literal["requires_creation"] = "requires_creation"
    process_name: str | None = None

@dataclass(slots=True)
class ProcessDataset:
    process_information: dict[str, Any]
    modelling_and_validation: dict[str, Any]
    administrative_information: dict[str, Any]
    exchanges: list[dict[str, Any]] = field(default_factory=list)
    notes: Any | None = None
    process_data_set: dict[str, Any] | None = None

@dataclass(slots=True)
class WorkflowResult:
    process_datasets: list[ProcessDataset]
    alignment: list[dict[str, Any]]
    validation_report: list[TidasValidationFinding]
```

## 4. Flow Search
- `FlowSearchClient` 使用 `MCPToolClient.invoke_json_tool` 访问远程 `Search_flows_Tool`，根据 `FlowQuery` 自动构造检索上下文。
- `stage3_align_flows.py` 是唯一入口：不要人工在 Stage 2 直接拼接 `referenceToFlowDataSet`，而是让 Stage 3 读取 Stage 2 的 `process_blocks` 并触发检索。
- 运行 Stage 3 前先快速抽样核查：选 1~2 个交换量搭建 `FlowQuery` 调试，确认服务是否返回候选（避免整批跑空）。
- 每个交换量至少发起一次 MCP 检索；如果 3 次以内仍失败，才将该交换标记为 `UnmatchedFlow` 并写入原因。
- 采用指数退避重试；捕获 `httpx.HTTPStatusError` / `McpError`，必要时剥离上下文以规避 413/5xx。
- `FlowSearchService` 负责相似度过滤、缓存命中记录与 `UnmatchedFlow` 组合；Stage 3 结束后需根据日志统计确认命中率，并记录仍未命中的交换量。
- 如果日志出现大量 `flow_search.filtered_out` 且无命中，优先检查：① `exchangeName`/`unit` 是否缺失或拼写异常；② `clean_text` 是否传入过长上下文导致噪声；③ `.secrets` 中是否设置更大的 `timeout` 以应对慢响应。
- `mcp_tool_client.close_failed` 警告通常由请求完成后清理协程触发，属正常现象；若频繁超时，可调低 `flow_search_max_parallel` 或分批执行 Stage 3。

## 5. Flow Alignment
- 每个流程块的交换量在独立线程提交检索任务，聚合 `matched_flows` 与 `origin_exchanges`；未命中只在日志中计数提醒。
- 匹配成功时写回 `referenceToFlowDataSet`，失败则保留原始交换量并记录原因。
- 过程中输出 `flow_alignment.start`、`flow_alignment.exchange_failed` 等结构化日志，便于诊断。

## 6. Process Extraction
- 顺序执行 `extract_sections` → `classify_process` → `normalize_location` → `finalize`。
- 处理要点：
  - `extract_sections` 按父级或别名分段；若未命中则回退全篇文本。
  - 若 LLM 未返回 `processDataSets` / `processDataSet`，抛出 `ProcessExtractionError`。
  - `finalize` 通过 `build_tidas_process_dataset` 补齐 ILCD/TIDAS 必填字段，并保留原始 `exchange_list`。
- LLM 输出校验清单：
  1. 顶层必须是 `processDataSets` 数组；
  2. 每个流程需包含 `processInformation.dataSetInformation.specinfo` 的四个字段；
  3. 所有 `exchanges.exchange` 项需带 `exchangeDirection`、`meanAmount`、`unit`。
- 若需要对表格数值做清洗（单位补全、重复行合并），先写纯 Python 脚本验证逻辑，再落回 `ProcessExtractionService`；避免直接在回答里逐行手填。
- `merge_results` 合入对齐候选并生成功能单位字符串。

## 7. TIDAS Validation
- `TidasValidationService` 逐个数据集调用 `Tidas_Data_Validate_Tool`，解析 JSON 或结构化错误文本生成 `TidasValidationFinding`。
- 当远程工具暂不可用时，可在 `stage5_validate.py` 传入 `--skip` 继续流程。

## 8. 工作流编排
- `WorkflowOrchestrator` 顺序执行：`preprocess` → `extract_processes` → `align_flows` → `merge_datasets` → `validate` → `finalize`。
- 返回 `WorkflowResult`，供 `stage6_finalize.py` 或外部集成直接消费。

## 9. 验证建议
- 单元测试优先覆盖：`json_utils` 清洗、`FlowSearchService` 过滤/缓存、`FlowAlignmentService` 降级处理、流程抽取各阶段的错误分支与 `merge_results` 容错。
- 集成验证：使用最小论文样例依次执行 `stage1`→`stage6`，核对产物 schema、命中统计与 TIDAS 报告。
- 观测：启用 `configure_logging` JSON 输出并筛选 `flow_alignment.exchange_failed`、`process_extraction.parents_uncovered` 等关键事件，快速定位异常阶段。

## 10. 分类与地理辅助资源
- `tidas_processes_category.json` (`src/tidas/schemas/tidas_processes_category.json`) 是流程分类的权威来源，覆盖 ISIC 树的各级代码与描述。若 Codex 需要确认分类路径，先使用 `uv run python scripts/list_process_category_children.py <code>` 逐层展开（`<code>` 为空时输出顶层，例如 `uv run python scripts/list_process_category_children.py 01`）。必要时可通过 `tiangong_lca_spec.tidas.get_schema_repository().resolve_with_references("tidas_processes_category.json")` 读取局部节点，再将相关分支文本粘贴到对 Codex 的提问里，帮助其在有限上下文里挑选正确的 `@classId` / `#text`。
- 地理编码沿用 `tidas_locations_category.json` (`src/tidas/schemas/tidas_locations_category.json`)；用法与上面一致，命令为 `uv run python scripts/list_location_children.py <code>`（例如 `uv run python scripts/list_location_children.py CN` 查看中国内部层级）。在向 Codex 说明地理选项时，同样只摘录与当前流程相关的分支，避免传送整棵树。
- 若流程还涉及流分类，可按需调用 `uv run python scripts/list_flow_category_children.py <code>`，其数据源为 `tidas_flows_product_category.json`。
