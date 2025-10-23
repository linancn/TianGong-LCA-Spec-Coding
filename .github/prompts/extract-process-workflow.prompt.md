# 天工 LCA 工作流（Codex 分阶段指引）

本说明汇总工作流相关信息：模块职责、分阶段脚本、核心数据结构与关键校验要点，帮助 Codex 在多阶段协作中保持一致行为。

## 1. 模块概览（`src/tiangong_lca_spec`）
- `core/`
  - `config.py`：集中管理 MCP/TIDAS 端点、重试与并发策略及产物目录。
  - `exceptions.py`：定义 `SpecCodingError` 及细分异常，统一错误语义。
  - `models.py`：声明 `FlowQuery`, `FlowCandidate`, `ProcessDataset`, `WorkflowResult` 等数据结构。
  - `logging.py`：基于 `structlog` 输出 JSON 日志。
  - `json_utils.py`：清洗 LLM 输出，修正 JSON 与括号不平衡。
  - `mcp_client.py`：使用官方 `mcp` SDK（Streamable HTTP + `ClientSession`）建立持久会话，提供同步 `invoke_tool` / `invoke_json_tool`。
- `flow_search/`：封装 MCP 流检索（重试、候选过滤、命中/未命中组装）。
- `flow_alignment/`：并行对齐交换量，生成 `matched/unmatched` 及 `origin_exchanges`。
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
| 3 | `stage3_align_flows.py` | `stage3_alignment.json` | 调用 `FlowAlignmentService` 对齐交换量。 |
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
- 采用指数退避重试；捕获 `httpx.HTTPStatusError` / `McpError`，必要时剥离上下文以规避 413/5xx。
- `FlowSearchService` 负责相似度过滤、缓存命中记录与 `UnmatchedFlow` 组合。

## 5. Flow Alignment
- 每个流程块的交换量在独立线程提交检索任务，聚合 `matched_flows`、`unmatched_flows` 与 `origin_exchanges`。
- 匹配成功时写回 `referenceToFlowDataSet`，失败则保留原始交换量并记录原因。
- 过程中输出 `flow_alignment.start`、`flow_alignment.exchange_failed` 等结构化日志，便于诊断。

## 6. Process Extraction
- 顺序执行 `extract_sections` → `classify_process` → `normalize_location` → `finalize`。
- 处理要点：
  - `extract_sections` 按父级或别名分段；若未命中则回退全篇文本。
  - 若 LLM 未返回 `processDataSets` / `processDataSet`，抛出 `ProcessExtractionError`。
  - `finalize` 通过 `build_tidas_process_dataset` 补齐 ILCD/TIDAS 必填字段，并保留原始 `exchange_list`。
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
