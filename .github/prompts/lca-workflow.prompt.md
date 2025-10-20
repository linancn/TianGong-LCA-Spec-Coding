# 天工 LCA 规范编码工作流（uv + LangGraph 实施准则）

本说明同步仓库当前实现：利用 `uv` 管理依赖、LangGraph 编排多阶段 Agent 工作流，并通过 `tiangong_lca_spec` 包暴露统一接口。内容覆盖环境要求、模块拆分、数据结构、格式化规范与后续扩展建议。

## 1. 环境与工具
- **Python**：>= 3.12（可通过 `uv toolchain` 安装）；仓库默认解释器路径位于 `.venv/`。
- **包管理**：运行 `uv sync` 初始化运行依赖，`uv sync --group dev` 同步开发工具。支持通过 `UV_PYPI_URL=https://pypi.tuna.tsinghua.edu.cn/simple` 使用清华镜像。
- **主要依赖**：`langchain-mcp-adapters`, `anyio`, `pydantic`, `pydantic-settings`, `tenacity`, `structlog`, `langchain-core`, `langgraph`, `python-dotenv`。
- **构建体系**：`hatchling` 负责构建编辑/发行版；`pyproject.toml` 已在 `[tool.hatch.build.targets.wheel]` 中声明 `src/tiangong_lca_spec` 为打包目录。
- **机密配置**：使用 `.secrets/secrets.toml` 注入 OpenAI、LangSmith、远程 MCP/TIDAS 凭据。首次配置可执行 `cp .secrets/secrets.example.toml .secrets/secrets.toml` 后替换占位符。
- **代码规范**：
  - 运行方式：`uv run black .`、`uv run ruff check`。

### 1.1 LangSmith / LangGraph 追踪设置
- `.secrets/secrets.toml` 中的 `[LANGSMITH]` 段定义 `API_KEY`, `ENDPOINT`, `PROJECT`, `SESSION`, `TAGS`, `METADATA` 等字段。
- `tiangong_lca_spec.core.config.Settings` 在初始化时会将上述配置转换成 `LANGSMITH_*`/`LANGCHAIN_*` 环境变量，并在 `configure_logging()` 调用时确保注入到进程环境。
- 默认启用 `LANGCHAIN_TRACING_V2=true` 与 `LANGCHAIN_CALLBACKS_BACKGROUND=true`，LangGraph/ LangSmith 将同步记录工作流 run tree，可通过 LangSmith 控制台追踪节点执行。

## 2. 包结构概览（src/tiangong_lca_spec）
- `core/`
  - `config.py`：`pydantic-settings` 驱动的 `Settings`，集中管理 MCP/TIDAS 基础配置、LangSmith 凭据、重试与并发策略、缓存目录、日志等级，并提供 `apply_langsmith_environment()` 自动写入运行时环境。
  - `exceptions.py`：`SpecCodingError` 及子类（`FlowSearchError`, `FlowAlignmentError`, `ProcessExtractionError`, `TidasValidationError`）。
  - `models.py`：核心 dataclass（`FlowQuery`, `FlowCandidate`, `UnmatchedFlow`, `ProcessDataset`, `TidasValidationFinding`, `WorkflowResult`, `SettingsProfile`）。
  - `logging.py`：`structlog` JSON 日志初始化与 logger 工厂，统一设置 `langgraph*` logger，并在调用前确保 LangSmith 环境变量就绪。
  - `json_utils.py`：剥离 `<think>`、去除 Markdown 代码块、括号平衡截断等 JSON 清洗能力。
  - `mcp_client.py`：封装 `MultiServerMCPClient` 与 `anyio` 阻塞门户，供同步服务通过 MCP 工具调用。
- `flow_search/`
  - `client.py`：基于 `MCPToolClient` 的 MCP 工具调用封装，使用 `tenacity` 重试并解析工具响应。
  - `service.py`：缓存的高层搜索服务；负责候选过滤、封装 `UnmatchedFlow`。
  - `validators.py`：名称相似度、地理匹配等本地校验逻辑。
- `flow_alignment/`：`FlowAlignmentService` 使用 `ThreadPoolExecutor` 按并发配置批量搜索，并返回 matched/unmatched 及 `origin_exchanges`。
- `process_extraction/`
  - `preprocess.py`：解析 Markdown JSON、剔除参考文献/附录、长度裁剪。
  - `extractors.py`：定义 LLM 协议、抽取/分类/地理规范化 prompt。
  - `service.py`：LangGraph pipeline（extract_sections -> classify_process -> normalize_location -> finalize）。
  - `merge.py`：整合匹配结果、补写 `referenceToFlowDataSet`、推断功能单位。
- `tidas_validation/`：共用 `MCPToolClient` 调用 `Tidas_Data_Validate_Tool`，将结果转化为 `TidasValidationFinding`。
- `orchestrator/`：`WorkflowOrchestrator` LangGraph 工作流（preprocess → extract → align → merge → validate → finalize），输出 `WorkflowResult`。

## 3. 核心数据结构（tiangong_lca_spec.core.models）
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

@dataclass(slots=True)
class WorkflowResult:
    process_datasets: list[ProcessDataset]
    alignment: list[dict[str, Any]]
    validation_report: list[TidasValidationFinding]
```

## 4. Flow Search 模块
- 接口：`search_flows(query) -> tuple[list[FlowCandidate], list[UnmatchedFlow]]`。
- 关键点：
  1. `FlowSearchClient` 通过 `MCPToolClient` 调用 `FlowDataSearch` 工具，结合 `tenacity` 实现指数退避重试，并在客户端层完成候选列表解包。
  2. 远程 `TianGong_LCA_Remote` MCP 服务在检索前使用大语言模型生成多语言同义词/重写描述，随后执行语义检索与全文检索的混合查询，以提供高召回的候选流数据集。
  3. `FlowSearchService` 过滤低相似度候选，并将被过滤项记录为 `UnmatchedFlow`；结果写入 LRU 缓存。
  4. 遇到 MCP 错误或响应异常抛出 `FlowSearchError`；日志记录请求参数、工具返回结构与过滤原因。

## 5. Flow Alignment 模块
- 接口：`FlowAlignmentService.align_exchanges(process_dataset, paper_md)`。
- 流程：
  1. 解析 `processInformation` / `exchanges`，兼容 `exchange_list`。
  2. 依照 `Settings.profile.concurrency` 用线程池并行发起 flow search。
  3. 汇总 `matched_flows`、`unmatched_flows`、`origin_exchanges`，并保留失败的异常信息。
- 失败的搜索会自动降级成 `UnmatchedFlow`，并写日志 `flow_alignment.exchange_failed`。

## 6. Process Extraction 模块
- 预处理：`preprocess_paper` 聚合 markdown 段落、剔除 `<think>` 与参考文献、限制最大长度。
- LangGraph 节点：
  - `extract_sections`：依赖注入的 LLM（实现 `LanguageModelProtocol`）输出流程信息、行政信息、模型信息和 `exchange_list`。
  - `classify_process`：补写 ISIC 分类并挂载至 `classificationInformation.classification`。
  - `normalize_location`：更新 `process_information.geography`。
  - `finalize`：生成 `process_blocks`，同时保留 `exchange_list` 与 `{"exchange": ...}` 结构。
- 合并：`merge_results` 将匹配流写回 exchange 并添加 `matchingDetail`；`determine_functional_unit` 选取首个非废弃输出构建功能单位字符串。

## 7. TIDAS Validation 模块
- 接口：`validate_with_tidas(process_datasets)`。
- 步骤：
  1. 将 `ProcessDataset` 转换为字典，通过共享的 `MCPToolClient` 调用 `Tidas_Data_Validate_Tool`。
  2. 解析工具返回并映射为 `TidasValidationFinding`（`severity`, `message`, `path`, `suggestion`）。
  3. 所有异常统一抛出 `TidasValidationError`；日志包含批次数与状态。

## 8. Orchestrator 工作流
- 入口：`WorkflowOrchestrator`，支持 `run(paper_md_json)` 与 `run_from_path(path)`。
- 节点顺序：`preprocess` → `extract_processes` → `align_flows` → `merge_datasets` → `validate` → `finalize`。
- `WorkflowState` 使用 `TypedDict` 管理中间状态；`__enter__` / `__exit__` 提供上下文管理以关闭线程池与 TIDAS 客户端。
- 输出：`WorkflowResult` 包含最终 `process_datasets`、对齐详情与 TIDAS 报告，便于落盘或进一步处理。

## 9. 测试与可靠性建议
- **单元测试重点**：
  - `json_utils` 的清洗/解析逻辑（含 `<think>`、双重转义、括号截断）。
  - `FlowSearchService` 过滤与缓存行为（Mock MCP 响应）。
  - `FlowAlignmentService` 并发降级策略。
  - `merge_results` 在缺失候选、功能单位推断失败时的容错。
  - LangGraph 节点，通过注入 Fake LLM 验证状态流转。
- **集成测试**：构造最小 Markdown JSON 驱动完整 orchestrator，断言输出 schema 与错误分支。
- **可观测性**：启用 `configure_logging` 输出 JSON 日志，并通过 LangSmith 控制台观察 LangGraph run tree；建议在 orchestrator 入口绑定 `settings.profile`、输入文档 ID 等上下文。

## 10. 目录与后续迭代
- `src/tiangong_lca_spec/core`: 配置、日志、模型、通用工具（含 `mcp_client` 同步封装）。
- `src/tiangong_lca_spec/flow_search`: MCP 查询与候选过滤。
- `src/tiangong_lca_spec/flow_alignment`: 交换量与流对齐。
- `src/tiangong_lca_spec/process_extraction`: 文献解析与结果合并。
- `src/tiangong_lca_spec/tidas_validation`: TIDAS 校验封装。
- `src/tiangong_lca_spec/orchestrator`: LangGraph orchestrator。

**下一步建议**：
1. 将 MCP/TIDAS 客户端抽象为接口或协议，实现本地 Mock 以支持 CI 离线测试。
2. 按需扩展缓存、断点恢复、指标上报等横切能力，并考虑将 `WorkflowResult` 与日志落盘至 `artifacts/`。

## 11. 维护与检查要求（必须要做）
- **静态检查 / 格式化**：每次代码更新后必须执行
  ```bash
  uv run black .
  uv run ruff check
  ```

- **文档同步**：如果实现或流程发生变化，务必同步更新当前文档（`.github/prompts/lca-workflow.prompt.md`），确保准则与代码保持一致。
