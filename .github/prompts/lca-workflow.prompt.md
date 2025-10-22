# 天工 LCA 规范编码工作流（Codex 分阶段脚本准则）

本说明同步仓库当前实现：利用 `uv` 管理依赖，通过一组可执行 Python 脚本驱动 Codex CLI 逐阶段完成规范编码流程。脚本直接调用 `tiangong_lca_spec` 内的流程抽取、对齐、合并与校验模块，并在每个阶段落盘中间产物，便于审查、回溯与局部重跑。内容覆盖环境要求、模块拆分、数据结构、脚本串联方式与后续扩展建议。

## 1. 环境与工具
- **Python**：>= 3.12（可通过 `uv toolchain` 安装）；仓库默认解释器路径位于 `.venv/`。
- **包管理**：运行 `uv sync` 初始化运行依赖，`uv sync --group dev` 同步开发工具。支持通过 `UV_PYPI_URL=https://pypi.tuna.tsinghua.edu.cn/simple` 使用清华镜像。
- **主要依赖**：`langchain-mcp-adapters`, `anyio`, `pydantic`, `pydantic-settings`, `tenacity`, `structlog`, `langchain-core`, `python-dotenv`。
- **构建体系**：`hatchling` 负责构建编辑/发行版；`pyproject.toml` 已在 `[tool.hatch.build.targets.wheel]` 中声明 `src/tiangong_lca_spec` 为打包目录。
- **机密配置**：使用 `.secrets/secrets.toml` 注入 OpenAI、远程 MCP/TIDAS 凭据。首次配置可执行 `cp .secrets/secrets.example.toml .secrets/secrets.toml` 后替换占位符。
- **代码规范**：
  - 运行方式：`uv run black .`、`uv run ruff check`。

## 2. 包结构概览（src/tiangong_lca_spec）
- `core/`
  - `config.py`：`pydantic-settings` 驱动的 `Settings`，集中管理 MCP/TIDAS 基础配置、重试与并发策略、缓存目录、日志等级等。
  - `exceptions.py`：`SpecCodingError` 及子类（`FlowSearchError`, `FlowAlignmentError`, `ProcessExtractionError`, `TidasValidationError`）。
  - `models.py`：核心 dataclass（`FlowQuery`, `FlowCandidate`, `UnmatchedFlow`, `ProcessDataset`, `TidasValidationFinding`, `WorkflowResult`, `SettingsProfile`）。
  - `logging.py`：`structlog` JSON 日志初始化与 logger 工厂，统一配置日志级别与输出格式。
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
  - `service.py`：分步执行的流程抽取管线（extract_sections -> classify_process -> normalize_location -> finalize）。
  - `merge.py`：整合匹配结果、补写 `referenceToFlowDataSet`、推断功能单位。
- `tidas_validation/`：共用 `MCPToolClient` 调用 `Tidas_Data_Validate_Tool`，将结果转化为 `TidasValidationFinding`。
- `orchestrator/`：保留 `WorkflowOrchestrator` 以便一次性执行或复用旧接口，但 Codex 默认通过阶段脚本串联执行。
- `scripts/`
  - `_workflow_common.py`：OpenAI Responses LLM 包装、机密加载与 JSON 落盘工具。
  - `stage1_preprocess.py` 至 `stage6_finalize.py`：分阶段 CLI 脚本，按顺序驱动完整流程并在 `artifacts/` 输出中间文件。
  - `run_test_workflow.py`：仍支持一次性跑通 orchestrator，便于回归测试。

## 3. Codex 分阶段执行流程
分阶段脚本位于 `scripts/` 目录，约定各阶段输入/输出均为 JSON 文件，默认写入 `artifacts/`。Codex 在执行时按顺序运行以下脚本：
- **阶段 1 - 预处理** (`stage1_preprocess.py`)：读取原始论文 Markdown/JSON（`--paper`），输出 `stage1_clean_text.json`，字段 `clean_text` 提供供后续阶段使用的纯文本。
- **阶段 2 - 流程抽取** (`stage2_extract_processes.py`)：加载 `clean_text`，使用 OpenAI Responses API 调用流程抽取服务，输出 `stage2_process_blocks.json`，包含候选 `processDataSet` 区块。需要 `.secrets/secrets.toml` 提供 `OPENAI` 凭据。
- **阶段 3 - 交换量对齐** (`stage3_align_flows.py`)：对每个 `processDataSet` 调用 `FlowAlignmentService` 对齐候选流，输出 `stage3_alignment.json`，记录 `matched_flows`、`unmatched_flows` 与 `origin_exchanges`。
- **阶段 4 - 数据集合并** (`stage4_merge_datasets.py`)：将流程块与对齐结果合并，补齐 `referenceToFlowDataSet`、推断功能单位，输出 `stage4_process_datasets.json`（保持 `ProcessDataset` 字段结构）。
- **阶段 5 - TIDAS 校验** (`stage5_validate.py`)：把合并后的数据集发送到远程 TIDAS MCP 工具，输出 `stage5_validation.json`；如需跳过可添加 `--skip`。
- **阶段 6 - 汇总产物** (`stage6_finalize.py`)：汇总流程数据、对齐信息与校验报告，输出最终 `workflow_result.json`，结构与 `WorkflowResult` 对齐，便于后续导入或提交。

推荐的 Codex 执行序列（均在仓库根目录运行）：
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

所有阶段均可通过 `--output` 参数重定向产物路径；若某阶段失败，可修复后从该阶段继续执行。

## 4. 核心数据结构（tiangong_lca_spec.core.models）
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

## 5. Flow Search 模块
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
- 执行步骤：
  - `extract_sections`：先用 `ParentProcessExtractor` 枚举文中所有父级/总过程，再将文本按父级关键词切片，并对每个父级分别调用 `SectionExtractor` 获取流程数据；缺失的父级会回退到全量文本重试，并记录未覆盖告警，同时监控截断输出。
    - 第一次提示中明确约束：只有在原文以章节、表格或叙述明确给出了独立单元过程及其 LCI 时才新增条目；原料预处理、共享公用工程等未具备独立功能单位的内容需并入对应子过程的 `common:generalComment`。
  - `classify_process`：补写 ISIC 分类并挂载至 `classificationInformation.common:classification`。
  - `normalize_location`：更新 `process_information.geography`。
  - `finalize`：生成 `process_blocks`，同时保留 `exchange_list` 与 `{"exchange": ...}` 结构；`build_tidas_process_dataset` 会补齐全部必填字段（含 `modellingAndValidation` 中的数据完整性与审查引用信息），规范化 `common:dataSetVersion` 等版本号格式，并保留 `LCIMethodAndAllocation.typeOfDataSet` 等关键可选字段。
- 合并：`merge_results` 将匹配流写回 exchange 并添加 `matchingDetail`；`determine_functional_unit` 选取首个非废弃输出构建功能单位字符串。

## 7. TIDAS Validation 模块
- 接口：`validate_with_tidas(process_datasets)`。
- 步骤：
  1. 将 `ProcessDataset` 转换为字典，通过共享的 `MCPToolClient` 调用 `Tidas_Data_Validate_Tool`。
  2. 解析工具返回并映射为 `TidasValidationFinding`（`severity`, `message`, `path`, `suggestion`）。
  3. 所有异常统一抛出 `TidasValidationError`；日志包含批次数与状态。

## 8. 测试与可靠性建议
- **单元测试重点**：
  - `json_utils` 的清洗/解析逻辑（含 `<think>`、双重转义、括号截断）。
  - `FlowSearchService` 过滤与缓存行为（Mock MCP 响应）。
  - `FlowAlignmentService` 并发降级策略。
  - `merge_results` 在缺失候选、功能单位推断失败时的容错。
  - 流程抽取各步骤，通过注入 Fake LLM 验证状态流转。
- **集成测试**：可使用阶段脚本构造最小 Markdown JSON，按顺序跑通 `stage1`→`stage6`，断言输出 schema 与错误分支；也可保留 `run_test_workflow.py` 回归 orchestrator。
- **可观测性**：各脚本均通过 `configure_logging` 输出 JSON 日志，可结合自定义日志分析阶段内节点表现；建议在阶段命令中传递文档 ID 作为日志上下文。

## 9. 目录与后续迭代
- `src/tiangong_lca_spec/core`: 配置、日志、模型、通用工具（含 `mcp_client` 同步封装）。
- `src/tiangong_lca_spec/flow_search`: MCP 查询与候选过滤。
- `src/tiangong_lca_spec/flow_alignment`: 交换量与流对齐。
- `src/tiangong_lca_spec/process_extraction`: 文献解析与结果合并。
- `src/tiangong_lca_spec/tidas_validation`: TIDAS 校验封装。
- `src/tiangong_lca_spec/orchestrator`: 顺序式 orchestrator（保留旧接口）。
- `scripts/stage*.py`: Codex 分阶段执行入口。

**下一步建议**：
1. 将 MCP/TIDAS 客户端抽象为接口或协议，实现本地 Mock 以支持 CI 离线测试。
2. 按需扩展缓存、断点恢复、指标上报等横切能力，并考虑将 `WorkflowResult` 与日志落盘至 `artifacts/`。

## 10. 维护与检查要求（必须要做）
- **静态检查 / 格式化**：每次代码更新后必须执行
  ```bash
  uv run black .
  uv run ruff check
  ```

- **文档同步**：如果实现或流程发生变化，务必同步更新当前文档（`.github/prompts/lca-workflow.prompt.md`），确保准则与代码保持一致。
