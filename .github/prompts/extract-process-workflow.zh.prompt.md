# 天工 LCA 数据抽取工作流指引

本说明聚焦数据抽取与流程编排：梳理分阶段脚本、核心模块、数据结构与关键校验要点，支持 Codex 在业务执行中保持统一策略。关于开发环境与通用协作规范，请参阅仓库根目录的 `AGENTS.md`。**注意：工作站没有裸露的 `python` 命令，必须通过 `uv` 调用 Python。** 对于脚本执行使用 `uv run python …`（或 `uv run -- python script.py`），单行脚本以 `uv run python - <<'PY'` 方式运行，执行模块使用 `uv run python -m module`。

流程抽取工作流的角色划分如下：
- **Stage 1（预处理）**：解析原始论文/资料，输出结构化 `clean_text` 供后续调用。
- **Stage 2（流程生成）**：Codex 驱动 LLM 提取流程块，补充 `FlowSearch hints` 与换算假设，是后续对齐的基础资料。
- **Stage 3（流对齐与制品导出）**：结合 MCP 检索结果与人工上下文，确认每个交换量的标准流，并进一步合并结果、生成 `artifacts/<run_id>/exports/` 下的 ILCD 制品、执行本地校验，最终汇总 `workflow_result.json`。

每次运行都会在 `artifacts/<run_id>/` 下创建独立目录，`run_id` 为 UTC 时间戳（Stage 1 会打印并写入 `artifacts/.latest_run_id`）。目录结构包含：
- `cache/`：保存阶段产物（`stage1_clean_text.md`、`stage2_process_blocks.json` 等）以及 LLM 的磁盘缓存。
- `exports/`：仅包含 `processes/`、`flows/`、`sources/` 三个目录，供 TIDAS 校验读取。
后续阶段如未显式传入 `--run-id`，默认回落到最近一次运行的标识，因此执行 Stage 1 后可直接启动 Stage 2/Stage 3。

## 0. 执行约定（避免无效迭代）
- **先读原始资料**：动手前快速梳理论文原文或 `clean_text`，确认章节结构、数据表与功能单位。
- **直接执行标准命令**：默认沿用下方列出的 Stage 1~3 CLI 模板（输入/输出路径遵循仓库约定），无需反复运行 `--help`。如需自定义参数，再单独查阅帮助。
- **必须走标准阶段脚本**：除非用户特别说明，优先调用 `stage1`→`stage3`，不要手写长 JSON 或跳步生成中间文件。若缺少凭据（OpenAI、MCP、TIDAS），需第一时间告知用户并等待指示。Stage 3 校验通过后会在使用同一批命令行参数的前提下自动触发发布，直接将流程、流与文献信息入库（无测试模式）。如 `artifacts/<run_id>/cache/published.json` 已存在，则默认跳过自动发布；若确认需要重发，删除该文件或在 Stage 3 中追加 `--force-publish`。只有在需要复核/调试时再手动执行 `stage4_publish`，正式发布会在完成后写回同名 `published.json` 摘要以便追踪。
- **默认凭据已备妥**：标准环境下 `.secrets/secrets.toml` 由运维预先配置，Codex 直接开始执行各阶段即可；只有在脚本实际抛出缺少凭据或连接失败的异常时，再回溯检查密钥配置。
- **在调用 LLM/MCP 前做输入校验**：例如检查 `clean_text` 是否非空、是否含有表格与单位，必要时提示用户补充。
- **终态 JSON 要求**：最终交付的 `workflow_result.json` 必须基于已通过 Stage 3 制品校验的数据生成，去除调试字段、空结构或临时备注，确保各流程数据集严格符合 schema、内容“干干净净”可直接入库。
- **MCP 预检一次**：随手写个 5 行 Python（导入 `FlowSearchService` + 构造 `FlowQuery`）测试单个交换量，确认凭据与网络正常，再启动 Stage 3，避免长时间超时才发现配置错误。
- **控制交换数量**：Stage 3 的流检索串行执行（`flow_search_max_parallel=1`），每个 `exchange` 都会独立调用 MCP。Stage 2 现要求逐行复刻文献表格——每条原始清单行都要生成独立的 `exchange`（不得合并、平均或省略）。如表格含情景或脚注信息，请完整写入 `generalComment`，以便 Stage 3 能按原始来源逐条对齐。
- **补充检索线索**：为每个 `exchange` 的 `generalComment` 写入常见同义词（语义近似描述，如“electric power supply”）、别名或缩写（如“COG”“DAC”）、化学式/CAS 号，以及中英文对照的关键参数，这样 FlowSearchService 的多语言同义词扩展能利用更丰富的上下文提升召回率。高频基础流（`Electricity, medium voltage`、`Water, process`、`Steam, low pressure`、`Oxygen`、`Hydrogen`、`Natural gas, processed` 等）要求至少列出 2~3 个中英文别称或典型描述（如“grid electricity 10–30 kV”“中压电”“technological water”“饱和蒸汽 0.4 MPa”“O₂, CAS 7782-44-7”），并说明状态/纯度/来源。`generalComment` 必须以 `FlowSearch hints:` 开头，并按 `en_synonyms=... | zh_synonyms=... | abbreviation=... | formula_or_CAS=... | state_purity=... | source_or_pathway=... | usage_context=...` 的结构填写，所有字段都要填入可信的双语描述，禁止使用 “NA”/“N/A” 等占位符。若文献未明示，请结合上下文推断（如典型纯度、供应路径）或用严谨语言描述最佳可得信息，再在末尾补充表格引用或换算假设。缺少这些线索时 MCP 往往只返回中文短名或低相似度候选，Stage 3 会落回占位符。
- **Stage 2 产物自检**：在进入 Stage 3 前抽样查看 `artifacts/<run_id>/cache/stage2_process_blocks.json`，确保每个 `exchange.generalComment` 都包含上述 `FlowSearch hints` 结构、关键同义词与中英对照参数；若发现仍是“Table X”式的简短描述，必须回到 Stage 2 重新生成或手动补写上下文，否则 Stage 3 会因为缺少语义信号导致大量 `unmatched:placeholder`。
- **规范流名称**：优先采用 Tiangong/ILCD 常用流名，不保留论文里的括号或工艺限定（如 `Electricity for electrolysis (PV)`）。规范名称能显著提高 Stage 3 命中率，减少重复检索与超时。
- **长耗时命令提前调参**：Stage 2/3 可能超过 15 分钟；在受限环境下先提升命令超时（如外层 CLI 15min 限制）或增加 `.secrets` 中的 `timeout` 字段，避免半途被杀导致反复重跑。
- **限定重试次数**：对同一 LLM/MCP 调用的重试不超过 2 次，且每次调整 prompt 或上下文都要说明理由；若问题持续，转为人工分析并同步用户。
- **记录关键假设**：任何推断（单位补全、地理默认值、分类路径）都要写入 `generalComment`，避免后续比对时反复确认。

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
- `scripts/`：阶段化 CLI（`stage1`~`stage4`）和回归入口 `run_test_workflow.py`。

## 2. 分阶段脚本
脚本默认读写 `artifacts/<run_id>/cache/` 下的中间文件，可通过参数重定向。

| 阶段 | 脚本 | 产物 | 说明 |
| ---- | ---- | ---- | ---- |
| 1 | `stage1_preprocess.py` | `artifacts/<run_id>/cache/stage1_clean_text.md` | 解析论文 Markdown/JSON，输出 `clean_text`。 |
| 2 | `stage2_extract_processes.py` | `artifacts/<run_id>/cache/stage2_process_blocks.json` | 使用 OpenAI Responses 生成流程块。 |
| 3 | `stage3_align_flows.py` | `artifacts/<run_id>/cache/stage3_alignment.json`、`…/cache/process_datasets.json`、`…/cache/tidas_validation.json`、`…/cache/workflow_result.json`，以及 `artifacts/<run_id>/exports/processes|flows|sources/` 下的 ILCD JSON | 调用 `FlowAlignmentService` 对齐交换量并验证提示质量，随后合并结果、生成 ILCD 制品、运行 `tidas_tools.validate`，并在校验通过后自动尝试入库（若检测到 `cache/published.json` 且未指定 `--force-publish` 则跳过）。 |
| 4 (可选) | `stage4_publish.py` | `artifacts/<run_id>/cache/stage4_publish_preview.json` | 读取 Stage 3 产物，构造 `Database_CRUD_Tool` 负载；默认由 Stage 3 在校验成功后触发（受 `published.json` 标志控制），仅在需要重新发布或调试时手动运行。 |

推荐执行序列（仓库根目录）：
```bash
RUN_ID=$(date -u +"%Y%m%dT%H%M%SZ")  # 可选，Stage 1 会输出生成的 run_id
uv run python scripts/stage1_preprocess.py --paper path/to/paper.json --run-id "$RUN_ID"
uv run python scripts/stage2_extract_processes.py --run-id "$RUN_ID"
uv run python scripts/stage3_align_flows.py --run-id "$RUN_ID"
uv run python scripts/stage4_publish.py --run-id "$RUN_ID" \
  --publish-flows --publish-processes \
  --update-alignment --update-datasets
```
Stage 3 校验通过后会自动调用上述发布脚本完成入库（全流程无干跑）。若 `artifacts/<run_id>/cache/published.json` 已存在，将默认跳过自动发布；确认需要重新提交时，删除该文件或在 Stage 3 指定 `--force-publish`。

- `stage3_align_flows.py` 若检测到 `.secrets/secrets.toml` 中的 OpenAI 凭据，会自动启用 LLM 评分评估 MCP 返回的 10 个候选；否则退回本地相似度匹配。脚本会在对齐前校验每个交换是否同时具备 `exchangeName` 与 `FlowSearch hints`（字段要求见 §0），缺项时默认中断（仅可用 `--allow-missing-hints` 放行提示缺失）。当缺少 `exchangeName` 时，会优先从 `FlowSearch hints` 的多语言同义词中自动补足。输出的 `artifacts/<run_id>/cache/stage3_alignment.json` 同步携带 `process_id`、`matched_flows`、`unmatched_flows` 与 `origin_exchanges`，并在 CLI 中打印各流程的命中统计。
- Stage 3 继续复用已有的 ILCD format 数据源（UUID `a97a0155-0234-4b87-b4ce-a45da52f2a40`）以及共享的 ILCD entry-level 合规体系 UUID (`d92a1a12-2545-49e2-a585-55c259997756`)，仅在流程与流数据集中写入这些引用块，不会在 `exports/sources/` 目录重新生成这些共享数据集的本地文件。
- 所有导出的 ILCD 文件在相对 `@uri` 引用中统一采用带版本号的命名方式：`../processes/{uuid}_{version}.xml`、`../flows/{uuid}_{version}.xml`、`../sources/{uuid}_{version}.xml`，便于下游系统直接定位到具体版本。

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
    process_data_set: dict[str, Any] | None = None

@dataclass(slots=True)
class WorkflowResult:
    process_datasets: list[ProcessDataset]
    alignment: list[dict[str, Any]]
    validation_report: list[TidasValidationFinding]
```

## 4. Flow Search
- `FlowSearchClient` 使用 `MCPToolClient.invoke_json_tool` 访问远程 `Search_flows_Tool`，根据 `FlowQuery` 自动构造检索上下文。
- `FlowQuery.description` 直接来自 Stage 2 `exchange.generalComment` 的 `FlowSearch hints` 字符串；保持字段顺序与分隔符一致，便于 QueryFlow Service 提取多语言同义词和物性信息。
- 远程 `tiangong_lca_remote` 工具内部已接入 LLM，会基于整段 `generalComment` 自动扩展同义词并执行全文 + 语义混合检索，因此无需在 Stage 3 手动再造额外提示。
- 只要 `generalComment` 内容完整且精炼，就可以信任 `tiangong_lca_remote` 返回的候选；重点是从结果中挑选最贴合的流并补写必要说明。
- `stage3_align_flows.py` 是唯一入口：不要在 Stage 2 直接拼接 `referenceToFlowDataSet`，而是让 Stage 3 读取 Stage 2 的 `process_blocks` 并触发检索。
- 运行 Stage 3 前先快速抽样核查：选 1~2 个交换量搭建 `FlowQuery` 调试，确认服务是否返回候选（避免整批跑空）。
- 每个交换量至少发起一次 MCP 检索；如果 3 次以内仍失败，才将该交换标记为 `UnmatchedFlow` 并写入原因。
- 采用指数退避重试；捕获 `httpx.HTTPStatusError` / `McpError`，必要时剥离上下文以规避 413/5xx。
- `FlowSearchService` 负责相似度过滤、缓存命中记录与 `UnmatchedFlow` 组合；Stage 3 结束后需根据日志统计确认命中率，并记录仍未命中的交换量。
- 如果日志出现大量 `flow_search.filtered_out` 且无命中，优先检查：① `exchangeName`/`unit` 是否缺失或拼写异常；② `clean_text` 是否传入过长上下文导致噪声；③ `.secrets` 中是否设置更大的 `timeout` 以应对慢响应。
- `mcp_tool_client.close_failed` 警告通常由请求完成后清理协程触发，属正常现象；若频繁超时，可调低 `flow_search_max_parallel` 或分批执行 Stage 3。

## 5. Flow Alignment
- 每个流程块的交换量在独立线程提交检索任务，聚合 `matched_flows` 与 `origin_exchanges`；未命中只在日志中计数提醒。
- Stage 3 脚本会按 §0 的 `FlowSearch hints` 规范校验 Stage 2 产物，必要时从同义词字段推断缺失名称，避免缺乏语义标签的交换直接进入 MCP 检索。
- 匹配成功时写回 `referenceToFlowDataSet`，失败则保留原始交换量并记录原因。
- 对于命中的流，Stage 3 必须将 `referenceToFlowDataSet.common:shortDescription` 写成 `baseName; treatmentStandardsRoutes; mixAndLocationTypes; flowProperties` 的拼接字符串（缺失字段填 `-`），确保流程交换量中可以直接读出名称、处理/路线、位置/场合以及数量信息。
- 过程中输出 `flow_alignment.start`、`flow_alignment.exchange_failed` 等结构化日志，便于诊断。

## 6. Process Extraction
- 顺序执行 `extract_sections` → `classify_process` → `normalize_location` → `finalize`。
- 处理要点：
  - `extract_sections` 按父级或别名分段；若未命中则回退全篇文本。
  - 若 LLM 未返回 `processDataSets` / `processDataSet`，抛出 `ProcessExtractionError`。
  - 表格字段需统一换算到易于对齐的基础单位（例如 t→kg、Nm³ 保持立方米、体积按密度说明假设），并在 `generalComment` 中标注换算逻辑。
  - `finalize` 通过 `build_tidas_process_dataset` 补齐 ILCD/TIDAS 必填字段，产出仅含 `processDataSet`（附 `process_id` 等元数据）的流程块；Stage 2 不再返回旧版 `exchange_list` 缓存。
  - **禁止** 在 Stage 2 输出中写入 ILCD 根级元数据（`@xmlns`、`@xmlns:common`、`@xmlns:xsi`、`@xsi:schemaLocation`、`@version`、`@locations` 等）；这些字段由 Stage 3/TIDAS 归一化器统一注入，任何上游自带值都会被覆盖。
- LLM 输出校验清单：
  1. 顶层必须是 `processDataSets` 数组；
  2. 每个流程需包含 `processInformation.dataSetInformation.name` 中的四个子字段：`baseName`、`treatmentStandardsRoutes`、`mixAndLocationTypes`、`functionalUnitFlowProperties`；
  3. 所有 `exchanges.exchange` 项需带 `exchangeDirection`、`meanAmount`、`unit`。
- 引导 LLM 不输出 `referenceToFlowDataSet` 占位符，Stage 3 会在对齐后写回；保留 `@dataSetInternalID` 以支撑 Stage 3 的制品生成与校验。
- 若需要对表格数值做清洗（单位补全、重复行合并），先写纯 Python 脚本验证逻辑，再落回 `ProcessExtractionService`；避免直接在回答里逐行手填。
- `merge_results` 合入对齐候选并生成功能单位字符串。
- Stage 3 相关逻辑在反序列化 `process_blocks` 时，务必从 `processDataSet.exchanges` 获取交换量，勿再依赖 `exchange_list`。

## 7. TIDAS Validation
- Stage 3 的制品导出步骤会执行 `uv run python -m tidas_tools.validate -i artifacts/<run_id>/exports` 校验 ILCD 结构，并将结果写入 `artifacts/<run_id>/cache/tidas_validation.json`。
- 若本地校验工具暂不可用，可在 Stage 3 追加 `--skip-artifact-validation` 并记录原因；在恢复校验前请避免额外触发发布，待校验恢复后再重新执行 Stage 3 以完成入库。

## 8. 工作流编排
- `WorkflowOrchestrator` 顺序执行：`preprocess` → `extract_processes` → `align_flows` → `merge_datasets` → `validate` → `finalize`。
- 返回 `WorkflowResult`，Stage 3 会写入 `workflow_result.json`，外部集成也可直接消费。

## 9. 验证建议
- 单元测试优先覆盖：`json_utils` 清洗、`FlowSearchService` 过滤/缓存、`FlowAlignmentService` 降级处理、流程抽取各阶段的错误分支与 `merge_results` 容错。
- 集成验证：使用最小论文样例依次执行 `stage1`→`stage3`，核对产物 schema、命中统计与校验报告；如需复核发布负载，可额外手动运行 `stage4_publish`，当前工作流在校验通过后会自动入库。
- 观测：启用 `configure_logging` JSON 输出并筛选 `flow_alignment.exchange_failed`、`process_extraction.parents_uncovered` 等关键事件，快速定位异常阶段。

## 10. 分类与地理辅助资源
- `tidas_processes_category.json` (`src/tidas/schemas/tidas_processes_category.json`) 是流程分类的权威来源，覆盖 ISIC 树的各级代码与描述。若 Codex 需要确认分类路径，先使用 `uv run python scripts/list_process_category_children.py <code>` 逐层展开（`<code>` 为空时输出顶层，例如 `uv run python scripts/list_process_category_children.py 01`）。必要时可通过 `tiangong_lca_spec.tidas.get_schema_repository().resolve_with_references("tidas_processes_category.json")` 读取局部节点，再将相关分支文本粘贴到对 Codex 的提问里，帮助其在有限上下文里挑选正确的 `@classId` / `#text`。
- 地理编码沿用 `tidas_locations_category.json` (`src/tidas/schemas/tidas_locations_category.json`)；用法与上面一致，命令为 `uv run python scripts/list_location_children.py <code>`（例如 `uv run python scripts/list_location_children.py CN` 查看中国内部层级）。在向 Codex 说明地理选项时，同样只摘录与当前流程相关的分支，避免传送整棵树。
- 若流程涉及流分类，可调用 `uv run python scripts/list_product_flow_category_children.py <code>` 查看产品流分类（数据源 `tidas_flows_product_category.json`），或调用 `uv run python scripts/list_elementary_flow_category_children.py <code>` 查看初级流分类（数据源 `tidas_flows_elementary_category.json`）。

## 11. Stage 4 发布与数据库 CRUD
- `stage4_publish.py` 调用 `tiangong_lca_remote` 的 `Database_CRUD_Tool` 将 `flows`、`processes`、`sources` 入库；Stage 3 校验成功后会自动触发（若检测到 `cache/published.json` 且未指定 `--force-publish`，自动发布会跳过）。手动重跑时，`insert` 请求的 `id` 必须直接沿用导出制品中的 UUID：分别取 `flowInformation.dataSetInformation.common:UUID`、`processInformation.dataSetInformation.common:UUID`、`sourceInformation.dataSetInformation.common:UUID`。禁止生成其他标识或复用旧 run 的 ID。
- 成功发布会在 `artifacts/<run_id>/cache/published.json` 写入时间戳及提交数量摘要；如需重新发布，删除该文件或在 Stage 3 添加 `--force-publish`。
- `Database_CRUD_Tool` 入参包含：
  - `operation`：`"select"`、`"insert"`、`"update"`、`"delete"`。
  - `table`：`"flows"`、`"processes"`、`"sources"`、`"contacts"`、`"lifecyclemodels"`。
- `jsonOrdered`：`insert` / `update` 必填，传入完整的 ILCD 文档（如 `{"processDataSet": {...}}`），保持命名空间、时间戳、引用字段。
  - `id`：`insert` / `update` / `delete` 必填，取自数据集 `dataSetInformation` 的 UUID。
  - `version`：`update` / `delete` 必填，对应 Supabase `version` 列。
  - 可选 `filters`、`limit` 仅在 `select` 时使用。
- 实际运行中如果 `insert` 因 UUID 冲突失败，发布器会自动改用 `update` 重试；请确保 `administrativeInformation.publicationAndOwnership.common:dataSetVersion` 与目标记录保持一致。
- 流属性映射已经整理为 `src/tiangong_lca_spec/tidas/flow_property_registry.py`，可通过 `uv run python scripts/flow_property_cli.py list` 查看全部映射，`show`/`match-unit` 根据名称或单位定位，`emit-block` 直接输出可嵌入的 ILCD `flowProperties` 片段。该 CLI 直接读取 `flowproperty_unitgroup_mapping.json`，脚本/自动化会与映射保持同步。
- Stage 3 与 Stage 4 发布策略更新：
  - 若对齐命中了现有流但缺少 `flow_properties` 信息，Stage 4 会基于原 UUID 重建数据集，引入正确的流属性块，并将 `common:dataSetVersion` 的补丁位 `+1` 后以 `update` 提交，从而在库内就地升级。
  - 若库中没有任何匹配流，则继续沿用原逻辑生成新流数据集（发布时分配新 UUID），同样会根据映射补齐参考单位组。
  - 初级流仍然只能复用现有条目，若 Stage 3 仍输出 `unmatched:placeholder`，需先补充提示或人工映射后再发布。
- Stage 4 额外暴露 `--default-flow-property <uuid>`（自定义找不到明确映射时的兜底属性，默认仍为质量属性 `93a60a56-a3c8-11da-a746-0800200b9a66`）与 `--flow-property-overrides overrides.json`（载入 `{"exchange": "...", "flow_property_uuid": "...", "process": "...", "mean_value": "..."}` 形式的覆盖项，可按交换名称或“流程+交换”精准指定属性及均值）。
- 成功响应会回显 `id`、`version` 以及 `data` 数组；校验失败会抛出 `SpecCodingError`，直接根据错误中的 JSON 路径修正数据集再重试。
- 入库自查清单：
  - 保留 ILCD 根级属性（`@xmlns`、`@xmlns:common`、`@xmlns:xsi`、schemaLocation）并写入 `administrativeInformation.dataEntryBy.common:timeStamp`、`common:referenceToDataSetFormat`、`common:referenceToPersonOrEntityEnteringTheData`。
  - 保持合规引用（ILCD 格式 UUID `a97a0155-0234-4b87-b4ce-a45da52f2a40`、权属 UUID `f4b4c314-8c4c-4c83-968f-5b3c7724f6a8`、数据录入人 UUID `f4b4c314-8c4c-4c83-968f-5b3c7724f6a8`）并声明 `modellingAndValidation.LCIMethod.typeOfDataSet`。
  - 流数据需补齐 `flowProperties.flowProperty`（质量属性 UUID `93a60a56-a3c8-11da-a746-0800200b9a66`）、`quantitativeReference.referenceToReferenceFlowProperty` 以及合适的 `classificationInformation` 或 `elementaryFlowCategorization`。
  - 流程数据保留 Stage 3 的功能单位、交换量与 `modellingAndValidation`；来源数据保留文献元信息与发布时间。
- 批量发布时优先按 `flows`→`processes`→`sources` 顺序提交，及时记录返回的 `id` / `version` 以备审计。
