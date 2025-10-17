# 天工 LCA 规范编码工作流（Python 实现草案）

本说明将原有的 Dify 工作流拆解为可直接用 Python 编排的可执行组件，约束输入输出结构、算法步骤与错误处理策略，便于在仓库内完成 spec coding。

## 1. 架构概览
- 建议实现一个 Python Orchestrator（脚本或包入口），依次调用四个阶段化模块：
  1. `flow_search`：面向远程 LCA 数据库的流检索。
  2. `flow_alignment`：将文献 exchange 与数据库候选流对齐。
  3. `process_extraction`：从文献构建完整的 ILCD `processDataSets` 并融合匹配结果。
  4. `tidas_validation`：调用 TIDAS 服务校验最终 JSON。
- 每个阶段定义清晰的函数接口与数据模型（建议使用 `dataclasses` 或 Pydantic），禁止依赖 Dify 节点 ID。
- 模块间通过结构化对象传递数据，输入/输出一律使用 UTF-8 JSON，可直接序列化落盘或作为下游调用参数。

### 1.1 推荐数据结构
```python
@dataclass
class FlowQuery:
    exchange_name: str
    description: str
    process_name: str
    paper_md: str

@dataclass
class FlowCandidate:
    uuid: str | None
    base_name: str
    treatment_standards_routes: str | None
    mix_and_location_types: str | None
    flow_properties: str | None
    version: str | None
    general_comment: str | None
    geography: dict | None
    classification: list[dict] | None
    reasoning: str

@dataclass
class UnmatchedFlow:
    base_name: str
    general_comment: str | None
    status: Literal["requires_creation"]
    process_name: str

@dataclass
class ProcessDataset:
    process_information: dict
    modelling_and_validation: dict
    administrative_information: dict
    exchanges: list[dict]
```

## 2. Flow Search 模块（原阶段 2）
- **接口**：`search_flows(query: FlowQuery) -> tuple[list[FlowCandidate], list[UnmatchedFlow]]`
- **职责**：
  1. 调用 MCP HTTP API（`https://lcamcp.tiangong.earth/mcp`），发送 `exchange_name` 为核心的查询；推荐封装在 `flow_search.client` 中，统一鉴权头、超时、重试策略。
  2. 对响应结果执行本地校验：名称相似度、描述关键词匹配、单位与 flow property 一致性、地理匹配等（可将规则独立为 `validators.py`）。
  3. 将通过校验的记录序列化为 `FlowCandidate`；未通过的记录以 `UnmatchedFlow(status="requires_creation")` 形式保留。
- **实现要点**：
  - 解析响应时先剥离 `<think>`、Markdown 代码块，再做括号配对确保 `json.loads` 成功；对双重编码字符串执行两次解码尝试。
  - 对空结果或 API 错误抛出自定义异常（如 `FlowSearchError`），由 orchestrator 决定回退策略。
  - 详细记录日志：请求参数、筛选理由、最终候选数。

## 3. Flow Alignment 模块（原阶段 3）
- **接口**：`align_exchanges(process_dataset: dict, paper_md: str) -> dict`
  - `process_dataset` 至少包含 `processInformation` 与 `exchanges.exchange`。
  - 返回结构建议：
    ```python
    {
        "process_name": str,
        "matched_flows": list[FlowCandidate],
        "unmatched_flows": list[UnmatchedFlow]
    }
    ```
- **算法步骤**：
  1. 遍历 `exchanges.exchange`，抽取 `exchangeName`、`exchangeDirection`、`generalComment1` 生成 `FlowQuery` 列表。
  2. 对每个 `FlowQuery` 调用 `flow_search.search_flows`。若无命中，按 `UnmatchedFlow` 记录。
  3. 汇总所有结果，构造 `exchangeName -> FlowCandidate` 映射，供 `process_extraction` 合并。
- **实现提示**：
  - 非列表的 `exchange` 自动包装为单元素列表。
  - 支持批量/并行：可在 orchestrator 通过线程池或异步调度执行多个 `FlowQuery`。
  - 输出结果应将原始 exchange 信息一并返回，避免重复解析。

## 4. Process Extraction 模块（原阶段 6）
- **目标**：给定 LCA 文献 Markdown（通常是段落数组组成的 JSON 字符串），产出完整的 `ProcessDataset` 列表，并融合数据库匹配结果与原始 exchange。
- **主要函数**：
  1. `preprocess_paper(md_json: str) -> str`
     - 解析 JSON，拼接正文，移除参考文献/附录，限制长度（默认 ≤120000 字符）。
  2. `extract_sections(clean_text: str) -> dict`
     - 可通过 LLM 或规则分别生成：
       - `process_information`
       - `administrative_information`
       - `modelling_and_validation`
       - `exchange_list`
       - `notes`（可选，用于记录子过程、数据缺口）
  3. `classify_process(process_info: dict) -> dict`
     - 基于本地 ISIC 枚举表匹配 1–4 层分类；返回 `[{ "@level": "...", "@classId": "...", "#text": "..." }, ...]`。
  4. `normalize_location(process_info: dict) -> dict`
     - 首先匹配宏观区域（GLO、RER 等）；若落在中国，再细分省市（CN-XX 枚举）；返回 `{"code": "...", "description": "..."}`。
  5. `merge_results(process_blocks: list[dict], matched_lookup: dict[str, list[FlowCandidate]], origin_exchanges: dict[str, list[dict]]) -> list[ProcessDataset]`
     - 以 `processInformation.dataSetInformation.name` 为键，整合流程信息、模型信息、行政信息、匹配流结果与原始 exchange。
- **合并策略**：
  - `matched_lookup` 由 `align_exchanges` 提供，键为 exchange 名称或 process 名称；在 `merge_results` 中根据实际需求选择索引方式。
  - 对匹配到 UUID 的流补写 `referenceToFlowDataSet`，缺失项保留占位字段并在 `UnmatchedFlow` 列表中追踪。
  - `determine_functional_unit` 根据首个非废弃输出（排除 `waste`、`flue gas` 等关键词），组合 `resultingAmount + unit + exchangeName`。
- **异常处理**：
  - 所有 JSON 响应先去除 `<think>` 与 ```json 包裹，再使用括号计数截取有效对象。
  - 对 LL M 返回结果、分类/地理命中失败抛出显式异常，以便 orchestrator 选择重试或人工介入。

## 5. TIDAS Validation 模块（原阶段 10）
- **接口**：`validate_with_tidas(process_datasets: list[ProcessDataset]) -> list[dict]`
- **流程**：
  1. 将 `ProcessDataset` 序列化为符合 TIDAS 输入要求的 JSON。
  2. 通过 MCP HTTP（`http://192.168.1.140:9278/mcp`）调用 `Tidas_Data_Validate_Tool`，传入 JSON 文本。
  3. 解析返回值为结构化报告（字段：`severity`、`message`、`path`、`suggestion` 等）。
  4. 失败时抛出 `TidasValidationError`，可选择重试/降级。

## 6. Orchestrator 运行示例
```python
def run_pipeline(paper_path: Path) -> None:
    raw_md_json = paper_path.read_text(encoding="utf-8")
    clean_text = preprocess_paper(raw_md_json)
    process_blocks = extract_sections(clean_text)

    alignment_results = []
    for block in process_blocks:
        alignment = align_exchanges(block, clean_text)
        alignment_results.append(alignment)

    matched_lookup = {
        result["process_name"]: result["matched_flows"]
        for result in alignment_results
    }
    origin_exchanges = {
        result["process_name"]: block["exchanges"]["exchange"]
        for result, block in zip(alignment_results, process_blocks)
    }

    final_datasets = merge_results(process_blocks, matched_lookup, origin_exchanges)
    validation_report = validate_with_tidas(final_datasets)

    write_outputs(final_datasets, validation_report)
```
- `write_outputs` 负责落盘 JSON、校验报告与日志（建议保存到 `artifacts/`）。
- 通过配置文件管理 API URL、鉴权、批处理上限等参数；可使用 `pydantic.BaseSettings` 或 `dotenv`。

## 7. 实施与测试建议
- 为每个模块编写独立的单元测试，覆盖：
  - JSON 清洗函数对 `<think>`、双重编码、空结果的处理。
  - 分类与地理匹配遇到未知值时的降级逻辑。
  - `merge_results` 在缺少 `FlowCandidate`、功能单位推断失败时的行为（应提供默认值或错误提示）。
- 抽象 HTTP 客户端接口，便于在测试中使用 Mock。
- 优先实现纯函数部分，再集成外部调用，降低调试复杂度。

## 8. 目录与下一步
1. 在 `src/` 下建立 `flow_search/`, `flow_alignment/`, `process_extraction/`, `tidas_validation/`, `orchestrator/` 等模块目录，并在 `__init__.py` 暴露公共接口。
2. 整理配置与密钥管理方案（环境变量或 `.env`），避免在代码中硬编码令牌。
3. 根据业务需求逐步补充日志、缓存、并发、失败重试等横切能力。
