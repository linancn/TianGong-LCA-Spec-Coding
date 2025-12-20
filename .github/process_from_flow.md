# Process From Flow 工作流说明

本文件说明 `src/tiangong_lca_spec/process_from_flow/service.py` 中的 LangGraph 工作流，帮助在不做多余动作的前提下清晰复用或扩展。

## 目标与输入
- 目标：从一份参考 flow 数据集（ILCD JSON）推导出对应的 process 数据集（ILCD 格式），必要时用占位符补齐缺失信息。
- 核心入口：`ProcessFromFlowService.run(flow_path, operation="produce", initial_state=None, stop_after=None)`。
- 依赖：LLM 为必选，用于技术描述、过程拆分、交换生成与候选选择；同时依赖 flow 搜索函数 `search_flows`（可注入自定义）与候选选择器（建议 LLM 版本）。
- `stop_after` 支持 `"tech"|"processes"|"exchanges"|"matches"`，用于调试时提前终止。

## 状态字段
工作流以状态字典传递数据，关键字段：
- `flow_path`：输入文件路径。
- `flow_dataset` / `flow_summary`：解析后的原始 flow 与摘要（名称、分类、注释、UUID、版本）。
- `technical_description` / `assumptions` / `scope`：技术路线描述。
- `processes`：过程拆分计划（process_id、名称、描述、是否参考流过程）。
- `process_exchanges`：每个过程的交换清单（仅结构，无匹配信息）。
- `matched_process_exchanges`：为每个交换附上 flow 搜索结果与已选候选。
- `process_datasets`：最终生成的 ILCD process 数据集。

## 节点顺序与行为
各节点会首先检查相应字段是否已存在，避免重复工作。
- load_flow：读取 `flow_path` JSON，生成 `flow_summary`（多语言名称、分类、通用注释等）。
- describe_technology：调用 `TECH_DESCRIPTION_PROMPT` 生成技术描述/假设/范围。
- split_processes：调用 `PROCESS_SPLIT_PROMPT` 拆分多个过程并标记参考流过程。
- generate_exchanges：调用 `EXCHANGES_PROMPT` 产出各过程的输入/输出交换（生产用 Output，处置/处理用 Input 作为参考流）。
- match_flows：对每个交换执行 flow 搜索（最多保留前 10 个），用 LLM 选择器挑选最合适的候选，并记录决策理由与未匹配项。
- build_process_datasets：组合前述信息生成 ILCD process 数据集（参考流方向随 operation 调整）：
  - 使用 `ProcessClassifier` 进行分类，失败时落到默认 Manufacturing。
  - 根据匹配结果引用真实 flow；缺失时创建占位 flow 引用。
  - 强制存在参考流交换；空量值回退为 `"1.0"`。
  - 自动填充功能单位、时间/地域、合规声明、数据录入与版权块；使用 `tidas_sdk.create_process` 进行模型校验（失败仅记录警告）。

## 产出与调试
- 正常运行返回完整状态，其中 `process_datasets` 为生成结果（可直接写出或继续处理）。
- 调试时可配合 `stop_after` 查看中间态，例如设置为 `"matches"` 只跑到流匹配阶段。

## 使用建议
- 确保 LLM 配置正确；未配置 LLM 时不应运行该流程。
- 在自定义 `flow_search_fn` 或选择器时保持返回/入参协议一致（`FlowQuery` → `(candidates, unmatched)`，候选含 uuid/base_name 等字段）。
