# Product Flow 手工新建/入库规范（Manual Adapter）

本文档定位为“新建 Product Flow”的手工/半自动执行规范。  
它对应统一能力中的 `manual_insert_adapter`，不是 `process_from_flow` 主链文档。

## 适用范围
- 适用：人工确认后新建/更新 product flow（单条或批量）。
- 不适用：`process_from_flow` 的自动流程编排、Stage3 对齐逻辑、JSON-LD 抽取流程。

## 与其他文档/脚本关系
- 本文：规范层（做什么、怎么做才合规）。
- `scripts/md/bulk_insert_product_flows.py`：当前 manual adapter 的批量执行脚本。
- `scripts/product_flow/product_flow_sdk_insert.py`：classification 批量任务脚本（另一类 adapter）。
- `src/tiangong_lca_spec/product_flow_creation/service.py`：统一 `ProductFlowCreationService`（公共 builder，负责 ILCD 组装 + `tidas_sdk` 校验）。
- `src/tiangong_lca_spec/product_flow_creation/dedup.py`：`FlowDedupService`（发布前动作决策：insert/update/reuse），当前由 `FlowPublisher` 发布链路使用。
- `src/tiangong_lca_spec/publishing/crud.py`、`src/tiangong_lca_spec/workflow/artifacts.py`：已接入同一 builder；`FlowPublisher.publish` 已调用 dedup 决策避免重复插入。
- `scripts/origin/process_from_flow_langgraph.py`：process_from_flow 主链入口（非 manual）。

## 统一能力约束（硬规则）
1. 分类路径必须来自 `tidas_flows_product_category.json`，禁止人工猜测。
2. `classificationInformation` 必填。
3. `common:synonyms` 必须提供 EN/ZH 两条；缺失时用 `baseName` 回填，不允许空。
4. `flowProperties` 默认使用 Mass（UUID `93a60a56-a3c8-11da-a746-0800200b9a66`，`meanValue=1.0`）。
5. 当前 `bulk_insert_product_flows.py --commit` 执行策略为直接 `insert`；若需 `reuse/update`，需先 `--select-id` 或改走 `FlowPublisher` 发布链路。
6. 单条只做一次发布动作；失败先修 payload 再重试，禁止盲目循环重跑。

## 当前执行入口（bulk 手工批量）
`scripts/md/bulk_insert_product_flows.py`

- 技术路线：先按默认策略+输入覆盖生成 `ProductFlowCreateRequest`，再调用 `ProductFlowCreationService` 组装并走 `tidas_sdk.create_flow(validate=True)` 校验与标准化，最后执行 CRUD 发布（不再走未校验 JSON 直发）。

- 输入字段：
  - 必填：`class_id`、`leaf_name`
  - 建议：`leaf_name_zh`、`desc`
  - 可选覆盖：`base_en`、`base_zh`、`en_synonyms`、`zh_synonyms`、`treatment`、`mix`、`comment`
- 运行：
  ```bash
  uv run python scripts/md/bulk_insert_product_flows.py --input <json_or_jsonl>
  uv run python scripts/md/bulk_insert_product_flows.py --input <json_or_jsonl> --commit
  ```
- 查询：
  ```bash
  uv run python scripts/md/bulk_insert_product_flows.py --select-id <uuid>
  ```

## 发布前检查清单
- 分类路径完整且层级有序。
- `baseName`/`treatment`/`mix` 语义清晰，不含分号。
- `common:synonyms` 已有 EN/ZH 且非空。
- `common:generalComment` 有可追溯来源说明。
- 确认本次动作是 `insert` 还是 `update`（避免同 UUID 误插入）。

## 迁移说明（统一收束）
manual 入口持续保留，核心构建逻辑已收束到统一 `ProductFlowCreationService`；`FlowPublisher`/`artifacts` 已复用该能力。dedup 动作决策当前已在 `FlowPublisher` 落地，manual/bulk 入口后续可按同一策略接入。
