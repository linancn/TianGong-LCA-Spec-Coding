# 产品流生成与入库（ProductFlowCreationService + Database_CRUD_Tool，LLM 约束 treatment/mix）

面向 `input_data/origin/*` 的 classification 批量入口。脚本先生成结构化请求，再交给统一 builder 产出并校验 ILCD flow；`--commit` 时调用远端 `Database_CRUD_Tool`。

## 工作要点
- 命令统一使用 `uv run python ...`。
- 分类路径：固定使用 SDK 产品流分类导航（`tidas_flows_product_category.json`），不再支持外部分类文件。
- 输入：`input_data/origin/manual_flows/flow_class_with_desc.json`（字段：`class_id`, `leaf_name`, `leaf_name_zh`, `desc`）。
- 输出：`artifacts/cache/manual_flows/{classid}_{uuid}_{version}.json|.xml`，汇总 `artifacts/cache/manual_flows/product_flow_sdk_insert_summary.json`，LLM 选择日志 `llm_mix_rules.jsonl`。
- 组装链路：`scripts/product_flow/product_flow_sdk_insert.py` 只负责 LLM 选择/翻译与参数准备，ILCD 组装与 `tidas_sdk.create_flow(validate=True)` 校验由 `src/tiangong_lca_spec/product_flow_creation/service.py`（`ProductFlowCreationService`）统一完成。
- 复用现状：同一 `ProductFlowCreationService` 也被 `scripts/md/bulk_insert_product_flows.py`、`src/tiangong_lca_spec/publishing/crud.py`（`FlowPublisher`）和 `src/tiangong_lca_spec/workflow/artifacts.py` 复用。
- 去重现状：发布期 dedup（`insert/update/reuse`）目前在 `FlowPublisher` 路径由 `FlowDedupService` 承担；本脚本 `--commit` 仍按 `insert` 执行，不自动做 `select+update` 切换。
- 名称/注释：`baseName` 与 `common:generalComment` 均输出 EN/ZH；`--translate-desc` 会补齐另一语种；`common:synonyms` 固定写 EN/ZH 两条（缺失时回填 `baseName`）；`quantitativeReference.referenceToReferenceFlowProperty="0"`。
- 流属性：Mass（UUID `93a60a56-a3c8-11da-a746-0800200b9a66`，版本 `03.00.003`，`meanValue="1.0"`）。
- 治理默认值：Compliance=`ILCD Data Network - Entry-level`；联系人/所有者=`Tiangong LCA Data Working Group`；版本默认 `01.01.000`。
- 时间戳：UTC 字符串 `YYYY-MM-DDTHH:MM:SSZ`。
- Treatment/Mix：仅允许 LLM 从固定枚举中选择；失败则该条报错；分号（全角/半角）会被替换为逗号。

## Treatment/Mix 受限 LLM 提示词
用于自动化内置选择，仅允许返回给定选项：
- `treatmentStandardsRoutes`：处理方式/标准/品质/用途/路线限定（逗号分隔）。
- `mixAndLocationTypes`：生产/消费混合与交付位置（如 at plant / at farm gate / at landing site / to consumer）。

模型来源：优先 `.secrets/secrets.toml` 的 `[openai].model`，否则 `gpt-4o-mini`。

选项：
- treatment: `Seed-grade, cleaned for sowing` | `Harvested grain, unprocessed` | `Fresh, unprocessed produce` | `Raw milk, chilled` | `Eggs, shell-on` | `Greasy wool, unscoured` | `Raw honey` | `Unprocessed roundwood` | `Unprocessed catch, landing quality` | `Live animal, unprocessed` | `Finished product, manufactured` | `Unspecified treatment`
- mix: `Production mix, at farm gate` | `Production mix, at forest roadside` | `Production mix, at landing site` | `Production mix, at plant` | `Consumption mix, at plant` | `Production mix, to consumer` | `Consumption mix, to consumer`

提示词：
```text
You provide two ILCD fields for a product flow:
- treatmentStandardsRoutes: technical qualifiers (treatment received, standard fulfilled, product quality, use info, production route name), comma-separated.
- mixAndLocationTypes: production/consumption mix and delivery point (e.g., at plant / at farm gate / at forest roadside / at landing site / to consumer), comma-separated.
Select ONLY from the given options; do not invent new text. If the flow is a finished manufactured product, prefer 'Finished product, manufactured' + 'Production mix, at plant'.
If the flow is clearly agricultural/livestock/forestry/fish, pick the matching farm gate / forest roadside / landing site + corresponding treatment. Otherwise keep plant.
Respond strict JSON: {"treatment_en": <option>, "mix_en": <option>} with no extra keys.
class_id: <...>
leaf_name: <...>
description: <... or N/A>
treatment_options: [...]
mix_options: [...]
```

## CLI 用法
- 干运行（仅生成文件）：  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --class-id 23161 44428`
- 指定模型（默认读 `.secrets`）：  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --class-id 23161 --llm-model gpt-4o`
- 入库（单次会话复用 MCP 客户端）：  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --class-id 23161 44428 --commit`

## 注意事项
- 脚本自身不做 dedup 动作切换；如同 UUID 已存在，需先修正输入策略（改 UUID / 改走 update 路径）再执行。
- 失败时先修 payload/分类再重试，不要盲目循环调用。
- 分类必须来自 TIDAS 产品流分类，不能传 CPC 分类 JSON。
