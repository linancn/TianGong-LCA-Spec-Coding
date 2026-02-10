# 产品流生成与入库（tidas_sdk + Database_CRUD_Tool，LLM 约束 treatment/mix）

面向“原点”目录的自动化脚本，输入类目清单自动生成 ILCD flow，选定时写入远端 DB。默认使用 SDK 内置的产品流分类（`tidas_flows_product_category.json`），输入数据放在 `input_data/origin/*`。

## 工作要点
- 命令全部用 `uv run python ...`。
- 分类：使用 SDK 的产品流分类导航器（`tidas_flows_product_category.json`）；不再支持传入其他分类文件。
- 输入：`input_data/origin/manual_flows/flow_class_with_desc.json`（字段：`class_id`, `leaf_name`, `leaf_name_zh`, `desc`）。
- 输出：`artifacts/cache/manual_flows/{classid}_{uuid}_{version}.json|.xml`，汇总 `artifacts/cache/manual_flows/product_flow_sdk_insert_summary.json`，LLM 选择日志 `llm_mix_rules.jsonl`。
- 名称/注释：`baseName` 中英各一条；`common:generalComment` 中英文各一条，支持 `--translate-desc`：若原描述是英文则译成中文，若原描述包含中文则译成英文；不开启则两边复用同一文本；`common:synonyms` 不写；脚本会将分号（含全角）替换为逗号。
- 属性：流属性 Mass（UUID `93a60a56-a3c8-11da-a746-0800200b9a66`，版本 `03.00.003`，`meanValue=1.0`），`referenceToReferenceFlowProperty="0"`。
- 时间戳：UTC 字符串 `YYYY-MM-DDTHH:MM:SSZ`。
- 治理：Compliance 参考 ILCD Entry-level，联系人写 Tiangong LCA Data Working Group，版本默认 `01.01.000`。
- Treatment/Mix：仅用 LLM 在固定选项内选择，LLM 失败则该条报错；分号（含全角）统一替换为逗号。

## 单条/小批量运行
- 干运行（仅生成文件，不入库）：  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --class-id 23161`
- 指定起止或数量：  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --start-index 100 --limit 20`
- 指定 LLM 模型（默认读取 .secrets [openai].model，否则用 gpt-4o-mini）：  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --class-id 23161 --llm-model gpt-4o`
- 真正入库（MCP Database_CRUD_Tool）：  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --class-id 23161 44428 --commit`

## 全量/后台跑
- 全量（新 UUID，每次运行都会新增）：  
  `uv run python scripts/product_flow/product_flow_sdk_insert.py --commit`
- 后台运行（防止会话中断）：  
  ```bash
  mkdir -p logs
  nohup uv run python scripts/product_flow/product_flow_sdk_insert.py --commit \
    > logs/product_flow_sdk_insert_all.log 2>&1 &
  ```
  查看进度：`tail -f logs/product_flow_sdk_insert_all.log`

## 避免与注意
- 避免对同一 UUID 反复 insert；若需更新请用 update 或重新生成新 UUID。
- 覆盖输入路径时，确保保持 `input_data/origin/*` 与 `scripts/origin/*` 一致约定。
- 如果 LLM 选择异常或入库失败，先修正 payload/分类再重试，避免盲目重复调用。
- 分类 schema 必须是产品流类别（TIDAS）；不要再传 CPC 分类 JSON。
