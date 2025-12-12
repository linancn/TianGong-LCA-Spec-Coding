# 产品流入库速查（含批量脚本）

面向直接调用 `tiangong_lca_remote` 的 `Database_CRUD_Tool` 入库产品流，避免多次重试/循环。先干运行（dry-run），确认无误再提交。

## 单条入库步骤
1. 环境：`.secrets/secrets.toml` 配好 `[tiangong_lca_remote]`，命令用 `uv run python ...`。
2. 分类路径：用 `tidas_flows_product_category.json` 查路径（不要人工猜）。参考 `product_flow_insert_manual.md` 里的 Python 片段，输入 `class_id` 打印全路径。
3. 填充字段：
   - 名称：`baseName` 中英各一条，可用 `leaf_name` / `leaf_name_zh` 作为默认值；`treatment`、`mix` 写明技术路线/交付场所。
   - 同义词：`common:synonyms` 英/中文各一条，避免空数组；未提供时用名称兜底。
   - 注释：`common:generalComment` 用来源描述英文原文。
   - 属性：默认 Mass（UUID `93a60a56-a3c8-11da-a746-0800200b9a66`），`meanValue` 设 1.0。
4. 构造 payload 调一次 MCP，成功后再处理下一条；如需核对，运行 `operation=select` 查询该 UUID。

## 批量脚本 `scripts/md/bulk_insert_product_flows.py`
- 输入：JSON/JSONL 数组，字段支持 `class_id`、`leaf_name`、`leaf_name_zh`、`desc`，可选 `base_en`、`base_zh`、`en_synonyms`、`zh_synonyms`、`treatment`、`mix`、`comment`。
- 干运行（默认）：  
  ```bash
  uv run python scripts/md/bulk_insert_product_flows.py --input flow_class_with_desc.json
  ```
- 真正提交加 `--commit`：  
  ```bash
  uv run python scripts/md/bulk_insert_product_flows.py --input flow_class_with_desc.json --commit
  ```
- 查询已入库：`--select-id <uuid>`；日志输出 `artifacts/bulk_insert_product_flows_log.csv`。

## 避免
- 不要对同一 UUID 反复 insert；如需修改用 `update` 或换新 UUID。
- 发现报错先看 credential / payload 结构 / 分类路径是否缺失，避免盲目重试。
- 不在这里触发 Stage 3 对齐逻辑，本流程仅做直接入库。***
