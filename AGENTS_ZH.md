# 天工 LCA Spec Coding 开发须知

本指南聚焦工程协同的通用约定：帮助团队理解仓库定位、开发环境、凭据管理与质量自检流程。流程抽取与分阶段脚本的详细职责在 `.github/prompts/extract-process-workflow.prompt.md` 中展开说明。

## 1. 项目概览
- **目标**：实现天工 LCA Spec Coding 工作流的端到端自动化，涵盖论文清洗、流程抽取、交换量对齐、数据合并、TIDAS 校验与最终交付。
- **核心目录**：
  - `src/tiangong_lca_spec/`：工作流服务、MCP 客户端、数据模型与日志工具。
  - `scripts/md/`：`stage1_preprocess.py` ~ `stage4_publish.py` 等阶段化 CLI，以及回归入口 `run_test_workflow.py`。
  - `scripts/jsonld/`：面向 OpenLCA JSON-LD 的抽取、校验与发布脚本。
  - `.github/prompts/`：对 Codex 的提示词说明，其中 `extract-process-workflow.prompt.md` 专门描述流程抽取任务。
  - `scripts/kb/`：知识库工具（如用于导入的 `import_ris.py`、用于从对象存储同步解析结果的 `minio_fetch.py`、用于检索验收的 `retrieve.py`），用于将参考文献 PDF 批量导入天工数据集。
- **协作接口**：标准工作流依赖 `.secrets/secrets.toml` 中配置的 OpenAI、tiangong LCA Remote 与 TIDAS 验证服务。首次接入时请先完成凭据校验，再批量运行 Stage 3+。
- **更多参考**：各阶段产物要求、对齐策略和异常处理见 `.github/prompts/extract-process-workflow.prompt.md`；若需补充分类或地理信息，可查看 `scripts/md/list_*_children.py` 提供的辅助 CLI。
- **Stage 4 流发布**：当需要补齐缺失的 Flow 时，发布器会调用配置好的 LLM 自动推断流类型，并借助 `scripts/md/list_product_flow_category_children.py` 逐级细化产品分类，确保最终落到最具体的类别。请确认凭据就绪，避免发布阶段因无法访问 LLM 而退回默认分类。

## 2. 开发环境与依赖
- **Python 版本**：≥ 3.12，推荐通过 `uv toolchain` 管理，默认虚拟环境位于 `.venv/`。
- **命令约定**：工作站不暴露系统级 `python`，请使用 `uv run python …` 或 `uv run -- python script.py`；单行脚本可写成 `uv run python - <<'PY'`。
- **依赖安装**：
  ```bash
  uv sync                # 安装运行依赖
  uv sync --upgrade  # 升级所有依赖到最新允许版本
  uv sync --group dev    # 安装含 black/ruff 的开发依赖
  ```
  如需镜像可临时设置 `UV_PYPI_URL=https://pypi.tuna.tsinghua.edu.cn/simple`。
- **关键运行库**：`anyio`, `httpx`, `mcp`, `pydantic`, `pydantic-settings`, `tenacity`, `structlog`, `python-dotenv`, `jsonschema`, `openai`。
- **构建体系**：项目使用 `hatchling`，`pyproject.toml` 在 `[tool.hatch.build.targets.wheel]` 下声明 `src/tiangong_lca_spec` 为构建目标。

## 3. 凭据与远程服务
1. 复制模板并生成本地配置：`cp .secrets/secrets.example.toml .secrets/secrets.toml`。
2. 编辑 `.secrets/secrets.toml`：
   - `[openai]`：`api_key`, `model`（默认 `gpt-5` 可覆盖）。
   - `[tiangong_lca_remote]`：`url`, `service_name`, `tool_name`, `api_key`。
   - `[kb]`：`base_url`, `dataset_id`, `api_key` 以及可选 `timeout`/`metadata_fields`（默认包含 `meta` 与 `category` 两个字段）。
   - `[kb.pipeline]`：`datasource_type`, `start_node_id`, `is_published`, `response_mode` 与可选 `inputs`，用于驱动 RAG pipeline。`start_node_id` 需从可视化编排器中复制。
   - `[minio]`：配置解析产物所在的 MinIO 桶，需提供 `endpoint`, `access_key`, `secret_key`, `bucket_name`, `prefix`，可选 `secure`（默认按 `endpoint` 协议判断）与 `session_token`。
3. `api_key` 字段直接写入明文 token，框架会自动带上 `Bearer` 前缀。
4. 建议在跑 Stage 3 前，先用 1~2 个样例交换调用 `FlowSearchService` 进行连通性自测（可参考工作流提示文档中的 Python 片段）。
- 若运维已预先配置 `.secrets/secrets.toml`，Codex 默认直接使用，无需在执行前反复确认。仅当脚本报出缺少凭据或连接失败时，再检查本地配置。

**知识库导入**
- 在 `.secrets/secrets.toml` 的 `[kb]` 中填入真实的 host（示例：`https://<kb-host>/v1`）、数据集 ID 与 API key。
- 通过 `[kb.pipeline]` 配置 RAG pipeline：`datasource_type` 与文件节点类型一致（通常是 `local_file`），`start_node_id` 需在 UI 中查看节点详情后填写，`inputs` 可按需提供必填的输入字段。未设置时使用 pipeline 默认值。
- 默认元数据包含 `meta`（自动拼接作者/年份/期刊/DOI/URL 的引文）与 `category`（取 `input_data/` 下的首层子目录名称，如 `battery`）。若需覆盖，可传 `--category`。
- 使用 `uv run python scripts/kb/import_ris.py --ris-dir input_data/<目录>`（或 `--ris-path ...`）导入 RIS 文献；若只需验证流程可加 `--dry-run`。附件需与 RIS 文件位于同一 `input_data/<目录>` 下。
- 若需快速验证知识库检索效果，可运行 `uv run python scripts/kb/retrieve.py --query "<文本>" --top-k 5`，该 CLI 支持配置搜索方式、元数据过滤与 rerank 选项，方便在接入前做连通性检查。
- 导入过程改为调用 `/pipeline/file-upload` + `/pipeline/run`，确保和 UI 中的 pipeline 完全一致，pipeline 完成后脚本再为生成的 document 附加 `meta` 与 `category`。
- 若需要从 MinIO 同步解析后的 `meta.txt`/`parsed.json`/`pages/`/`source.pdf`，先填写 `[minio]`，再使用 `uv run python scripts/kb/minio_fetch.py list --path <远程子目录>` 查看可用对象，或用 `uv run python scripts/kb/minio_fetch.py download --path <远程子目录> --output input_data/<目录> --include-source` 拉取到本地（如不需要 PDF 可省略 `--include-source`）。支持 `--dry-run` 预览即将下载的文件。

## 4. 质量保障与自检
- 在修改 Python 源代码后，按序执行：
  ```bash
  uv run black .
  uv run ruff check
  uv run python -m compileall src scripts
  uv run pytest
  ```
- 若涉及流程抽取或对齐逻辑，优先跑一次最小化 Stage 1→Stage 6 端到端流程，命令示例与阶段要求见 `.github/prompts/extract-process-workflow.prompt.md`。
- 结构化日志默认使用 `structlog`，可在运行 CLI 时关注 `flow_alignment.*`、`process_extraction.*` 等事件以快速定位异常。

## 5. 支持与沟通
- 代码疑问先查阅 `src/tiangong_lca_spec/` 对应模块的 docstring 与类型定义，保持术语一致。
- 当外部服务不可用或长时间超时时，记录复现步骤与日志片段，第一时间同步到运维或流程负责人。
- 如果需要扩展提示词或梳理流程分工，请优先更新 `.github/prompts/` 目录下对应文档，保持与本文件的角色划分一致。
