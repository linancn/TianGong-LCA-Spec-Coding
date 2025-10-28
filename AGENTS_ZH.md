# 天工 LCA Spec Coding 开发须知

本指南聚焦工程协同的通用约定：帮助团队理解仓库定位、开发环境、凭据管理与质量自检流程。流程抽取与分阶段脚本的详细职责在 `.github/prompts/extract-process-workflow.prompt.md` 中展开说明。

## 1. 项目概览
- **目标**：实现天工 LCA Spec Coding 工作流的端到端自动化，涵盖论文清洗、流程抽取、交换量对齐、数据合并、TIDAS 校验与最终交付。
- **核心目录**：
  - `src/tiangong_lca_spec/`：工作流服务、MCP 客户端、数据模型与日志工具。
  - `scripts/`：`stage1_preprocess.py` ~ `stage7_publish.py` 等阶段化 CLI，以及回归入口 `run_test_workflow.py`。
  - `.github/prompts/`：对 Codex 的提示词说明，其中 `extract-process-workflow.prompt.md` 专门描述流程抽取任务。
- **协作接口**：标准工作流依赖 `.secrets/secrets.toml` 中配置的 OpenAI、tiangong LCA Remote 与 TIDAS 验证服务。首次接入时请先完成凭据校验，再批量运行 Stage 3+。
- **更多参考**：各阶段产物要求、对齐策略和异常处理见 `.github/prompts/extract-process-workflow.prompt.md`；若需补充分类或地理信息，可查看 `scripts/list_*_children.py` 提供的辅助 CLI。

## 2. 开发环境与依赖
- **Python 版本**：≥ 3.12，推荐通过 `uv toolchain` 管理，默认虚拟环境位于 `.venv/`。
- **命令约定**：工作站不暴露系统级 `python`，请使用 `uv run python …` 或 `uv run -- python script.py`；单行脚本可写成 `uv run python - <<'PY'`。
- **依赖安装**：
  ```bash
  uv sync                # 安装运行依赖
  uv sync --group dev    # 安装含 black/ruff 的开发依赖
  ```
  如需镜像可临时设置 `UV_PYPI_URL=https://pypi.tuna.tsinghua.edu.cn/simple`。
- **关键运行库**：`anyio`, `httpx`, `mcp`, `pydantic`, `pydantic-settings`, `tenacity`, `structlog`, `python-dotenv`, `jsonschema`, `openai`。
- **构建体系**：项目使用 `hatchling`，`pyproject.toml` 在 `[tool.hatch.build.targets.wheel]` 下声明 `src/tiangong_lca_spec` 为构建目标。

## 3. 凭据与远程服务
1. 复制模板并生成本地配置：`cp .secrets/secrets.example.toml .secrets/secrets.toml`。
2. 编辑 `.secrets/secrets.toml`：
   - `[OPENAI]`：`API_KEY`, `MODEL`（默认 `gpt-5` 可覆盖）。
   - `[tiangong_lca_remote]`：`url`, `service_name`, `tool_name`, `api_key`。
   - `[tidas_data_validate]`：`url`, `tool_name`, 可选 `api_key`。
3. `api_key` 字段直接写入明文 token，框架会自动带上 `Bearer` 前缀。
4. 建议在跑 Stage 3 前，先用 1~2 个样例交换调用 `FlowSearchService` 进行连通性自测（可参考工作流提示文档中的 Python 片段）。
- 若运维已预先配置 `.secrets/secrets.toml`，Codex 默认直接使用，无需在执行前反复确认。仅当脚本报出缺少凭据或连接失败时，再检查本地配置。

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
- 提交前请确认 `artifacts/` 中的中间文件不会被误提交，必要时在本地清理或加入 `.gitignore`。

## 5. 支持与沟通
- 代码疑问先查阅 `src/tiangong_lca_spec/` 对应模块的 docstring 与类型定义，保持术语一致。
- 当外部服务不可用或长时间超时时，记录复现步骤与日志片段，第一时间同步到运维或流程负责人。
- 如果需要扩展提示词或梳理流程分工，请优先更新 `.github/prompts/` 目录下对应文档，保持与本文件的角色划分一致。
