# 天工 LCA Spec Coding 开发须知

本说明汇总仓库的环境配置、依赖管理、开发命令与辅助脚本要求，确保在着手/维护工作流实现前具备一致的工程基础。

## 1. 环境准备
- **Python**：版本需 ≥ 3.12，推荐通过 `uv toolchain` 管理；仓库默认虚拟环境位于 `.venv/`。
- **依赖同步**：
  ```bash
  uv sync                # 安装运行依赖
  uv sync --group dev    # 安装含 black/ruff 的开发依赖
  ```
  如需使用清华镜像，可在命令前加 `UV_PYPI_URL=https://pypi.tuna.tsinghua.edu.cn/simple`。
- **关键运行依赖**：`anyio`, `httpx`, `mcp`, `pydantic`, `pydantic-settings`, `tenacity`, `structlog`, `python-dotenv`, `jsonschema`, `openai`。
- **构建体系**：`hatchling` 负责 wheel 构建；`pyproject.toml` 在 `[tool.hatch.build.targets.wheel]` 中声明 `src/tiangong_lca_spec`。

## 2. 密钥与服务配置
1. 复制示例配置：`cp .secrets/secrets.example.toml .secrets/secrets.toml`。
2. 在 `.secrets/secrets.toml` 填写：
   - `[OPENAI]`：`API_KEY`, `MODEL`（默认 `gpt-5` 可覆盖）。
   - `[tiangong_lca_remote]`：`url`, `service_name`, `tool_name`, `api_key`。
   - `[tidas_data_validate]`：`url`, `tool_name`, 可选 `api_key`。
3. 所有 `api_key` 字段填写裸 token，框架会自动添加 `Bearer` 前缀。

## 3. 常用开发命令
- 每次 agent 运行最后都要执行的步骤：
  ```bash
  uv run black .
  uv run ruff check
  uv run python -m compileall src scripts
  uv run pytest
  ```
- 工作流示例运行位于 `scripts/` 目录，可通过 `uv run python scripts/<stage>.py ...` 执行。

## 4. 辅助脚本
- `python scripts/list_location_children.py [编码]`
- `python scripts/list_flow_category_children.py [编码]`
- `python scripts/list_process_category_children.py [编码]`

上述脚本用于查询地理、流、过程分类层级：若省略参数返回顶层；传入编码后返回下一级节点。

## 5. 文档同步要求
- 每次 agent 运行最后都要执行的步骤。
