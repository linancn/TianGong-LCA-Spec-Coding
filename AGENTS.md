# 天工 LCA Spec Coding 开发须知

面向工程开发的统一指引：涵盖环境搭建、依赖管理、密钥配置以及质量检查流程，确保在实现与维护代码时保持一致标准。

## 1. 开发环境
- **Python 版本**：≥ 3.12，推荐用 `uv toolchain` 管理；默认虚拟环境位于 `.venv/`。
- **依赖安装**：
  ```bash
  uv sync                # 安装运行依赖
  uv sync --group dev    # 安装含 black/ruff 的开发依赖
  ```
  需要镜像时可临时设置 `UV_PYPI_URL=https://pypi.tuna.tsinghua.edu.cn/simple`。
- **关键库**：`anyio`, `httpx`, `mcp`, `pydantic`, `pydantic-settings`, `tenacity`, `structlog`, `python-dotenv`, `jsonschema`, `openai`。
- **构建体系**：`hatchling` 负责 wheel 构建，`pyproject.toml` 在 `[tool.hatch.build.targets.wheel]` 中声明 `src/tiangong_lca_spec`。

## 2. 密钥与服务
1. 复制模板：`cp .secrets/secrets.example.toml .secrets/secrets.toml`。
2. 在 `.secrets/secrets.toml` 填写：
   - `[OPENAI]`：`API_KEY`, `MODEL`（默认 `gpt-5` 可覆盖）。
   - `[tiangong_lca_remote]`：`url`, `service_name`, `tool_name`, `api_key`。
   - `[tidas_data_validate]`：`url`, `tool_name`, 可选 `api_key`。
3. `api_key` 字段直接写明文 token，框架会自动加 `Bearer`。

## 3. 开发流程
- **源代码变更后的统一检查**（仅在修改了 Python 源文件后执行）：
  ```bash
  uv run black .
  uv run ruff check
  uv run python -m compileall src scripts
  uv run pytest
  ```
- **集成演练**：位于 `scripts/` 的阶段化 CLI（`stage1_preprocess.py` ~ `stage6_finalize.py`）可用于端到端验证，运行前确认依赖与密钥已配置。
