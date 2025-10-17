## 天工 LCA Spec Coding 项目

基于 LangGraph 的 Tiangong LCA 规范化工作流，实现流程检索、对齐、提取与 TIDAS 校验的一体化管线。本仓库使用 `uv` 作为官方包管理与环境构建工具，并要求 Python 版本 **>= 3.12**。

### 运行前提

- Python 3.12 或更新版本（推荐通过 [`uv toolchain`](https://github.com/astral-sh/uv) 安装管理）。
- 安装 `uv`：  
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
  若希望下载脚本时使用清华镜像，可通过代理或预先设定 `HTTPS_PROXY`，也可以直接从镜像仓库获取安装脚本。

### 使用 uv 初始化与同步依赖

1. **创建虚拟环境并安装依赖**
   ```bash
   uv sync
   ```
   `uv` 会读取 `pyproject.toml`，在 `.venv/` 下创建隔离的虚拟环境并安装所有运行依赖。
   如果需要使用清华 PyPI 镜像源，也可使用一次性的命令前缀：
   ```bash
   UV_PYPI_URL=https://pypi.tuna.tsinghua.edu.cn/simple uv sync
   ```
   若希望同时安装开发工具（如 Black、Ruff），可执行：
   ```bash
   uv sync --group dev
   ```

2. **激活虚拟环境**
   ```
   source .venv/bin/activate
   ```
   在 Windows PowerShell 中可以使用：
   ```
   .venv\Scripts\Activate.ps1
   ```

3. **运行静态检查或类型检查（可选）**
   ```bash
   uv run ruff check
   ```
   自动格式化代码：
   ```bash
   uv run black src/tiangong_lca_spec
   ```

4. **执行流程编排示例（需自行实现 LLM 客户端后）**
   ```bash
   uv run python -m tiangong_lca_spec.orchestrator.workflow_demo
   ```
   *该入口示例留作扩展，可在 `src/orchestrator` 目录中补充。*

### uv 常用命令速查

- `uv sync --frozen`：按照 `pyproject.lock`（若存在）进行可重复安装。
- `uv sync --group dev`：安装包含 Black、Ruff 等开发依赖的虚拟环境。
- `uv add <package>`：添加新依赖并自动更新 `pyproject.toml`。
- `uv run <cmd>`：在虚拟环境里执行命令，免去显式激活。
- `uv pip list`：查看虚拟环境内安装的包。

### 目录概览

- `src/tiangong_lca_spec/core`：配置、日志、数据模型与通用工具。
- `src/tiangong_lca_spec/flow_search`：MCP 流检索客户端与验证逻辑。
- `src/tiangong_lca_spec/flow_alignment`：交换量与候选流对齐策略。
- `src/tiangong_lca_spec/process_extraction`：文献解析、LangGraph 抽取与结果合并。
- `src/tiangong_lca_spec/tidas_validation`：TIDAS 校验调用封装。
- `src/tiangong_lca_spec/orchestrator`：整体编排管线。

完成 `uv sync` 后即可进行模块开发与单元测试，确保在符合 Python 3.12 及以上版本的环境中运行。
