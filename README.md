## 天工 LCA Spec Coding 项目

基于分阶段 Python 脚本的 Tiangong LCA 规范化工作流，实现流程检索、对齐、提取与 TIDAS 校验的一体化管线。本仓库使用 `uv` 作为官方包管理与环境构建工具，并要求 Python 版本 **>= 3.12**。

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
   uv run black .
   ```

4. **执行流程编排示例（需自行实现 LLM 客户端后）**
   ```bash
   uv run python scripts/md/run_test_workflow.py --skip-tidas
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
- `src/tiangong_lca_spec/process_extraction`：文献解析、提取与结果合并的分步实现。
- `src/tiangong_lca_spec/tidas_validation`：TIDAS 校验调用封装。
- `src/tiangong_lca_spec/orchestrator`：整体编排管线。

完成 `uv sync` 后即可进行模块开发与单元测试，确保在符合 Python 3.12 及以上版本的环境中运行。

### 导出数据约定

- 生成的流程数据集中，行政信息里的 `common:referenceToCommissioner`、`common:referenceToPersonOrEntityEnteringTheData` 与 `common:referenceToOwnershipOfDataSet` 均指向默认联系人，并使用 `../contacts/<uuid>_<version>.xml` 形式的 `@uri`，即在联系人文件名后附加 `_@version` 以匹配对应的版本号。

### MCP 服务配置与校验

`tiangong_lca_spec.core.config.Settings` 默认提供 `tiangong_lca_remote` MCP 端点，用于流程数据检索并需要 Bearer Token。配置信息统一保存在 `.secrets/secrets.toml` 中（已在 `.gitignore` 中排除），初始模板示例如下：

```toml
[tiangong_lca_remote]
transport = "streamable_http"
service_name = "tiangong_lca_remote"
url = "https://lcamcp.tiangong.earth/mcp"
api_key = "<replace-with-tiangong-token>"
```

> `api_key` 字段只需要填写裸 token，程序会自动补全 `Bearer ` 前缀。

填入后，运行时代码可通过辅助函数直接生成供 MCP Agent 使用的配置块：

```python
from tiangong_lca_spec.core.config import get_mcp_service_configs

service_configs = get_mcp_service_configs()
# {
#   "tiangong_lca_remote": {
#       "transport": "streamable_http",
#       "url": "https://lcamcp.tiangong.earth/mcp",
#       "headers": {"Authorization": "Bearer ..."}
#   }
# }
```

TIDAS 校验已改为本地 CLI，Stage 3 及整体编排会自动运行：

```bash
uv run tidas-validate -i artifacts
```

如需调整服务名称或地址，可直接在 `.secrets/secrets.toml` 对应节内覆盖 `service_name`、`url` 等字段，或使用实际环境变量覆盖 `LCA_*` 前缀的设置。

### 将提示转换为内联代码并执行示例
- **文献流程**
```bash
uv run python scripts/md/convert_prompt_to_inline.py --source-json test/data/test_process.json

# 危险操作：直接执行转换后的内联prompt（请确保已了解风险）
codex exec --dangerously-bypass-approvals-and-sandbox "$(cat inline_prompt.txt)"
```

- **JSON-LD 数据**
  ```bash
  uv run python scripts/jsonld/convert_prompt_to_inline.py \
    --prompt-path .github/prompts/convert_json.prompt.md \
    --source-json test/data/json_ld/processes \
    --output inline_prompt_jsonld.txt

  codex exec --dangerously-bypass-approvals-and-sandbox "$(cat inline_prompt_jsonld.txt)"
  ```

### 文献 vs JSON-LD 工作流入口

- **文献（非结构化）数据**：使用 `scripts/md/` 下的 Stage 1→4。
- **JSON-LD（OpenLCA）数据**：使用 `scripts/jsonld/` 下的 Stage 1→3（或 `run_pipeline.py` 一键执行），共享相同的 `_workflow_common`/TIDAS/发布基础设施。

| 数据源 | Stage 1 | Stage 2 | Stage 3 | Stage 4 | 说明 |
|--------|---------|---------|---------|---------|------|
| 文献 Cleantext | `scripts/md/stage1_preprocess.py` | `scripts/md/stage2_extract_processes.py` | `scripts/md/stage3_align_flows.py` | `scripts/md/stage4_publish.py` | 原始流程，LLM 提取 + FlowSearch 对齐 |
| JSON-LD | `scripts/jsonld/stage1_jsonld_extract.py` | `scripts/jsonld/stage2_jsonld_validate.py` | `scripts/jsonld/stage3_jsonld_publish.py` | *(自动隶属于 Stage 3)* | 针对 OpenLCA JSON-LD 的 LLM 重建、校验与发布 |

快速运行 JSON-LD 管线：

```bash
uv run python scripts/jsonld/run_pipeline.py \
  --process-dir test/data/json_ld/processes \
  --flows-dir test/data/json_ld/flows \
  --sources-dir test/data/json_ld/sources \
  --prompt .github/prompts/convert_json.prompt.md \
  --secrets .secrets/secrets.toml \
  --clean-exports \
  --dry-run-publish
```

去掉 `--dry-run-publish` 即可在 Stage 3 真正提交至 `Database_CRUD_Tool`。

若想复用 “先生成 inline prompt 再一键触发 Stage 1→Stage 3” 的工作流，可直接执行：

```bash
uv run python scripts/jsonld/jsonld_inline_run.py \
  --process-dir test/data/json_ld/processes \
  --flows-dir test/data/json_ld/flows \
  --sources-dir test/data/json_ld/sources \
  --clean-exports \
  --dry-run-publish
```

该命令会自动将 `.github/prompts/convert_json.prompt.md` 转换为单行 `inline_prompt_jsonld.txt`，并通过 `run_pipeline.py` 调用 Stage 1→Stage 3（可使用 `--inline-only` 仅生成 inline、或 `--prompt-path` 指向自定义提示）。

- Stage 1 会同时遍历 `--process-dir` 与 `--flows-dir`，并通过 LLM 输出符合 `tidas_processes.json` / `tidas_flows.json` 的数据集。结果分别缓存到 `artifacts/<run_id>/cache/stage1_process_blocks.json`、`stage1_flow_blocks.json`。
- Stage 2 默认读取上述缓存并重映射所有 UUID；只有当 `stage1_flow_blocks.json` 不存在时，才会退回 `--json-ld-flows` 指向的原始目录进行临时转换。
- 当流程内引用全新来源时，Stage 1 也会同步生成 `stage1_source_blocks.json`，Stage 2 将优先消费该缓存（若缺失才使用 `--json-ld-sources`）。
