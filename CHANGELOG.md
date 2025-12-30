# Changelog

本文档记录 Tiangong LCA Spec Coding 项目的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added

#### [2025-12-26] 科学文献集成功能 (Scientific References Integration)

**概述**
- `process_from_flow` 工作流现已集成 `tiangong_kb_remote` 的 `Search_Sci_Tool`
- 在 Step 1-3 之前自动检索相关科学文献，让 LLM 基于真实的科学参考资料做出决策
- 显著提升了技术路径识别、过程分解和交换清单生成的准确性

**新增内容**
- 在 `src/tiangong_lca_spec/process_from_flow/service.py` 中添加科学文献检索功能：
  - `_search_scientific_references()` - 调用 MCP Search_Sci_Tool 检索文献
  - `_format_references_for_prompt()` - 格式化文献用于 LLM prompt
  - 修改 `describe_technology`、`split_processes`、`generate_exchanges` 三个节点
  - 更新 `ProcessFromFlowService` 类添加 `mcp_client` 参数支持

**测试**
- 新增 `test/test_scientific_references.py` - 科学文献检索功能测试脚本

**文档**
- 更新 `.github/prompts/process_from_flow.prompt.md` - 添加科学文献集成章节
- 更新 `AGENTS.md` - 添加英文使用说明
- 更新 `AGENTS_ZH.md` - 添加中文使用说明

**配置**
- 需要在 `.secrets/secrets.toml` 中配置 `[tiangong_kb_remote]` 服务
- 如不配置则自动回退到 LLM common sense（向后兼容）

**性能影响**
- 每次文献检索约 1-2 秒
- 完整工作流增加约 3-6 秒（3 次检索）
- 不影响工作流的可靠性和稳定性

**技术细节**
- 自动创建和管理 MCP 客户端连接
- 智能构建搜索查询（基于 flow 名称、操作类型、技术上下文）
- 优雅的错误处理（检索失败不会阻塞主工作流）
- 完整的日志记录（`process_from_flow.search_references` 等）

**相关链接**
- 详细文档：`.github/prompts/process_from_flow.prompt.md`
- 测试脚本：`test/test_scientific_references.py`
- 实现代码：`src/tiangong_lca_spec/process_from_flow/service.py`

---

## 历史版本

### [之前的版本]
- 项目基础架构搭建
- 文献数据流程（Stage 1-4）实现
- JSON-LD 数据流程（Stage 1-3）实现
- Flow 检索和对齐功能
- TIDAS 校验集成
- 知识库导入功能
- 多 MCP 服务支持

---

## 如何贡献

如果您发现任何问题或有改进建议，请：
1. 在 GitHub Issues 中提交问题
2. 或直接提交 Pull Request

## 版本说明

- **Added** - 新增功能
- **Changed** - 现有功能的变更
- **Deprecated** - 即将移除的功能
- **Removed** - 已移除的功能
- **Fixed** - 任何 bug 修复
- **Security** - 安全性相关修复
