# NetOps Agent Lab

智能网管多工具 Agent 与可靠执行平台的原创学习工程。当前阶段为 **M0 + M1**：工程初始化与最小 Agent 闭环。

只使用自行编写的代码、公开技术资料和模拟告警数据。当前不连接真实 LLM、内部网络或涉密接口。

## 当前开发边界

- FastAPI `POST /runs` 和 `GET /runs/{run_id}`。
- Mock Planner、Tool Registry、Tool Runtime 和一个 `find_warning` 工具。
- Pydantic v2 参数校验、统一 ToolResult、内存 Run 存储。
- pytest 自动化测试及本机 HTTP 验证。

运行说明、接口示例和实际验证结果将在 M1 验证后补齐。
