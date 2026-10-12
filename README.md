# NetOps Agent Lab

智能网管多工具 Agent 与可靠执行平台的原创学习工程。当前阶段为 **M0 + M1**：工程初始化与最小 Agent 闭环。

只使用自行编写的代码、公开技术资料和模拟告警数据。当前不连接真实 LLM、内部网络或涉密接口。

## 当前开发边界

- FastAPI `POST /runs` 和 `GET /runs/{run_id}`。
- Mock Planner、Tool Registry、Tool Runtime 和一个 `find_warning` 工具。
- Pydantic v2 参数校验、统一 ToolResult、内存 Run 存储。
- pytest 自动化测试及本机 HTTP 验证。

## 目录与职责

```text
.
├── main.py
├── netops_agent/
│   ├── __init__.py
│   ├── models.py
│   ├── planner.py
│   ├── registry.py
│   ├── runtime.py
│   ├── tools.py
│   └── service.py
├── tests/
│   ├── conftest.py
│   ├── test_runs.py
│   ├── test_runtime.py
│   └── test_planner.py
├── README.md
├── requirements.txt
├── pyproject.toml
├── .gitignore
└── .env.example
```

| 文件 | 核心职责 |
| --- | --- |
| `main.py` | `create_app()` 组装依赖，提供 POST/GET 接口，每个应用拥有独立内存存储 |
| `netops_agent/__init__.py` | Python 包声明 |
| `netops_agent/models.py` | 请求、ToolCall、ToolResult、RunRecord 和 RunError 数据契约 |
| `netops_agent/planner.py` | `Planner` 协议、`MockPlanner.plan()` 和业务时区的“昨天”换算 |
| `netops_agent/registry.py` | `ToolDefinition` 和 `ToolRegistry`；注册、查找、导出 JSON Schema |
| `netops_agent/runtime.py` | `ToolRuntime.execute()`；校验参数、await 工具、校验返回值、转换执行错误 |
| `netops_agent/tools.py` | `FindWarningArgs`、等级枚举、时间范围校验和模拟 `find_warning()` |
| `netops_agent/service.py` | `RunService.create_run()/get_run()`；任务状态、结果汇总和内存快照 |
| `tests/conftest.py` | 固定业务时钟、独立应用、asyncio 测试后端及工具测试夹具 |
| `tests/test_runs.py` | API 成功、空结果、失败、查询、输入校验、结果快照测试 |
| `tests/test_runtime.py` | 枚举、必填、时区、时间范围、过滤、工具查找和执行结果校验测试 |
| `tests/test_planner.py` | 自然语言映射、业务时区和不支持的请求测试 |
| `requirements.txt` | 固定此次验证的直接依赖版本，包含运行和测试依赖 |
| `pyproject.toml` | pytest 目录、导入路径及 marker 配置 |
| `.gitignore` | 排除虚拟环境、缓存、日志、构建产物和 `.env` |
| `.env.example` | 无秘密的环境变量示例；不自动加载 `.env` |
| `README.md` | 运行手册、契约说明、验证记录和当前边界 |

## 运行

要求 Python 3.11+。本次实际验证使用 Windows / Python 3.12.6。
在项目根目录执行以下命令（Windows 的 PowerShell 或 Git Bash）：

```text
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
.venv/Scripts/python.exe -m uvicorn main:app --host 127.0.0.1 --port 8000
```

访问 <http://127.0.0.1:8000/docs> 可交互提交请求。本项目当前不提供认证，演示启动绑定本机地址。

Linux/macOS 对应使用 `.venv/bin/python`。默认业务时区为 `Asia/Shanghai`，可通过进程环境变量
`NETOPS_TIMEZONE` 修改；`tzdata` 提供 Windows 所需的时区数据。`.env.example` 仅作说明，
应用当前不自动读取 `.env`，也不需要任何 API Key。

## 业务调用链

```text
POST /runs {message}
  -> FastAPI / CreateRunRequest 校验请求
  -> RunService 创建 running Run，并保存到内存
  -> MockPlanner.plan(message, registry.schemas())
  -> ToolCall(id, name="find_warning", arguments)
  -> ToolRuntime / ToolRegistry.get(name)
  -> FindWarningArgs.model_validate(arguments)
  -> await find_warning(args)
  -> ToolResult 校验并关联 tool_call_id
  -> RunService 生成确定性的模拟回答
  -> 保存 succeeded / failed Run
  -> HTTP 201 + Location: /runs/{run_id}

GET /runs/{run_id}
  -> 读取内存快照 -> HTTP 200
```

POST 在请求内等待工具执行完毕，不使用后台任务、Queue 或 Worker。HTTP 201 表示 Run 资源创建成功，
执行是否成功看 `status`。不支持的自然语言也会保存为失败 Run，便于通过 GET 查看原因。

当前支持“查询 node_编号 昨天的严重/警告/信息告警”，可省略节点两侧空格。
Mock Planner 将三种中文等级分别映射到 `CRITICAL/WARNING/INFO`，并返回一次工具调用。
其他表达明确返回 `UNSUPPORTED_REQUEST`，不会偷偷改为默认节点查询。

## 数据契约与错误边界

`ToolCall` 使用 `id`、`name`、`arguments`，其中 arguments 是 JSON 对象。
未来 LLM 适配器需将供应商响应转换到此模型，继续复用 Registry 和 Runtime。

`find_warning` 参数均必填：

| 参数 | 校验 |
| --- | --- |
| `node_id` | 非空字符串 |
| `severe_level` | `INFO` / `WARNING` / `CRITICAL` |
| `begin_time` | 带时区的 datetime，JSON 输入使用 ISO 8601 字符串 |
| `end_time` | 带时区的 datetime，必须严格晚于 begin_time |

查询采用 `[begin_time, end_time)`。未知参数被拒绝，参数校验失败不会执行工具。
跨字段时间顺序由 Pydantic model validator 检查，不能仅依赖 JSON Schema 表达。

`ToolResult` 始终包含四个字段：

| 字段 | 含义 |
| --- | --- |
| `type` | `success` 或 `error`；空结果为 success |
| `metadata` | 来源、记录数、工具名、tool_call_id；失败时包含 error_code 和 message |
| `content` | JSON 结构化记录列表；失败或查询无记录时为 `[]` |
| `indexes` | content 对应记录的 ID 列表；无记录时为 `[]` |

Runtime 对未知工具、非法参数、工具异常、非法返回结构分别返回
`UNKNOWN_TOOL / INVALID_ARGUMENTS / TOOL_EXECUTION_ERROR / INVALID_TOOL_RESULT`。
参数校验错误仅保留字段位置和错误说明，不返回原始输入及异常上下文。
未预期异常详情仅进入应用日志，API 返回安全的错误信息。
`ToolResult` 开启实例重新校验，工具返回模型被修改后仍不能绕过校验。

HTTP 请求格式非法时返回 422，未知 Run 返回 404。Planner 失败保存为 failed Run。
协程取消会记录 failed/CANCELLED 并继续向上传播取消信号。

## 请求与响应示例

服务启动后，在 Git Bash 中执行：

```bash
curl -i -X POST http://127.0.0.1:8000/runs \
  -H 'Content-Type: application/json' \
  --data '{"message":"查询node_20昨天的严重告警"}'
```

下面是 2026-10-12 本机真实 HTTP 验证的一次响应；ID 与运行时间每次不同：

```json
{
  "run_id": "3fc746b9-4eb9-4e34-8f42-2ada75019156",
  "message": "查询node_20昨天的严重告警",
  "status": "succeeded",
  "created_at": "2026-10-12T07:32:10.593892Z",
  "completed_at": "2026-10-12T07:32:10.595400Z",
  "tool_call": {
    "id": "eef9b94d-0df9-4a06-a67c-8b7baffc159c",
    "name": "find_warning",
    "arguments": {
      "node_id": "node_20",
      "severe_level": "CRITICAL",
      "begin_time": "2026-10-11T00:00:00+08:00",
      "end_time": "2026-10-12T00:00:00+08:00"
    }
  },
  "result": {
    "type": "success",
    "metadata": {
      "source": "mock",
      "count": 1,
      "tool_name": "find_warning",
      "tool_call_id": "eef9b94d-0df9-4a06-a67c-8b7baffc159c"
    },
    "content": [{
      "warning_id": "mock-warning-001",
      "node_id": "node_20",
      "severe_level": "CRITICAL",
      "occurred_at": "2026-10-11T10:00:00+08:00",
      "description": "模拟：CPU 使用率超过严重告警阈值"
    }],
    "indexes": ["mock-warning-001"]
  },
  "answer": "【模拟结果】node_20 昨天共有 1 条严重告警。",
  "error": null
}
```

用这次请求返回的 run_id 查询，结果与 POST 一致：

```bash
curl http://127.0.0.1:8000/runs/3fc746b9-4eb9-4e34-8f42-2ada75019156
```

将输入改为“查询 node_404 昨天的严重告警”，会得到 succeeded、success、count=0、
content=[]、indexes=[]。空结果表示查询成功，不会转换成工具失败。

模拟数据为请求起始日生成四条固定样本，分别覆盖节点、告警等级和结束时间边界。
这是可重复演示的测试数据，不是历史告警数据库；不连接真实网络。

## 实际验证

```text
.venv/Scripts/python.exe -m pytest -q -W error
32 passed in 0.15s

.venv/Scripts/python.exe -m pip check
No broken requirements found.

.venv/Scripts/python.exe -m compileall -q main.py netops_agent tests
退出码 0
```

测试覆盖成功查询、空结果、非法枚举、错误时间范围、必填字段、无时区时间、半开区间过滤、
未知工具、重复注册、工具异常、非法返回结构、Planner 异常、Schema、API 和结果快照隔离。
测试时冻结业务时钟，保证“昨天”的断言不随日期变化。

另实际启动了临时 Uvicorn 子进程，通过本机 TCP/HTTP 验证 POST 201、GET 200、空结果成功、
失败 Run、422、404、`/docs` 和 `/openapi.json`。验证后已关闭该临时服务。

## Git 与秘密管理

旧 main.py 保存在初始 Git 提交中，新工程没有继续调用旧实现。M0、M1 分阶段维护提交。
`.env`、虚拟环境、缓存和日志均不纳入 Git；只有无秘密的 `.env.example` 会提交。
推送前检查暂存文件及常见秘密格式。不要将真实网络数据或秘密加入后续提交。

## 当前限制与后续方向

- 仅一个模拟工具和规则 Planner，最终回答由代码汇总，尚未接入 LLM。
- 内存存储只适用于单进程；重启、热重载会丢失 Run，多个进程不共享数据，也没有容量回收。
- 没有后台 Worker、队列、持久化、Checkpoint、幂等和任务恢复。
- 没有多工具编排、并发控制、执行超时与重试策略。
- 没有 SSE、结构化 Trace、RAG、Elasticsearch、Rerank、Redis、PostgreSQL、Docker 或 CI。
- 没有用户认证和生产部署配置。

下一阶段建议先独立完善 Tool Runtime 的受控超时和可重试错误策略，继续保留 Mock Planner，
用故障注入测试确认行为后，再分阶段替换为真实 LLM Planner。本次停止于 M0 + M1。

## 公开技术参考

- [FastAPI 异步测试](https://fastapi.tiangolo.com/advanced/async-tests/)
- [Pydantic 模型](https://docs.pydantic.dev/latest/concepts/models/)
- [Pydantic 校验器](https://docs.pydantic.dev/latest/concepts/validators/)

项目业务代码与模拟数据为本工程原创实现，以上链接仅用于 API 用法核对。
