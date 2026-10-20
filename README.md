# NetOps Agent Lab

> 最新可演示阶段为 **M3.1：多工具并发执行与结果汇总**。已支持“综合查询 node_20 昨天的告警和流量”，
> Swagger 入口为 `http://127.0.0.1:8000/docs`；复现步骤、实际响应截图和最新测试记录见
> [M3.1 交付总结与演示](docs/milestones/M3.1.md#5-本机-swagger-demo-与复现步骤2026-10-20)。
> 下文保留 M2 阶段的说明和历史验证记录，当前 M3.1 能力及演示以该文档为准。全部网络数据均为模拟数据。

智能网管多工具 Agent 与可靠执行平台的原创学习工程。在 **M0 + M1** 工程初始化与最小 Agent 闭环基础上，
已实现 **M2：Tool Runtime 可靠性改造**，包含单次执行超时、有限重试和异常分类。
代码实现、自动化测试通过与整体验收是不同状态，见下文验证记录。

只使用自行编写的代码、公开技术资料和模拟告警数据。当前不连接真实 LLM、内部网络或涉密接口。

## 当前开发边界

- FastAPI `POST /runs` 和 `GET /runs/{run_id}`。
- Mock Planner、Tool Registry、Tool Runtime 和一个 `find_warning` 工具。
- Pydantic v2 参数校验、统一 ToolResult、内存 Run 存储。
- Tool Runtime 单次异步执行超时、有限重试，以及显式的临时错误和业务错误分类。
- pytest 自动化测试及本机 HTTP 验证。

本次 M2 改造沿用现有 Planner、API 和 ToolResult 数据结构，没有新增调度或持久化模块。

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
| `netops_agent/runtime.py` | `ToolRuntime.execute()`；参数校验、单次执行超时、有限重试、返回值校验；定义 TemporaryToolError / BusinessToolError 并转换错误 |
| `netops_agent/tools.py` | `FindWarningArgs`、等级枚举、时间范围校验和模拟 `find_warning()` |
| `netops_agent/service.py` | `RunService.create_run()/get_run()`；任务状态、结果汇总和内存快照 |
| `tests/conftest.py` | 固定业务时钟、独立应用、asyncio 测试后端及工具测试夹具 |
| `tests/test_runs.py` | API 成功、空结果、失败、查询、输入校验、结果快照测试 |
| `tests/test_runtime.py` | 参数与结果校验、工具查找、超时与重试、实际执行次数、异常分类、取消清理及配置边界测试 |
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
  -> asyncio.wait_for(handler(args), timeout=5.0)
     -> 成功返回：退出执行循环
     -> TimeoutError / TemporaryToolError：在共享预算内额外重试，最多执行 4 次
     -> BusinessToolError / 其他未预期异常：立即返回对应错误
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
同一个逻辑 ToolCall 可以在 Runtime 内多次尝试执行；重试不重新调用 Planner，tool_call_id 保持一致。
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
| `type` | `success` 或 `error`；find_warning 查询无记录时为 success |
| `metadata` | JSON 元数据；find_warning 提供来源和记录数，Runtime 关联工具名和 tool_call_id；Runtime 生成的错误包含 error_code 和 message |
| `content` | JSON 结构化记录列表；find_warning 无记录或 Runtime 生成错误时为 `[]` |
| `indexes` | content 对应记录的 ID 列表；find_warning 无记录或 Runtime 生成错误时为 `[]` |

参数校验错误仅保留字段位置和错误说明，不返回原始输入及异常上下文。
未预期异常详情仅进入应用日志，API 返回安全的错误信息。
`ToolResult` 开启实例重新校验，工具返回模型被修改后仍不能绕过校验。
该模型没有强制要求所有工具自行返回的 error 结果都包含 error_code/message；Runtime 生成的错误会提供这两个字段。

HTTP 请求格式非法时返回 422，未知 Run 返回 404。Planner 失败保存为 failed Run。
协程取消会记录 failed/CANCELLED 并继续向上传播取消信号。

## M2：Tool Runtime 可靠性

### 默认配置与重试预算

`ToolRuntime` 构造函数的默认参数为：

```python
ToolRuntime(registry, timeout=5.0, max_retries=3)
```

- `timeout=5.0` 秒，约束每一次 handler 异步执行，不是整个 Run 的总超时。
- `max_retries=3` 表示首次执行失败后，最多额外重试 3 次，总执行次数最多为 `1 + 3 = 4`。
- 超时也算一次执行尝试。超时与 `TemporaryToolError` 共享同一重试预算，不分别计数。
- 首次成功即返回；在第 4 次尝试成功仍属于允许范围。预算耗尽时，按最后一次失败返回错误码。
- 参数校验发生在执行循环前，返回值校验发生在执行循环后；它们的失败不会触发重试。
- 每次尝试创建新的 handler 协程。当前没有重试退避或抖动策略。

配置只通过 Runtime 构造参数注入；当前 `main.py` 使用默认值，没有新增 HTTP 或环境变量配置入口。
测试可传入更短超时，例如 `ToolRuntime(registry, timeout=0.01, max_retries=3)`。
`timeout` 必须大于 0 且有限；`max_retries` 必须是非负整数，设为 0 时仅执行首次尝试。

### 异常与返回值处理规则

`TemporaryToolError` 和 `BusinessToolError` 均在 `netops_agent/runtime.py` 中定义。
工具需要显式抛出相应异常；Runtime 不通过异常文本猜测是否可重试，也不对所有 Exception 重试。

| 情况 | 是否重试 | 最终处理 |
| --- | --- | --- |
| 正常返回合法的成功结果，含空结果 | 否 | 返回 `ToolResult(type="success")` |
| `TimeoutError`，包括 wait_for 产生的超时 | 在剩余预算内重试 | 耗尽后返回 `TIMEOUT` |
| `TemporaryToolError` | 在剩余预算内重试 | 耗尽后返回 `TEMPORARY_ERROR` |
| `BusinessToolError` | 否 | 返回 `BUSINESS_ERROR` |
| 非法参数 | 否，handler 执行次数为 0 | 保留 `INVALID_ARGUMENTS` |
| 未知工具 | 否，handler 执行次数为 0 | 保留 `UNKNOWN_TOOL` |
| 其他未预期异常，例如普通 RuntimeError / ConnectionError / ValueError | 否 | 保留 `TOOL_EXECUTION_ERROR` 及安全错误信息 |
| 非法工具返回值 | 否 | 保留 `INVALID_TOOL_RESULT` |
| handler 返回合法的 `ToolResult(type="error")` | 否 | 作为已返回的结果保留，不因 type 或其中的错误码自动重试 |

即使此前发生过可重试错误，后续一旦出现业务错误或其他不可重试情况，也会立即停止。
Runtime 生成的失败结果保留 tool_call_id，content/indexes 为空；不会把异常原始文本直接放入结果。
RunService 继续读取结果中的错误码和说明保存 failed Run；工具自行返回的错误结果缺少错误码时，沿用 `TOOL_ERROR` 回退值。

### 超时与取消的边界

当前使用 `asyncio.wait_for` 控制单次异步执行的超时时间。超时后会请求取消 handler，
等待该次执行完成取消处理，再进入下一次尝试。测试通过 handler 的 finally 清理计数验证了这一顺序。
外部 `asyncio.CancelledError` 继续向上传播，不会转换为临时错误或自动重试。

取消清理依赖 handler 正确配合取消机制。handler 应在 finally 中释放资源，处理取消后继续传播取消信号。
`asyncio.wait_for` 不能强制终止任意代码：同步阻塞操作可能阻塞事件循环，不配合取消的 handler
可能继续执行、延迟返回或使超时行为不符合预期。取消清理本身也可能耗时，
因此默认 5 秒不代表硬性终止时限，4 次尝试也不能推导为整个 Run 必定在 20 秒内结束。
当前没有进程级强制终止或隔离执行能力。

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

用当前服务中新请求返回的 run_id 查询，结果与 POST 一致。下面沿用历史示例 ID 展示路径；
历史临时服务已关闭，重新启动后应先 POST，再替换为当前有效的 run_id：

```bash
curl http://127.0.0.1:8000/runs/3fc746b9-4eb9-4e34-8f42-2ada75019156
```

将输入改为“查询 node_404 昨天的严重告警”，会得到 succeeded、success、count=0、
content=[]、indexes=[]。空结果表示查询成功，不会转换成工具失败。

模拟数据为请求起始日生成四条固定样本，分别覆盖节点、告警等级和结束时间边界。
这是可重复演示的测试数据，不是历史告警数据库；不连接真实网络。

## 验证记录与验收状态

### 当前实现和验收状态

| 层面 | 状态与依据 |
| --- | --- |
| M0 + M1 代码 | 工程初始化、模拟 Agent 闭环及内存 Run 查询已实现，保留原有行为 |
| M2 代码 | Tool Runtime 超时、有限重试、显式异常分类已实现；实现提交为 `837c1579c7ef348a004296f199c20171287b493b` |
| M2 自动化测试 | 本次 README 更新前核对代码并复跑完整套件，60 项通过；更新后的完整回归结果也记录于下文 |
| M2 整体工程验收 | 待确认，尚未标记全部完成；本次工作核对文档和自动化回归，未执行新的完整工程验收流程 |

代码已提交或推送不等于整体验收完成。历史 HTTP 检查记录作为已有证据保留，
不冒充本次文档更新新执行的检查，也不据此宣称整体项目已完成。

### 当前完整测试套件

本次 README 更新使用 Windows / Python 3.12.6 执行现有完整套件：

```text
.venv/Scripts/python.exe -B -m pytest -q -W error -p no:cacheprovider
60 passed
```

`-p no:cacheprovider` 仅关闭 pytest 结果缓存，以避开本机旧缓存目录权限问题；
保留全部测试和警告检查。`-B` 避免写入字节码。该记录只说明已执行的自动化测试结果。

原有覆盖包括成功查询、空结果、非法枚举、错误时间范围、必填字段、无时区时间、半开区间过滤、
未知工具、重复注册、工具异常、非法返回结构、Planner 异常、Schema、API 和结果快照隔离。
本次可靠性改造的现有测试还覆盖：

| 测试场景 | 实际断言 |
| --- | --- |
| 首次成功及空成功结果 | handler 执行 1 次 |
| 超时后成功、临时错误后成功 | 执行 2 次或 4 次，允许最后一次尝试成功 |
| 超时/临时错误耗尽预算 | max_retries 为 0/1/3 时，实际执行 1/2/4 次；错误码对应最终失败原因 |
| 超时与临时错误混合 | 共用预算，最多执行 4 次，保留最后一次失败的分类 |
| 非法参数、未知工具 | handler 执行 0 次 |
| 业务错误、普通未预期异常、非法返回结构、已返回的 error 结果 | 不重试，相关计数测试断言执行 1 次 |
| 重试途中出现不可重试异常 | 首次临时错误后立即终止，共执行 2 次 |
| 超时取消与外部取消 | 超时清理完成后再尝试；外部取消不重试，handler 执行及清理各 1 次 |
| 非法 timeout/max_retries 配置 | 构造时拒绝，handler 执行 0 次 |

这些次数由测试中的 handler 实际计数验证，不是仅检查配置值。当前 ToolResult 没有新增尝试次数字段。
测试时冻结业务时钟，保证“昨天”的断言不随日期变化；短超时通过构造参数注入。

### 历史 M1 验证（2026-10-12）

以下保留原有阶段验证记录；32 项是当时的测试规模，不是当前完整套件数量。
依赖检查和编译检查同样属于该次历史记录：

```text
.venv/Scripts/python.exe -m pytest -q -W error
32 passed in 0.15s

.venv/Scripts/python.exe -m pip check
No broken requirements found.

.venv/Scripts/python.exe -m compileall -q main.py netops_agent tests
退出码 0
```

当时另实际启动了临时 Uvicorn 子进程，通过本机 TCP/HTTP 验证 POST 201、GET 200、空结果成功、
失败 Run、422、404、`/docs` 和 `/openapi.json`。验证后已关闭该临时服务。

### 历史 M2 可靠性改造验证

此前可靠性改造完成时，完整 pytest 套件已实际通过 60 项，并通过临时 Uvicorn 服务做过 HTTP 故障注入：
超时和临时错误耗尽时各执行 4 次，业务错误和未知异常各执行 1 次，重试后成功的场景各执行 2 次，
POST/GET 能保存并查询相同的结果和错误码。该临时服务验证后已关闭。
这是此前执行记录，不属于仓库中独立的 HTTP 验收脚本，也未在本次 README 更新中重新执行。

## Git 与秘密管理

旧 main.py 保存在初始 Git 提交中，新工程没有继续调用旧实现。M0、M1、M2 代码分阶段维护提交。
`.env`、虚拟环境、缓存和日志均不纳入 Git；只有无秘密的 `.env.example` 会提交。
推送前检查暂存文件及常见秘密格式。不要将真实网络数据或秘密加入后续提交。

## 当前限制与后续方向

- 仅一个模拟工具和规则 Planner，最终回答由代码汇总，尚未接入 LLM。
- 内存存储只适用于单进程；重启、热重载会丢失 Run，多个进程不共享数据，也没有容量回收。
- 没有后台 Worker、队列、持久化、Checkpoint、幂等和任务恢复。
- 已有单次异步执行超时与有限重试；尚无重试退避、抖动、整个 Run 的总超时或按工具分别配置的执行策略。
- 取消依赖 handler 配合，尚无同步阻塞隔离或对任意代码强制终止的能力。
- 没有多工具编排和并发控制；重试也不提供业务操作幂等保证。
- 没有 SSE、结构化 Trace、RAG、Elasticsearch、Rerank、Redis、PostgreSQL、Docker 或 CI。
- 没有用户认证和生产部署配置。

下一步先明确 M2 整体工程验收清单，结合现有测试与历史记录，核对故障注入和 Run API 的端到端行为，
补齐验收结论。已实现的超时和有限重试不再列为待开发功能。
后续可另立阶段扩展重试退避、幂等及结构化 Trace，再逐步接入真实 LLM Planner、编排和持久化模块；
这些都尚未实现。本次只同步 README，不扩展功能，也不自动提交或推送。

## 公开技术参考

- [FastAPI 异步测试](https://fastapi.tiangolo.com/advanced/async-tests/)
- [Pydantic 模型](https://docs.pydantic.dev/latest/concepts/models/)
- [Pydantic 校验器](https://docs.pydantic.dev/latest/concepts/validators/)

项目业务代码与模拟数据为本工程原创实现，以上链接仅用于 API 用法核对。
