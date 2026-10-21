# NetOps Agent Lab

> 最新可演示阶段为 **M3.2：DAG 依赖调度与上游结果传递**。支持单工具、独立工具并发和四步依赖诊断。
> Swagger 入口为 `http://127.0.0.1:8000/docs`；已有旧服务占用端口时，可另用 8001 启动当前版本。
> 阶段记录：[M3.1 交付总结与演示](docs/milestones/M3.1.md)、[M3.2 交付总结与执行记录](docs/milestones/M3.2.md)。
> 所有设备、告警、流量和诊断数据均为模拟数据，不代表真实网络监控结果。

智能网管多工具 Agent 与可靠执行平台的原创学习工程。在 **M0 + M1** 工程初始化与最小 Agent 闭环基础上，
已完成 M2 的单次执行超时、有限重试和异常分类，M3.1 的独立工具并发，以及 M3.2 的 DAG 调度与结果绑定。
代码实现、自动化测试通过与整体验收是不同状态，见下文验证记录。

只使用自行编写的代码、公开技术资料和模拟数据。当前不连接真实 LLM、真实网络或涉密接口。

## 当前开发边界

- FastAPI `POST /runs` 和 `GET /runs/{run_id}`。
- Mock Planner 输出 `ToolCall`、`ParallelPlan` 或 `DAGPlan`，由显式契约决定执行路径。
- Tool Registry 注册 `find_warning`、`get_traffic`、`get_device`、`diagnose_node` 四个模拟工具。
- Pydantic v2 参数及计划校验、统一 ToolResult、单进程内存 Run 存储和深拷贝查询。
- Tool Runtime 单次异步执行超时、有限重试，以及显式的临时错误和业务错误分类。
- `asyncio.gather` 执行独立调用；DAG 按就绪批次复用并发执行，并向下游绑定上游结果。
- 逐工具记录、失败隔离、依赖失败阻断、取消传播与确定性模拟汇总。
- pytest 自动化测试及已有的本机 HTTP 验证记录。

当前没有真实 LLM、Queue、Worker、Checkpoint、数据库持久化、SSE 或 RAG。
M3.2 复用 M2 Runtime 和 M3.1 并发模块，没有重写原有可靠性逻辑。

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
│   ├── orchestration.py
│   ├── dag.py
│   └── service.py
├── tests/
│   ├── conftest.py
│   ├── test_runs.py
│   ├── test_runtime.py
│   ├── test_planner.py
│   ├── test_parallel_runs.py
│   ├── test_traffic.py
│   ├── test_dag_models.py
│   ├── test_dag_runs.py
│   └── test_diagnostic_tools.py
├── docs/
│   └── milestones/
│       ├── M3.1.md
│       ├── M3.2.md
│       └── assets/
│           ├── m3.1/
│           └── m3.2/
├── M2_ACCEPTANCE_CHECKLIST.md
├── README.md
├── requirements.txt
├── pyproject.toml
├── .gitignore
└── .env.example
```

| 文件 | 核心职责 |
| --- | --- |
| `main.py` | `create_app()` 注册四个工具、组装依赖，提供 POST/GET；每个应用拥有独立内存存储 |
| `netops_agent/__init__.py` | Python 包声明 |
| `netops_agent/models.py` | 请求、ToolCall、ParallelPlan、DAGPlan、DAGStep、ResultBinding、执行记录及 Run 契约；检查依赖和绑定规则 |
| `netops_agent/planner.py` | `Planner` 协议、三类请求的 Mock 计划和业务时区的“昨天”换算 |
| `netops_agent/registry.py` | `ToolDefinition` 和 `ToolRegistry`；注册、查找、导出 JSON Schema |
| `netops_agent/runtime.py` | `ToolRuntime.execute()`；参数校验、单次执行超时、有限重试、返回值校验；定义 TemporaryToolError / BusinessToolError 并转换错误 |
| `netops_agent/tools.py` | 四个工具及参数/证据模型；模拟设备、告警、流量，以及根据输入证据计算的诊断 |
| `netops_agent/orchestration.py` | `execute_parallel()`；独立调用并发、收集分支结果，取消时清理子任务并传播信号 |
| `netops_agent/dag.py` | `execute_dag()`；就绪批次、content 深拷贝绑定、blocked 传播和 DAG 取消记录 |
| `netops_agent/service.py` | `RunService.create_run()/get_run()`；选择三条执行路径，保存计划、调用、结果、错误和汇总 |
| `tests/conftest.py` | 固定业务时钟、独立应用、asyncio 测试后端及工具测试夹具 |
| `tests/test_runs.py` | 基础 API、M2 故障保存、重试成功、服务取消及结果快照测试 |
| `tests/test_runtime.py` | 参数与结果校验、工具查找、超时与重试、实际执行次数、异常分类、取消清理及配置边界测试 |
| `tests/test_planner.py` | 自然语言映射、业务时区和不支持的请求测试 |
| `tests/test_parallel_runs.py` | ParallelPlan、确定性并发、部分失败、独立重试、取消及 POST/GET 回归 |
| `tests/test_traffic.py` | 流量工具注册、参数校验、统一结构和模拟结果重复性 |
| `tests/test_dag_models.py` | DAG 结构、依赖、绑定及可变模型重新校验 |
| `tests/test_dag_runs.py` | DAG Planner、就绪步骤并发、结果传递、失败阻断、取消清理及 API 保存 |
| `tests/test_diagnostic_tools.py` | 设备/诊断工具注册、输入证据校验及空证据行为 |
| `docs/milestones/M3.1.md`、`M3.2.md` | 分阶段实现、测试和 HTTP 证据；assets 保存实际模拟响应与演示截图 |
| `M2_ACCEPTANCE_CHECKLIST.md` | M2 早期只读审查快照，不作为当前测试覆盖状态的唯一依据 |
| `requirements.txt` | 固定运行与测试的直接依赖版本 |
| `pyproject.toml` | pytest 目录、导入路径及 marker 配置 |
| `.gitignore` | 排除虚拟环境、缓存、日志、构建产物和 `.env` |
| `.env.example` | 无秘密的环境变量示例；不自动加载 `.env` |
| `README.md` | 运行手册、契约说明、验证记录和当前边界 |

## 运行

要求 Python 3.11+。已有阶段验证记录使用 Windows / Python 3.12.6。
在项目根目录执行以下命令（Windows 的 PowerShell 或 Git Bash）：

```text
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
.venv/Scripts/python.exe -m uvicorn main:app --host 127.0.0.1 --port 8000
```

访问 <http://127.0.0.1:8000/docs> 可交互提交请求。本项目当前不提供认证，演示启动绑定本机地址。

已运行的旧进程不会因为磁盘代码更新而自动升级。若 8000 被旧演示占用，可保留它，另启当前代码：

```text
.venv/Scripts/python.exe -m uvicorn main:app --host 127.0.0.1 --port 8001
```

对应访问 <http://127.0.0.1:8001/docs>，并把后文请求地址的端口改为 8001。
里程碑中的端口、Run ID 和当时提交状态属于历史快照，不能据此假定某个旧进程目前仍在运行。

Linux/macOS 对应使用 `.venv/bin/python`。默认业务时区为 `Asia/Shanghai`，可通过进程环境变量
`NETOPS_TIMEZONE` 修改；`tzdata` 提供 Windows 所需的时区数据。`.env.example` 仅作说明，
应用当前不自动读取 `.env`，也不需要任何 API Key。

## 业务调用链

```text
POST /runs {message}
  -> FastAPI / CreateRunRequest 校验请求
  -> RunService 创建 running Run，并保存到内存
  -> MockPlanner.plan(message, registry.schemas())
     -> ToolCall：直接调用 ToolRuntime.execute()
     -> ParallelPlan：execute_parallel() 并发调用各工具的 Runtime
     -> DAGPlan：execute_dag() 分批选择依赖均成功的步骤
        -> 按显式绑定复制上游 content 到当前参数
        -> execute_parallel() 并发执行本批次的 Runtime 调用
        -> 保存结果，继续调度；依赖失败的步骤标记 blocked
  -> 各 ToolRuntime：Registry 查找 -> Pydantic 参数校验
     -> wait_for(handler(args), timeout=5.0)，在预算内处理超时/临时错误重试
     -> ToolResult 校验并关联 tool_name / tool_call_id
  -> RunService 生成确定性的模拟回答
  -> 保存 succeeded / failed Run
  -> HTTP 201 + Location: /runs/{run_id}

GET /runs/{run_id}
  -> 读取已保存记录的深拷贝 -> HTTP 200，不重新执行工具
```

POST 在请求内等待工具执行完毕，不使用后台任务、Queue 或 Worker。HTTP 201 表示 Run 资源创建成功，
执行是否成功看 `status`。不支持的自然语言也会保存为失败 Run，便于通过 GET 查看原因。

Mock Planner 当前支持以下三类明确表达，可省略节点两侧空格：

| 请求示例 | 计划与执行 |
| --- | --- |
| `查询 node_20 昨天的严重告警` | ToolCall：find_warning；也支持“警告/信息”，分别映射 WARNING/INFO，严重映射 CRITICAL |
| `综合查询 node_20 昨天的告警和流量` | ParallelPlan：find_warning 与 get_traffic 独立并发 |
| `诊断 node_20 昨天的设备状态、告警和流量` | DAGPlan：get_device 后执行告警/流量，最后根据上游结果诊断 |

组合查询与诊断中的告警等级固定为 `CRITICAL`。告警和流量共用同一节点及业务时区下昨天的起止时间。
同一个逻辑 ToolCall 可以在 Runtime 内多次尝试执行；重试不重新调用 Planner，tool_call_id 保持一致。
其他表达明确返回 `UNSUPPORTED_REQUEST`，不会偷偷改为默认节点查询。

### M3.2 诊断依赖与绑定

| 步骤 | 直接依赖 | 参数与结果传递 |
| --- | --- | --- |
| `get_device` | 无 | 按 node_id 返回模拟设备基础信息 |
| `find_warning` | get_device | 成功后才允许执行；节点、严重等级和昨天时间来自 Planner |
| `get_traffic` | get_device | 与告警步骤同批并发；节点和昨天时间来自 Planner |
| `diagnose_node` | get_device、find_warning、get_traffic | 分别将上游 content 绑定到 device_info、warnings、traffic，结合 node_id 生成诊断 |

告警和流量对设备步骤是控制依赖，不会从设备 content 中提取查询参数。诊断显式直接依赖设备步骤，
以满足设备结果绑定必须来自直接依赖的规则。get_device 不接收时间范围，不提供历史设备状态查询。

DAGStep 复用 ToolCall.id 作为步骤 ID。模型拒绝空计划、重复 ID、未知依赖、自依赖、环、重复依赖、
非直接依赖来源绑定、重复绑定目标，以及绑定与静态参数冲突；DAG 入口也重新校验被修改过的 ToolCall 实例。
绑定只支持完整 `ToolResult.content` 到顶层参数，不支持任意 JSONPath、表达式或嵌套目标。

调度按就绪批次执行：本批全部结束后再计算下一批。上游成功 content 会深拷贝注入下游，再经过 Runtime 参数校验；
原计划不会写入绑定值，执行记录保存实际注入后的调用参数。诊断只计算传入证据，记录来源 ID，不在工具内部另造一份设备、告警或流量结果。

某步骤失败时，其他独立分支仍继续完成。失败依赖的下游为 `blocked`、错误码 `DEPENDENCY_FAILED`，
`result=null`，不会执行 handler，也不会把错误结果当成空成功传入下游。告警失败时仍保留成功流量结果，但诊断不执行。
所有步骤均成功时 Run 为 succeeded；存在 failed 或 blocked 步骤时为 failed，保留各分支实际结果和可解释错误。

## 数据契约与错误边界

`ToolCall` 使用 `id`、`name`、`arguments`，其中 arguments 是 JSON 对象。
未来 LLM 适配器需将供应商响应转换到此模型，继续复用 Registry 和 Runtime。

四个工具均使用 Pydantic 参数模型，下表字段均为必填：

| 工具 | 参数与校验 |
| --- | --- |
| `find_warning` | node_id 非空字符串；severe_level 为 INFO / WARNING / CRITICAL；begin_time、end_time 带时区且 end_time 严格晚于 begin_time |
| `get_traffic` | node_id 非空字符串；begin_time、end_time 同样校验时区和先后关系 |
| `get_device` | node_id 非空字符串 |
| `diagnose_node` | node_id、device_info、warnings、traffic；设备列表恰好 1 条，告警/流量列表允许为空；记录符合各自模型且节点一致，流量时间范围有效 |

时间输入使用 ISO 8601 字符串。告警查询采用 `[begin_time, end_time)`；流量工具使用传入时间窗口生成模拟记录。
未知参数被拒绝，参数校验失败不会执行工具。
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

Run 保存 `run_id`、message、状态、创建/完成时间、answer 和顶层 error，执行内容按计划类型区分：

| 执行路径 | `tool_call` / `result` | `plan` / `tool_executions` |
| --- | --- | --- |
| 单工具 | 保存原有调用及结果 | null / [] |
| ParallelPlan | null / null | 保存并行计划，以及按计划顺序保存的各调用、状态和结果 |
| DAGPlan | null / null | 保存原始 DAG，以及各步骤的实际调用、依赖、绑定、状态、结果和 error |

DAG 步骤状态包含 pending、running、succeeded、failed、blocked、cancelled；Run 状态仍为 running、succeeded、failed。
ParallelPlan 错误沿用逐工具 `result.metadata` 和顶层 Run.error，执行记录的独立 error 字段默认 null；DAG 路径会填充逐步骤 error。
多工具失败时保留成功分支；顶层错误按计划顺序选取首个失败，DAG 优先选实际 failed 步骤，其次才是 blocked 步骤。

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

该预算针对每个工具调用独立计算，ParallelPlan / DAG 不额外增加重试，也不让其他成功分支重复执行。

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

并行及 DAG 取消时会取消未完成子任务、等待清理后继续传播 CancelledError；DAG 尚未调度的步骤也标记 cancelled，
已完成结果保留。取消路径保存 Run 的 failed/CANCELLED 和完成时间，不会将取消转换为临时故障重试。

## 请求与响应示例

### 当前 DAG 诊断请求

在 Swagger 中展开 `POST /runs`，点击 **Try it out**，提交：

```json
{"message":"诊断 node_20 昨天的设备状态、告警和流量"}
```

Git Bash 中的等价请求为：

```bash
curl -i -X POST http://127.0.0.1:8000/runs \
  -H 'Content-Type: application/json' \
  --data '{"message":"诊断 node_20 昨天的设备状态、告警和流量"}'
```

在当前模拟样本中，四步成功时可看到设备 UP、1 条严重告警、流量 72.5% / HIGH 和派生诊断 CRITICAL。
这些都是模拟结果，不能宣传为真实网络观测或真实故障结论。完整响应参考
[M3.2 交付时实际保存的 DAG 记录](docs/milestones/assets/m3.2/dag-run.json)，不是本次 README 更新重新请求所得。

POST 正常返回 201 及 Location；复制本次返回的 run_id，调用 `GET /runs/{run_id}`，可取回相同的状态、结果和汇总。
在 Git Bash 中把下面变量替换为新建 Run 的实际 ID：

```bash
run_id='替换为本次POST返回的run_id'
curl "http://127.0.0.1:8000/runs/$run_id"
```

独立并发演示使用 `{"message":"综合查询 node_20 昨天的告警和流量"}`；单工具使用
`{"message":"查询 node_20 昨天的严重告警"}`。三种请求共用 POST/GET 接口。

### 历史 M1 单工具响应（2026-10-12）

服务启动后，在 Git Bash 中执行：

```bash
curl -i -X POST http://127.0.0.1:8000/runs \
  -H 'Content-Type: application/json' \
  --data '{"message":"查询node_20昨天的严重告警"}'
```

下面保留 2026-10-12 本机 HTTP 验证的原始单工具响应。它是 M1 的历史字段结构；
当前单工具响应另外包含默认 `plan=null`、`tool_executions=[]`。ID 与运行时间每次不同：

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
| M3.1 代码 | 独立工具并发、逐工具结果保存和汇总已实现；随 `fed1b6a` 提交保留了 M2 的 API 测试及文档 |
| M3.2 代码 | 静态 DAG 依赖校验、结果绑定、就绪批次调度及失败阻断已实现；当前 main 实现提交为 `0924ce0` |
| M3.2 自动化测试 | 交付文档记录完整套件 `157 passed in 2.56s`，包括此前 96 项和新增 61 项 |
| 工程验收边界 | 已有下述自动化和历史 HTTP 证据；不将这些用例扩大为大规模图、任意 handler 或整个项目全部功能的验收通过 |

代码已提交或推送不等于整体验收完成。里程碑文档保留各阶段当时的审查和提交状态，当前代码状态以 main 为准。
历史 HTTP 检查记录作为已有证据保留，不冒充本次 README 更新新执行的检查。

### M3.2 交付测试记录（2026-10-20）

以下命令和结果摘自 [M3.2 交付文档第 3 节](docs/milestones/M3.2.md#3-测试命令及实际结果)，
是该次交付实际执行的完整套件记录：

```text
.venv/Scripts/python.exe -B -m pytest -q -W error -p no:cacheprovider
157 passed in 2.56s
退出码：0
```

**本次 README 更新只核对代码、测试源码、交付文档和 Git Diff，没有重新运行 pytest、启动服务或执行 HTTP 验证。**
上述耗时和通过数量不是本次文档更新新产生的结果。M3.2 交付时新增测试记录为 `61 passed in 0.98s`。

`-p no:cacheprovider` 仅关闭 pytest 结果缓存，以避开本机旧缓存目录权限问题；
保留全部测试和警告检查。`-B` 避免写入字节码。该记录只说明已执行的自动化测试结果。

当前测试源码覆盖单工具与 ParallelPlan 回归、DAG 结构/绑定校验、无序计划、根节点和中间就绪批次并发、
依赖顺序、深拷贝、绑定后参数校验、失败阻断、独立分支保留、真实上游证据驱动诊断、POST/GET 一致性及取消清理。
并发测试使用 asyncio.Event 建立握手，不仅通过执行时间差推测并发。

M2 的 Runtime 测试及后补的 Run API 测试也保留在完整套件中。现有 `tests/test_runs.py` 已覆盖
TIMEOUT / TEMPORARY_ERROR / BUSINESS_ERROR 的保存查询、重试成功及服务层取消；
[M2 早期验收清单](M2_ACCEPTANCE_CHECKLIST.md) 中相关缺口是当时的审查快照，不能继续当作当前缺少这些用例的结论。

### 保留的 M2 Runtime 测试断言

以下是现有测试源码中的断言范围，源于 M2 可靠性改造，不是本次重新执行的结果：

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

### 历史 M3.1 / M3.2 HTTP 演示与验证

- [M3.1 交付及 Swagger 演示](docs/milestones/M3.1.md)：初次总结复跑完整套件为 `96 passed in 1.39s`，
  随后的演示复跑为 `96 passed in 1.36s`；实际验证组合 POST 201、GET 200、响应一致和单工具回归，保存了截图与原始 JSON。
- [M3.2 交付记录](docs/milestones/M3.2.md)：在临时 8001 服务上验证四步 DAG、绑定参数与上游 content 相等、
  POST 201 / GET 200 完整一致，以及单工具和 ParallelPlan 回归。该临时服务验证后正常关闭，验证期间保留了旧 8000 演示服务。

这些属于历史执行证据。本次没有重跑以上演示；Run 仍只保存于相应服务进程的内存中。

## Git 与秘密管理

旧 main.py 保存在初始 Git 提交中，新工程没有继续调用旧实现。M0、M1、M2、M3.1、M3.2 代码分阶段维护提交。
`.env`、虚拟环境、缓存和日志均不纳入 Git；只有无秘密的 `.env.example` 会提交。
推送前检查暂存文件及常见秘密格式。不要将真实网络数据或秘密加入后续提交。

## 当前限制与后续方向

- 四个工具均为模拟工具；Planner 只识别上述三类规则表达，最终回答由代码确定性汇总，没有真实 LLM。
- 内存存储只适用于单进程；重启、热重载会丢失 Run，多个进程不共享数据，也没有容量回收。
- 没有后台 Worker、队列、持久化、Checkpoint、幂等和任务恢复。
- 已有单次异步执行超时与有限重试；尚无重试退避、抖动、整个 Run 的总超时或按工具分别配置的执行策略。
- 取消依赖 handler 配合，尚无同步阻塞隔离或对任意代码强制终止的能力。
- 已有独立工具并发和静态 DAG 调度；尚无并发数量上限，大规模 DAG 性能未经验证，也没有条件/可选步骤或动态增图。
- 结果绑定仅限直接依赖的完整 content 到顶层参数；所有 DAG 步骤都视为必要步骤，失败会影响整个 Run。
- get_device 不提供历史设备状态库；get_traffic 固定返回模拟利用率 72.5% / HIGH，不能作为真实网络监控指标。
- 连续多次取消、吞掉取消信号的 handler 等边界尚未专门验证；重试也不提供业务操作幂等保证。
- 没有 SSE、结构化 Trace、RAG、Elasticsearch、Rerank、Redis、PostgreSQL、Docker 或 CI。
- 没有用户认证和生产部署配置。

后续可单独规划并发上限、大规模图和取消边界验证，再按阶段扩展重试退避、幂等、Trace、
真实 LLM 或持久化等能力。已有的 Timeout/Retry、ParallelPlan 和静态 DAG 调度不再列为待开发功能。
本次只更新 README，不扩展功能，不提交或推送 Git。

## 公开技术参考

- [FastAPI 异步测试](https://fastapi.tiangolo.com/advanced/async-tests/)
- [Pydantic 模型](https://docs.pydantic.dev/latest/concepts/models/)
- [Pydantic 校验器](https://docs.pydantic.dev/latest/concepts/validators/)

项目业务代码与模拟数据为本工程原创实现，以上链接仅用于 API 用法核对。
