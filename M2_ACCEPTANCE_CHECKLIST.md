# NetOps Agent Lab — M2 验收清单与证据

## 1. 审查范围与结论

本文整理 2026-10-19 上一轮只读审查的结果。审查代码基线为
`837c1579c7ef348a004296f199c20171287b493b`。

**结论：M2 核心 Runtime 行为已有当前自动化测试证据，但还不能宣告整体验收完成。**
主要缺口是新增故障码在 Run API 中的保存、查询及取消状态保存，缺少对应的当前自动化覆盖。

证据判定规则：

- 以实际代码和现有测试为主要依据，README 及历史记录只作为辅助证据。
- 每个验收项分别记录实现状态、自动化测试状态、验证证据和最终结论。
- “已实现”不等于“测试通过”，“测试通过”也不等于“整体工程验收完成”。
- 仅有前次手工执行记录的项目标记为“仅有历史验证”；没有对应运行证据的项目标记为“尚未验证”或“证据不足”。
- 未验证不等于已发现实现错误，也不意味着需要在本次审查中补写功能。

上一轮审查未修改任何文件，未提交或推送。审查前已有的 README 未提交改动保持不变。
本次操作仅将该审查结果整理为本文档，不修改既有文件，也不提交或推送 Git。
下面的测试结果来自上一轮实际执行，不是本次整理文档时重新运行所得。

## 2. 实际执行的测试命令及结果

在项目根目录执行：

```bash
.venv/Scripts/python.exe -B -m pytest -vv -W error -p no:cacheprovider
```

实际结果：

```text
60 passed in 0.85s
退出码：0
```

| 测试文件 | 实际通过数量 |
| --- | ---: |
| `tests/test_runtime.py` | 42 |
| `tests/test_runs.py` | 13 |
| `tests/test_planner.py` | 5 |
| 合计 | 60 |

执行环境为 Windows / Python 3.12.6，pytest 9.1.1，AnyIO 4.15.1。
`-B` 禁止字节码写入；`-p no:cacheprovider` 仅关闭 pytest 结果缓存，以避开本机旧缓存目录权限问题；
`-W error` 保留警告检查，全部现有测试均执行。结果只证明该环境和这些用例的行为。

## 3. 证据索引

以下表格中的测试文件简称为：

- **R**：[tests/test_runtime.py](tests/test_runtime.py)
- **A**：[tests/test_runs.py](tests/test_runs.py)

`R::函数名` 表示 `tests/test_runtime.py` 中的准确测试函数名；`A::函数名` 同理。
参数化场景的具体参数另行列明，不虚构额外测试函数。

关键实现位置：

| 文件与位置 | 核对内容 |
| --- | --- |
| [runtime.py：构造函数](netops_agent/runtime.py#L31) | timeout 和 max_retries 默认值、配置边界 |
| [runtime.py：工具查找与参数校验](netops_agent/runtime.py#L53) | 未知工具、非法参数在执行循环前返回 |
| [runtime.py：执行循环](netops_agent/runtime.py#L68) | 单次 wait_for、共享预算、异常分类与重试 |
| [runtime.py：返回值校验](netops_agent/runtime.py#L92) | 返回值校验在重试循环外，关联 tool_call_id |
| [models.py：ToolResult](netops_agent/models.py#L28) | 统一结构及模型实例重新校验 |
| [service.py：Run 创建与结果保存](netops_agent/service.py#L21) | 成功/失败状态、错误映射、内存保存 |
| [service.py：取消分支](netops_agent/service.py#L44) | CANCELLED 记录与取消信号传播 |
| [main.py：Run API](main.py#L29) | POST、Location、GET 与 404 |

## 4. Runtime、异常及配置验收清单

表中“通过”仅针对对应证据覆盖的范围。

| 验收项 | 实现状态 | 自动化测试状态 | 验证证据 | 最终结论 |
| --- | --- | --- | --- | --- |
| 默认 `timeout=5.0`、`max_retries=3` | 已实现，见 Runtime 构造函数 | 本次审查通过 | `R::test_first_attempt_succeeds_without_retry` 断言两个默认值 | 通过默认值验收；未测量真实 5 秒计时精度 |
| 首次成功立即返回，空成功结果不重试 | 已实现 | 本次审查通过 | `R::test_first_attempt_succeeds_without_retry`，断言成功、空 content、实际调用 1 次 | 通过 |
| 超时计入 attempt，重试后可成功 | 已实现，使用 `asyncio.wait_for` | 本次审查通过 | `R::test_timeout_then_retry_succeeds`，失败 1/3 次后分别在第 2/4 次成功 | 通过协作式异步 handler 场景 |
| `TemporaryToolError` 可以重试 | 已实现显式异常类型 | 本次审查通过 | `R::test_temporary_error_then_retry_succeeds`，实际调用 2/4 次 | 通过 |
| 重试耗尽，最多执行 4 次 | 已实现 `range(max_retries + 1)` | 本次审查通过 | `R::test_retry_exhaustion_counts_actual_attempts`，配置 0/1/3 时调用 1/2/4 次，分别验证 TIMEOUT、TEMPORARY_ERROR | 通过 |
| 超时与临时错误共享预算 | 已实现同一循环计数 | 本次审查通过 | `R::test_timeout_and_temporary_errors_share_retry_budget`，两种交替顺序均调用 4 次，并按最后一次失败分类 | 通过 |
| `BusinessToolError` 不重试 | 已实现，返回 BUSINESS_ERROR | 本次审查通过 | `R::test_business_error_is_not_retried`，调用 1 次，异常原文未进入结果 | 通过 |
| 普通异常不自动重试 | 已实现，返回 TOOL_EXECUTION_ERROR | 本次审查通过 | `R::test_tool_exception_becomes_safe_error_result`，覆盖 RuntimeError、ConnectionError、ValueError，均调用 1 次 | 通过这些异常类型 |
| 重试途中遇到不可重试异常立即停止 | 已实现 | 本次审查通过 | `R::test_non_retryable_error_stops_existing_retry_sequence`，首次临时错误，随后业务错误或 RuntimeError，总调用 2 次 | 通过 |
| 非法参数不执行 handler、不重试 | 已实现，校验在循环前 | 本次审查通过 | `R::test_invalid_enum_is_rejected_before_execution` 明确断言调用 0 次；另有 `R::test_invalid_time_range_is_rejected`、`R::test_required_arguments_cannot_be_omitted`、`R::test_naive_datetime_is_rejected` | 通过；后面三类主要断言错误码，没有逐项统计调用次数 |
| 未知工具不执行 handler、不重试 | 已实现，查找在循环前 | 本次审查通过 | `R::test_unknown_tool_is_rejected`，UNKNOWN_TOOL、调用 0 次 | 通过 |
| 非法返回值不重试 | 已实现，返回值校验在循环后 | 本次审查通过 | `R::test_invalid_tool_output_becomes_error_result` 断言 INVALID_TOOL_RESULT、调用 1 次；`R::test_mutated_result_with_non_json_value_is_rejected` 覆盖模型被修改 | 通过 |
| handler 返回合法 error 结果不自动重试 | 已实现，正常返回即退出执行循环 | 本次审查通过 | `R::test_returned_error_result_is_not_retried`，返回携带 BUSINESS_ERROR 的 error 结果，调用 1 次 | 通过该用例；其他 metadata 错误码未逐一测试 |
| `max_retries=0` | 已实现，仅首次执行 | 本次审查通过 | `R::test_retry_exhaustion_counts_actual_attempts` 的两种故障均只调用 1 次 | 通过 |
| 非法配置构造时拒绝 | 已实现 | 本次审查通过 | `R::test_invalid_execution_config_is_rejected`：timeout 为 0、负数、NaN、Inf、True；重试次数为负数、小数、True，均拒绝且调用 0 次 | 通过这 8 种配置，不代表穷尽所有输入类型 |

配置语义：`max_retries=3` 表示首次失败后最多额外重试 3 次，因此最多执行 4 次。
超时本身计入一次尝试。超时和临时错误不分别拥有独立预算。

可重试类型仅包括 `TimeoutError` 和显式的 `TemporaryToolError`。
`BusinessToolError`、普通未预期异常、参数校验失败、未知工具、非法返回值均不自动重试。
已返回的 `ToolResult(type="error")` 是结果，不会仅因 type 或 metadata 中的错误码再次执行 handler。

## 5. 取消、资源清理和 Run API 验收清单

| 验收项 | 实现状态 | 自动化测试状态 | 验证证据 | 最终结论 |
| --- | --- | --- | --- | --- |
| 超时后完成取消清理，再开始下一次尝试 | 已实现协作式取消机制 | 本次审查通过 | `R::test_timeout_then_retry_succeeds` 在每次进入 handler 时检查前次清理计数；耗尽测试也断言清理次数等于调用次数 | 通过测试 handler 的清理顺序验收 |
| 外部任务取消不重试、继续传播 | 已实现 | 本次审查通过 | `R::test_external_cancellation_is_not_retried`：task.cancel() 后抛出 CancelledError，执行和清理各 1 次 | Runtime 层通过 |
| 真实连接、文件或锁正确释放 | 由 handler 自行负责；Runtime 不负责关闭具体资源 | 尚未验证 | 当前测试的清理动作是 finally 中计数，没有真实资源关闭断言 | 证据不足，不能把计数等同于资源释放 |
| 同步阻塞或不配合取消的 handler | 当前不提供强制终止能力 | 尚未验证这些场景 | 执行点使用 wait_for；现有测试均配合取消 | 只确认能力边界，不列为已通过的硬超时验收 |
| 成功 Run 创建、保存及 GET 一致 | 已实现 | 本次审查通过 | `A::test_successful_run_can_be_retrieved`：POST 201、Location、结果、completed_at、GET 完整响应一致 | 通过 ASGI 接口集成验收 |
| 空告警列表为成功 | 已实现 | 本次审查通过 | `A::test_empty_result_is_success`：succeeded、success、count=0、空 content/indexes | 通过 POST 行为；此用例未单独 GET 空结果 |
| 参数错误、请求不支持、Planner 异常的失败结果保存 | 已实现 | 本次审查通过 | `A::test_tool_validation_failure_is_saved_in_run`、`A::test_unsupported_request_is_saved_as_failed_run`、`A::test_planner_exception_is_saved_without_internal_details`，均检查 GET 与创建结果一致 | 通过这些失败路径 |
| Run 快照不能反向修改保存结果 | 已实现深拷贝 | 本次审查通过 | `A::test_returned_run_snapshot_cannot_modify_saved_result` 修改返回对象后，保存结果仍保留记录 | 通过 |
| M2 三类错误码经 POST/GET 正确保存与查询 | 通用错误映射与保存分支已实现 | 无对应的当前自动化用例 | 本对话前次 Uvicorn 故障注入记录；具体结果见第 6 节 | 仅有历史验证；本次审查未重跑 |
| Run 被取消后保存 failed/CANCELLED 和完成时间 | 取消处理与 finally 已实现 | 尚未验证该服务层路径 | 当前取消测试只调用 Runtime，没有通过 RunService 检查取消后的保存记录 | 尚未验证，不能判为验收通过 |
| 重试成功后的 Run API 状态及保存结果 | 通用成功分支已实现 | 无对应的当前自动化用例 | 前次 HTTP 记录中，超时后成功、临时错误后成功均调用 2 次，POST/GET 一致 | 仅有历史验证 |

现有 API 自动化使用 [HTTPX ASGITransport](tests/conftest.py#L46)。
本次审查没有启动独立 Uvicorn/TCP 服务，不把 ASGI 接口测试等同于新的 TCP/HTTP 验收。

### 取消机制的验收边界

- 当前单次执行由 `asyncio.wait_for` 控制超时，取消清理完成后再进入下一次尝试。
- 清理依赖 handler 正确配合取消机制。现有测试证明 finally 被执行及执行顺序，不证明真实资源已关闭。
- 外部取消在 Runtime 层的传播与不重试行为已有测试；RunService 取消后的保存状态尚未验证。
- wait_for 不能强制终止任意代码。同步阻塞或不配合取消的 handler 可能使行为不符合预期。
- 默认 5 秒不代表对任意代码的硬性终止时限，也不能据此保证整个 Run 必定在 20 秒内结束。

## 6. 仅有历史验证的证据记录

本对话前次可靠性改造期间，实际启动过临时 Uvicorn 服务，注入故障并通过真实 HTTP 调用 POST/GET。
当时输出的 handler 实际调用次数如下：

| 模拟节点 | 历史结果 | 实际 handler 调用次数 |
| --- | --- | ---: |
| node_20 | SUCCESS | 1 |
| node_404 | SUCCESS，空结果 | 1 |
| node_501 | TIMEOUT | 4 |
| node_502 | TEMPORARY_ERROR | 4 |
| node_503 | BUSINESS_ERROR | 1 |
| node_504 | TOOL_EXECUTION_ERROR | 1 |
| node_505 | 临时错误后 SUCCESS | 2 |
| node_506 | 超时后 SUCCESS | 2 |

当时验证了 POST 201、GET 200、保存结果一致、错误分类和调用次数，并关闭了临时服务。
这些节点只用于模拟故障注入，不是真实网络数据。

证据限制：

- 该记录来自本对话此前实际运行的临时验证过程，不是本次审查重新运行的结果。
- 当时通过 Python 内联脚本启动服务；脚本没有保存在仓库中，没有可引用的独立 pytest 测试函数。
- [README 历史 M2 记录](README.md#历史-m2-可靠性改造验证) 只是辅助归档，不能单独证明当前实现或测试通过。
- 因此上述新增故障码和重试成功路径的真实 HTTP 表现仍标记为“仅有历史验证”。

## 7. 最终验收判断

### 已满足的验收项

- 默认配置、有限重试次数、超时计入 attempt，以及共享重试预算。
- 可重试与不可重试异常的显式分类，重试途中遇到不可重试异常立即停止。
- 参数、工具查找、返回值及已返回 error 结果的处理边界。
- 协作式 handler 的取消清理顺序、Runtime 外部取消传播。
- 现有基础 Run API 的成功、部分失败路径、保存查询及快照隔离行为。

以上结论均受对应测试范围约束，不扩展为对全部输入、全部资源或全部运行环境的保证。

### 尚未满足当前验收证据要求的项目

- M2 故障码在 Run API 中的保存、查询，以及重试成功路径，缺少当前可重复的自动化验证。
- Run 取消后的 failed/CANCELLED、完成时间和保存结果，缺少对应服务层验证。

这些是验收证据缺口，不是已经确认的业务实现失败。

### 证据不足的项目

- 真实连接、文件、锁等资源的释放。
- 同步阻塞、不配合取消等边界场景的实际行为。

未验证不意味着本次应扩展功能。本清单不新增硬终止、Queue、Worker、Checkpoint、真实 LLM 等要求，
也没有为通过验收而补写任何实现或测试。

### M2 是否达到整体验收标准

**暂不判定整体验收通过。当前状态为：核心 Runtime 自动化通过，整体验收待补证。**

本次审查没有发现现有 60 项测试中的失败，但现有证据仍不足以将全部工程验收标为完成。
代码提交或推送状态不作为整体验收通过的依据。
