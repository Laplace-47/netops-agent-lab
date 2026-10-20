"""已校验 DAG 的分批调度与 content 绑定，不管理 Runtime 的重试策略。"""

import asyncio
from copy import deepcopy

from netops_agent.models import RunError, ToolExecution
from netops_agent.orchestration import execute_parallel
from netops_agent.runtime import ToolRuntime


async def execute_dag(runtime: ToolRuntime, executions: list[ToolExecution]) -> None:
    """输入由 RunService 从通过 DAGPlan 校验的计划初始化；原地保存执行状态。"""
    by_id = {entry.tool_call.id: entry for entry in executions}
    try:
        while pending := [entry for entry in executions if entry.status == "pending"]:
            blocked_any = False
            for entry in pending:
                failed_dependencies = [step_id for step_id in entry.depends_on
                                       if by_id[step_id].status in ("failed", "blocked", "cancelled")]
                if failed_dependencies:
                    entry.status = "blocked"
                    entry.error = RunError(
                        code="DEPENDENCY_FAILED",
                        message=f"依赖步骤未成功：{', '.join(failed_dependencies)}。",
                    )
                    blocked_any = True
            ready = [entry for entry in pending if entry.status == "pending"
                     and all(by_id[step_id].status == "succeeded" for step_id in entry.depends_on)]
            if not ready:
                if blocked_any:
                    continue  # 计划顺序可任意，下一轮继续传播 blocked。
                raise RuntimeError("已校验 DAG 中没有可执行步骤")
            for entry in ready:
                arguments = deepcopy(entry.tool_call.arguments)
                for binding in entry.bindings:
                    source = by_id[binding.source_step_id]
                    # 只有成功依赖才能注入；失败结果和 blocked 均不能冒充空成功。
                    if source.result is None or source.result.type != "success":
                        raise RuntimeError("绑定来源没有成功结果")
                    arguments[binding.target_argument] = deepcopy(source.result.content)
                entry.tool_call = entry.tool_call.model_copy(update={"arguments": arguments}, deep=True)
            await execute_parallel(runtime, ready)
    except asyncio.CancelledError:
        # execute_parallel 已取消并等待本批子任务，尚未调度的步骤也保存取消状态。
        for entry in executions:
            if entry.status in ("pending", "running", "cancelled"):
                entry.status = "cancelled"
                entry.error = RunError(code="CANCELLED", message="任务执行已取消。")
        raise
    finally:
        for entry in executions:
            if entry.status == "failed" and entry.result is not None:
                entry.error = RunError(
                    code=str(entry.result.metadata.get("error_code", "TOOL_ERROR")),
                    message=str(entry.result.metadata.get("message", "工具执行失败。")),
                )
