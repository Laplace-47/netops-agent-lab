"""只编排无依赖的并行调用；每个工具仍由现有 Runtime 执行。"""

import asyncio
import logging

from netops_agent.models import ToolExecution, ToolResult
from netops_agent.runtime import ToolRuntime

logger = logging.getLogger(__name__)


async def execute_parallel(runtime: ToolRuntime, executions: list[ToolExecution]) -> None:
    async def execute_one(entry: ToolExecution) -> None:
        entry.status = "running"
        try:
            entry.result = await runtime.execute(entry.tool_call)
        except asyncio.CancelledError:
            entry.status = "cancelled"
            raise
        except Exception:
            # Runtime 之外的未预期异常也不能丢失其他分支；不增加任何重试。
            logger.exception("并行调用 %s 执行异常", entry.tool_call.id)
            entry.result = ToolResult(
                type="error",
                metadata={
                    "tool_name": entry.tool_call.name, "tool_call_id": entry.tool_call.id,
                    "error_code": "INTERNAL_ERROR", "message": "任务执行发生异常。",
                },
                content=[], indexes=[],
            )
        entry.status = "succeeded" if entry.result.type == "success" else "failed"

    tasks = [asyncio.create_task(execute_one(entry)) for entry in executions]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        # gather 的某个子任务取消时，不会自动取消其余子任务，必须显式清理。
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for entry in executions:
            if entry.status in ("pending", "running"):
                entry.status = "cancelled"
        raise
