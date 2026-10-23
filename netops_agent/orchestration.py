"""只编排无依赖的并行调用；每个工具仍由现有 Runtime 执行。"""

import asyncio
import logging
from collections.abc import Callable

from netops_agent.models import ToolExecution, ToolResult
from netops_agent.runtime import ToolRuntime

logger = logging.getLogger(__name__)


async def execute_parallel(
    runtime: ToolRuntime, executions: list[ToolExecution], *, on_change: Callable[[], None] | None = None,
) -> None:
    async def execute_one(entry: ToolExecution) -> None:
        entry.status = "running"
        if on_change is not None:
            on_change()
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
        if on_change is not None:
            on_change()

    tasks = [asyncio.create_task(execute_one(entry)) for entry in executions]
    try:
        await asyncio.gather(*tasks)
    except (asyncio.CancelledError, Exception):
        # 子任务取消或状态保存失败时，显式清理其他分支后继续传播异常。
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for entry in executions:
            if entry.status in ("pending", "running"):
                entry.status = "cancelled"
        raise
