"""请求内完成 Run 执行，并在单进程内存中保存可查询的快照。"""

import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from netops_agent.models import RunError, RunRecord, ToolCall
from netops_agent.planner import Planner, UnsupportedRequest
from netops_agent.runtime import ToolRuntime

logger = logging.getLogger(__name__)


class RunService:
    def __init__(self, planner: Planner, runtime: ToolRuntime):
        self.planner = planner
        self.runtime = runtime
        self._runs: dict[UUID, RunRecord] = {}

    async def create_run(self, message: str) -> RunRecord:
        run = RunRecord(message=message)
        self._runs[run.run_id] = run
        try:
            call = await self.planner.plan(message, self.runtime.registry.schemas())
            run.tool_call = ToolCall.model_validate(call)
            run.result = await self.runtime.execute(run.tool_call)
            if run.result.type == "success":
                run.status = "succeeded"
                levels = {"INFO": "信息", "WARNING": "警告", "CRITICAL": "严重"}
                node = run.tool_call.arguments["node_id"]
                level = levels[run.tool_call.arguments["severe_level"]]
                run.answer = f"【模拟结果】{node} 昨天共有 {len(run.result.content)} 条{level}告警。"
            else:
                run.status = "failed"
                run.error = RunError(
                    code=str(run.result.metadata.get("error_code", "TOOL_ERROR")),
                    message=str(run.result.metadata.get("message", "工具执行失败。")),
                )
                run.answer = f"查询失败：{run.error.message}"
        except UnsupportedRequest as exc:
            run.status = "failed"
            run.error = RunError(code="UNSUPPORTED_REQUEST", message=str(exc))
        except asyncio.CancelledError:
            run.status = "failed"
            run.error = RunError(code="CANCELLED", message="任务执行已取消。")
            raise
        except Exception:
            logger.exception("Run %s 执行失败", run.run_id)
            run.status = "failed"
            run.error = RunError(code="INTERNAL_ERROR", message="任务执行发生异常。")
        finally:
            run.completed_at = datetime.now(timezone.utc)
        return run.model_copy(deep=True)

    def get_run(self, run_id: UUID) -> RunRecord:
        return self._runs[run_id].model_copy(deep=True)
