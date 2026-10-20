"""请求内完成 Run 执行，并在单进程内存中保存可查询的快照。"""

import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from netops_agent.dag import execute_dag
from netops_agent.models import DAGPlan, ParallelPlan, RunError, RunRecord, ToolCall, ToolExecution
from netops_agent.orchestration import execute_parallel
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
            if isinstance(call, DAGPlan):
                await self._execute_dag(run, call)
            elif isinstance(call, ParallelPlan):
                await self._execute_parallel(run, call)
            else:
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

    async def _execute_parallel(self, run: RunRecord, plan: ParallelPlan) -> None:
        run.plan = ParallelPlan.model_validate(plan)
        run.tool_executions = [ToolExecution(tool_call=call) for call in run.plan.tool_calls]
        await execute_parallel(self.runtime, run.tool_executions)
        failures = [entry for entry in run.tool_executions if entry.status == "failed"]
        run.status = "failed" if failures else "succeeded"
        if failures:
            # 多个错误时按计划顺序选首个作为 Run 错误，详细结果仍全部保留。
            metadata = failures[0].result.metadata
            run.error = RunError(
                code=str(metadata.get("error_code", "TOOL_ERROR")),
                message=str(metadata.get("message", "工具执行失败。")),
            )
        run.answer = self._summarize_parallel(run.tool_executions)

    async def _execute_dag(self, run: RunRecord, plan: DAGPlan) -> None:
        # 模型可能在构造后被修改，执行前再次校验并深拷贝，避免修改 Planner 的计划。
        run.plan = DAGPlan.model_validate(plan).model_copy(deep=True)
        run.tool_executions = [ToolExecution(
            tool_call=step.tool_call, depends_on=step.depends_on, bindings=step.bindings,
        ) for step in run.plan.steps]
        await execute_dag(self.runtime, run.tool_executions)
        failed = [entry for entry in run.tool_executions if entry.status == "failed"]
        blocked = [entry for entry in run.tool_executions if entry.status == "blocked"]
        run.status = "failed" if failed or blocked else "succeeded"
        if failed or blocked:
            entry = (failed or blocked)[0]
            run.error = RunError(
                code=entry.error.code,
                message=f"步骤 {entry.tool_call.id}（{entry.tool_call.name}）：{entry.error.message}",
            )
        parts = []
        for entry in run.tool_executions:
            if entry.status == "succeeded":
                summaries = [row["summary"] for row in entry.result.content
                             if isinstance(row.get("summary"), str)]
                parts.extend(summaries or [f"{entry.tool_call.name} 成功"])
            else:
                parts.append(f"{entry.tool_call.name} {entry.status}（{entry.error.code}）")
        run.answer = "【模拟结果】" + "；".join(parts)

    @staticmethod
    def _summarize_parallel(executions: list[ToolExecution]) -> str:
        parts = []
        levels = {"INFO": "信息", "WARNING": "警告", "CRITICAL": "严重"}
        for entry in executions:
            call, result = entry.tool_call, entry.result
            label = {"find_warning": "告警查询", "get_traffic": "流量查询"}.get(call.name, call.name)
            if result.type == "error":
                parts.append(f"{label}失败（{result.metadata.get('error_code', 'TOOL_ERROR')}）")
            elif call.name == "find_warning":
                level = levels.get(call.arguments.get("severe_level"), "")
                parts.append(f"告警查询：{call.arguments['node_id']} 昨天共有 {len(result.content)} 条{level}告警")
            elif call.name == "get_traffic":
                samples = "、".join(
                    f"利用率 {row.get('utilization_percent')}%，等级 {row.get('traffic_level')}"
                    for row in result.content
                )
                parts.append(f"流量查询：{call.arguments['node_id']} 昨天共 {len(result.content)} 条记录"
                             + (f"，{samples}" if samples else ""))
            else:
                parts.append(f"{label}成功，共 {len(result.content)} 条记录")
        return "【模拟结果】" + "；".join(parts) + "。"

    def get_run(self, run_id: UUID) -> RunRecord:
        return self._runs[run_id].model_copy(deep=True)
