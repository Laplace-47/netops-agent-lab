"""带租约的 Run 执行、心跳和过期回收；所有执行中写入均校验 fencing token。"""

import asyncio
import logging
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID, uuid4

from netops_agent.dag import execute_dag
from netops_agent.models import DAGPlan, ParallelPlan, RunError, RunRecord, ToolCall, ToolExecution
from netops_agent.orchestration import execute_parallel
from netops_agent.planner import Planner, UnsupportedRequest
from netops_agent.repository import LeaseLostError, RunClaim, RunRepository, SQLiteRunRepository
from netops_agent.runtime import ToolRuntime

logger = logging.getLogger(__name__)


async def _wait_for_cleanup(*tasks: asyncio.Task) -> bool:
    """屏蔽重复取消，等待子任务清理；返回调用者期间是否收到取消请求。"""
    cleanup = asyncio.gather(*tasks, return_exceptions=True)
    cancelled = False
    while not cleanup.done():
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            cancelled = True
    cleanup.result()
    return cancelled


@dataclass
class _ExecutionState:
    claim: RunClaim
    lost: bool = False
    finalized: bool = False


class RunService:
    def __init__(
        self, planner: Planner, runtime: ToolRuntime, repository: RunRepository | None = None, *,
        lease_seconds: float = 30.0, heartbeat_interval: float = 10.0, reaper_interval: float = 1.0,
    ):
        for name, value in (("lease_seconds", lease_seconds), ("heartbeat_interval", heartbeat_interval),
                            ("reaper_interval", reaper_interval)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} 必须是大于 0 的有限秒数")
        if heartbeat_interval >= lease_seconds:
            raise ValueError("heartbeat_interval 必须小于 lease_seconds")
        self.planner = planner
        self.runtime = runtime
        self.repository = repository if repository is not None else SQLiteRunRepository()
        self.queue: asyncio.Queue[UUID] = asyncio.Queue()
        self._enqueued_ids: set[UUID] = set()
        self._active_ids: set[UUID] = set()
        self._worker_task: asyncio.Task | None = None
        self._reaper_task: asyncio.Task | None = None
        self._run_tasks: set[asyncio.Task] = set()
        self._heartbeat_tasks: set[asyncio.Task] = set()
        self._execution_tasks: set[asyncio.Task] = set()
        self.worker_id = str(uuid4())
        self.lease_seconds, self.heartbeat_interval, self.reaper_interval = lease_seconds, heartbeat_interval, reaper_interval
        self._closing = False

    async def create_run(self, message: str) -> RunRecord:
        """只保存和入队，不执行 Planner，也不为每个 Run 创建后台任务。"""
        if self._closing:
            raise RuntimeError("RunService 正在关闭，不能提交任务。")
        run = RunRecord(message=message)
        self.repository.save(run)
        # 无界内存队列的入队不让出控制权，返回的快照始终为 queued。
        self._enqueue(run.run_id)
        return run.model_copy(deep=True)

    def _enqueue(self, run_id: UUID) -> None:
        # 仅做本实例队列去重，不是跨进程 Claim 或幂等保证。
        if run_id not in self._enqueued_ids:
            self.queue.put_nowait(run_id)
            self._enqueued_ids.add(run_id)

    async def start(self) -> None:
        if self._worker_task is not None and not self._worker_task.done():
            if not self._closing and (self._reaper_task is None or self._reaper_task.done()):
                self._enqueue_reclaimable()
                self._reaper_task = asyncio.create_task(self._reap(), name="netops-lease-reaper")
            return
        if self._reaper_task is not None and not self._reaper_task.done():
            self._reaper_task.cancel()
            await asyncio.gather(self._reaper_task, return_exceptions=True)
        self.repository.initialize()
        self._enqueue_reclaimable()
        self._closing = False
        self._worker_task = asyncio.create_task(self._work(), name="netops-run-worker")
        self._reaper_task = asyncio.create_task(self._reap(), name="netops-lease-reaper")

    def _enqueue_reclaimable(self) -> None:
        for run in self.repository.list_reclaimable():
            self._enqueue(run.run_id)

    async def _reap(self) -> None:
        while True:
            await asyncio.sleep(self.reaper_interval)
            if self._closing:
                return
            try:
                self._enqueue_reclaimable()
            except Exception:
                logger.exception("回收器查询待领取任务失败")

    async def stop(self) -> None:
        """停止派发和回收，取消执行父任务并等待其心跳、handler 清理。"""
        parents = {task for task in (self._worker_task, self._reaper_task) if task is not None} | self._run_tasks
        if not self._closing:
            for task in parents:
                if not task.done():
                    task.cancel()
        self._closing = True
        try:
            cancelled = await _wait_for_cleanup(*parents)
        finally:
            while not self.queue.empty():
                run_id = self.queue.get_nowait()
                self._enqueued_ids.discard(run_id)
                self.queue.task_done()
        if cancelled:
            raise asyncio.CancelledError()

    async def _work(self) -> None:
        while True:
            run_id = await self.queue.get()
            self._enqueued_ids.discard(run_id)
            self._active_ids.add(run_id)
            try:
                await self.execute_run(run_id)
            except LeaseLostError:
                # 失去租约绝不能进入通用错误兜底，覆盖新执行者的记录。
                logger.info("Run %s 已失去租约，旧执行者退出", run_id)
            except asyncio.CancelledError:
                self._record_failure(run_id, "CANCELLED", "任务执行已取消。")
                if asyncio.current_task().cancelling():
                    raise
                # handler/编排子任务自行取消只结束当前 Run，消费者继续服务。
            except Exception:
                logger.exception("Worker 处理 Run %s 失败", run_id)
                self._record_failure(run_id, "INTERNAL_ERROR", "任务执行发生异常。")
            finally:
                self._active_ids.discard(run_id)
                self.queue.task_done()

    def _record_failure(self, run_id: UUID, code: str, message: str) -> None:
        try:
            self.repository.fail_if_queued(run_id, RunError(code=code, message=message))
        except Exception:
            # 存储自身故障可能无法记录终态；记录日志，避免永久失去消费者。
            logger.exception("无法保存 Run %s 的失败状态", run_id)

    async def execute_run(self, run_id: UUID) -> RunRecord:
        """由 Worker 调用；复用已有的单工具、ParallelPlan 和 DAG 执行链。"""
        claim = self.repository.claim(run_id, self.worker_id, self.lease_seconds)
        if claim is None:
            return self.get_run(run_id)
        state = _ExecutionState(claim)
        parent = asyncio.current_task()
        self._run_tasks.add(parent)
        execution = asyncio.create_task(self._execute_claimed(state), name=f"netops-run-execution-{run_id}")
        heartbeat = asyncio.create_task(self._heartbeat(state, execution), name=f"netops-lease-heartbeat-{run_id}")
        self._execution_tasks.add(execution)
        self._heartbeat_tasks.add(heartbeat)
        try:
            # 父任务的重复取消不能再次打断 handler 正在进行的异步清理。
            return await asyncio.shield(execution)
        except LeaseLostError:
            if parent.cancelling():
                raise asyncio.CancelledError() from None
            raise
        except asyncio.CancelledError:
            if parent.cancelling():
                if not execution.done() and not execution.cancelling():
                    execution.cancel()
                await _wait_for_cleanup(execution)
            if state.lost and not parent.cancelling():
                raise LeaseLostError(f"Run {run_id} 的心跳无法证明所有权") from None
            if not state.lost and not state.finalized:
                self._record_claimed_failure(state, "CANCELLED", "任务执行已取消。")
            raise
        except Exception:
            if not state.lost and not state.finalized:
                self._record_claimed_failure(state, "INTERNAL_ERROR", "任务执行发生异常。")
            raise
        finally:
            if not heartbeat.done() and not heartbeat.cancelling():
                heartbeat.cancel()
            if not execution.done() and not execution.cancelling():
                execution.cancel()
            cancelled = await _wait_for_cleanup(execution, heartbeat)
            self._heartbeat_tasks.discard(heartbeat)
            self._execution_tasks.discard(execution)
            self._run_tasks.discard(parent)
            if cancelled:
                raise asyncio.CancelledError()

    async def _heartbeat(self, state: _ExecutionState, execution: asyncio.Task) -> None:
        claim = state.claim
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            if execution.done() or state.finalized or state.lost:
                return
            try:
                renewed = self.repository.renew_lease(claim.run.run_id, claim.owner_id,
                                                      claim.fencing_token, self.lease_seconds)
            except Exception:
                logger.exception("Run %s 心跳失败，停止旧执行者", claim.run.run_id)
                renewed = False
            if not renewed:
                state.lost = True
                execution.cancel()
                return

    def _save_execution(self, state: _ExecutionState) -> None:
        claim = state.claim
        if state.lost:
            raise LeaseLostError(f"Run {claim.run.run_id} 已失去租约")
        try:
            self.repository.save_claimed(claim.run, claim.owner_id, claim.fencing_token)
        except LeaseLostError:
            state.lost = True
            raise
        if claim.run.status in ("succeeded", "failed"):
            state.finalized = True

    def _record_claimed_failure(self, state: _ExecutionState, code: str, message: str) -> None:
        run = state.claim.run
        run.status, run.error, run.answer = "failed", RunError(code=code, message=message), None
        run.completed_at = datetime.now(timezone.utc)
        try:
            self._save_execution(state)
        except LeaseLostError:
            logger.info("Run %s 的错误终态未写入：租约已丢失", run.run_id)
        except Exception:
            logger.exception("无法保存 Run %s 的受保护失败状态", run.run_id)

    async def _execute_claimed(self, state: _ExecutionState) -> RunRecord:
        run = state.claim.run
        save = lambda: self._save_execution(state)
        cancelled = False
        try:
            call = await self.planner.plan(run.message, self.runtime.registry.schemas())
            if isinstance(call, DAGPlan):
                await self._execute_dag(run, call, save)
            elif isinstance(call, ParallelPlan):
                await self._execute_parallel(run, call, save)
            else:
                run.tool_call = ToolCall.model_validate(call)
                save()
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
        except LeaseLostError:
            state.lost = True
            raise
        except asyncio.CancelledError:
            cancelled = True
            run.status = "failed"
            run.error = RunError(code="CANCELLED", message="任务执行已取消。")
            raise
        except Exception:
            logger.exception("Run %s 执行失败", run.run_id)
            run.status = "failed"
            run.error = RunError(code="INTERNAL_ERROR", message="任务执行发生异常。")
        finally:
            if not state.lost:
                run.completed_at = datetime.now(timezone.utc)
                try:
                    save()
                except LeaseLostError:
                    if not cancelled:
                        raise
                except Exception:
                    if not cancelled:
                        raise
                    logger.exception("Run %s 取消状态保存失败，继续传播取消。", run.run_id)
        return run.model_copy(deep=True)

    async def _execute_parallel(self, run: RunRecord, plan: ParallelPlan, save: Callable[[], None]) -> None:
        run.plan = ParallelPlan.model_validate(plan)
        run.tool_executions = [ToolExecution(tool_call=call) for call in run.plan.tool_calls]
        save()
        await execute_parallel(self.runtime, run.tool_executions, on_change=save)
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

    async def _execute_dag(self, run: RunRecord, plan: DAGPlan, save: Callable[[], None]) -> None:
        # 模型可能在构造后被修改，执行前再次校验并深拷贝，避免修改 Planner 的计划。
        run.plan = DAGPlan.model_validate(plan).model_copy(deep=True)
        run.tool_executions = [ToolExecution(
            tool_call=step.tool_call, depends_on=step.depends_on, bindings=step.bindings,
        ) for step in run.plan.steps]
        save()
        await execute_dag(self.runtime, run.tool_executions, on_change=save)
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
        return self.repository.get(run_id).model_copy(deep=True)
