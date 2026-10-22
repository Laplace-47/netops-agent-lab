"""API、Planner 和 Runtime 之间共享的数据契约。"""

from datetime import datetime, timezone
from graphlib import CycleError, TopologicalSorter
from typing import Literal, Self
from uuid import UUID, uuid4

from pydantic import (
    AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator,
)


class Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True, allow_inf_nan=False,
    )


class CreateRunRequest(Contract):
    message: str = Field(min_length=1, max_length=1000)


class ToolCall(Contract):
    """Planner 的内部标准输出；未来 LLM 适配器也返回此结构。"""

    id: str = Field(default_factory=lambda: str(uuid4()), min_length=1)
    name: str = Field(min_length=1)
    arguments: dict[str, JsonValue]


class ParallelPlan(Contract):
    """调用之间无依赖；列表顺序也是执行记录和汇总结果的展示顺序。"""

    model_config = ConfigDict(revalidate_instances="always")

    mode: Literal["parallel"] = "parallel"
    tool_calls: list[ToolCall] = Field(min_length=2)

    @model_validator(mode="after")
    def check_unique_call_ids(self) -> Self:
        if len({call.id for call in self.tool_calls}) != len(self.tool_calls):
            raise ValueError("并行计划的 ToolCall.id 必须唯一")
        return self


class ResultBinding(Contract):
    """只绑定直接依赖步骤的完整 ToolResult.content，目标是一个顶层参数。"""

    source_step_id: str = Field(min_length=1)
    target_argument: str = Field(min_length=1)


class DAGStep(Contract):
    """tool_call.id 同时作为步骤 ID，不另存容易不一致的第二个 ID。"""

    model_config = ConfigDict(revalidate_instances="always")

    tool_call: ToolCall
    depends_on: list[str] = Field(default_factory=list)
    bindings: list[ResultBinding] = Field(default_factory=list)

    @field_validator("tool_call", mode="before")
    @classmethod
    def revalidate_call(cls, value):
        # 只加强 DAG 边界，不改变原 ToolCall 对单工具和 ParallelPlan 的语义。
        if isinstance(value, ToolCall):
            return {"id": value.id, "name": value.name, "arguments": value.arguments}
        return value

    @model_validator(mode="after")
    def check_dependencies_and_bindings(self) -> Self:
        if len(set(self.depends_on)) != len(self.depends_on):
            raise ValueError("步骤不能重复声明同一依赖")
        if self.tool_call.id in self.depends_on:
            raise ValueError("步骤不能依赖自身")
        targets = set()
        for binding in self.bindings:
            if binding.source_step_id not in self.depends_on:
                raise ValueError("绑定来源必须是直接依赖步骤")
            if binding.target_argument in targets or binding.target_argument in self.tool_call.arguments:
                raise ValueError("绑定目标不能重复，也不能与静态参数冲突")
            targets.add(binding.target_argument)
        return self


class DAGPlan(Contract):
    model_config = ConfigDict(revalidate_instances="always")

    mode: Literal["dag"] = "dag"
    steps: list[DAGStep] = Field(min_length=1)

    @model_validator(mode="after")
    def check_graph(self) -> Self:
        ids = {step.tool_call.id for step in self.steps}
        if len(ids) != len(self.steps):
            raise ValueError("DAG 步骤 ID 必须唯一")
        graph = {step.tool_call.id: step.depends_on for step in self.steps}
        if any(dependency not in ids for dependencies in graph.values() for dependency in dependencies):
            raise ValueError("依赖必须引用存在的步骤")
        try:
            TopologicalSorter(graph).prepare()
        except CycleError as exc:
            raise ValueError("DAG 不允许形成环") from exc
        return self


class ToolResult(Contract):
    """content 为结构化记录，indexes 为对应记录 ID；空列表可成功。"""

    model_config = ConfigDict(revalidate_instances="always")

    type: Literal["success", "error"]
    metadata: dict[str, JsonValue]
    content: list[dict[str, JsonValue]]
    indexes: list[str]


class RunError(Contract):
    code: str
    message: str


class ToolExecution(Contract):
    tool_call: ToolCall
    status: Literal["pending", "running", "succeeded", "failed", "cancelled", "blocked"] = "pending"
    result: ToolResult | None = None
    depends_on: list[str] = Field(default_factory=list)
    bindings: list[ResultBinding] = Field(default_factory=list)
    error: RunError | None = None


class RunRecord(Contract):
    run_id: UUID = Field(default_factory=uuid4)
    message: str
    status: Literal["queued", "running", "succeeded", "failed"] = "queued"
    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: AwareDatetime | None = None
    tool_call: ToolCall | None = None
    result: ToolResult | None = None
    plan: ParallelPlan | DAGPlan | None = None
    tool_executions: list[ToolExecution] = Field(default_factory=list)
    answer: str | None = None
    error: RunError | None = None
