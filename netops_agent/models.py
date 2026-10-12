"""API、Planner 和 Runtime 之间共享的数据契约。"""

from datetime import datetime, timezone
from typing import Literal
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue


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


class RunRecord(Contract):
    run_id: UUID = Field(default_factory=uuid4)
    message: str
    status: Literal["running", "succeeded", "failed"] = "running"
    created_at: AwareDatetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: AwareDatetime | None = None
    tool_call: ToolCall | None = None
    result: ToolResult | None = None
    answer: str | None = None
    error: RunError | None = None
