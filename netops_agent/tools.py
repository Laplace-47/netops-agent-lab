"""find_warning 参数契约与原创模拟告警工具；不包含外部请求。"""

from datetime import timedelta
from enum import Enum
from typing import Self

from pydantic import AwareDatetime, Field, model_validator

from netops_agent.models import Contract, ToolResult


class SevereLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class FindWarningArgs(Contract):
    node_id: str = Field(min_length=1)
    severe_level: SevereLevel
    begin_time: AwareDatetime
    end_time: AwareDatetime

    @model_validator(mode="after")
    def check_time_range(self) -> Self:
        if self.end_time <= self.begin_time:
            raise ValueError("end_time 必须晚于 begin_time")
        return self


class WarningRecord(Contract):
    warning_id: str
    node_id: str
    severe_level: SevereLevel
    occurred_at: AwareDatetime
    description: str


async def find_warning(args: FindWarningArgs) -> ToolResult:
    # 为请求起始日生成固定模拟样本，使演示不依赖运行日期或外部数据。
    # 这不是历史告警数据库，也不声称这些告警真实发生过。
    day = args.begin_time.replace(hour=0, minute=0, second=0, microsecond=0)
    samples = [
        WarningRecord(warning_id="mock-warning-001", node_id="node_20",
                      severe_level=SevereLevel.CRITICAL, occurred_at=day + timedelta(hours=10),
                      description="模拟：CPU 使用率超过严重告警阈值"),
        WarningRecord(warning_id="mock-warning-002", node_id="node_20",
                      severe_level=SevereLevel.WARNING, occurred_at=day + timedelta(hours=12),
                      description="模拟：接口丢包率升高"),
        WarningRecord(warning_id="mock-warning-003", node_id="node_21",
                      severe_level=SevereLevel.CRITICAL, occurred_at=day + timedelta(hours=15),
                      description="模拟：链路不可用"),
        WarningRecord(warning_id="mock-warning-004", node_id="node_20",
                      severe_level=SevereLevel.CRITICAL, occurred_at=day + timedelta(days=1),
                      description="模拟：下一日边界告警"),
    ]
    warnings = [row for row in samples if
                row.node_id == args.node_id and row.severe_level == args.severe_level
                and args.begin_time <= row.occurred_at < args.end_time]
    return ToolResult(
        type="success",
        metadata={"source": "mock", "count": len(warnings)},
        content=[row.model_dump(mode="json") for row in warnings],
        indexes=[row.warning_id for row in warnings],
    )
