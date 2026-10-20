"""设备、告警、流量及派生诊断的参数契约与模拟数据；不包含外部请求。"""

from datetime import timedelta
from enum import Enum
from typing import Literal, Self

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


class GetTrafficArgs(Contract):
    node_id: str = Field(min_length=1)
    begin_time: AwareDatetime
    end_time: AwareDatetime

    @model_validator(mode="after")
    def check_time_range(self) -> Self:
        if self.end_time <= self.begin_time:
            raise ValueError("end_time 必须晚于 begin_time")
        return self


class TrafficRecord(Contract):
    traffic_id: str
    node_id: str
    begin_time: AwareDatetime
    end_time: AwareDatetime
    utilization_percent: float = Field(ge=0, le=100)
    traffic_level: Literal["LOW", "MEDIUM", "HIGH"]
    description: str


class GetDeviceArgs(Contract):
    node_id: str = Field(min_length=1)


class DeviceRecord(Contract):
    device_id: str
    node_id: str
    name: str
    state: Literal["UP", "DOWN"]
    description: str


class DiagnoseNodeArgs(Contract):
    node_id: str = Field(min_length=1)
    device_info: list[DeviceRecord] = Field(min_length=1, max_length=1)
    warnings: list[WarningRecord]
    traffic: list[TrafficRecord]

    @model_validator(mode="after")
    def check_evidence(self) -> Self:
        if any(row.node_id != self.node_id for row in [*self.device_info, *self.warnings, *self.traffic]):
            raise ValueError("诊断证据的 node_id 必须与请求节点一致")
        if any(row.end_time <= row.begin_time for row in self.traffic):
            raise ValueError("流量证据的 end_time 必须晚于 begin_time")
        return self


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


async def get_traffic(args: GetTrafficArgs) -> ToolResult:
    # 固定演示利用率；对任意合法节点和窗口返回相同等级，不代表真实观测值。
    record = TrafficRecord(
        traffic_id="mock-traffic-001", node_id=args.node_id,
        begin_time=args.begin_time, end_time=args.end_time,
        utilization_percent=72.5, traffic_level="HIGH",
        description="模拟：请求时间范围内的演示流量利用率为 72.5%，等级 HIGH",
    )
    return ToolResult(
        type="success", metadata={"source": "mock", "count": 1},
        content=[record.model_dump(mode="json")], indexes=[record.traffic_id],
    )


async def get_device(args: GetDeviceArgs) -> ToolResult:
    record = DeviceRecord(
        device_id=f"mock-device-{args.node_id}", node_id=args.node_id,
        name=f"模拟设备 {args.node_id}", state="UP", description="模拟：设备当前状态为 UP",
    )
    return ToolResult(
        type="success", metadata={"source": "mock", "count": 1},
        content=[record.model_dump(mode="json")], indexes=[record.device_id],
    )


async def diagnose_node(args: DiagnoseNodeArgs) -> ToolResult:
    # 只计算传入证据；不调用其他工具，也不生成新的设备、告警或流量观测值。
    device = args.device_info[0]
    critical = sum(row.severe_level == SevereLevel.CRITICAL for row in args.warnings)
    peak = max((row.utilization_percent for row in args.traffic), default=None)
    traffic_levels = sorted({row.traffic_level for row in args.traffic})
    if device.state == "DOWN" or critical:
        level = "CRITICAL"
    elif args.warnings or "HIGH" in traffic_levels:
        level = "WARNING"
    else:
        level = "NORMAL" if args.traffic else "UNKNOWN"
    utilization = f"{peak}%" if peak is not None else "无流量数据"
    summary = (f"【模拟诊断】{args.node_id}：设备状态 {device.state}；严重告警 {critical} 条"
               f"（告警总数 {len(args.warnings)}）；最大流量利用率 {utilization}，"
               f"等级 {','.join(traffic_levels) or '未知'}；诊断等级 {level}。")
    diagnosis_id = f"mock-diagnosis-{args.node_id}"
    return ToolResult(
        type="success", metadata={"source": "mock", "count": 1},
        content=[{
            "diagnosis_id": diagnosis_id, "node_id": args.node_id, "device_state": device.state,
            "warning_count": len(args.warnings), "critical_warning_count": critical,
            "max_utilization_percent": peak, "traffic_levels": traffic_levels,
            "diagnosis_level": level, "summary": summary,
            "evidence": {
                "device_ids": [row.device_id for row in args.device_info],
                "warning_ids": [row.warning_id for row in args.warnings],
                "traffic_ids": [row.traffic_id for row in args.traffic],
            },
        }], indexes=[diagnosis_id],
    )
