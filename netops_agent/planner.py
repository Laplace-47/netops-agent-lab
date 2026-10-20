"""可替换的 Planner 接口与有限规则的模拟实现。"""

import re
from collections.abc import Callable
from datetime import datetime, time, timedelta, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

from netops_agent.models import DAGPlan, DAGStep, ParallelPlan, ResultBinding, ToolCall


class UnsupportedRequest(ValueError):
    """用户请求超出当前 Mock Planner 支持范围。"""


class Planner(Protocol):
    async def plan(self, message: str, tools: list[dict]) -> ToolCall | ParallelPlan | DAGPlan: ...


class MockPlanner:
    def __init__(
        self,
        clock: Callable[[], datetime] | None = None,
        timezone_name: str = "Asia/Shanghai",
    ):
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._timezone = ZoneInfo(timezone_name)

    async def plan(self, message: str, tools: list[dict]) -> ToolCall | ParallelPlan | DAGPlan:
        match = re.fullmatch(
            r"查询\s*(node_\d+)\s*昨天的\s*(严重|警告|信息)告警", message.strip(),
        )
        combined = re.fullmatch(
            r"综合查询\s*(node_\d+)\s*昨天的告警和流量", message.strip(),
        )
        diagnostic = re.fullmatch(
            r"诊断\s*(node_\d+)\s*昨天的设备状态、告警和流量", message.strip(),
        )
        if not match and not combined and not diagnostic:
            raise UnsupportedRequest(
                "当前支持：查询 node_编号 昨天的严重/警告/信息告警；"
                "综合查询 node_编号 昨天的告警和流量；"
                "诊断 node_编号 昨天的设备状态、告警和流量。",
            )
        available = {tool["function"]["name"] for tool in tools}
        if "find_warning" not in available:
            raise UnsupportedRequest("当前没有可用的 find_warning 工具。")
        if combined and "get_traffic" not in available:
            raise UnsupportedRequest("当前没有可用的 get_traffic 工具。")
        if diagnostic:
            for name in ("get_device", "get_traffic", "diagnose_node"):
                if name not in available:
                    raise UnsupportedRequest(f"当前没有可用的 {name} 工具。")

        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("Planner clock 必须返回带时区的 datetime")
        today = now.astimezone(self._timezone).date()
        end_time = datetime.combine(today, time.min, tzinfo=self._timezone)
        begin_time = end_time - timedelta(days=1)
        common = {
            "node_id": (diagnostic or combined or match).group(1),
            "begin_time": begin_time.isoformat(),
            "end_time": end_time.isoformat(),
        }
        if diagnostic:
            device = ToolCall(name="get_device", arguments={"node_id": common["node_id"]})
            warning = ToolCall(name="find_warning", arguments={**common, "severe_level": "CRITICAL"})
            traffic = ToolCall(name="get_traffic", arguments=common)
            diagnosis = ToolCall(name="diagnose_node", arguments={"node_id": common["node_id"]})
            return DAGPlan(steps=[
                DAGStep(tool_call=device),
                DAGStep(tool_call=warning, depends_on=[device.id]),
                DAGStep(tool_call=traffic, depends_on=[device.id]),
                DAGStep(tool_call=diagnosis, depends_on=[device.id, warning.id, traffic.id], bindings=[
                    ResultBinding(source_step_id=device.id, target_argument="device_info"),
                    ResultBinding(source_step_id=warning.id, target_argument="warnings"),
                    ResultBinding(source_step_id=traffic.id, target_argument="traffic"),
                ]),
            ])
        if combined:
            # 此组合示例固定查询严重告警；两个调用没有数据依赖。
            return ParallelPlan(tool_calls=[
                ToolCall(name="find_warning", arguments={**common, "severe_level": "CRITICAL"}),
                ToolCall(name="get_traffic", arguments=common),
            ])
        levels = {"信息": "INFO", "警告": "WARNING", "严重": "CRITICAL"}
        return ToolCall(name="find_warning", arguments={
            **common,
            "severe_level": levels[match.group(2)],
        })
