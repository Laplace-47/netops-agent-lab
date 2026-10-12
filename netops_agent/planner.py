"""可替换的 Planner 接口与有限规则的模拟实现。"""

import re
from collections.abc import Callable
from datetime import datetime, time, timedelta, timezone
from typing import Protocol
from zoneinfo import ZoneInfo

from netops_agent.models import ToolCall


class UnsupportedRequest(ValueError):
    """用户请求超出当前 Mock Planner 支持范围。"""


class Planner(Protocol):
    async def plan(self, message: str, tools: list[dict]) -> ToolCall: ...


class MockPlanner:
    def __init__(
        self,
        clock: Callable[[], datetime] | None = None,
        timezone_name: str = "Asia/Shanghai",
    ):
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._timezone = ZoneInfo(timezone_name)

    async def plan(self, message: str, tools: list[dict]) -> ToolCall:
        match = re.fullmatch(
            r"查询\s*(node_\d+)\s*昨天的\s*(严重|警告|信息)告警", message.strip(),
        )
        if not match:
            raise UnsupportedRequest("当前仅支持：查询 node_编号 昨天的严重/警告/信息告警。")
        if not any(tool["function"]["name"] == "find_warning" for tool in tools):
            raise UnsupportedRequest("当前没有可用的 find_warning 工具。")

        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("Planner clock 必须返回带时区的 datetime")
        today = now.astimezone(self._timezone).date()
        end_time = datetime.combine(today, time.min, tzinfo=self._timezone)
        begin_time = end_time - timedelta(days=1)
        levels = {"信息": "INFO", "警告": "WARNING", "严重": "CRITICAL"}
        return ToolCall(name="find_warning", arguments={
            "node_id": match.group(1),
            "severe_level": levels[match.group(2)],
            "begin_time": begin_time.isoformat(),
            "end_time": end_time.isoformat(),
        })
