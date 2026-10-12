import asyncio
from datetime import datetime, timezone

import pytest

from netops_agent.planner import MockPlanner, UnsupportedRequest


@pytest.mark.parametrize("label, expected", [
    ("严重", "CRITICAL"), ("警告", "WARNING"), ("信息", "INFO"),
])
def test_planner_maps_level_and_yesterday(planner, registry, label, expected):
    call = asyncio.run(planner.plan(f"查询node_20昨天的{label}告警", registry.schemas()))
    assert call.name == "find_warning"
    assert call.arguments == {
        "node_id": "node_20", "severe_level": expected,
        "begin_time": "2026-10-11T00:00:00+08:00",
        "end_time": "2026-10-12T00:00:00+08:00",
    }


def test_yesterday_uses_business_timezone_instead_of_utc(registry):
    planner = MockPlanner(clock=lambda: datetime(2026, 10, 11, 18, tzinfo=timezone.utc))
    call = asyncio.run(planner.plan("查询 node_20 昨天的严重告警", registry.schemas()))
    assert call.arguments["end_time"] == "2026-10-12T00:00:00+08:00"


def test_unsupported_message_does_not_silently_query_default_node(planner, registry):
    with pytest.raises(UnsupportedRequest):
        asyncio.run(planner.plan("删除 node_20", registry.schemas()))
