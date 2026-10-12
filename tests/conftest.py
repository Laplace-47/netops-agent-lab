from datetime import datetime, timedelta, timezone

import pytest
from httpx import ASGITransport, AsyncClient

from main import create_app
from netops_agent.planner import MockPlanner
from netops_agent.registry import ToolDefinition, ToolRegistry
from netops_agent.runtime import ToolRuntime
from netops_agent.tools import FindWarningArgs, find_warning


@pytest.fixture
def fixed_now():
    return datetime(2026, 10, 12, 12, tzinfo=timezone(timedelta(hours=8)))


@pytest.fixture
def planner(fixed_now):
    return MockPlanner(clock=lambda: fixed_now)


@pytest.fixture
def registry():
    registry = ToolRegistry()
    registry.register(ToolDefinition(
        name="find_warning",
        description="查询模拟网络告警",
        args_model=FindWarningArgs,
        handler=find_warning,
    ))
    return registry


@pytest.fixture
def runtime(registry):
    return ToolRuntime(registry)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client(planner):
    async with AsyncClient(
        transport=ASGITransport(app=create_app(planner=planner)), base_url="http://test",
    ) as client:
        yield client


@pytest.fixture
def valid_arguments():
    return {
        "node_id": "node_20",
        "severe_level": "CRITICAL",
        "begin_time": "2026-10-11T00:00:00+08:00",
        "end_time": "2026-10-12T00:00:00+08:00",
    }
