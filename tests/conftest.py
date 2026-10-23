import asyncio
from contextlib import asynccontextmanager
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


class RunClient(AsyncClient):
    """显式排空队列再查最终结果；不改变 POST 的后台执行契约。"""

    def __init__(self, app):
        super().__init__(transport=ASGITransport(app=app), base_url="http://test")
        self.service = app.state.run_service

    async def completed_run(self, response):
        assert response.status_code == 202
        assert response.json()["status"] == "queued"
        assert response.json()["completed_at"] is None
        assert response.headers["location"] == f"/runs/{response.json()['run_id']}"
        await asyncio.wait_for(self.service.queue.join(), timeout=2)
        fetched = await self.get(response.headers["location"])
        assert fetched.status_code == 200
        return fetched.json()


@pytest.fixture
def client_factory():
    @asynccontextmanager
    async def build(app):
        # ASGITransport 不自动运行 lifespan，必须显式启动和关闭 Worker。
        async with app.router.lifespan_context(app), RunClient(app) as client:
            yield client
    return build


@pytest.fixture
async def client(planner, client_factory):
    async with client_factory(create_app(planner=planner)) as client:
        yield client


@pytest.fixture
async def run_to_completion():
    services = set()

    async def execute(service, message):
        # 执行层单元测试独立驱动一次出队，便于直接验证 CancelledError 传播。
        # 真正的长期 Worker 和生命周期另有 API 集成测试覆盖。
        services.add(service)
        submitted = await service.create_run(message)
        run_id = service.queue.get_nowait()
        assert run_id == submitted.run_id
        try:
            return await service.execute_run(run_id)
        finally:
            service._enqueued_ids.discard(run_id)
            service.queue.task_done()
    try:
        yield execute
    finally:
        for service in services:
            await service.stop()
            service.repository.close()


@pytest.fixture
def valid_arguments():
    return {
        "node_id": "node_20",
        "severe_level": "CRITICAL",
        "begin_time": "2026-10-11T00:00:00+08:00",
        "end_time": "2026-10-12T00:00:00+08:00",
    }
