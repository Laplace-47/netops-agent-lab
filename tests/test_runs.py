import asyncio
from datetime import datetime
from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from main import create_app
from netops_agent.models import ToolCall
from netops_agent.service import RunService
from netops_agent.runtime import BusinessToolError, TemporaryToolError, ToolRuntime
from netops_agent.tools import find_warning

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("message", [
    "查询 node_20 昨天的严重告警",
    "查询node_20昨天的严重告警",
])
async def test_successful_run_can_be_retrieved(client, message):
    response = await client.post("/runs", json={"message": message})
    assert response.status_code == 201
    run = response.json()
    assert run["status"] == "succeeded"
    assert run["error"] is None
    assert run["completed_at"] is not None
    assert run["tool_call"]["name"] == "find_warning"
    assert run["tool_call"]["arguments"]["severe_level"] == "CRITICAL"
    result = run["result"]
    assert result["type"] == "success"
    assert result["metadata"]["source"] == "mock"
    assert result["metadata"]["tool_call_id"] == run["tool_call"]["id"]
    assert result["metadata"]["count"] == 1
    assert result["indexes"] == ["mock-warning-001"]
    assert result["content"][0]["node_id"] == "node_20"
    assert "1 条" in run["answer"]
    location = response.headers["location"]
    assert location == f"/runs/{run['run_id']}"
    assert (await client.get(location)).json() == run


async def test_empty_result_is_success(client):
    response = await client.post("/runs", json={"message": "查询 node_404 昨天的严重告警"})
    run = response.json()
    assert run["status"] == "succeeded"
    assert run["result"]["type"] == "success"
    assert run["result"]["content"] == []
    assert run["result"]["indexes"] == []
    assert run["result"]["metadata"]["count"] == 0
    assert run["error"] is None
    assert "0 条" in run["answer"]


async def test_unknown_run_returns_404(client):
    assert (await client.get(f"/runs/{uuid4()}")).status_code == 404


@pytest.mark.parametrize("body", [{}, {"message": "   "}, {"message": 123},
                                       {"message": "查询", "extra": True}])
async def test_invalid_request_returns_422(client, body):
    assert (await client.post("/runs", json=body)).status_code == 422


async def test_unsupported_request_is_saved_as_failed_run(client):
    response = await client.post("/runs", json={"message": "重启所有设备"})
    assert response.status_code == 201
    run = response.json()
    assert run["status"] == "failed"
    assert run["error"]["code"] == "UNSUPPORTED_REQUEST"
    assert run["tool_call"] is None
    assert run["result"] is None
    assert (await client.get(response.headers["location"])).json() == run


async def test_tool_validation_failure_is_saved_in_run(valid_arguments):
    class InvalidPlanner:
        async def plan(self, message, tools):
            return ToolCall(name="find_warning", arguments={
                **valid_arguments, "severe_level": "URGENT",
            })

    async with AsyncClient(
        transport=ASGITransport(app=create_app(planner=InvalidPlanner())), base_url="http://test",
    ) as client:
        response = await client.post("/runs", json={"message": "模拟错误参数"})
        run = response.json()
        assert run["status"] == "failed"
        assert run["result"]["type"] == "error"
        assert run["error"]["code"] == "INVALID_ARGUMENTS"
        assert (await client.get(response.headers["location"])).json() == run


async def test_openapi_contains_both_run_endpoints(client):
    schema = (await client.get("/openapi.json")).json()
    assert "post" in schema["paths"]["/runs"]
    assert "get" in schema["paths"]["/runs/{run_id}"]


async def test_planner_exception_is_saved_without_internal_details():
    class BrokenPlanner:
        async def plan(self, message, tools):
            raise RuntimeError("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    async with AsyncClient(
        transport=ASGITransport(app=create_app(planner=BrokenPlanner())), base_url="http://test",
    ) as client:
        response = await client.post("/runs", json={"message": "模拟 Planner 失败"})
        run = response.json()
        assert run["status"] == "failed"
        assert run["error"]["code"] == "INTERNAL_ERROR"
        assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in response.text
        assert (await client.get(response.headers["location"])).json() == run


async def test_returned_run_snapshot_cannot_modify_saved_result(planner, registry):
    service = RunService(planner, ToolRuntime(registry))
    run = await service.create_run("查询 node_20 昨天的严重告警")
    run.result.content.clear()
    assert len(service.get_run(run.run_id).result.content) == 1


@pytest.fixture
def app_with_handler(monkeypatch, planner):
    def build(handler):
        # 沿用 create_app 的工具注册和现有 Planner 夹具，仅替换工具执行函数。
        monkeypatch.setattr("main.find_warning", handler)
        app = create_app(planner=planner)
        service = app.state.run_service
        service.runtime = ToolRuntime(service.runtime.registry, timeout=0.02)
        return app

    return build


@pytest.mark.parametrize("error_code, expected_calls", [
    ("TIMEOUT", 4), ("TEMPORARY_ERROR", 4), ("BUSINESS_ERROR", 1),
])
async def test_runtime_error_is_saved_and_retrievable(app_with_handler, error_code, expected_calls):
    calls = 0
    cleanups = 0

    async def handler(args):
        nonlocal calls, cleanups
        calls += 1
        if error_code == "TIMEOUT":
            try:
                await asyncio.Event().wait()
            finally:
                cleanups += 1
        if error_code == "TEMPORARY_ERROR":
            raise TemporaryToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
        raise BusinessToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    app = app_with_handler(handler)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/runs", json={"message": "查询node_20昨天的严重告警"})
        assert response.status_code == 201
        run = response.json()
        assert run["status"] == "failed"
        assert run["error"]["code"] == error_code
        assert run["result"]["type"] == "error"
        assert run["result"]["metadata"]["error_code"] == error_code
        assert run["result"]["metadata"]["message"] == run["error"]["message"]
        assert run["result"]["metadata"]["tool_call_id"] == run["tool_call"]["id"]
        assert run["result"]["content"] == [] and run["result"]["indexes"] == []
        assert run["completed_at"] is not None
        assert datetime.fromisoformat(run["completed_at"]) >= datetime.fromisoformat(run["created_at"])
        assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in response.text
        assert calls == expected_calls
        if error_code == "TIMEOUT":
            assert cleanups == calls

        fetched = await client.get(response.headers["location"])
        assert fetched.status_code == 200
        assert fetched.json() == run
        assert calls == expected_calls  # GET 只读保存结果，不再次执行 handler。


@pytest.mark.parametrize("first_failure", ["timeout", "temporary"])
async def test_retry_success_is_saved_and_retrievable(app_with_handler, first_failure):
    calls = 0
    cleanups = 0

    async def handler(args):
        nonlocal calls, cleanups
        calls += 1
        if calls == 1:
            if first_failure == "temporary":
                raise TemporaryToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
            try:
                await asyncio.Event().wait()
            finally:
                cleanups += 1
        return await find_warning(args)

    app = app_with_handler(handler)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/runs", json={"message": "查询node_20昨天的严重告警"})
        assert response.status_code == 201
        run = response.json()
        assert run["status"] == "succeeded"
        assert run["error"] is None
        assert run["completed_at"] is not None
        assert run["result"]["type"] == "success"
        assert "error_code" not in run["result"]["metadata"]
        assert run["result"]["metadata"]["count"] == 1
        assert run["result"]["metadata"]["tool_call_id"] == run["tool_call"]["id"]
        assert run["result"]["indexes"] == ["mock-warning-001"]
        assert run["result"]["content"][0]["node_id"] == "node_20"
        assert "1 条" in run["answer"]
        assert calls == 2
        assert cleanups == (1 if first_failure == "timeout" else 0)

        fetched = await client.get(response.headers["location"])
        assert fetched.status_code == 200
        assert fetched.json() == run  # 保存的是最终成功结果，而非第一次的失败。
        assert calls == 2


async def test_cancelled_run_is_saved_and_retrievable(app_with_handler):
    calls = 0
    cleanups = 0
    started = asyncio.Event()
    lock = asyncio.Lock()

    async def handler(args):
        nonlocal calls, cleanups
        calls += 1
        async with lock:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanups += 1

    app = app_with_handler(handler)
    service = app.state.run_service
    # 外部取消用较长超时，避免与短超时的重试场景竞争。
    service.runtime = ToolRuntime(service.runtime.registry, timeout=5.0)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        task = asyncio.create_task(service.create_run("查询node_20昨天的严重告警"))
        try:
            await asyncio.wait_for(started.wait(), timeout=1)
            # 当前没有列出 Run 的接口，仅从内存获取新任务 ID；状态使用公开查询验证。
            run_id, = service._runs
            running = service.get_run(run_id)
            assert running.status == "running"
            assert running.completed_at is None
            assert lock.locked()

            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=1)

            saved = service.get_run(run_id)
            assert saved.status == "failed"
            assert saved.error.code == "CANCELLED"
            assert saved.completed_at is not None
            assert saved.completed_at >= saved.created_at
            assert saved.tool_call.id == running.tool_call.id
            assert saved.result is None  # 没有把取消保存为可重试的工具错误。
            assert calls == 1
            assert cleanups == 1
            assert not lock.locked()

            fetched = await client.get(f"/runs/{run_id}")
            assert fetched.status_code == 200
            assert fetched.json() == saved.model_dump(mode="json")
            assert calls == 1
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
