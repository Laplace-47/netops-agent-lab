from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from main import create_app
from netops_agent.models import ToolCall
from netops_agent.service import RunService
from netops_agent.runtime import ToolRuntime

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
