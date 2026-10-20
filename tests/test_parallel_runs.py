import asyncio

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from main import create_app
from netops_agent.models import ParallelPlan, ToolCall
from netops_agent.planner import UnsupportedRequest
from netops_agent.runtime import BusinessToolError, TemporaryToolError, ToolRuntime
from netops_agent.tools import FindWarningArgs, GetTrafficArgs, find_warning, get_traffic

pytestmark = pytest.mark.anyio
COMBINED_MESSAGE = "综合查询 node_20 昨天的告警和流量"


@pytest.fixture
def parallel_app(planner):
    return create_app(planner=planner)


@pytest.mark.parametrize("message", [COMBINED_MESSAGE, "综合查询node_20昨天的告警和流量"])
async def test_combined_planner_produces_explicit_parallel_plan(planner, parallel_app, message):
    registry = parallel_app.state.run_service.runtime.registry
    plan = await planner.plan(message, registry.schemas())
    assert isinstance(plan, ParallelPlan)
    assert plan.mode == "parallel"
    assert [call.name for call in plan.tool_calls] == ["find_warning", "get_traffic"]
    assert len({call.id for call in plan.tool_calls}) == 2
    warning, traffic = plan.tool_calls
    assert warning.arguments["severe_level"] == "CRITICAL"
    assert "severe_level" not in traffic.arguments
    FindWarningArgs.model_validate(warning.arguments)
    GetTrafficArgs.model_validate(traffic.arguments)
    for key in ("node_id", "begin_time", "end_time"):
        assert warning.arguments[key] == traffic.arguments[key]
    assert traffic.arguments == {
        "node_id": "node_20", "begin_time": "2026-10-11T00:00:00+08:00",
        "end_time": "2026-10-12T00:00:00+08:00",
    }


@pytest.mark.parametrize("missing", ["find_warning", "get_traffic"])
async def test_combined_planner_requires_both_registered_tools(planner, parallel_app, missing):
    schemas = parallel_app.state.run_service.runtime.registry.schemas()
    with pytest.raises(UnsupportedRequest):
        await planner.plan(COMBINED_MESSAGE, [s for s in schemas if s["function"]["name"] != missing])


@pytest.mark.parametrize("case", ["too_few", "duplicate_id", "wrong_mode"])
async def test_parallel_plan_contract_rejects_ambiguous_plan(valid_arguments, case):
    warning = ToolCall(name="find_warning", arguments=valid_arguments)
    traffic = ToolCall(name="get_traffic", arguments={})
    payload = {"mode": "parallel", "tool_calls": [warning, traffic]}
    if case == "too_few":
        payload["tool_calls"] = [warning]
    elif case == "duplicate_id":
        traffic.id = warning.id
    else:
        payload["mode"] = "sequential"
    with pytest.raises(ValidationError):
        ParallelPlan.model_validate(payload)


async def test_combined_post_and_get_preserve_both_results(client):
    response = await client.post("/runs", json={"message": COMBINED_MESSAGE})
    assert response.status_code == 201
    run = response.json()
    assert run["status"] == "succeeded" and run["error"] is None
    assert run["tool_call"] is None and run["result"] is None
    assert run["plan"]["mode"] == "parallel"
    records = run["tool_executions"]
    assert [r["tool_call"]["name"] for r in records] == ["find_warning", "get_traffic"]
    assert [r["tool_call"] for r in records] == run["plan"]["tool_calls"]
    for record in records:
        assert record["status"] == "succeeded"
        assert record["result"]["type"] == "success"
        assert record["result"]["metadata"]["source"] == "mock"
        assert record["result"]["metadata"]["tool_call_id"] == record["tool_call"]["id"]
    assert records[0]["result"]["indexes"] == ["mock-warning-001"]
    assert records[1]["result"]["content"][0]["utilization_percent"] == 72.5
    assert "严重告警" in run["answer"] and "72.5%" in run["answer"] and "HIGH" in run["answer"]
    assert run["completed_at"] is not None
    fetched = await client.get(response.headers["location"])
    assert fetched.status_code == 200 and fetched.json() == run


async def test_single_query_keeps_original_fields_and_call_type(client, planner, registry):
    call = await planner.plan("查询 node_20 昨天的严重告警", registry.schemas())
    assert isinstance(call, ToolCall)
    run = (await client.post("/runs", json={"message": "查询 node_20 昨天的严重告警"})).json()
    assert run["status"] == "succeeded"
    assert run["tool_call"]["name"] == "find_warning"
    assert run["result"]["type"] == "success"
    assert run["plan"] is None and run["tool_executions"] == []


async def test_parallel_tools_start_before_either_is_released(monkeypatch, planner):
    started = {name: asyncio.Event() for name in ("warning", "traffic")}
    release = asyncio.Event()
    traffic_finished = asyncio.Event()
    calls = {"warning": 0, "traffic": 0}

    async def warning_handler(args):
        calls["warning"] += 1
        started["warning"].set()
        await release.wait()
        await traffic_finished.wait()
        return await find_warning(args)

    async def traffic_handler(args):
        calls["traffic"] += 1
        started["traffic"].set()
        await release.wait()
        result = await get_traffic(args)
        traffic_finished.set()
        return result

    monkeypatch.setattr("main.find_warning", warning_handler)
    monkeypatch.setattr("main.get_traffic", traffic_handler)
    app = create_app(planner=planner)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        request = asyncio.create_task(client.post("/runs", json={"message": COMBINED_MESSAGE}))
        try:
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started.values())), timeout=1)
            assert calls == {"warning": 1, "traffic": 1}
            assert not request.done()  # 两个 handler 已启动，但都没拿到放行信号。
            release.set()
            response = await asyncio.wait_for(request, timeout=1)
            run = response.json()
            assert run["status"] == "succeeded"
            assert [r["tool_call"]["name"] for r in run["tool_executions"]] == ["find_warning", "get_traffic"]
        finally:
            release.set()
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)


@pytest.mark.parametrize("failed_tool", ["find_warning", "get_traffic"])
async def test_failed_branch_still_collects_later_success(monkeypatch, planner, failed_tool):
    failed_finished = asyncio.Event()
    success_started = asyncio.Event()
    calls = {"find_warning": 0, "get_traffic": 0}
    originals = {"find_warning": find_warning, "get_traffic": get_traffic}
    successful_tool = next(name for name in calls if name != failed_tool)

    async def broken(args):
        calls[failed_tool] += 1
        await success_started.wait()
        raise BusinessToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    async def successful(args):
        calls[successful_tool] += 1
        success_started.set()
        await failed_finished.wait()
        return await originals[successful_tool](args)

    monkeypatch.setattr(f"main.{failed_tool}", broken)
    monkeypatch.setattr(f"main.{successful_tool}", successful)
    app = create_app(planner=planner)
    runtime = app.state.run_service.runtime
    original_execute = runtime.execute

    async def observed_execute(call):
        result = await original_execute(call)
        if call.name == failed_tool:
            failed_finished.set()
        return result

    monkeypatch.setattr(runtime, "execute", observed_execute)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await asyncio.wait_for(client.post("/runs", json={"message": COMBINED_MESSAGE}), timeout=1)
        run = response.json()
        assert response.status_code == 201 and run["status"] == "failed"
        assert run["error"]["code"] == "BUSINESS_ERROR"
        records = {r["tool_call"]["name"]: r for r in run["tool_executions"]}
        assert records[failed_tool]["status"] == "failed"
        assert records[failed_tool]["result"]["metadata"]["error_code"] == "BUSINESS_ERROR"
        assert records[successful_tool]["status"] == "succeeded"
        assert records[successful_tool]["result"]["type"] == "success"
        assert "告警查询" in run["answer"] and "流量查询" in run["answer"]
        assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in response.text
        assert (await client.get(response.headers["location"])).json() == run
        assert calls == {"find_warning": 1, "get_traffic": 1}


@pytest.mark.parametrize("failure", ["timeout", "temporary"])
async def test_exhausted_traffic_keeps_successful_warning(monkeypatch, planner, failure):
    calls = {"warning": 0, "traffic": 0}

    async def warning_handler(args):
        calls["warning"] += 1
        return await find_warning(args)

    async def traffic_handler(args):
        calls["traffic"] += 1
        if failure == "timeout":
            await asyncio.Event().wait()
        raise TemporaryToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    monkeypatch.setattr("main.find_warning", warning_handler)
    monkeypatch.setattr("main.get_traffic", traffic_handler)
    app = create_app(planner=planner)
    service = app.state.run_service
    service.runtime = ToolRuntime(service.runtime.registry, timeout=0.01)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await asyncio.wait_for(client.post("/runs", json={"message": COMBINED_MESSAGE}), timeout=1)
        run = response.json()
        code = "TIMEOUT" if failure == "timeout" else "TEMPORARY_ERROR"
        assert run["status"] == "failed" and run["error"]["code"] == code
        assert [r["status"] for r in run["tool_executions"]] == ["succeeded", "failed"]
        assert run["tool_executions"][0]["result"]["indexes"] == ["mock-warning-001"]
        assert run["tool_executions"][1]["result"]["metadata"]["error_code"] == code
        assert (await client.get(response.headers["location"])).json() == run
        assert calls == {"warning": 1, "traffic": 4}


async def test_traffic_retry_does_not_repeat_successful_peer(monkeypatch, planner):
    calls = {"warning": 0, "traffic": 0}

    async def warning_handler(args):
        calls["warning"] += 1
        return await find_warning(args)

    async def traffic_handler(args):
        calls["traffic"] += 1
        if calls["traffic"] == 1:
            raise TemporaryToolError("temporary")
        return await get_traffic(args)

    monkeypatch.setattr("main.find_warning", warning_handler)
    monkeypatch.setattr("main.get_traffic", traffic_handler)
    app = create_app(planner=planner)
    run = await app.state.run_service.create_run(COMBINED_MESSAGE)
    assert run.status == "succeeded"
    assert calls == {"warning": 1, "traffic": 2}


@pytest.mark.parametrize("cancel_source", ["external", "child"])
async def test_parallel_cancellation_propagates_and_cleans_children(monkeypatch, planner, cancel_source):
    started = {name: asyncio.Event() for name in ("warning", "traffic")}
    trigger = asyncio.Event()
    calls = {"warning": 0, "traffic": 0}
    cleanups = {"warning": 0, "traffic": 0}

    async def warning_handler(args):
        calls["warning"] += 1
        started["warning"].set()
        try:
            await trigger.wait()
            raise asyncio.CancelledError()
        finally:
            cleanups["warning"] += 1

    async def traffic_handler(args):
        calls["traffic"] += 1
        started["traffic"].set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanups["traffic"] += 1

    monkeypatch.setattr("main.find_warning", warning_handler)
    monkeypatch.setattr("main.get_traffic", traffic_handler)
    app = create_app(planner=planner)
    service = app.state.run_service
    task = asyncio.create_task(service.create_run(COMBINED_MESSAGE))
    try:
        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started.values())), timeout=1)
        run_id, = service._runs
        running = service.get_run(run_id)
        assert [r.status for r in running.tool_executions] == ["running", "running"]
        if cancel_source == "external":
            task.cancel()
        else:
            trigger.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        saved = service.get_run(run_id)
        assert saved.status == "failed" and saved.error.code == "CANCELLED"
        assert saved.completed_at is not None
        assert [r.status for r in saved.tool_executions] == ["cancelled", "cancelled"]
        assert all(r.result is None for r in saved.tool_executions)
        assert calls == cleanups == {"warning": 1, "traffic": 1}
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get(f"/runs/{run_id}")
            assert response.status_code == 200
            assert response.json() == saved.model_dump(mode="json")
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_combined_snapshot_isolation(parallel_app):
    service = parallel_app.state.run_service
    run = await service.create_run(COMBINED_MESSAGE)
    run.tool_executions[0].result.content.clear()
    run.plan.tool_calls.clear()
    saved = service.get_run(run.run_id)
    assert len(saved.plan.tool_calls) == 2
    assert len(saved.tool_executions[0].result.content) == 1
