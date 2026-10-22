import asyncio
from collections import Counter

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import Field, JsonValue

from main import create_app
from netops_agent.models import Contract, DAGPlan, DAGStep, ResultBinding, ToolCall, ToolResult
from netops_agent.planner import UnsupportedRequest
from netops_agent.registry import ToolDefinition
from netops_agent.runtime import BusinessToolError, TemporaryToolError, ToolRuntime
from netops_agent.tools import find_warning, get_device, get_traffic, diagnose_node

pytestmark = pytest.mark.anyio
DIAGNOSIS = "诊断 node_20 昨天的设备状态、告警和流量"


class ProbeArgs(Contract):
    tag: str
    records: list[dict[str, JsonValue]] = Field(default_factory=list)


def success(content=None):
    return ToolResult(type="success", metadata={"source": "mock"},
                      content=content if content is not None else [], indexes=[])


def step(step_id, dependencies=(), bindings=(), arguments=None, name=None):
    return DAGStep(tool_call=ToolCall(
        id=step_id, name=name or step_id,
        arguments={"tag": step_id} if arguments is None else arguments,
    ), depends_on=list(dependencies), bindings=list(bindings))


@pytest.fixture
def dag_app_factory():
    def build(plan, handlers, **config):
        class FixedPlanner:
            async def plan(self, message, tools):
                return plan
        app = create_app(planner=FixedPlanner())
        service = app.state.run_service
        for name, handler in handlers.items():
            service.runtime.registry.register(ToolDefinition(name, "模拟 DAG 测试工具", ProbeArgs, handler))
        service.runtime = ToolRuntime(service.runtime.registry, **config)
        return app
    return build


@pytest.mark.parametrize("message", [DIAGNOSIS, "诊断node_20昨天的设备状态、告警和流量"])
async def test_diagnostic_planner_declares_dependencies_and_content_bindings(planner, message):
    registry = create_app().state.run_service.runtime.registry
    plan = await planner.plan(message, registry.schemas())
    assert isinstance(plan, DAGPlan) and plan.mode == "dag"
    device, warning, traffic, diagnosis = plan.steps
    assert [s.tool_call.name for s in plan.steps] == [
        "get_device", "find_warning", "get_traffic", "diagnose_node",
    ]
    assert device.depends_on == []
    assert warning.depends_on == traffic.depends_on == [device.tool_call.id]
    assert diagnosis.depends_on == [s.tool_call.id for s in (device, warning, traffic)]
    assert {(b.source_step_id, b.target_argument) for b in diagnosis.bindings} == {
        (device.tool_call.id, "device_info"), (warning.tool_call.id, "warnings"),
        (traffic.tool_call.id, "traffic"),
    }
    assert diagnosis.tool_call.arguments == {"node_id": "node_20"}
    assert warning.tool_call.arguments["begin_time"] == traffic.tool_call.arguments["begin_time"]
    assert warning.tool_call.arguments["end_time"] == "2026-10-12T00:00:00+08:00"


@pytest.mark.parametrize("missing", ["get_device", "find_warning", "get_traffic", "diagnose_node"])
async def test_diagnostic_planner_requires_all_tools(planner, missing):
    schemas = create_app().state.run_service.runtime.registry.schemas()
    with pytest.raises(UnsupportedRequest):
        await planner.plan(DIAGNOSIS, [s for s in schemas if s["function"]["name"] != missing])


async def test_diagnostic_post_get_preserve_plan_bound_arguments_and_results(client):
    response = await client.post("/runs", json={"message": DIAGNOSIS})
    assert response.status_code == 202
    run = await client.completed_run(response)
    assert run["status"] == "succeeded" and run["error"] is None
    assert run["tool_call"] is None and run["result"] is None
    assert run["plan"]["mode"] == "dag"
    records = run["tool_executions"]
    assert [r["tool_call"]["name"] for r in records] == [
        "get_device", "find_warning", "get_traffic", "diagnose_node",
    ]
    for entry in records:
        assert entry["status"] == "succeeded" and entry["error"] is None
        assert entry["result"]["metadata"]["source"] == "mock"
        assert entry["result"]["metadata"]["tool_call_id"] == entry["tool_call"]["id"]
    diagnosis = records[-1]
    for field, index in [("device_info", 0), ("warnings", 1), ("traffic", 2)]:
        assert diagnosis["tool_call"]["arguments"][field] == records[index]["result"]["content"]
    assert run["plan"]["steps"][-1]["tool_call"]["arguments"] == {"node_id": "node_20"}
    assert "模拟" in run["answer"] and "72.5%" in run["answer"]
    assert run["completed_at"] is not None
    assert (await client.get(response.headers["location"])).json() == run


@pytest.mark.parametrize("failed_tool", ["get_device", "find_warning"])
async def test_diagnostic_failure_blocks_required_descendants_without_losing_traffic(
    monkeypatch, planner, client_factory, failed_tool,
):
    calls = Counter()
    originals = {"get_device": get_device, "find_warning": find_warning,
                 "get_traffic": get_traffic, "diagnose_node": diagnose_node}

    def observed(name):
        async def handler(args):
            calls[name] += 1
            if name == failed_tool:
                raise BusinessToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
            return await originals[name](args)
        return handler

    for name in originals:
        monkeypatch.setattr(f"main.{name}", observed(name))
    app = create_app(planner)
    async with client_factory(app) as client:
        response = await client.post("/runs", json={"message": DIAGNOSIS})
        run = await client.completed_run(response)
        entries = {entry["tool_call"]["name"]: entry for entry in run["tool_executions"]}
        assert run["status"] == "failed" and run["error"]["code"] == "BUSINESS_ERROR"
        assert entries[failed_tool]["status"] == "failed" and calls[failed_tool] == 1
        assert entries["diagnose_node"]["status"] == "blocked" and calls["diagnose_node"] == 0
        assert entries["diagnose_node"]["error"]["code"] == "DEPENDENCY_FAILED"
        if failed_tool == "get_device":
            for name in ("find_warning", "get_traffic"):
                assert entries[name]["status"] == "blocked" and calls[name] == 0
        else:
            assert entries["get_traffic"]["status"] == "succeeded" and calls["get_traffic"] == 1
            assert entries["get_traffic"]["result"]["content"][0]["utilization_percent"] == 72.5
        assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in str(run)
        assert (await client.get(response.headers["location"])).json() == run


async def test_root_steps_run_concurrently_and_unordered_dependency_waits(dag_app_factory, run_to_completion):
    started = {name: asyncio.Event() for name in ("a", "b")}
    release = asyncio.Event()
    calls = Counter()

    async def root(args):
        calls[args.tag] += 1
        started[args.tag].set()
        await release.wait()
        return success([{"source": args.tag}])

    async def sink(args):
        calls["sink"] += 1
        assert all(event.is_set() for event in started.values())
        assert release.is_set()
        return success()

    plan = DAGPlan(steps=[step("sink", ["a", "b"]), step("b"), step("a")])
    service = dag_app_factory(plan, {"a": root, "b": root, "sink": sink}).state.run_service
    task = asyncio.create_task(run_to_completion(service, "模拟并发根节点"))
    try:
        await asyncio.wait_for(asyncio.gather(*(e.wait() for e in started.values())), timeout=1)
        assert calls == {"a": 1, "b": 1} and not task.done()
        release.set()
        run = await asyncio.wait_for(task, timeout=1)
        assert run.status == "succeeded" and calls["sink"] == 1
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_device_then_ready_branches_then_diagnosis_uses_event_barriers(monkeypatch, planner, run_to_completion):
    device_started, release_device = asyncio.Event(), asyncio.Event()
    branch_started = {name: asyncio.Event() for name in ("warning", "traffic")}
    release_branches = asyncio.Event()
    finished = set()
    calls = Counter()

    async def device(args):
        calls["device"] += 1
        device_started.set()
        await release_device.wait()
        return await get_device(args)

    async def warning(args):
        calls["warning"] += 1
        branch_started["warning"].set()
        await release_branches.wait()
        result = await find_warning(args)
        finished.add("warning")
        return result

    async def traffic(args):
        calls["traffic"] += 1
        branch_started["traffic"].set()
        await release_branches.wait()
        result = await get_traffic(args)
        finished.add("traffic")
        return result

    async def diagnosis(args):
        calls["diagnosis"] += 1
        assert finished == {"warning", "traffic"}
        return await diagnose_node(args)

    for name, handler in [("get_device", device), ("find_warning", warning),
                          ("get_traffic", traffic), ("diagnose_node", diagnosis)]:
        monkeypatch.setattr(f"main.{name}", handler)
    service = create_app(planner).state.run_service
    task = asyncio.create_task(run_to_completion(service, DIAGNOSIS))
    try:
        await asyncio.wait_for(device_started.wait(), timeout=1)
        assert calls == {"device": 1}
        assert not any(e.is_set() for e in branch_started.values())
        release_device.set()
        await asyncio.wait_for(asyncio.gather(*(e.wait() for e in branch_started.values())), timeout=1)
        assert calls == {"device": 1, "warning": 1, "traffic": 1}
        assert calls["diagnosis"] == 0 and not task.done()
        release_branches.set()
        run = await asyncio.wait_for(task, timeout=1)
        assert run.status == "succeeded" and calls["diagnosis"] == 1
    finally:
        release_device.set()
        release_branches.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_content_bindings_are_deep_copies_and_leave_plan_unchanged(dag_app_factory, run_to_completion):
    mutated = asyncio.Event()
    payload = [{"nested": {"values": [7]}}]

    async def source(args):
        return success(payload)

    async def mutate(args):
        assert args.records == payload
        args.records[0]["nested"]["values"].append(8)
        mutated.set()
        return success(args.records)

    async def observe(args):
        await mutated.wait()
        assert args.records == [{"nested": {"values": [7]}}]
        return success(args.records)

    binding = ResultBinding(source_step_id="source", target_argument="records")
    plan = DAGPlan(steps=[step("source"), step("mutate", ["source"], [binding]),
                          step("observe", ["source"], [binding])])
    service = dag_app_factory(plan, {"source": source, "mutate": mutate,
                                     "observe": observe}).state.run_service
    run = await run_to_completion(service, "模拟绑定")
    assert run.status == "succeeded"
    assert run.tool_executions[0].result.content == [{"nested": {"values": [7]}}]
    assert run.tool_executions[1].tool_call.arguments["records"] == payload
    assert "records" not in plan.steps[1].tool_call.arguments


@pytest.mark.parametrize("failure, code, attempts", [
    ("business", "BUSINESS_ERROR", 1), ("temporary", "TEMPORARY_ERROR", 4),
    ("timeout", "TIMEOUT", 4), ("unknown", "UNKNOWN_TOOL", 0),
    ("invalid", "INVALID_ARGUMENTS", 0), ("bad_result", "INVALID_TOOL_RESULT", 1),
    ("returned_error", "TEMPORARY_ERROR", 1), ("unexpected", "TOOL_EXECUTION_ERROR", 1),
])
async def test_failed_upstream_blocks_descendants_but_preserves_other_branch(
    dag_app_factory, monkeypatch, client_factory, failure, code, attempts,
):
    calls, cleanups = Counter(), Counter()
    bad_finished = asyncio.Event()

    async def source(args):
        calls["source"] += 1
        return success()

    async def bad(args):
        calls["bad"] += 1
        if failure == "timeout":
            try:
                await asyncio.Event().wait()
            finally:
                cleanups["bad"] += 1
        if failure == "temporary":
            raise TemporaryToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
        if failure == "bad_result":
            return {"type": "invalid"}
        if failure == "returned_error":
            return ToolResult(type="error", metadata={"error_code": code}, content=[], indexes=[])
        if failure == "unexpected":
            raise RuntimeError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
        raise BusinessToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    async def good(args):
        calls["good"] += 1
        # 超时分支会耗费四次预算，健康 handler 不应等待超过自身的单次期限。
        # 其他故障场景用握手验证失败之后仍会收集成功分支。
        if failure != "timeout":
            await bad_finished.wait()
        return success([{"kept": True}])

    async def descendant(args):
        calls[args.tag] += 1
        return success()

    bad_step = step("bad", ["source"], arguments={"tag": 123} if failure == "invalid" else None,
                    name="missing" if failure == "unknown" else "bad")
    plan = DAGPlan(steps=[
        step("blocked", ["bad"], [ResultBinding(source_step_id="bad", target_argument="records")]),
        step("good_child", ["good"]), step("source"), bad_step, step("good", ["source"]),
        step("join", ["bad", "good"]),
    ])
    app = dag_app_factory(plan, {"source": source, "bad": bad, "good": good,
                                "blocked": descendant, "join": descendant, "good_child": descendant},
                          timeout=0.01)
    runtime = app.state.run_service.runtime
    execute = runtime.execute

    async def observed_execute(call):
        result = await execute(call)
        if call.id == "bad":
            bad_finished.set()
        return result
    monkeypatch.setattr(runtime, "execute", observed_execute)
    async with client_factory(app) as client:
        response = await asyncio.wait_for(client.post("/runs", json={"message": "模拟失败 DAG"}), timeout=1)
        run = await client.completed_run(response)
        records = {e["tool_call"]["id"]: e for e in run["tool_executions"]}
        assert run["status"] == "failed" and run["error"]["code"] == code
        assert records["bad"]["status"] == "failed"
        assert records["bad"]["error"]["code"] == code
        for name in ("blocked", "join"):
            assert records[name]["status"] == "blocked"
            assert records[name]["error"]["code"] == "DEPENDENCY_FAILED"
            assert records[name]["result"] is None and calls[name] == 0
        assert records["good"]["result"]["content"] == [{"kept": True}]
        assert records["good_child"]["status"] == "succeeded"
        assert calls["source"] == calls["good"] == calls["good_child"] == 1
        assert calls["bad"] == attempts
        assert cleanups["bad"] == (attempts if failure == "timeout" else 0)
        assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in str(run)
        assert (await client.get(response.headers["location"])).json() == run


async def test_bound_arguments_are_validated_by_runtime_before_handler(dag_app_factory, run_to_completion):
    calls = Counter()

    async def source(args):
        calls["source"] += 1
        return success([{"not": "a string"}])

    async def sink(args):
        calls["sink"] += 1
        return success()

    plan = DAGPlan(steps=[step("source"), step("sink", ["source"], [
        ResultBinding(source_step_id="source", target_argument="tag"),
    ], arguments={})])
    service = dag_app_factory(plan, {"source": source, "sink": sink}).state.run_service
    run = await run_to_completion(service, "模拟校验")
    assert run.status == "failed" and run.error.code == "INVALID_ARGUMENTS"
    assert calls == {"source": 1}
    assert run.tool_executions[1].tool_call.arguments["tag"] == [{"not": "a string"}]


@pytest.mark.parametrize("first_failure", ["temporary", "timeout"])
async def test_dag_retry_success_only_passes_final_success_downstream(dag_app_factory, run_to_completion, first_failure):
    calls = Counter()

    async def source(args):
        calls["source"] += 1
        if calls["source"] == 1:
            if first_failure == "timeout":
                await asyncio.Event().wait()
            raise TemporaryToolError("temporary")
        return success([{"attempt": calls["source"]}])

    async def sink(args):
        calls["sink"] += 1
        assert args.records == [{"attempt": 2}]
        return success(args.records)
    plan = DAGPlan(steps=[step("source"), step("sink", ["source"], [
        ResultBinding(source_step_id="source", target_argument="records"),
    ])])
    service = dag_app_factory(plan, {"source": source, "sink": sink}, timeout=0.01).state.run_service
    run = await run_to_completion(service, "模拟重试")
    assert run.status == "succeeded" and calls == {"source": 2, "sink": 1}


async def test_diagnosis_uses_modified_upstream_evidence_instead_of_fixed_samples(
    monkeypatch, planner, valid_arguments, run_to_completion,
):
    async def device(args):
        return success([{"device_id": "sentinel-device", "node_id": args.node_id,
                         "name": "模拟替代设备", "state": "DOWN", "description": "模拟"}])

    async def warning(args):
        return success([{"warning_id": f"sentinel-warning-{index}", "node_id": args.node_id,
                         "severe_level": "CRITICAL", "occurred_at": valid_arguments["begin_time"],
                         "description": "模拟替代告警"} for index in (1, 2)])

    async def traffic(args):
        return success([{"traffic_id": "sentinel-traffic", "node_id": args.node_id,
                         "begin_time": valid_arguments["begin_time"], "end_time": valid_arguments["end_time"],
                         "utilization_percent": 5.0, "traffic_level": "LOW", "description": "模拟替代流量"}])
    for name, handler in [("get_device", device), ("find_warning", warning), ("get_traffic", traffic)]:
        monkeypatch.setattr(f"main.{name}", handler)
    run = await run_to_completion(create_app(planner).state.run_service, DIAGNOSIS)
    assert run.status == "succeeded"
    result = run.tool_executions[-1].result.content[0]
    assert result["device_state"] == "DOWN"
    assert result["critical_warning_count"] == result["warning_count"] == 2
    assert result["max_utilization_percent"] == 5.0 and result["traffic_levels"] == ["LOW"]
    assert result["evidence"] == {
        "device_ids": ["sentinel-device"],
        "warning_ids": ["sentinel-warning-1", "sentinel-warning-2"], "traffic_ids": ["sentinel-traffic"],
    }
    assert "DOWN" in run.answer and "5.0%" in run.answer


@pytest.mark.parametrize("stage", ["root", "branches"])
@pytest.mark.parametrize("cancel_source", ["external", "child"])
async def test_dag_cancel_propagates_waits_for_cleanup_and_saves_future_steps(
    dag_app_factory, run_to_completion, stage, cancel_source,
):
    names = ("root", "left", "right", "sink")
    started = {name: asyncio.Event() for name in names}
    cleanup_started = {name: asyncio.Event() for name in names}
    locks = {name: asyncio.Lock() for name in names}
    cleanup_release, trigger = asyncio.Event(), asyncio.Event()
    calls, cleanups = Counter(), Counter()
    active = ("root",) if stage == "root" else ("left", "right")

    async def handler(args):
        name = args.tag
        calls[name] += 1
        async with locks[name]:
            started[name].set()
            try:
                if name in active:
                    if cancel_source == "child" and name == active[0]:
                        await trigger.wait()
                        raise asyncio.CancelledError()
                    await asyncio.Event().wait()
                return success()
            finally:
                if name in active:
                    cleanup_started[name].set()
                    await cleanup_release.wait()
                cleanups[name] += 1

    plan = DAGPlan(steps=[step("root"), step("left", ["root"]), step("right", ["root"]),
                          step("sink", ["left", "right"])])
    app = dag_app_factory(plan, {name: handler for name in names})
    service = app.state.run_service
    task = asyncio.create_task(run_to_completion(service, "模拟取消 DAG"))
    try:
        await asyncio.wait_for(asyncio.gather(*(started[name].wait() for name in active)), timeout=1)
        run_id, = service._runs
        if cancel_source == "external":
            task.cancel()
        else:
            trigger.set()
            # 子任务取消先完成自身清理，再由 gather 把取消通知编排层。
            await asyncio.wait_for(cleanup_started[active[0]].wait(), timeout=1)
            cleanup_release.set()
        await asyncio.wait_for(asyncio.gather(*(cleanup_started[name].wait() for name in active)), timeout=1)
        if cancel_source == "external":
            assert not task.done() and all(locks[name].locked() for name in active)
        cleanup_release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        run = service.get_run(run_id)
        assert run.status == "failed" and run.error.code == "CANCELLED"
        assert run.completed_at is not None
        records = {entry.tool_call.id: entry for entry in run.tool_executions}
        for name in names:
            if stage == "branches" and name == "root":
                assert records[name].status == "succeeded" and records[name].result.type == "success"
            else:
                assert records[name].status == "cancelled" and records[name].error.code == "CANCELLED"
            assert not locks[name].locked()
            assert calls[name] == cleanups[name]
        assert calls["sink"] == 0 and all(calls[name] == 1 for name in active)
        if stage == "root":
            assert calls["left"] == calls["right"] == 0
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get(f"/runs/{run_id}")).json() == run.model_dump(mode="json")
    finally:
        cleanup_release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
