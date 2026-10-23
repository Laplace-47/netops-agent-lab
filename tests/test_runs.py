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
    assert response.status_code == 202
    run = await client.completed_run(response)
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
    run = await client.completed_run(response)
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
    assert response.status_code == 202
    run = await client.completed_run(response)
    assert run["status"] == "failed"
    assert run["error"]["code"] == "UNSUPPORTED_REQUEST"
    assert run["tool_call"] is None
    assert run["result"] is None
    assert (await client.get(response.headers["location"])).json() == run


async def test_tool_validation_failure_is_saved_in_run(valid_arguments, client_factory):
    class InvalidPlanner:
        async def plan(self, message, tools):
            return ToolCall(name="find_warning", arguments={
                **valid_arguments, "severe_level": "URGENT",
            })

    async with client_factory(create_app(planner=InvalidPlanner())) as client:
        response = await client.post("/runs", json={"message": "模拟错误参数"})
        run = await client.completed_run(response)
        assert run["status"] == "failed"
        assert run["result"]["type"] == "error"
        assert run["error"]["code"] == "INVALID_ARGUMENTS"
        assert (await client.get(response.headers["location"])).json() == run


async def test_openapi_contains_both_run_endpoints(client):
    schema = (await client.get("/openapi.json")).json()
    assert "post" in schema["paths"]["/runs"]
    assert "get" in schema["paths"]["/runs/{run_id}"]


async def test_planner_exception_is_saved_without_internal_details(client_factory):
    class BrokenPlanner:
        async def plan(self, message, tools):
            raise RuntimeError("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    async with client_factory(create_app(planner=BrokenPlanner())) as client:
        response = await client.post("/runs", json={"message": "模拟 Planner 失败"})
        run = await client.completed_run(response)
        assert run["status"] == "failed"
        assert run["error"]["code"] == "INTERNAL_ERROR"
        assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in str(run)
        assert (await client.get(response.headers["location"])).json() == run


async def test_returned_run_snapshot_cannot_modify_saved_result(planner, registry, run_to_completion):
    service = RunService(planner, ToolRuntime(registry))
    run = await run_to_completion(service, "查询 node_20 昨天的严重告警")
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
async def test_runtime_error_is_saved_and_retrievable(app_with_handler, client_factory, error_code, expected_calls):
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
    async with client_factory(app) as client:
        response = await client.post("/runs", json={"message": "查询node_20昨天的严重告警"})
        assert response.status_code == 202
        run = await client.completed_run(response)
        assert run["status"] == "failed"
        assert run["error"]["code"] == error_code
        assert run["result"]["type"] == "error"
        assert run["result"]["metadata"]["error_code"] == error_code
        assert run["result"]["metadata"]["message"] == run["error"]["message"]
        assert run["result"]["metadata"]["tool_call_id"] == run["tool_call"]["id"]
        assert run["result"]["content"] == [] and run["result"]["indexes"] == []
        assert run["completed_at"] is not None
        assert datetime.fromisoformat(run["completed_at"]) >= datetime.fromisoformat(run["created_at"])
        assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in str(run)
        assert calls == expected_calls
        if error_code == "TIMEOUT":
            assert cleanups == calls

        fetched = await client.get(response.headers["location"])
        assert fetched.status_code == 200
        assert fetched.json() == run
        assert calls == expected_calls  # GET 只读保存结果，不再次执行 handler。


@pytest.mark.parametrize("first_failure", ["timeout", "temporary"])
async def test_retry_success_is_saved_and_retrievable(app_with_handler, client_factory, first_failure):
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
    async with client_factory(app) as client:
        response = await client.post("/runs", json={"message": "查询node_20昨天的严重告警"})
        assert response.status_code == 202
        run = await client.completed_run(response)
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


async def test_cancelled_run_is_saved_and_retrievable(app_with_handler, run_to_completion):
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
        task = asyncio.create_task(run_to_completion(service, "查询node_20昨天的严重告警"))
        try:
            await asyncio.wait_for(started.wait(), timeout=1)
            # Repository 是权威存储，借助状态查询观测本次运行 ID。
            running_record, = service.repository.list_by_status("running")
            run_id = running_record.run_id
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


@pytest.mark.parametrize("fails", [False, True])
async def test_post_queues_slow_run_and_worker_saves_final_snapshot(monkeypatch, planner, fails):
    started, release, finished = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        try:
            if fails:
                raise BusinessToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
            return await find_warning(args)
        finally:
            finished.set()

    monkeypatch.setattr("main.find_warning", handler)
    app = create_app(planner)
    service = app.state.run_service
    async with app.router.lifespan_context(app), AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test",
    ) as client:
        try:
            response = await asyncio.wait_for(client.post(
                "/runs", json={"message": "查询 node_20 昨天的严重告警"},
            ), timeout=1)
            submitted = response.json()
            assert response.status_code == 202 and submitted["status"] == "queued"
            assert submitted["completed_at"] is None and submitted["result"] is None
            assert not finished.is_set()
            location = response.headers["location"]
            assert location == f"/runs/{submitted['run_id']}"
            await asyncio.wait_for(started.wait(), timeout=1)
            running = (await client.get(location)).json()
            assert running["status"] == "running" and running["completed_at"] is None
            assert not finished.is_set() and calls == 1
            release.set()
            await asyncio.wait_for(service.queue.join(), timeout=1)
            final = (await client.get(location)).json()
            assert final["status"] == ("failed" if fails else "succeeded")
            assert final["completed_at"] is not None
            assert final["result"]["type"] == ("error" if fails else "success")
            if fails:
                assert final["error"]["code"] == "BUSINESS_ERROR"
            else:
                assert final["error"] is None
            assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in str(final)
            assert final["run_id"] == submitted["run_id"] and calls == 1
            assert submitted["status"] == "queued"  # POST 快照不随后台执行变动。
            assert (await client.get(location)).json() == final
        finally:
            release.set()


async def test_submission_does_not_call_planner_and_returns_independent_queued_snapshot(planner, registry):
    class ObservedPlanner:
        calls = 0

        async def plan(self, message, tools):
            self.calls += 1
            return await planner.plan(message, tools)

    observed = ObservedPlanner()
    service = RunService(observed, ToolRuntime(registry))
    submitted = await service.create_run("查询 node_20 昨天的严重告警")
    assert submitted.status == "queued" and observed.calls == 0
    assert service.queue.qsize() == 1
    submitted.message = "不应写回"
    assert service.get_run(submitted.run_id).message == "查询 node_20 昨天的严重告警"
    await service.start()
    try:
        await asyncio.wait_for(service.queue.join(), timeout=1)
        assert observed.calls == 1
        assert service.get_run(submitted.run_id).status == "succeeded"
    finally:
        await service.stop()
        service.repository.close()


async def test_worker_contains_unexpected_run_exception_and_keeps_consuming(monkeypatch, planner):
    app = create_app(planner)
    service = app.state.run_service
    execute = service.execute_run
    calls = 0

    async def broken_once(run_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
        return await execute(run_id)

    monkeypatch.setattr(service, "execute_run", broken_once)
    async with app.router.lifespan_context(app), AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test",
    ) as client:
        responses = [await client.post("/runs", json={"message": message}) for message in (
            "查询 node_20 昨天的严重告警", "综合查询 node_20 昨天的告警和流量",
            "诊断 node_20 昨天的设备状态、告警和流量",
        )]
        assert all(r.status_code == 202 and r.json()["status"] == "queued" for r in responses)
        await asyncio.wait_for(service.queue.join(), timeout=1)
        runs = [(await client.get(r.headers["location"])).json() for r in responses]
        assert runs[0]["status"] == "failed" and runs[0]["error"]["code"] == "INTERNAL_ERROR"
        assert runs[0]["completed_at"] is not None
        assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in str(runs[0])
        assert runs[1]["status"] == runs[2]["status"] == "succeeded"
        assert len(runs[1]["tool_executions"]) == 2 and len(runs[2]["tool_executions"]) == 4
        assert calls == 3 and not service._worker_task.done()


async def test_single_worker_preserves_fifo_and_get_does_not_execute(monkeypatch, planner):
    started, release = asyncio.Event(), asyncio.Event()
    nodes = []

    async def handler(args):
        nodes.append(args.node_id)
        if args.node_id == "node_20":
            started.set()
            await release.wait()
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", handler)
    app = create_app(planner)
    service = app.state.run_service
    async with app.router.lifespan_context(app), AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test",
    ) as client:
        worker = service._worker_task
        await service.start()  # 重复启动不会创建第二个消费者。
        assert service._worker_task is worker
        try:
            first = await client.post("/runs", json={"message": "查询 node_20 昨天的严重告警"})
            await asyncio.wait_for(started.wait(), timeout=1)
            second = await client.post("/runs", json={"message": "查询 node_21 昨天的严重告警"})
            for _ in range(3):
                queued = (await client.get(second.headers["location"])).json()
                assert queued["status"] == "queued" and queued["tool_call"] is None
            assert nodes == ["node_20"] and service.queue.qsize() == 1
            release.set()
            await asyncio.wait_for(service.queue.join(), timeout=1)
            for response in (first, second):
                assert (await client.get(response.headers["location"])).json()["status"] == "succeeded"
            assert nodes == ["node_20", "node_21"] and service._worker_task is worker
        finally:
            release.set()


@pytest.mark.parametrize("message", [
    "查询 node_20 昨天的严重告警", "综合查询 node_20 昨天的告警和流量",
    "诊断 node_20 昨天的设备状态、告警和流量",
])
async def test_lifespan_shutdown_cleans_active_and_queued_runs(monkeypatch, planner, tmp_path, message):
    started, cleanup_started, release_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = 0
    lock = asyncio.Lock()

    async def handler(args):
        nonlocal calls
        calls += 1
        async with lock:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()
                await release_cleanup.wait()

    monkeypatch.setattr("main.find_warning", handler)
    app = create_app(planner, db_path=tmp_path / "shutdown.sqlite")
    service = app.state.run_service
    lifetime = app.router.lifespan_context(app)
    await lifetime.__aenter__()
    worker = service._worker_task
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        active = await client.post("/runs", json={"message": message})
        await asyncio.wait_for(started.wait(), timeout=1)
        queued = await client.post("/runs", json={"message": message})
        shutdown = asyncio.create_task(lifetime.__aexit__(None, None, None))
        try:
            await asyncio.wait_for(cleanup_started.wait(), timeout=1)
            assert lock.locked() and not shutdown.done() and not worker.done()
            release_cleanup.set()
            await asyncio.wait_for(shutdown, timeout=1)
            await asyncio.wait_for(service.queue.join(), timeout=1)
            assert service.repository._connection is None
            # 仅重开连接观测关闭后的文件，不重启 Worker 或恢复队列。
            service.repository.initialize()
            run = (await client.get(active.headers["location"])).json()
            assert run["status"] == "failed" and run["error"]["code"] == "CANCELLED"
            assert run["completed_at"] is not None
            queued_run = (await client.get(queued.headers["location"])).json()
            assert queued_run["status"] == "queued" and queued_run["error"] is None
            assert queued_run["completed_at"] is None
            assert queued_run["plan"] is None and queued_run["tool_call"] is None
            assert calls == 1 and not lock.locked() and worker.done()
            assert service.queue.empty()
            assert not [task for task in asyncio.all_tasks() if task.get_name() == "netops-run-worker"]
        finally:
            release_cleanup.set()
            await asyncio.gather(shutdown, return_exceptions=True)
            service.repository.close()


@pytest.mark.parametrize("wait_for_get", [False, True])
async def test_idle_worker_shutdown_balances_queue_and_leaves_no_task(monkeypatch, planner, wait_for_get):
    app = create_app(planner)
    service = app.state.run_service
    get_started = asyncio.Event()
    get = service.queue.get

    async def observed_get():
        get_started.set()
        return await get()

    monkeypatch.setattr(service.queue, "get", observed_get)
    async with app.router.lifespan_context(app):
        worker = service._worker_task
        assert worker is not None and not worker.done()
        if wait_for_get:
            await asyncio.wait_for(get_started.wait(), timeout=1)
    await asyncio.wait_for(service.queue.join(), timeout=1)
    assert worker.done() and service.queue.empty()


@pytest.mark.parametrize("message", [
    "查询 node_20 昨天的严重告警", "综合查询 node_20 昨天的告警和流量",
    "诊断 node_20 昨天的设备状态、告警和流量",
])
async def test_child_cancellation_fails_only_current_run_and_worker_continues(
    monkeypatch, planner, client_factory, message,
):
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise asyncio.CancelledError()
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", handler)
    app = create_app(planner)
    async with client_factory(app) as client:
        response = await client.post("/runs", json={"message": message})
        cancelled = await client.completed_run(response)
        assert cancelled["status"] == "failed" and cancelled["error"]["code"] == "CANCELLED"
        assert cancelled["completed_at"] is not None and calls == 1
        worker = app.state.run_service._worker_task
        assert not worker.done() and worker.cancelling() == 0
        next_response = await client.post("/runs", json={"message": message})
        succeeded = await client.completed_run(next_response)
        assert succeeded["status"] == "succeeded" and succeeded["error"] is None
        assert calls == 2 and app.state.run_service._worker_task is worker


async def test_cancelling_shutdown_waits_for_cleanup_then_propagates(monkeypatch, planner, tmp_path):
    started, cleanup_started, release_cleanup = asyncio.Event(), asyncio.Event(), asyncio.Event()
    cleaned = asyncio.Event()

    async def handler(args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await release_cleanup.wait()
            cleaned.set()

    monkeypatch.setattr("main.find_warning", handler)
    app = create_app(planner, db_path=tmp_path / "cancel-shutdown.sqlite")
    service = app.state.run_service
    lifetime = app.router.lifespan_context(app)
    await lifetime.__aenter__()
    active = await service.create_run("查询 node_20 昨天的严重告警")
    await asyncio.wait_for(started.wait(), timeout=1)
    queued = await service.create_run("查询 node_21 昨天的严重告警")
    shutdown = asyncio.create_task(lifetime.__aexit__(None, None, None))
    try:
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        shutdown.cancel()
        cancellation_turn = asyncio.Event()
        asyncio.get_running_loop().call_soon(cancellation_turn.set)
        await cancellation_turn.wait()
        assert not shutdown.done() and not cleaned.is_set() and not service._worker_task.done()
        release_cleanup.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(shutdown, timeout=1)
        assert cleaned.is_set() and service._worker_task.done()
        await asyncio.wait_for(service.queue.join(), timeout=1)
        assert service.repository._connection is None
        service.repository.initialize()
        saved = service.get_run(active.run_id)
        assert saved.status == "failed" and saved.error.code == "CANCELLED"
        assert saved.completed_at is not None
        queued_saved = service.get_run(queued.run_id)
        assert queued_saved.status == "queued" and queued_saved.error is None
        assert queued_saved.completed_at is None
    finally:
        release_cleanup.set()
        await asyncio.gather(shutdown, return_exceptions=True)
        service.repository.close()


async def test_shutdown_rejects_new_submissions_without_creating_run(planner, tmp_path):
    app = create_app(planner, db_path=tmp_path / "reject.sqlite")
    service = app.state.run_service
    async with app.router.lifespan_context(app):
        pass
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/runs", json={"message": "查询 node_20 昨天的严重告警"})
        assert response.status_code == 503
        service.repository.initialize()
        try:
            assert all(service.repository.list_by_status(status) == []
                       for status in ("queued", "running", "succeeded", "failed"))
            assert service.queue.empty()
        finally:
            service.repository.close()
    await asyncio.wait_for(service.queue.join(), timeout=1)
