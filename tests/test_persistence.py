import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from collections import Counter
from contextlib import AsyncExitStack, closing
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from main import create_app
from netops_agent.models import RunRecord
from netops_agent.repository import SQLiteRunRepository
from netops_agent.runtime import BusinessToolError
from netops_agent.tools import find_warning, get_device, get_traffic

pytestmark = pytest.mark.anyio
MESSAGES = ["查询 node_20 昨天的严重告警", "综合查询 node_20 昨天的告警和流量",
            "诊断 node_20 昨天的设备状态、告警和流量", "不支持的请求"]


@pytest.mark.parametrize("message", MESSAGES)
async def test_completed_run_survives_new_app_and_get_never_reexecutes(tmp_path, planner, client_factory, message):
    path = tmp_path / "runs.sqlite"
    app = create_app(planner, db_path=path)
    async with client_factory(app) as client:
        response = await client.post("/runs", json={"message": message})
        saved = await client.completed_run(response)
        assert saved["status"] == ("failed" if message == MESSAGES[-1] else "succeeded")
        connection = app.state.run_service.repository._connection
    assert app.state.run_service.repository._connection is None
    with pytest.raises(sqlite3.ProgrammingError):
        connection.execute("SELECT 1")

    class MustNotPlan:
        calls = 0

        async def plan(self, message, tools):
            self.calls += 1
            raise AssertionError("GET 不得重新执行 Planner")

    forbidden = MustNotPlan()
    restarted = create_app(forbidden, db_path=path)
    async with client_factory(restarted) as client:
        for _ in range(2):
            fetched = await client.get(response.headers["location"])
            assert fetched.status_code == 200 and fetched.json() == saved
        assert (await client.get(f"/runs/{uuid4()}")).status_code == 404
        assert forbidden.calls == 0 and restarted.state.run_service.queue.empty()
    assert restarted.state.run_service._worker_task.done()


@pytest.mark.parametrize("after_enqueue", [False, True])
async def test_committed_queued_survives_abrupt_process_exit_and_only_queued_recovers(
    tmp_path, planner, client_factory, after_enqueue,
):
    path = tmp_path / "crash.sqlite"
    script = """
import asyncio, json, os, sys
from main import create_app
from netops_agent.models import RunRecord
app = create_app(db_path=sys.argv[1])
service = app.state.run_service
service.repository.initialize()
if sys.argv[2] == 'True':
    queued = asyncio.run(service.create_run('查询 node_20 昨天的严重告警'))
else:
    queued = RunRecord(message='查询 node_20 昨天的严重告警')
    service.repository.save(queued)
running = RunRecord(message='不可擅自重跑', status='running')
service.repository.save(running)
print(json.dumps([str(queued.run_id), str(running.run_id)]), flush=True)
os._exit(0)
"""
    process = subprocess.run([sys.executable, "-B", "-c", script, str(path), str(after_enqueue)],
                             capture_output=True, text=True, encoding="utf-8", timeout=15, check=True)
    queued_id, running_id = json.loads(process.stdout)
    with closing(sqlite3.connect(path)) as observer:
        assert dict(observer.execute("SELECT run_id, status FROM runs")) == {
            queued_id: "queued", running_id: "running",
        }
    app = create_app(planner, db_path=path)
    async with client_factory(app) as client:
        await asyncio.wait_for(app.state.run_service.queue.join(), timeout=2)
        completed = (await client.get(f"/runs/{queued_id}")).json()
        stranded = (await client.get(f"/runs/{running_id}")).json()
        assert completed["status"] == "succeeded" and completed["completed_at"] is not None
        assert stranded["status"] == "running" and stranded["completed_at"] is None
        assert stranded["tool_call"] is None  # 不恢复崩溃时已 running 的记录。


@pytest.mark.parametrize("failed", [False, True])
async def test_status_is_committed_before_planner_and_no_transaction_spans_handler(
    tmp_path, planner, monkeypatch, client_factory, failed,
):
    path = tmp_path / "states.sqlite"
    planner_started, release = asyncio.Event(), asyncio.Event()
    calls = 0

    class PausedPlanner:
        async def plan(self, message, tools):
            with closing(sqlite3.connect(path)) as observer:
                assert observer.execute("SELECT status FROM runs").fetchone()[0] == "running"
            planner_started.set()
            await release.wait()
            return await planner.plan(message, tools)

    async def handler(args):
        nonlocal calls
        calls += 1
        assert not service.repository._connection.in_transaction
        # 第二个连接能写入，证明 Planner/handler 执行期间未持有写事务。
        observer = SQLiteRunRepository(path)
        observer.initialize()
        try:
            observer.save(RunRecord(message="独立写入探针", status="failed"))
        finally:
            observer.close()
        if failed:
            raise BusinessToolError("模拟失败")
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", handler)
    app = create_app(PausedPlanner(), db_path=path)
    service = app.state.run_service
    submitted = await service.create_run(MESSAGES[0])
    with closing(sqlite3.connect(path)) as observer:
        assert observer.execute("SELECT status FROM runs").fetchone()[0] == "queued"
    async with client_factory(app) as client:
        try:
            await asyncio.wait_for(planner_started.wait(), timeout=1)
            assert (await client.get(f"/runs/{submitted.run_id}")).json()["status"] == "running"
            release.set()
            await asyncio.wait_for(service.queue.join(), timeout=2)
            saved = service.get_run(submitted.run_id)
            assert saved.status == ("failed" if failed else "succeeded")
            assert saved.completed_at is not None and calls == 1
            if failed:
                assert saved.error.code == "BUSINESS_ERROR"
            with closing(sqlite3.connect(path)) as observer:
                row = observer.execute("SELECT status, record_json FROM runs WHERE run_id=?",
                                       (str(saved.run_id),)).fetchone()
                assert row[0] == saved.status
                assert RunRecord.model_validate_json(row[1]) == saved
        finally:
            release.set()


async def test_startup_recovery_deduplicates_ids_already_in_memory_queue(tmp_path, planner, monkeypatch):
    calls = Counter()

    async def handler(args):
        calls[args.node_id] += 1
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", handler)
    app = create_app(planner, db_path=tmp_path / "dedup.sqlite")
    service = app.state.run_service
    await service.create_run(MESSAGES[0])
    await service.create_run("查询 node_21 昨天的严重告警")
    assert service.queue.qsize() == 2
    async with app.router.lifespan_context(app):
        await service.start()
        assert service.queue.qsize() == 2
        await asyncio.wait_for(service.queue.join(), timeout=2)
        assert calls == {"node_20": 1, "node_21": 1}


async def test_graceful_shutdown_keeps_queued_for_next_instance(tmp_path, planner, monkeypatch, client_factory):
    path = tmp_path / "shutdown.sqlite"
    started = asyncio.Event()
    calls, cleanups = Counter(), Counter()

    async def handler(args):
        calls[args.node_id] += 1
        if args.node_id == "node_20":
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanups[args.node_id] += 1
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", handler)
    first = create_app(planner, db_path=path)
    async with client_factory(first) as client:
        active = await client.post("/runs", json={"message": MESSAGES[0]})
        await asyncio.wait_for(started.wait(), timeout=1)
        queued = await client.post("/runs", json={"message": "查询 node_21 昨天的严重告警"})
    service = first.state.run_service
    await asyncio.wait_for(service.queue.join(), timeout=1)
    assert service._worker_task.done() and service.repository._connection is None
    assert calls == cleanups == {"node_20": 1}
    observer = SQLiteRunRepository(path)
    observer.initialize()
    try:
        active_saved = observer.get(UUID(active.json()["run_id"]))
        queued_saved = observer.get(UUID(queued.json()["run_id"]))
        assert active_saved.status == "failed" and active_saved.error.code == "CANCELLED"
        assert active_saved.completed_at is not None
        assert queued_saved.status == "queued" and queued_saved.error is None
        assert queued_saved.completed_at is None and queued_saved.tool_call is None
    finally:
        observer.close()
    second = create_app(planner, db_path=path)
    async with client_factory(second) as client:
        await asyncio.wait_for(second.state.run_service.queue.join(), timeout=2)
        assert (await client.get(active.headers["location"])).json() == active_saved.model_dump(mode="json")
        assert (await client.get(queued.headers["location"])).json()["status"] == "succeeded"
        assert calls == {"node_20": 1, "node_21": 1}


async def test_get_reads_repository_authority_instead_of_stale_memory(tmp_path, planner, client_factory):
    path = tmp_path / "authority.sqlite"
    app = create_app(planner, db_path=path)
    async with client_factory(app) as client:
        submitted = await client.post("/runs", json={"message": MESSAGES[0]})
        final = await client.completed_run(submitted)
        writer = SQLiteRunRepository(path)
        writer.initialize()
        try:
            run = writer.get(UUID(final["run_id"]))
            run.answer = "模拟：由另一个 Repository 更新的权威快照"
            writer.save(run)
            assert (await client.get(submitted.headers["location"])).json()["answer"] == run.answer
        finally:
            writer.close()


@pytest.mark.parametrize("configured", [False, True])
async def test_import_is_lazy_and_create_app_defaults_to_isolated_memory(tmp_path, configured):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    env.pop("NETOPS_DB_PATH", None)
    target = tmp_path / "configured" / "runs.sqlite"
    if configured:
        env["NETOPS_DB_PATH"] = str(target)
    script = """
import json, main
from pathlib import Path
assert not Path('data').exists()
assert not main.app.state.run_service.repository._connection
assert main.create_app().state.run_service.repository.db_path == ':memory:'
print(json.dumps(main.app.state.run_service.repository.db_path))
"""
    process = subprocess.run([sys.executable, "-B", "-W", "error", "-c", script], cwd=tmp_path,
                             env=env, capture_output=True, text=True, encoding="utf-8", timeout=15, check=True)
    assert json.loads(process.stdout) == (str(target) if configured else "data/netops_agent.db")
    assert not target.exists() and not target.parent.exists() and not (tmp_path / "data").exists()


async def test_dag_progress_is_visible_from_another_repository(tmp_path, planner, monkeypatch, client_factory):
    path = tmp_path / "progress.sqlite"
    traffic_started, warning_finished, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def traffic(args):
        traffic_started.set()
        await release.wait()
        return await get_traffic(args)

    async def warning(args):
        result = await find_warning(args)
        warning_finished.set()
        return result

    monkeypatch.setattr("main.get_traffic", traffic)
    monkeypatch.setattr("main.find_warning", warning)
    app = create_app(planner, db_path=path)
    observer = SQLiteRunRepository(path)
    async with client_factory(app) as client:
        try:
            response = await client.post("/runs", json={"message": MESSAGES[2]})
            await asyncio.wait_for(traffic_started.wait(), timeout=1)
            await asyncio.wait_for(warning_finished.wait(), timeout=1)
            observer.initialize()
            running = observer.get(UUID(response.json()["run_id"]))
            assert running.status == "running" and running.completed_at is None
            entries = running.tool_executions
            assert entries[0].status == "succeeded" and entries[0].result.metadata["source"] == "mock"
            assert entries[2].status == "running" and entries[3].status == "pending"
            assert (await client.get(response.headers["location"])).json() == running.model_dump(mode="json")
            release.set()
            final = await client.completed_run(response)
            assert final["status"] == "succeeded" and len(final["tool_executions"]) == 4
        finally:
            release.set()
            observer.close()


@pytest.mark.parametrize("failure", ["before_running", "execute", "child_cancel"])
async def test_worker_outer_failure_is_persisted_and_next_run_executes(
    tmp_path, planner, monkeypatch, client_factory, failure,
):
    path = tmp_path / "outer.sqlite"
    app = create_app(planner, db_path=path)
    service = app.state.run_service
    execute, claim = service.execute_run, service.repository.claim
    triggered = False

    async def fail_execution(run_id):
        nonlocal triggered
        if not triggered:
            triggered = True
            if failure == "child_cancel":
                raise asyncio.CancelledError()
            raise RuntimeError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
        return await execute(run_id)

    def fail_claim(run_id):
        nonlocal triggered
        if not triggered:
            triggered = True
            raise sqlite3.OperationalError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
        return claim(run_id)

    if failure == "before_running":
        monkeypatch.setattr(service.repository, "claim", fail_claim)
    else:
        monkeypatch.setattr(service, "execute_run", fail_execution)
    async with client_factory(app) as client:
        first = await client.post("/runs", json={"message": MESSAGES[0]})
        final = await client.completed_run(first)
        code = "CANCELLED" if failure == "child_cancel" else "INTERNAL_ERROR"
        assert final["status"] == "failed" and final["error"]["code"] == code
        assert final["completed_at"] is not None and "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in str(final)
        assert final["tool_call"] is None  # 未进入 Planner/handler。
        next_response = await client.post("/runs", json={"message": MESSAGES[2]})
        assert (await client.completed_run(next_response))["status"] == "succeeded"
    observer = SQLiteRunRepository(path)
    observer.initialize()
    try:
        assert observer.get(UUID(final["run_id"])).model_dump(mode="json") == final
    finally:
        observer.close()


async def test_failed_queue_publication_recovers_committed_run(tmp_path, planner, monkeypatch, client_factory):
    app = create_app(planner, db_path=tmp_path / "publication.sqlite")
    service = app.state.run_service

    def failed_put(run_id):
        raise RuntimeError("模拟：数据库提交后、内存入队前中断")

    with monkeypatch.context() as patch:
        patch.setattr(service.queue, "put_nowait", failed_put)
        with pytest.raises(RuntimeError, match="中断"):
            await service.create_run(MESSAGES[0])
    queued, = service.repository.list_by_status("queued")
    assert service.queue.empty() and service._enqueued_ids == set()
    async with client_factory(app) as client:
        await asyncio.wait_for(service.queue.join(), timeout=2)
        assert (await client.get(f"/runs/{queued.run_id}")).json()["status"] == "succeeded"


async def test_lifespan_start_failure_closes_initialized_repository(tmp_path, planner, monkeypatch):
    app = create_app(planner, db_path=tmp_path / "startup.sqlite")
    service = app.state.run_service
    connections = []

    async def failed_start():
        connections.append(service.repository._connection)
        raise RuntimeError("模拟启动失败")

    monkeypatch.setattr(service, "start", failed_start)
    with pytest.raises(RuntimeError, match="启动失败"):
        async with app.router.lifespan_context(app):
            raise AssertionError("不能进入未完成启动的应用")
    assert service.repository._connection is None and service._worker_task is None
    with pytest.raises(sqlite3.ProgrammingError):
        connections[0].execute("SELECT 1")


async def test_progress_save_failure_cleans_parallel_children_and_worker_continues(
    tmp_path, planner, monkeypatch, client_factory,
):
    traffic_started, cleaned = asyncio.Event(), asyncio.Event()
    lock = asyncio.Lock()

    async def warning(args):
        if args.node_id == "node_20":
            await traffic_started.wait()
        return await find_warning(args)

    async def traffic(args):
        async with lock:
            traffic_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()

    monkeypatch.setattr("main.find_warning", warning)
    monkeypatch.setattr("main.get_traffic", traffic)
    app = create_app(planner, db_path=tmp_path / "save-failure.sqlite")
    service = app.state.run_service
    save = service.repository.save
    triggered = False

    def fail_once(run):
        nonlocal triggered
        if not triggered and run.tool_executions and run.tool_executions[0].status == "succeeded":
            triggered = True
            raise sqlite3.OperationalError("模拟保存步骤结果失败")
        save(run)

    monkeypatch.setattr(service.repository, "save", fail_once)
    async with client_factory(app) as client:
        response = await client.post("/runs", json={"message": MESSAGES[1]})
        failed = await client.completed_run(response)
        assert failed["status"] == "failed" and failed["error"]["code"] == "INTERNAL_ERROR"
        assert cleaned.is_set() and not lock.locked()
        assert [entry["status"] for entry in failed["tool_executions"]] == ["succeeded", "cancelled"]
        response = await client.post("/runs", json={"message": "查询 node_21 昨天的严重告警"})
        assert (await client.completed_run(response))["status"] == "succeeded"


async def test_default_memory_apps_are_isolated(planner, client_factory):
    first, second = create_app(planner), create_app(planner)
    async with client_factory(first) as writer, client_factory(second) as reader:
        response = await writer.post("/runs", json={"message": MESSAGES[0]})
        assert (await writer.completed_run(response))["status"] == "succeeded"
        assert (await reader.get(response.headers["location"])).status_code == 404


@pytest.mark.parametrize("message", MESSAGES[:3])
@pytest.mark.parametrize("persistent_failure", [False, True])
async def test_shutdown_cancellation_survives_failed_terminal_save(
    tmp_path, planner, monkeypatch, client_factory, message, persistent_failure,
):
    path = tmp_path / "cancel-save.sqlite"
    started, cleaned, reentered_get = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = Counter()

    async def handler(args):
        calls[args.node_id] += 1
        if args.node_id == "node_20":
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.set()
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", handler)
    app = create_app(planner, db_path=path)
    service = app.state.run_service
    save, get = service.repository.save, service.queue.get
    cancel_save_attempts, get_calls = 0, 0

    def fail_cancel_save(run):
        nonlocal cancel_save_attempts
        if run.error is not None and run.error.code == "CANCELLED":
            cancel_save_attempts += 1
            if persistent_failure or cancel_save_attempts == 1:
                raise sqlite3.OperationalError("模拟取消终态保存失败")
        save(run)

    async def observed_get():
        nonlocal get_calls
        get_calls += 1
        if get_calls > 1:
            reentered_get.set()
        return await get()

    monkeypatch.setattr(service.repository, "save", fail_cancel_save)
    monkeypatch.setattr(service.queue, "get", observed_get)
    lifetime = client_factory(app)
    client = await lifetime.__aenter__()
    shutdown = reentered = None
    try:
        active = await client.post("/runs", json={"message": message})
        await asyncio.wait_for(started.wait(), timeout=1)
        queued = await client.post("/runs", json={"message": "查询 node_21 昨天的严重告警"})
        connection = service.repository._connection
        shutdown = asyncio.create_task(lifetime.__aexit__(None, None, None))
        reentered = asyncio.create_task(reentered_get.wait())
        done, _ = await asyncio.wait({shutdown, reentered}, timeout=2, return_when=asyncio.FIRST_COMPLETED)
        assert shutdown in done, "取消保存失败后 Worker 不得重新消费队列或阻止关闭"
        await shutdown
        assert cleaned.is_set() and calls == {"node_20": 1}
        assert not reentered_get.is_set() and service._worker_task.cancelled()
        assert service.repository._connection is None and service._enqueued_ids == set()
        await asyncio.wait_for(service.queue.join(), timeout=1)
        with pytest.raises(sqlite3.ProgrammingError):
            connection.execute("SELECT 1")
        assert cancel_save_attempts == 2  # 执行层保存及 Worker 外层兜底保存。
        observer = SQLiteRunRepository(path)
        try:
            saved = observer.get(UUID(active.json()["run_id"]))
            if persistent_failure:
                # 存储持续故障时只保留最后成功写入的状态，不能伪造持久化终态。
                assert saved.status == "running" and saved.completed_at is None
            else:
                assert saved.status == "failed" and saved.error.code == "CANCELLED"
                assert saved.completed_at is not None
            pending = observer.get(UUID(queued.json()["run_id"]))
            assert pending.status == "queued" and pending.completed_at is None
            assert pending.error is None and pending.tool_call is None
        finally:
            observer.close()
    finally:
        # 即使旧实现回归，也能结束失败测试，不让 pytest 遗留后台任务。
        monkeypatch.setattr(service.repository, "save", save)
        if reentered is not None:
            reentered.cancel()
            await asyncio.gather(reentered, return_exceptions=True)
        if service._worker_task is not None and not service._worker_task.done():
            service._worker_task.cancel()
        if shutdown is None:
            await lifetime.__aexit__(None, None, None)
        else:
            await asyncio.wait_for(shutdown, timeout=1)


async def test_child_cancellation_keeps_cancelled_code_when_terminal_save_fails(
    tmp_path, planner, monkeypatch, client_factory,
):
    calls = Counter()

    async def handler(args):
        calls[args.node_id] += 1
        if args.node_id == "node_20":
            raise asyncio.CancelledError()
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", handler)
    app = create_app(planner, db_path=tmp_path / "child-cancel-save.sqlite")
    service = app.state.run_service
    save = service.repository.save
    failures = 0

    def fail_once(run):
        nonlocal failures
        if failures == 0 and run.error is not None and run.error.code == "CANCELLED":
            failures += 1
            raise sqlite3.OperationalError("模拟取消终态保存失败")
        save(run)

    monkeypatch.setattr(service.repository, "save", fail_once)
    async with client_factory(app) as client:
        worker = service._worker_task
        response = await client.post("/runs", json={"message": MESSAGES[0]})
        saved = await client.completed_run(response)
        assert saved["status"] == "failed" and saved["error"]["code"] == "CANCELLED"
        assert saved["completed_at"] is not None and failures == 1
        response = await client.post("/runs", json={"message": "查询 node_21 昨天的严重告警"})
        assert (await client.completed_run(response))["status"] == "succeeded"
        assert service._worker_task is worker and not worker.done()
        assert calls == {"node_20": 1, "node_21": 1}


async def test_terminal_save_failure_without_cancellation_is_not_suppressed(
    tmp_path, planner, monkeypatch, client_factory,
):
    app = create_app(planner, db_path=tmp_path / "terminal-save.sqlite")
    service = app.state.run_service
    save = service.repository.save
    failures = 0

    def fail_once(run):
        nonlocal failures
        if failures == 0 and run.status == "succeeded":
            failures += 1
            raise sqlite3.OperationalError("模拟正常终态保存失败")
        save(run)

    monkeypatch.setattr(service.repository, "save", fail_once)
    async with client_factory(app) as client:
        response = await client.post("/runs", json={"message": MESSAGES[0]})
        saved = await client.completed_run(response)
        assert saved["status"] == "failed" and saved["error"]["code"] == "INTERNAL_ERROR"
        assert saved["completed_at"] is not None and failures == 1
        response = await client.post("/runs", json={"message": MESSAGES[0]})
        assert (await client.completed_run(response))["status"] == "succeeded"


@pytest.mark.parametrize("message, execution_count", [(MESSAGES[0], 1), (MESSAGES[1], 2), (MESSAGES[2], 4)])
async def test_two_services_recover_same_run_but_only_claim_winner_executes(
    tmp_path, planner, monkeypatch, client_factory, message, execution_count,
):
    path = tmp_path / "shared-claim.sqlite"
    run = RunRecord(message=message)
    publisher = SQLiteRunRepository(path)
    try:
        publisher.save(run)
    finally:
        publisher.close()
    permit_claims, planner_started, permit_planner = asyncio.Event(), asyncio.Event(), asyncio.Event()
    restored = {label: asyncio.Event() for label in ("first", "second")}
    planner_calls, runtime_calls, handler_calls = Counter(), Counter(), Counter()
    claim_outcomes = {label: [] for label in restored}

    class PausedPlanner:
        def __init__(self, label):
            self.label = label

        async def plan(self, message, tools):
            planner_calls[self.label] += 1
            planner_started.set()
            await permit_planner.wait()
            return await planner.plan(message, tools)

    def build(label):
        async def handler(args):
            handler_calls[label] += 1
            return await find_warning(args)

        monkeypatch.setattr("main.find_warning", handler)
        app = create_app(PausedPlanner(label), db_path=path)
        service = app.state.run_service
        claim, execute_run, execute_tool = service.repository.claim, service.execute_run, service.runtime.execute

        def observed_claim(run_id):
            claimed = claim(run_id)
            claim_outcomes[label].append(claimed is not None)
            return claimed

        async def gated_execution(run_id):
            assert run_id == run.run_id
            restored[label].set()
            await permit_claims.wait()
            return await execute_run(run_id)

        async def observed_runtime(call):
            runtime_calls[label] += 1
            return await execute_tool(call)

        monkeypatch.setattr(service.repository, "claim", observed_claim)
        monkeypatch.setattr(service, "execute_run", gated_execution)
        monkeypatch.setattr(service.runtime, "execute", observed_runtime)
        return app

    apps = {label: build(label) for label in restored}
    services = {label: app.state.run_service for label, app in apps.items()}
    async with AsyncExitStack() as stack:
        clients = {label: await stack.enter_async_context(client_factory(app)) for label, app in apps.items()}
        try:
            # 两个 Worker 都从启动恢复队列取得同一个 ID，再一起放行 Claim。
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in restored.values())), timeout=2)
            assert all(service._enqueued_ids == {run.run_id} for service in services.values())
            assert services["first"].get_run(run.run_id).status == "queued"
            permit_claims.set()
            await asyncio.wait_for(planner_started.wait(), timeout=1)
            assert sum(planner_calls.values()) == 1
            winner = next(iter(planner_calls))
            loser = next(label for label in services if label != winner)
            losing_service = services[loser]
            await asyncio.wait_for(losing_service.queue.join(), timeout=1)
            assert claim_outcomes == {winner: [True], loser: [False]}
            assert losing_service._enqueued_ids == set() and not losing_service._worker_task.done()
            assert runtime_calls == handler_calls == {}
            for client in clients.values():
                running = await client.get(f"/runs/{run.run_id}")
                assert running.status_code == 200 and running.json()["status"] == "running"

            # 模拟再次收到遗留队列项：running 不能重领，不能改变原任务状态。
            losing_service._enqueue(run.run_id)
            await asyncio.wait_for(losing_service.queue.join(), timeout=1)
            assert claim_outcomes[loser] == [False, False]
            assert losing_service.get_run(run.run_id).status == "running"
            assert sum(planner_calls.values()) == 1 and runtime_calls == handler_calls == {}

            permit_planner.set()
            await asyncio.wait_for(asyncio.gather(*(service.queue.join() for service in services.values())), timeout=2)
            responses = [await client.get(f"/runs/{run.run_id}") for client in clients.values()]
            assert all(response.status_code == 200 for response in responses)
            saved = responses[0].json()
            assert responses[1].json() == saved
            assert saved["status"] == "succeeded" and saved["completed_at"] is not None
            assert saved["error"] is None and saved["answer"].startswith("【模拟结果】")
            if execution_count == 1:
                assert saved["result"]["type"] == "success"
            else:
                assert len(saved["tool_executions"]) == execution_count
                assert all(entry["status"] == "succeeded" and entry["result"]["type"] == "success"
                           for entry in saved["tool_executions"])

            # succeeded 的遗留队列项同样不执行，完整结果不得被覆盖。
            losing_service._enqueue(run.run_id)
            await asyncio.wait_for(losing_service.queue.join(), timeout=1)
            assert claim_outcomes == {winner: [True], loser: [False, False, False]}
            assert planner_calls == {winner: 1} and handler_calls == {winner: 1}
            assert runtime_calls == {winner: execution_count}
            assert all(service.get_run(run.run_id).model_dump(mode="json") == saved for service in services.values())
            with closing(sqlite3.connect(path)) as observer:
                rows = observer.execute("SELECT status, record_json FROM runs").fetchall()
                assert len(rows) == 1 and rows[0][0] == "succeeded"
                assert RunRecord.model_validate_json(rows[0][1]).model_dump(mode="json") == saved
            assert all(service._enqueued_ids == set() for service in services.values())
        finally:
            permit_claims.set()
            permit_planner.set()
    for service in services.values():
        assert service._worker_task.done() and service.repository._connection is None
        assert service.queue.empty() and service._enqueued_ids == set()
        await asyncio.wait_for(service.queue.join(), timeout=1)


@pytest.mark.parametrize("status", ["running", "succeeded", "failed", None])
async def test_execute_run_without_claim_returns_snapshot_or_missing_without_execution(
    tmp_path, monkeypatch, client_factory, status,
):
    calls = Counter()

    class ForbiddenPlanner:
        async def plan(self, message, tools):
            calls["planner"] += 1
            raise AssertionError("未领取的任务不得执行 Planner")

    async def forbidden_runtime(call):
        calls["runtime"] += 1
        raise AssertionError("未领取的任务不得执行 Runtime 或 handler")

    app = create_app(ForbiddenPlanner(), db_path=tmp_path / "not-claimed.sqlite")
    service = app.state.run_service
    monkeypatch.setattr(service.runtime, "execute", forbidden_runtime)
    claim = service.repository.claim

    def observed_claim(run_id):
        calls["claim"] += 1
        return claim(run_id)

    monkeypatch.setattr(service.repository, "claim", observed_claim)
    run = RunRecord(message="模拟不可领取", status=status or "queued", answer="保留模拟结果")
    if status in ("succeeded", "failed"):
        run.completed_at = run.created_at
    async with client_factory(app) as client:
        if status is None:
            with pytest.raises(KeyError):
                await service.execute_run(run.run_id)
            assert (await client.get(f"/runs/{run.run_id}")).status_code == 404
        else:
            service.repository.save(run)
            snapshot = await service.execute_run(run.run_id)
            assert snapshot == run
            snapshot.answer = "修改快照不得写回数据库"
            assert service.get_run(run.run_id) == run
        assert calls == {"claim": 1}
        assert service._enqueued_ids == set()
        await asyncio.wait_for(service.queue.join(), timeout=1)
