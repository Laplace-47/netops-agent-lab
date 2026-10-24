import asyncio
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import AsyncExitStack, closing
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from main import create_app
from netops_agent.models import RunError, RunRecord, ToolCall, ToolResult
from netops_agent.repository import LeaseLostError, RunClaim, SQLiteRunRepository
from netops_agent.tools import find_warning


class Clock:
    def __init__(self):
        self.value = datetime(2026, 10, 24, tzinfo=timezone.utc)

    def now(self):
        return self.value

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


@pytest.fixture
def lease_store(tmp_path):
    clock = Clock()
    repository = SQLiteRunRepository(tmp_path / "leases.sqlite", clock=clock.now)
    try:
        yield repository, clock
    finally:
        repository.close()


def row(repository, run_id):
    with closing(sqlite3.connect(repository.db_path)) as connection:
        return connection.execute("SELECT * FROM runs WHERE run_id=?", (str(run_id),)).fetchone()


def test_migration_preserves_legacy_records_and_is_idempotent(tmp_path):
    path = tmp_path / "old.sqlite"
    records = [RunRecord(message="旧 queued"), RunRecord(message="旧 running", status="running")]
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("""CREATE TABLE runs (
            run_id TEXT PRIMARY KEY, status TEXT NOT NULL, created_at TEXT NOT NULL,
            completed_at TEXT, record_json TEXT NOT NULL)""")
        for run in records:
            connection.execute("INSERT INTO runs VALUES (?,?,?,?,?)",
                               (str(run.run_id), run.status, run.created_at.isoformat(), None, run.model_dump_json()))
    repository = SQLiteRunRepository(path)
    try:
        for _ in range(2):
            repository.initialize()
            assert [repository.get(run.run_id) for run in records] == records
            assert [r.run_id for r in repository.list_reclaimable()] == [records[0].run_id]
            assert repository.claim(records[1].run_id, "new-owner", 30) is None
            with closing(sqlite3.connect(path)) as observer:
                columns = [r[1] for r in observer.execute("PRAGMA table_info(runs)")]
                assert len(columns) == len(set(columns)) == 8
                assert {"lease_owner", "lease_until", "fencing_token"} <= set(columns)
                assert observer.execute("SELECT fencing_token FROM runs").fetchall() == [(0,), (0,)]
                assert any("lease" in r[1] for r in observer.execute("PRAGMA index_list(runs)"))
            repository.close()
    finally:
        repository.close()


def test_first_claim_has_internal_utc_lease_and_immutable_identity(lease_store):
    repository, clock = lease_store
    run = RunRecord(message="模拟首次租约")
    repository.save(run)
    claim = repository.claim(run.run_id, "worker-a", 30)
    assert isinstance(claim, RunClaim) and claim.fencing_token == 1 and claim.owner_id == "worker-a"
    assert claim.run.status == "running" and claim.run.completed_at is None
    assert claim.lease_until == clock.now() + timedelta(seconds=30)
    with pytest.raises(FrozenInstanceError):
        claim.fencing_token = 100
    saved = row(repository, run.run_id)
    assert saved[5:] == ("worker-a", claim.lease_until.isoformat(timespec="microseconds"), 1)
    assert RunRecord.model_validate_json(saved[4]) == claim.run
    assert not {"lease_owner", "lease_until", "fencing_token"} & claim.run.model_dump().keys()
    assert not repository._connection.in_transaction


def test_expired_takeover_resets_partial_state_and_fences_every_old_write(lease_store):
    repository, clock = lease_store
    run = RunRecord(message="模拟接管")
    repository.save(run)
    first = repository.claim(run.run_id, "a", 3)
    first.run.tool_call = ToolCall(name="old", arguments={})
    first.run.result = ToolResult(type="success", metadata={"source": "mock"}, content=[{"old": True}], indexes=[])
    first.run.answer = "旧中间结果"
    first.run.error = RunError(code="OLD", message="旧错误")
    repository.save_claimed(first.run, first.owner_id, first.fencing_token)
    assert repository.claim(run.run_id, "b", 3) is None
    assert not repository.renew_lease(run.run_id, "b", 1, 3)
    clock.advance(3)
    assert not repository.renew_lease(run.run_id, "a", 1, 3)
    second = repository.claim(run.run_id, "b", 3)
    assert second.fencing_token == 2 and second.run.created_at == run.created_at
    assert second.run.tool_call is second.run.result is second.run.plan is None
    assert second.run.answer is second.run.error is second.run.completed_at is None
    assert second.run.tool_executions == []
    expected = row(repository, run.run_id)
    for status in ("running", "succeeded", "failed"):
        first.run.status = status
        first.run.error = RunError(code="STALE", message="旧执行者不得覆盖")
        with pytest.raises(LeaseLostError):
            repository.save_claimed(first.run, "a", 1)
        assert row(repository, run.run_id) == expected
        assert not repository._connection.in_transaction
    assert not repository.renew_lease(run.run_id, "a", 1, 3)
    assert not repository.renew_lease(run.run_id, "b", 1, 3)


def test_expired_owner_cannot_write_even_before_takeover(lease_store):
    repository, clock = lease_store
    run = RunRecord(message="模拟到期无接管者")
    repository.save(run)
    claim = repository.claim(run.run_id, "a", 1)
    clock.advance(1)
    expected = row(repository, run.run_id)
    assert [r.run_id for r in repository.list_reclaimable()] == [run.run_id]
    assert not repository.renew_lease(run.run_id, "a", 1, 1)
    with pytest.raises(LeaseLostError):
        repository.save_claimed(claim.run, "a", 1)
    assert row(repository, run.run_id) == expected and not repository._connection.in_transaction


@pytest.mark.parametrize("status", ["succeeded", "failed"])
def test_terminal_save_clears_lease_and_retains_fencing_token(lease_store, status):
    repository, clock = lease_store
    run = RunRecord(message="模拟终态")
    repository.save(run)
    claim = repository.claim(run.run_id, "a", 3)
    claim.run.status = status
    claim.run.completed_at = clock.now()
    claim.run.answer = "保存完整模拟结果"
    repository.save_claimed(claim.run, "a", 1)
    saved = row(repository, run.run_id)
    assert saved[1] == status and saved[5:] == (None, None, 1)
    assert RunRecord.model_validate_json(saved[4]) == claim.run
    assert repository.claim(run.run_id, "b", 3) is None
    assert not repository.renew_lease(run.run_id, "a", 1, 3)
    with pytest.raises(LeaseLostError):
        repository.save_claimed(claim.run, "a", 1)
    assert row(repository, run.run_id) == saved and not repository._connection.in_transaction


def test_renewal_extends_lease_and_queued_write_is_rejected(lease_store):
    repository, clock = lease_store
    run = RunRecord(message="模拟续租")
    repository.save(run)
    claim = repository.claim(run.run_id, "a", 3)
    clock.advance(2)
    assert repository.renew_lease(run.run_id, "a", 1, 3)
    assert row(repository, run.run_id)[6] == (clock.now() + timedelta(seconds=3)).isoformat(timespec="microseconds")
    claim.run.status = "queued"
    with pytest.raises(ValueError):
        repository.save_claimed(claim.run, "a", 1)
    assert repository.get(run.run_id).status == "running"
    assert not repository._connection.in_transaction


@pytest.mark.parametrize("operation", ["claim", "renew", "save", "fail_queued"])
def test_lease_sql_exceptions_rollback_and_release_locks(lease_store, operation):
    repository, clock = lease_store
    run = RunRecord(message="模拟 SQL 异常")
    repository.save(run)
    claim = repository.claim(run.run_id, "a", 3) if operation in ("renew", "save") else None
    before = row(repository, run.run_id)
    connection = repository._connection
    connection.execute("""CREATE TRIGGER reject_update BEFORE UPDATE ON runs
                          BEGIN SELECT RAISE(ABORT, 'injected SQL failure'); END""")
    operations = {
        "claim": lambda: repository.claim(run.run_id, "a", 3),
        "renew": lambda: repository.renew_lease(run.run_id, "a", 1, 3),
        "save": lambda: repository.save_claimed(claim.run, "a", 1),
        "fail_queued": lambda: repository.fail_if_queued(run.run_id, RunError(code="TEST", message="mock")),
    }
    with pytest.raises(sqlite3.IntegrityError, match="injected SQL failure"):
        operations[operation]()
    assert not connection.in_transaction and row(repository, run.run_id) == before
    connection.execute("DROP TRIGGER reject_update")
    other = SQLiteRunRepository(repository.db_path, clock=clock.now)
    try:
        other.save(RunRecord(message="证明锁已释放"))
    finally:
        other.close()
    operations[operation]()
    assert not connection.in_transaction


def test_expired_claim_competition_has_one_winner_with_larger_token(lease_store):
    repository, clock = lease_store
    run = RunRecord(message="模拟真实线程接管竞争")
    repository.save(run)
    assert repository.claim(run.run_id, "old", 1).fencing_token == 1
    clock.advance(1)
    barrier = threading.Barrier(4)

    def compete(index):
        own = SQLiteRunRepository(repository.db_path, clock=clock.now)
        try:
            own.initialize()
            barrier.wait(timeout=5)
            result = own.claim(run.run_id, f"worker-{index}", 30)
            assert not own._connection.in_transaction
            return result
        finally:
            own.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(compete, i) for i in range(4)]
        results = [f.result(timeout=10) for f in futures]
    winners = [r for r in results if r is not None]
    assert len(winners) == 1 and sum(r is None for r in results) == 3
    assert winners[0].fencing_token == 2 and row(repository, run.run_id)[7] == 2


def lease_app(planner, path, clock, **settings):
    config = {"lease_seconds": 3.0, "heartbeat_interval": 0.01, "reaper_interval": 0.01, **settings}
    app = create_app(planner, db_path=path, **config)
    app.state.run_service.repository._clock = clock.now
    return app


def assert_stopped(service):
    assert service._worker_task.done() and service._reaper_task.done()
    assert not service._run_tasks and not service._heartbeat_tasks and not service._execution_tasks
    assert service.queue.empty() and service._enqueued_ids == service._active_ids == set()
    assert service.repository._connection is None


@pytest.mark.anyio
async def test_long_run_renews_and_cannot_be_stolen(tmp_path, planner, monkeypatch, client_factory):
    clock = Clock()
    started, renewed, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls = renewals = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", handler)
    app = lease_app(planner, tmp_path / "long.sqlite", clock)
    service = app.state.run_service
    original = service.repository.renew_lease

    def observe_renew(*args):
        nonlocal renewals
        clock.advance(1)
        assert original(*args)
        renewals += 1
        if renewals >= 5:
            renewed.set()
        return True

    monkeypatch.setattr(service.repository, "renew_lease", observe_renew)
    async with client_factory(app) as client:
        try:
            response = await client.post("/runs", json={"message": "查询 node_20 昨天的严重告警"})
            run_id = UUID(response.json()["run_id"])
            await asyncio.wait_for(started.wait(), timeout=1)
            await asyncio.wait_for(renewed.wait(), timeout=2)
            competitor = SQLiteRunRepository(service.repository.db_path, clock=clock.now)
            try:
                assert competitor.claim(run_id, "competitor", 3) is None
                assert competitor.get(run_id).status == "running"
            finally:
                competitor.close()
            release.set()
            saved = await client.completed_run(response)
            assert saved["status"] == "succeeded" and calls == 1 and renewals >= 5
            assert not {"lease_owner", "lease_until", "fencing_token"} & saved.keys()
        finally:
            release.set()
    assert_stopped(service)


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["false", "exception"])
async def test_heartbeat_loss_cancels_cleanup_without_any_late_write(
    tmp_path, planner, monkeypatch, client_factory, failure,
):
    clock = Clock()
    started, cleaned, loss = asyncio.Event(), asyncio.Event(), asyncio.Event()
    late_writes = []

    async def handler(args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.set()

    monkeypatch.setattr("main.find_warning", handler)
    app = lease_app(planner, tmp_path / "lost.sqlite", clock)
    service = app.state.run_service
    protected = service.repository.save_claimed

    def observe_save(*args):
        if loss.is_set():
            late_writes.append(args)
        return protected(*args)

    def lose(*args):
        loss.set()
        if failure == "exception":
            raise sqlite3.OperationalError("mock heartbeat storage failure")
        return False

    monkeypatch.setattr(service.repository, "save_claimed", observe_save)
    monkeypatch.setattr(service.repository, "renew_lease", lose)
    async with client_factory(app) as client:
        response = await client.post("/runs", json={"message": "查询 node_20 昨天的严重告警"})
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.wait_for(cleaned.wait(), timeout=2)
        await asyncio.wait_for(service.queue.join(), timeout=1)
        saved = (await client.get(response.headers["location"])).json()
        # 本地失去所有权证明时不能把 DB 中仍有效的 running 伪造为 CANCELLED。
        assert saved["status"] == "running" and saved["error"] is None and saved["completed_at"] is None
        assert loss.is_set() and late_writes == []
        assert not service._heartbeat_tasks and not service._execution_tasks
        assert not service._worker_task.done()
    assert_stopped(service)


@pytest.mark.anyio
@pytest.mark.parametrize("message", ["查询 node_20 昨天的严重告警", "综合查询 node_20 昨天的告警和流量",
                                   "诊断 node_20 昨天的设备状态、告警和流量"])
async def test_reaper_takeover_fences_late_old_worker_for_all_plans(
    tmp_path, planner, monkeypatch, client_factory, message,
):
    clock = Clock()
    path = tmp_path / "takeover.sqlite"
    started, cleaning, allow_late, cleaned, new_started = (asyncio.Event() for _ in range(5))
    calls = {"old": 0, "new": 0}

    async def old_handler(args):
        calls["old"] += 1
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleaning.set()
            await allow_late.wait()  # 可控模拟旧外部请求迟到返回。
            return await find_warning(args)
        finally:
            cleaned.set()

    async def new_handler(args):
        calls["new"] += 1
        new_started.set()
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", old_handler)
    old_app = lease_app(planner, path, clock, lease_seconds=1)
    monkeypatch.setattr("main.find_warning", new_handler)
    new_app = lease_app(planner, path, clock, lease_seconds=5)
    old, new = old_app.state.run_service, new_app.state.run_service
    assert old.worker_id != new.worker_id
    late_saves = []
    protected = old.repository.save_claimed
    ordinary = old.repository.save

    def forbid_unprotected(run):
        assert run.status == "queued", "执行中不得绕过 fencing 使用 save"
        ordinary(run)

    def observe_protected(*args):
        if cleaning.is_set():
            late_saves.append(args)
        protected(*args)

    monkeypatch.setattr(old.repository, "save", forbid_unprotected)
    monkeypatch.setattr(old.repository, "save_claimed", observe_protected)
    async with AsyncExitStack() as stack:
        old_client = await stack.enter_async_context(client_factory(old_app))
        try:
            response = await old_client.post("/runs", json={"message": message})
            run_id = UUID(response.json()["run_id"])
            await asyncio.wait_for(started.wait(), timeout=1)
            old_record = old.get_run(run_id)
            await stack.enter_async_context(client_factory(new_app))
            clock.advance(1)
            # 活动 ID 与队列 ID 分开：同实例过期活动项也可以再次进入队列。
            old._enqueue_reclaimable()
            old._enqueue_reclaimable()
            assert old._active_ids == {run_id} and old._enqueued_ids == {run_id} and old.queue.qsize() == 1
            await asyncio.wait_for(cleaning.wait(), timeout=2)
            await asyncio.wait_for(new_started.wait(), timeout=2)  # 新实例由周期回收器发现任务。
            await asyncio.wait_for(new.queue.join(), timeout=2)
            expected = row(new.repository, run_id)
            saved = new.get_run(run_id)
            assert saved.status == "succeeded" and expected[5:] == (None, None, 2)
            allow_late.set()
            await asyncio.wait_for(old.queue.join(), timeout=2)
            assert cleaned.is_set() and late_saves == []
            assert row(new.repository, run_id) == expected
            assert calls == {"old": 1, "new": 1}
            for status in ("running", "succeeded", "failed"):
                old_record.status = status
                old_record.error = RunError(code="STALE", message="不能覆盖新结果")
                with pytest.raises(LeaseLostError):
                    protected(old_record, old.worker_id, 1)
                assert row(new.repository, run_id) == expected
        finally:
            allow_late.set()
    assert_stopped(old)
    assert_stopped(new)


@pytest.mark.anyio
@pytest.mark.parametrize("message, outcome", [
    ("查询 node_20 昨天的严重告警", "success"),
    ("查询 node_20 昨天的严重告警", "error"),
    ("查询 node_20 昨天的严重告警", "cancel"),
    ("综合查询 node_20 昨天的告警和流量", "success"),
    ("诊断 node_20 昨天的设备状态、告警和流量", "success"),
])
async def test_database_fencing_rejects_late_result_before_heartbeat_notices(
    tmp_path, planner, monkeypatch, client_factory, message, outcome,
):
    clock = Clock()
    path = tmp_path / "database-fencing.sqlite"
    started, release = asyncio.Event(), asyncio.Event()

    async def delayed(args):
        started.set()
        await release.wait()
        if outcome == "error":
            raise RuntimeError("mock old failure")
        if outcome == "cancel":
            raise asyncio.CancelledError()
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", delayed)
    old_app = lease_app(planner, path, clock, lease_seconds=1)
    monkeypatch.setattr("main.find_warning", find_warning)
    new_app = lease_app(planner, path, clock)
    old, new = old_app.state.run_service, new_app.state.run_service
    # 模拟旧执行者尚未收到有效的失租信号，保护必须来自 DB，而非仅靠本地标记。
    monkeypatch.setattr(old.repository, "renew_lease", lambda *args: True)
    rejected = []
    protected = old.repository.save_claimed

    def observe(*args):
        try:
            protected(*args)
        except LeaseLostError:
            rejected.append(args[0].status)
            raise

    monkeypatch.setattr(old.repository, "save_claimed", observe)
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(client_factory(old_app))
        try:
            response = await client.post("/runs", json={"message": message})
            run_id = UUID(response.json()["run_id"])
            await asyncio.wait_for(started.wait(), timeout=1)
            clock.advance(1)
            await stack.enter_async_context(client_factory(new_app))
            await asyncio.wait_for(new.queue.join(), timeout=2)
            expected = row(new.repository, run_id)
            assert expected[1] == "succeeded" and expected[7] == 2
            release.set()
            await asyncio.wait_for(old.queue.join(), timeout=2)
            assert rejected  # 实际到达 SQLite 受保护保存，不能只靠本地提前跳过。
            assert row(new.repository, run_id) == expected
            assert new.get_run(run_id).error is None
        finally:
            release.set()
    assert_stopped(old)
    assert_stopped(new)


@pytest.mark.anyio
async def test_two_services_start_with_same_expired_run_and_only_one_takes_over(
    tmp_path, planner, monkeypatch, client_factory,
):
    clock = Clock()
    path = tmp_path / "expired-startup.sqlite"
    publisher = SQLiteRunRepository(path, clock=clock.now)
    run = RunRecord(message="查询 node_20 昨天的严重告警")
    try:
        publisher.save(run)
        assert publisher.claim(run.run_id, "crashed", 1).fencing_token == 1
    finally:
        publisher.close()
    clock.advance(1)
    release = asyncio.Event()
    ready = [asyncio.Event(), asyncio.Event()]
    counts = {"planner": 0, "handler": 0}

    class CountingPlanner:
        async def plan(self, message, tools):
            counts["planner"] += 1
            return await planner.plan(message, tools)

    async def handler(args):
        counts["handler"] += 1
        return await find_warning(args)

    monkeypatch.setattr("main.find_warning", handler)
    apps = [lease_app(CountingPlanner(), path, clock) for _ in range(2)]
    for app, event in zip(apps, ready):
        execute = app.state.run_service.execute_run

        async def gated(run_id, execute=execute, event=event):
            event.set()
            await release.wait()
            return await execute(run_id)

        monkeypatch.setattr(app.state.run_service, "execute_run", gated)
    async with AsyncExitStack() as stack:
        for app in apps:
            await stack.enter_async_context(client_factory(app))
        try:
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in ready)), timeout=1)
            release.set()
            await asyncio.wait_for(asyncio.gather(*(app.state.run_service.queue.join() for app in apps)), timeout=2)
            assert counts == {"planner": 1, "handler": 1}
            assert row(apps[0].state.run_service.repository, run.run_id)[7] == 2
        finally:
            release.set()
    for app in apps:
        assert_stopped(app.state.run_service)


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["error", "cancel"])
async def test_unclaimed_worker_failure_cannot_change_other_owner(
    tmp_path, planner, monkeypatch, client_factory, failure,
):
    clock = Clock()
    path = tmp_path / "before-claim.sqlite"
    app = lease_app(planner, path, clock)
    service = app.state.run_service
    observer = SQLiteRunRepository(path, clock=clock.now)
    taken, saved = asyncio.Event(), []

    def take_elsewhere(run_id, owner_id, lease_seconds):
        assert observer.claim(run_id, "other-owner", 30) is not None
        saved.append(row(observer, run_id))
        taken.set()
        if failure == "cancel":
            raise asyncio.CancelledError()
        raise RuntimeError("mock failure before claim")

    monkeypatch.setattr(service.repository, "claim", take_elsewhere)
    try:
        async with client_factory(app) as client:
            response = await client.post("/runs", json={"message": "查询 node_20 昨天的严重告警"})
            await asyncio.wait_for(taken.wait(), timeout=1)
            await asyncio.wait_for(service.queue.join(), timeout=1)
            assert row(observer, UUID(response.json()["run_id"])) == saved[0]
            assert saved[0][1] == "running" and saved[0][5] == "other-owner"
            assert not service._worker_task.done()
    finally:
        observer.close()
    assert_stopped(service)


@pytest.mark.anyio
@pytest.mark.parametrize("component", ["worker", "reaper"])
async def test_repeated_start_and_component_restart_do_not_leak_tasks(
    tmp_path, planner, client_factory, component,
):
    app = lease_app(planner, tmp_path / "restart.sqlite", Clock())
    service = app.state.run_service
    async with client_factory(app):
        worker, reaper = service._worker_task, service._reaper_task
        try:
            await service.start()
            assert service._worker_task is worker and service._reaper_task is reaper
            victim = worker if component == "worker" else reaper
            victim.cancel()
            await asyncio.gather(victim, return_exceptions=True)
            await service.start()
            assert not service._worker_task.done() and not service._reaper_task.done()
            if component == "worker":
                assert worker.done() and reaper.done()
            else:
                assert service._worker_task is worker and reaper.done()
        finally:
            orphans = [task for task in (worker, reaper)
                       if task not in (service._worker_task, service._reaper_task) and not task.done()]
            for task in orphans:
                task.cancel()
            await asyncio.gather(*orphans, return_exceptions=True)
    assert_stopped(service)


@pytest.mark.parametrize("setting, value", [("lease_seconds", 0), ("lease_seconds", float("inf")),
    ("heartbeat_interval", True), ("heartbeat_interval", 30), ("reaper_interval", -1)])
def test_invalid_lease_service_configuration_is_rejected(setting, value):
    with pytest.raises(ValueError):
        create_app(**{setting: value})


def test_failed_migration_rolls_back_schema_and_closes_connection(tmp_path, monkeypatch):
    path = tmp_path / "migration-failure.sqlite"
    connect = sqlite3.connect
    with closing(connect(path)) as connection:
        connection.execute("CREATE TABLE runs (run_id TEXT PRIMARY KEY, status TEXT, created_at TEXT, completed_at TEXT, record_json TEXT)")
    opened = []

    class FailIndexConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if "CREATE INDEX IF NOT EXISTS runs_status_lease_until" in sql:
                raise sqlite3.OperationalError("mock migration failure")
            return super().execute(sql, parameters)

    def failing_connect(*args, **kwargs):
        connection = connect(*args, factory=FailIndexConnection, **kwargs)
        opened.append(connection)
        return connection

    repository = SQLiteRunRepository(path)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(sqlite3, "connect", failing_connect)
            with pytest.raises(sqlite3.OperationalError, match="mock migration failure"):
                repository.initialize()
        assert repository._connection is None
        with pytest.raises(sqlite3.ProgrammingError):
            opened[0].execute("SELECT 1")
        with closing(connect(path)) as observer:
            assert len(observer.execute("PRAGMA table_info(runs)").fetchall()) == 5
        repository.initialize()
        assert len(repository._connection.execute("PRAGMA table_info(runs)").fetchall()) == 8
    finally:
        repository.close()


@pytest.mark.anyio
@pytest.mark.parametrize("target", ["shutdown", "worker"])
@pytest.mark.parametrize("message", ["查询 node_20 昨天的严重告警", "综合查询 node_20 昨天的告警和流量",
                                   "诊断 node_20 昨天的设备状态、告警和流量"])
async def test_repeated_external_cancellation_waits_for_cleanup_without_orphan_tasks(
    tmp_path, planner, monkeypatch, target, message,
):
    baseline = set(asyncio.all_tasks())
    started, cleaning, release, cleaned = (asyncio.Event() for _ in range(4))

    async def handler(args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release.wait()
            cleaned.set()

    async def checkpoint():
        ready = asyncio.Event()
        asyncio.get_running_loop().call_soon(ready.set)
        await ready.wait()

    monkeypatch.setattr("main.find_warning", handler)
    path = tmp_path / "repeated-cancellation.sqlite"
    app = create_app(planner, db_path=path)
    service = app.state.run_service
    lifetime = app.router.lifespan_context(app)
    await lifetime.__aenter__()
    lifetime_closed = False

    async def shutdown():
        nonlocal lifetime_closed
        try:
            await lifetime.__aexit__(None, None, None)
        finally:
            lifetime_closed = True

    parent = None
    try:
        run = await service.create_run(message)
        await asyncio.wait_for(started.wait(), timeout=1)
        children = service._execution_tasks | service._heartbeat_tasks
        if target == "shutdown":
            parent = asyncio.create_task(shutdown())
        else:
            parent = service._worker_task
            parent.cancel()
        await asyncio.wait_for(cleaning.wait(), timeout=1)
        for _ in range(2):
            parent.cancel()
            await checkpoint()
            await checkpoint()
            assert not parent.done(), "必须先等待 handler 清理，不能因重复取消提前退出"
            assert not cleaned.is_set() and service.repository._connection is not None
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(parent, timeout=2)
        assert cleaned.is_set() and all(task.done() for task in children)
        assert not service._execution_tasks and not service._heartbeat_tasks and not service._run_tasks
    finally:
        release.set()
        if parent is not None:
            await asyncio.gather(parent, return_exceptions=True)
        if not lifetime_closed:
            await shutdown()
    assert_stopped(service)
    await asyncio.wait_for(service.queue.join(), timeout=1)
    assert not set(asyncio.all_tasks()) - baseline
    observer = SQLiteRunRepository(path)
    try:
        saved = observer.get(run.run_id)
        assert saved.status == "failed" and saved.error.code == "CANCELLED"
        assert saved.completed_at is not None
    finally:
        observer.close()
