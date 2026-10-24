import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from netops_agent.models import (
    DAGPlan, DAGStep, ParallelPlan, ResultBinding, RunError, RunRecord,
    ToolCall, ToolExecution, ToolResult,
)
from netops_agent.repository import SQLiteRunRepository


@pytest.fixture
def repository(tmp_path):
    repository = SQLiteRunRepository(tmp_path / "nested" / "runs.sqlite")
    repository.initialize()
    try:
        yield repository
    finally:
        repository.close()


@pytest.mark.parametrize("mode", ["single", "parallel", "dag"])
@pytest.mark.parametrize("failed", [False, True])
def test_complete_record_round_trip_preserves_all_fields(repository, valid_arguments, mode, failed):
    first = ToolCall(name="find_warning", arguments=valid_arguments)
    second = ToolCall(name="probe", arguments={"nested": {"values": [1, None, True]}})
    result = ToolResult(
        type="error" if failed else "success",
        metadata={"source": "mock", "error_code": "BUSINESS_ERROR"} if failed else {"source": "mock"},
        content=[{"id": "mock-record", "nested": {"values": [1, None, True]}}],
        indexes=["mock-record"],
    )
    error = RunError(code="BUSINESS_ERROR", message="模拟业务失败") if failed else None
    run = RunRecord(
        message="模拟完整记录", status="failed" if failed else "succeeded",
        completed_at=datetime.now(timezone.utc), answer="模拟汇总", error=error,
    )
    if mode == "single":
        run.tool_call, run.result = first, result
    else:
        binding = ResultBinding(source_step_id=first.id, target_argument="records")
        run.plan = (ParallelPlan(tool_calls=[first, second]) if mode == "parallel" else DAGPlan(steps=[
            DAGStep(tool_call=first),
            DAGStep(tool_call=second, depends_on=[first.id], bindings=[binding]),
        ]))
        run.tool_executions = [
            ToolExecution(tool_call=first, status=run.status, result=result, error=error),
            ToolExecution(tool_call=second, status=run.status, result=result, error=error,
                          depends_on=[first.id] if mode == "dag" else [],
                          bindings=[binding] if mode == "dag" else []),
        ]
    expected = run.model_dump(mode="json")
    repository.save(run)
    assert repository.get(run.run_id).model_dump(mode="json") == expected
    fetched = repository.get(run.run_id)
    if mode == "single":
        fetched.result.content.clear()
    else:
        fetched.tool_executions[0].result.content.clear()
    assert repository.get(run.run_id).model_dump(mode="json") == expected
    with closing(sqlite3.connect(repository.db_path)) as observer:
        row = observer.execute(
            "SELECT status, completed_at, record_json FROM runs WHERE run_id = ?", (str(run.run_id),),
        ).fetchone()
        assert row[0] == run.status and row[1] is not None
        assert RunRecord.model_validate_json(row[2]).model_dump(mode="json") == expected


def test_upsert_status_query_order_missing_id_and_index(repository):
    now = datetime.now(timezone.utc)
    later = RunRecord(message="later", created_at=now + timedelta(seconds=1))
    earlier = RunRecord(message="earlier", created_at=now)
    repository.save(later)
    repository.save(earlier)
    assert [r.run_id for r in repository.list_by_status("queued")] == [earlier.run_id, later.run_id]
    earlier.status = "running"
    repository.save(earlier)
    assert [r.run_id for r in repository.list_by_status("queued")] == [later.run_id]
    assert repository.list_by_status("running")[0].run_id == earlier.run_id
    with pytest.raises(KeyError):
        repository.get(uuid4())
    with closing(sqlite3.connect(repository.db_path)) as observer:
        assert observer.execute("SELECT count(*) FROM runs").fetchone()[0] == 2
        indexes = observer.execute("PRAGMA index_list(runs)").fetchall()
        assert any(observer.execute(f"PRAGMA index_info('{index[1]}')").fetchall()[0][2] == "status"
                   for index in indexes)


def test_lazy_open_idempotent_initialize_and_explicit_close(tmp_path):
    path = tmp_path / "lazy" / "run.sqlite"
    repository = SQLiteRunRepository(path)
    assert not path.exists() and not path.parent.exists()
    repository.initialize()
    connection = repository._connection  # 明确观测连接关闭和事务状态。
    repository.initialize()
    assert repository._connection is connection
    repository.save(RunRecord(message="模拟"))
    assert not connection.in_transaction
    repository.close()
    repository.close()
    assert repository._connection is None
    with pytest.raises(sqlite3.ProgrammingError):
        connection.execute("SELECT 1")
    with pytest.raises(RuntimeError, match="关闭"):
        repository.list_by_status("queued")
    repository.initialize()
    try:
        assert len(repository.list_by_status("queued")) == 1
    finally:
        repository.close()


def test_failed_upsert_rolls_back_without_changing_existing_record(repository):
    run = RunRecord(message="模拟", status="queued")
    repository.save(run)
    run.status = "invalid"  # 构造后被修改，数据库 CHECK 仍必须拒绝。
    with pytest.raises(sqlite3.IntegrityError):
        repository.save(run)
    assert repository.get(run.run_id).status == "queued"
    assert not repository._connection.in_transaction


@pytest.mark.parametrize("mode", ["single", "parallel", "dag"])
def test_claim_preserves_complete_record_and_commits_columns_and_json(repository, valid_arguments, mode):
    first = ToolCall(name="find_warning", arguments=valid_arguments)
    second = ToolCall(name="probe", arguments={"node_id": "node_20"})
    result = ToolResult(type="success", metadata={"source": "mock"},
                        content=[{"nested": {"values": [1, None, True]}}], indexes=["mock-claim"])
    run = RunRecord(message="模拟领取完整记录", tool_call=first, result=result, answer="保留模拟字段",
                    error=RunError(code="MOCK", message="保留既有字段"))
    if mode != "single":
        binding = ResultBinding(source_step_id=first.id, target_argument="records")
        run.plan = (ParallelPlan(tool_calls=[first, second]) if mode == "parallel" else DAGPlan(steps=[
            DAGStep(tool_call=first),
            DAGStep(tool_call=second, depends_on=[first.id], bindings=[binding]),
        ]))
        run.tool_executions = [
            ToolExecution(tool_call=first, status="succeeded", result=result),
            ToolExecution(tool_call=second, depends_on=[first.id] if mode == "dag" else [],
                          bindings=[binding] if mode == "dag" else []),
        ]
    repository.save(run)
    expected = run.model_dump(mode="json")
    expected["status"] = "running"
    claimed = repository.claim(run.run_id, "test-owner", 30)
    assert claimed is not None and claimed.run.model_dump(mode="json") == expected
    assert claimed.run.completed_at is None and run.status == "queued"
    assert not repository._connection.in_transaction
    with closing(sqlite3.connect(repository.db_path)) as observer:
        before_second_claim = observer.execute("SELECT * FROM runs WHERE run_id=?", (str(run.run_id),)).fetchone()
        assert before_second_claim[1] == "running" and before_second_claim[3] is None
        assert datetime.fromisoformat(before_second_claim[2]) == run.created_at
        assert RunRecord.model_validate_json(before_second_claim[4]).model_dump(mode="json") == expected
        assert repository.claim(run.run_id, "other-owner", 30) is None
        assert observer.execute("SELECT * FROM runs WHERE run_id=?", (str(run.run_id),)).fetchone() == before_second_claim
    assert not repository._connection.in_transaction
    claimed.run.result.content.clear()
    assert repository.get(run.run_id).model_dump(mode="json") == expected


@pytest.mark.parametrize("status", ["running", "succeeded", "failed"])
def test_claim_nonqueued_and_missing_runs_do_not_change_records(repository, status):
    run = RunRecord(message="模拟不可领取记录", status=status, answer="保留模拟结果",
                    completed_at=None if status == "running" else datetime.now(timezone.utc))
    repository.save(run)
    with closing(sqlite3.connect(repository.db_path)) as observer:
        before = observer.execute("SELECT * FROM runs").fetchall()
        assert repository.claim(run.run_id, "test-owner", 30) is None
        assert not repository._connection.in_transaction
        assert repository.claim(uuid4(), "test-owner", 30) is None
        assert not repository._connection.in_transaction
        assert observer.execute("SELECT * FROM runs").fetchall() == before
    assert repository.get(run.run_id) == run


def test_claim_rolls_back_sql_failure_and_allows_subsequent_operations(repository):
    run = RunRecord(message="模拟 SQL 失败")
    repository.save(run)
    connection = repository._connection
    connection.execute("""
        CREATE TRIGGER reject_claim BEFORE UPDATE ON runs WHEN NEW.status = 'running'
        BEGIN SELECT RAISE(ABORT, 'claim rejected'); END
    """)
    with pytest.raises(sqlite3.IntegrityError, match="claim rejected"):
        repository.claim(run.run_id, "test-owner", 30)
    assert not connection.in_transaction and repository.get(run.run_id) == run
    observer = SQLiteRunRepository(repository.db_path)
    try:
        observer.save(RunRecord(message="证明回滚后其他连接可写"))
    finally:
        observer.close()
    connection.execute("DROP TRIGGER reject_claim")
    claimed = repository.claim(run.run_id, "test-owner", 30)
    assert claimed is not None and claimed.run.status == "running"
    claimed.run.status = "succeeded"
    repository.save(claimed.run)
    assert repository.get(run.run_id).status == "succeeded" and not connection.in_transaction


def test_claim_rolls_back_invalid_json_and_releases_write_lock(repository):
    run = RunRecord(message="模拟反序列化失败")
    repository.save(run)
    connection = repository._connection
    with connection:
        connection.execute("UPDATE runs SET record_json = ? WHERE run_id = ?", ("{invalid", str(run.run_id)))
    with pytest.raises(ValidationError):
        repository.claim(run.run_id, "test-owner", 30)
    assert not connection.in_transaction
    observer = SQLiteRunRepository(repository.db_path)
    try:
        observer.save(RunRecord(message="证明 JSON 异常未遗留写锁"))
    finally:
        observer.close()
    repository.save(run)
    assert repository.claim(run.run_id, "test-owner", 30).run.status == "running"


def test_claim_requires_exactly_one_updated_row(repository):
    run = RunRecord(message="模拟条件更新未生效")
    repository.save(run)
    connection = repository._connection
    connection.execute("""
        CREATE TRIGGER ignore_claim BEFORE UPDATE ON runs WHEN NEW.status = 'running'
        BEGIN SELECT RAISE(IGNORE); END
    """)
    assert repository.claim(run.run_id, "test-owner", 30) is None
    assert not connection.in_transaction and repository.get(run.run_id) == run
    connection.execute("DROP TRIGGER ignore_claim")
    assert repository.claim(run.run_id, "test-owner", 30).run.status == "running"


def test_claim_initializes_lazily_and_respects_closed_repository(tmp_path):
    path = tmp_path / "lazy-claim.sqlite"
    repository = SQLiteRunRepository(path)
    try:
        assert not path.exists()
        assert repository.claim(uuid4(), "test-owner", 30) is None and path.exists()
        assert not repository._connection.in_transaction
        repository.close()
        with pytest.raises(RuntimeError, match="关闭"):
            repository.claim(uuid4(), "test-owner", 30)
        repository.initialize()
        run = RunRecord(message="模拟重新初始化")
        repository.save(run)
        assert repository.claim(run.run_id, "test-owner", 30).run.status == "running"
    finally:
        repository.close()


@pytest.mark.parametrize("competitors", [2, 4])
@pytest.mark.parametrize("round_number", range(3))
def test_claim_has_exactly_one_winner_across_thread_connections(tmp_path, competitors, round_number):
    path = tmp_path / f"competition-{round_number}.sqlite"
    run = RunRecord(message="模拟真实线程竞争")
    publisher = SQLiteRunRepository(path)
    try:
        publisher.save(run)
    finally:
        publisher.close()
    barrier = threading.Barrier(competitors)

    def compete():
        # 连接必须在使用它的线程内创建、初始化和关闭。
        repository = SQLiteRunRepository(path)
        try:
            repository.initialize()
            barrier.wait(timeout=5)
            claimed = repository.claim(run.run_id, f"thread-{threading.get_ident()}", 30)
            assert not repository._connection.in_transaction
            return threading.get_ident(), claimed
        finally:
            repository.close()

    with ThreadPoolExecutor(max_workers=competitors) as pool:
        futures = [pool.submit(compete) for _ in range(competitors)]
        outcomes = [future.result(timeout=10) for future in futures]  # 任一线程异常都使测试失败。
    assert len({thread_id for thread_id, _ in outcomes}) == competitors
    winners = [claimed for _, claimed in outcomes if claimed is not None]
    assert len(winners) == 1 and sum(claimed is None for _, claimed in outcomes) == competitors - 1
    assert winners[0].run.status == "running" and winners[0].run.completed_at is None
    with closing(sqlite3.connect(path)) as observer:
        status, completed_at, record_json = observer.execute("SELECT status, completed_at, record_json FROM runs").fetchone()
        assert status == "running" and completed_at is None
        assert RunRecord.model_validate_json(record_json) == winners[0].run
