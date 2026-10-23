import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

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
