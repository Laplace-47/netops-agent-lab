"""完整 Run 快照的同步存储接口；SQLite 操作不跨越任何 await。"""

import sqlite3
from datetime import timezone
from pathlib import Path
from typing import Protocol
from uuid import UUID

from netops_agent.models import RunRecord


class RunRepository(Protocol):
    def initialize(self) -> None: ...
    def save(self, run: RunRecord) -> None: ...
    def claim(self, run_id: UUID) -> RunRecord | None: ...
    def get(self, run_id: UUID) -> RunRecord: ...
    def list_by_status(self, status: str) -> list[RunRecord]: ...
    def close(self) -> None: ...


class SQLiteRunRepository:
    """同一事件循环线程中使用，延迟连接；close 后需显式 initialize 才能重开。"""

    def __init__(self, db_path: str | Path = ":memory:"):
        self.db_path = str(db_path)
        if not self.db_path:
            raise ValueError("db_path 不能为空")
        self._connection: sqlite3.Connection | None = None
        self._closed = False

    def initialize(self) -> None:
        if self._connection is not None:
            return
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=1.0)
        try:
            with connection:
                connection.execute("""
                    CREATE TABLE IF NOT EXISTS runs (
                        run_id TEXT PRIMARY KEY,
                        status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed')),
                        created_at TEXT NOT NULL,
                        completed_at TEXT,
                        record_json TEXT NOT NULL
                    )
                """)
                connection.execute("""
                    CREATE INDEX IF NOT EXISTS runs_status_created_at
                    ON runs(status, created_at, run_id)
                """)
        except BaseException:
            connection.close()
            raise
        self._connection = connection
        self._closed = False

    def _get_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            if self._closed:
                raise RuntimeError("Run Repository 已关闭，请先 initialize。")
            self.initialize()
        return self._connection

    def save(self, run: RunRecord) -> None:
        # 序列化在事务前完成；每次保存只有一条 UPSERT，退出后提交/回滚。
        record_json = run.model_dump_json()
        connection = self._get_connection()
        with connection:
            connection.execute("""
                INSERT INTO runs(run_id, status, created_at, completed_at, record_json)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status=excluded.status, created_at=excluded.created_at,
                    completed_at=excluded.completed_at, record_json=excluded.record_json
            """, (str(run.run_id), run.status, run.created_at.astimezone(timezone.utc).isoformat(),
                  run.completed_at.isoformat() if run.completed_at is not None else None, record_json))

    def claim(self, run_id: UUID) -> RunRecord | None:
        """在短写事务内领取 queued；事务提交后才返回，不提供 Lease 或崩溃回收。"""
        connection = self._get_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status, record_json FROM runs WHERE run_id = ?", (str(run_id),),
            ).fetchone()
            if row is None or row[0] != "queued":
                connection.rollback()
                return None
            run = RunRecord.model_validate_json(row[1])
            run.status = "running"
            run.completed_at = None
            updated = connection.execute("""
                UPDATE runs SET status = 'running', completed_at = ?, record_json = ?
                WHERE run_id = ? AND status = 'queued'
            """, (None, run.model_dump_json(), str(run_id)))
            if updated.rowcount != 1:
                connection.rollback()
                return None
        # 连接上下文提交或在异常时回滚；Planner/工具不会在事务中执行。
        return run

    def get(self, run_id: UUID) -> RunRecord:
        row = self._get_connection().execute(
            "SELECT record_json FROM runs WHERE run_id = ?", (str(run_id),),
        ).fetchone()
        if row is None:
            raise KeyError(run_id)
        return RunRecord.model_validate_json(row[0])

    def list_by_status(self, status: str) -> list[RunRecord]:
        rows = self._get_connection().execute(
            "SELECT record_json FROM runs WHERE status = ? ORDER BY created_at, run_id", (status,),
        ).fetchall()
        return [RunRecord.model_validate_json(row[0]) for row in rows]

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        self._closed = True
