"""完整 Run 快照的同步存储接口；SQLite 操作不跨越任何 await。"""

import sqlite3
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Protocol
from uuid import UUID

from netops_agent.models import RunError, RunRecord


class LeaseLostError(Exception):
    """当前执行者不能再证明自己持有有效租约。"""


@dataclass(frozen=True)
class RunClaim:
    run: RunRecord
    owner_id: str
    fencing_token: int
    lease_until: datetime


class RunRepository(Protocol):
    def initialize(self) -> None: ...
    def save(self, run: RunRecord) -> None: ...
    def claim(self, run_id: UUID, owner_id: str, lease_seconds: float) -> RunClaim | None: ...
    def renew_lease(self, run_id: UUID, owner_id: str, fencing_token: int, lease_seconds: float) -> bool: ...
    def save_claimed(self, run: RunRecord, owner_id: str, fencing_token: int) -> None: ...
    def list_reclaimable(self) -> list[RunRecord]: ...
    def fail_if_queued(self, run_id: UUID, error: RunError) -> bool: ...
    def get(self, run_id: UUID) -> RunRecord: ...
    def list_by_status(self, status: str) -> list[RunRecord]: ...
    def close(self) -> None: ...


class SQLiteRunRepository:
    """同一事件循环线程中使用，延迟连接；close 后需显式 initialize 才能重开。"""

    def __init__(self, db_path: str | Path = ":memory:", *, clock: Callable[[], datetime] | None = None):
        self.db_path = str(db_path)
        if not self.db_path:
            raise ValueError("db_path 不能为空")
        self._connection: sqlite3.Connection | None = None
        self._closed = False
        self._clock = clock if clock is not None else lambda: datetime.now(timezone.utc)

    def _now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("Lease 时钟必须返回带时区的 datetime")
        return now.astimezone(timezone.utc)

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")

    @staticmethod
    def _validate_owner(owner_id: str) -> None:
        if not isinstance(owner_id, str) or not owner_id.strip():
            raise ValueError("owner_id 必须是非空字符串")

    @staticmethod
    def _validate_seconds(seconds: float) -> None:
        if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("lease_seconds 必须是大于 0 的有限秒数")

    @staticmethod
    def _validate_token(token: int) -> None:
        if isinstance(token, bool) or not isinstance(token, int) or token <= 0:
            raise ValueError("fencing_token 必须是正整数")

    def initialize(self) -> None:
        if self._connection is not None:
            return
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=1.0)
        try:
            with connection:
                # 迁移也串行化，避免多个实例同时 ALTER 同一张旧表。
                connection.execute("BEGIN IMMEDIATE")
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
                columns = {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
                for name, definition in (
                    ("lease_owner", "TEXT NULL"),
                    ("lease_until", "TEXT NULL"),
                    ("fencing_token", "INTEGER NOT NULL DEFAULT 0"),
                ):
                    if name not in columns:
                        connection.execute(f"ALTER TABLE runs ADD COLUMN {name} {definition}")
                connection.execute("""CREATE INDEX IF NOT EXISTS runs_status_lease_until
                                      ON runs(status, lease_until, created_at, run_id)""")
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

    def claim(self, run_id: UUID, owner_id: str, lease_seconds: float) -> RunClaim | None:
        """领取 queued 或过期 running；过期接管清除执行状态，从头重跑。"""
        self._validate_owner(owner_id)
        self._validate_seconds(lease_seconds)
        connection = self._get_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            now = self._now()
            stamp = self._timestamp(now)
            row = connection.execute(
                "SELECT status, record_json, lease_until, fencing_token FROM runs WHERE run_id = ?", (str(run_id),),
            ).fetchone()
            if row is None or not (row[0] == "queued" or
                                   (row[0] == "running" and row[2] is not None and row[2] <= stamp)):
                connection.rollback()
                return None
            run = RunRecord.model_validate_json(row[1])
            if row[0] == "running":
                run = RunRecord(run_id=run.run_id, message=run.message, created_at=run.created_at)
            run.status = "running"
            run.completed_at = None
            token = row[3] + 1
            if token > 2**63 - 1:
                raise OverflowError("fencing_token 已达到 SQLite INTEGER 上限")
            lease_until = now + timedelta(seconds=lease_seconds)
            updated = connection.execute("""
                UPDATE runs SET status = 'running', completed_at = NULL, record_json = ?,
                                lease_owner = ?, lease_until = ?, fencing_token = ?
                WHERE run_id = ? AND fencing_token = ? AND
                      (status = 'queued' OR (status = 'running' AND lease_until IS NOT NULL AND lease_until <= ?))
            """, (run.model_dump_json(), owner_id, self._timestamp(lease_until), token, str(run_id), row[3], stamp))
            if updated.rowcount != 1:
                connection.rollback()
                return None
        return RunClaim(run, owner_id, token, lease_until)

    def renew_lease(self, run_id: UUID, owner_id: str, fencing_token: int, lease_seconds: float) -> bool:
        self._validate_owner(owner_id)
        self._validate_token(fencing_token)
        self._validate_seconds(lease_seconds)
        connection = self._get_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            now = self._now()
            changed = connection.execute("""
                UPDATE runs SET lease_until = ?
                WHERE run_id = ? AND status = 'running' AND lease_owner = ?
                      AND fencing_token = ? AND lease_until > ?
            """, (self._timestamp(now + timedelta(seconds=lease_seconds)), str(run_id), owner_id,
                  fencing_token, self._timestamp(now))).rowcount
            if changed != 1:
                connection.rollback()
                return False
        return True

    def save_claimed(self, run: RunRecord, owner_id: str, fencing_token: int) -> None:
        self._validate_owner(owner_id)
        self._validate_token(fencing_token)
        if run.status not in ("running", "succeeded", "failed"):
            raise ValueError("执行中的保存不得将任务改回 queued")
        terminal = run.status in ("succeeded", "failed")
        record_json = run.model_dump_json()
        connection = self._get_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            now = self._timestamp(self._now())
            changed = connection.execute("""
                UPDATE runs SET status = ?, created_at = ?, completed_at = ?, record_json = ?,
                    lease_owner = CASE WHEN ? THEN NULL ELSE lease_owner END,
                    lease_until = CASE WHEN ? THEN NULL ELSE lease_until END
                WHERE run_id = ? AND status = 'running' AND lease_owner = ?
                      AND fencing_token = ? AND lease_until > ?
            """, (run.status, run.created_at.astimezone(timezone.utc).isoformat(),
                  run.completed_at.isoformat() if run.completed_at is not None else None, record_json,
                  terminal, terminal, str(run.run_id), owner_id, fencing_token, now)).rowcount
            if changed != 1:
                raise LeaseLostError(f"Run {run.run_id} 的租约已丢失")

    def list_reclaimable(self) -> list[RunRecord]:
        rows = self._get_connection().execute("""
            SELECT record_json FROM runs
            WHERE status = 'queued' OR (status = 'running' AND lease_until IS NOT NULL AND lease_until <= ?)
            ORDER BY created_at, run_id
        """, (self._timestamp(self._now()),)).fetchall()
        return [RunRecord.model_validate_json(row[0]) for row in rows]

    def fail_if_queued(self, run_id: UUID, error: RunError) -> bool:
        """Claim 前的故障只可结束仍未被领取的记录，不能覆盖其他实例的 running。"""
        connection = self._get_connection()
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT record_json FROM runs WHERE run_id = ? AND status = 'queued'",
                                     (str(run_id),)).fetchone()
            if row is None:
                connection.rollback()
                return False
            run = RunRecord.model_validate_json(row[0])
            run.status, run.error, run.answer = "failed", error, None
            run.completed_at = self._now()
            changed = connection.execute("""
                UPDATE runs SET status = 'failed', completed_at = ?, record_json = ?,
                                lease_owner = NULL, lease_until = NULL
                WHERE run_id = ? AND status = 'queued'
            """, (run.completed_at.isoformat(), run.model_dump_json(), str(run_id))).rowcount
            if changed != 1:
                connection.rollback()
                return False
        return True

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
