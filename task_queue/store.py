"""基于本地 SQLite 的持久化层。

线程/进程安全：
- 进程内：所有读写在一把可重入锁与单连接内串行完成；
- 进程间：写事务使用 BEGIN IMMEDIATE，由 SQLite 文件锁串行化，
  进程重启后状态从同一数据库文件恢复。
"""

from __future__ import annotations

import functools
import json
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from typing import Any, Self

from .errors import (
    CheckpointRejectedError,
    DuplicateIdempotencyKeyError,
    LeaseAlreadyHeldError,
    LeaseExpiredError,
    StorageError,
    TaskNotFoundError,
)
from .models import DeadLetterReason, Delivery, Task, TaskStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    task_type       TEXT NOT NULL,
    payload         TEXT NOT NULL,
    created_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_groups (
    name       TEXT PRIMARY KEY,
    next_seq   INTEGER NOT NULL DEFAULT 1,
    checkpoint INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    group_name       TEXT NOT NULL REFERENCES consumer_groups(name),
    task_id          TEXT NOT NULL REFERENCES tasks(id),
    seq              INTEGER NOT NULL,
    status           TEXT NOT NULL,
    attempt          INTEGER NOT NULL DEFAULT 0,
    visible_at       REAL NOT NULL DEFAULT 0,
    lease_owner      TEXT,
    lease_expires_at REAL,
    last_error       TEXT,
    dead_reason      TEXT,
    PRIMARY KEY (group_name, task_id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_fetch
    ON deliveries (group_name, status, visible_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_deliveries_group_seq
    ON deliveries (group_name, seq);
"""


def _translate_storage_error(func: Callable[..., Any]) -> Callable[..., Any]:
    """把底层 sqlite3.Error 统一包装成 StorageError，其余异常透传。"""

    @functools.wraps(func)
    def wrapper(self: SQLiteStore, *args: Any, **kwargs: Any) -> Any:
        try:
            return func(self, *args, **kwargs)
        except sqlite3.Error as exc:
            raise StorageError(f"storage failure in {func.__name__}: {exc}") from exc

    return wrapper


class SQLiteStore:
    """任务与投递状态的唯一事实来源。"""

    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None

    # ---- 生命周期 -------------------------------------------------------
    @_translate_storage_error
    def initialize(self) -> None:
        if self._conn is not None:
            return
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(_SCHEMA)
        self._conn = conn

    @_translate_storage_error
    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> Self:
        self.initialize()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise StorageError("store is not initialized; call initialize() first")
        return self._conn

    # ---- 内部工具 -------------------------------------------------------
    def _ensure_group(self, conn: sqlite3.Connection, group: str) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO consumer_groups (name, next_seq, checkpoint, created_at)"
            " VALUES (?, 1, 0, ?)",
            (group, time.time()),
        )

    @staticmethod
    def _row_to_delivery(row: sqlite3.Row) -> Delivery:
        return Delivery(
            task_id=row["task_id"],
            group=row["group_name"],
            seq=row["seq"],
            status=TaskStatus(row["status"]),
            attempt=row["attempt"],
            visible_at=row["visible_at"],
            lease_owner=row["lease_owner"],
            lease_expires_at=row["lease_expires_at"],
            last_error=row["last_error"],
            dead_reason=(
                DeadLetterReason(row["dead_reason"]) if row["dead_reason"] else None
            ),
        )

    # ---- 生产 -----------------------------------------------------------
    @_translate_storage_error
    def submit_task(
        self,
        task_id: str,
        idempotency_key: str,
        task_type: str,
        payload: dict[str, Any],
        groups: Iterable[str],
    ) -> tuple[Task, bool]:
        groups = list(groups)
        payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                created = False
                if row is not None:
                    # 幂等：相同内容必须返回同一任务；不同内容明确拒绝。
                    if row["task_type"] != task_type or json.loads(row["payload"]) != payload:
                        raise DuplicateIdempotencyKeyError(idempotency_key)
                    task = Task(
                        id=row["id"],
                        idempotency_key=row["idempotency_key"],
                        task_type=row["task_type"],
                        payload=json.loads(row["payload"]),
                        created_at=row["created_at"],
                    )
                else:
                    now = time.time()
                    conn.execute(
                        "INSERT INTO tasks (id, idempotency_key, task_type, payload, created_at)"
                        " VALUES (?, ?, ?, ?, ?)",
                        (task_id, idempotency_key, task_type, payload_json, now),
                    )
                    task = Task(
                        id=task_id,
                        idempotency_key=idempotency_key,
                        task_type=task_type,
                        payload=payload,
                        created_at=now,
                    )
                    created = True

                # 为每个消费者组建立（或补齐）投递，重复提交不产生重复行。
                for group in groups:
                    self._ensure_group(conn, group)
                    exists = conn.execute(
                        "SELECT 1 FROM deliveries WHERE group_name = ? AND task_id = ?",
                        (group, task.id),
                    ).fetchone()
                    if exists is None:
                        grow = conn.execute(
                            "SELECT next_seq FROM consumer_groups WHERE name = ?",
                            (group,),
                        ).fetchone()
                        seq = grow["next_seq"]
                        conn.execute(
                            "UPDATE consumer_groups SET next_seq = ? WHERE name = ?",
                            (seq + 1, group),
                        )
                        conn.execute(
                            "INSERT INTO deliveries"
                            " (group_name, task_id, seq, status, attempt, visible_at)"
                            " VALUES (?, ?, ?, ?, 0, 0)",
                            (group, task.id, seq, TaskStatus.PENDING.value),
                        )
                conn.commit()
            except DuplicateIdempotencyKeyError:
                conn.rollback()
                raise
            except Exception:
                conn.rollback()
                raise
            return task, created

    @_translate_storage_error
    def get_task(self, task_id: str) -> Task:
        with self._lock:
            row = self.conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise TaskNotFoundError(task_id)
        return Task(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            task_type=row["task_type"],
            payload=json.loads(row["payload"]),
            created_at=row["created_at"],
        )

    # ---- 消费 -----------------------------------------------------------
    @_translate_storage_error
    def register_group(self, group: str) -> None:
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._ensure_group(conn, group)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @_translate_storage_error
    def fetch_available(
        self,
        group: str,
        consumer: str,
        now: float,
        lease_seconds: float,
        limit: int = 1,
    ) -> list[Delivery]:
        """领取可投递任务（pending 或退避到期的 retrying），原子写入租约。"""
        claimed: list[Delivery] = []
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._ensure_group(conn, group)
                rows = conn.execute(
                    "SELECT * FROM deliveries"
                    " WHERE group_name = ?"
                    "   AND status IN (?, ?)"
                    "   AND visible_at <= ?"
                    " ORDER BY seq ASC LIMIT ?",
                    (
                        group,
                        TaskStatus.PENDING.value,
                        TaskStatus.RETRYING.value,
                        now,
                        limit,
                    ),
                ).fetchall()
                for row in rows:
                    # 条件更新兜底：即便上面的 SELECT 与 UPDATE 之间状态被改，
                    # 也只有持有者匹配时才能抢占成功。
                    cur = conn.execute(
                        "UPDATE deliveries SET status = ?, attempt = attempt + 1,"
                        "       lease_owner = ?, lease_expires_at = ?"
                        " WHERE group_name = ? AND task_id = ?"
                        "   AND status IN (?, ?) AND visible_at <= ?",
                        (
                            TaskStatus.INFLIGHT.value,
                            consumer,
                            now + lease_seconds,
                            group,
                            row["task_id"],
                            TaskStatus.PENDING.value,
                            TaskStatus.RETRYING.value,
                            now,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise LeaseAlreadyHeldError(row["task_id"], holder="<other>")
                    fresh = conn.execute(
                        "SELECT * FROM deliveries WHERE group_name = ? AND task_id = ?",
                        (group, row["task_id"]),
                    ).fetchone()
                    claimed.append(self._row_to_delivery(fresh))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return claimed

    def _guard_lease(
        self,
        conn: sqlite3.Connection,
        group: str,
        task_id: str,
        consumer: str,
        now: float,
    ) -> sqlite3.Row:
        """校验租约仍由该消费者持有，返回当前行；否则抛 LeaseExpiredError。"""
        row = conn.execute(
            "SELECT * FROM deliveries WHERE group_name = ? AND task_id = ?",
            (group, task_id),
        ).fetchone()
        if row is None:
            raise TaskNotFoundError(task_id)
        if (
            row["status"] != TaskStatus.INFLIGHT.value
            or row["lease_owner"] != consumer
            or row["lease_expires_at"] is None
            or row["lease_expires_at"] <= now
        ):
            raise LeaseExpiredError(task_id, consumer)
        return row

    @_translate_storage_error
    def complete(
        self,
        group: str,
        task_id: str,
        consumer: str,
        now: float,
    ) -> int:
        """标记成功并自动连续推进位点，返回推进后的位点。"""
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._guard_lease(conn, group, task_id, consumer, now)
                conn.execute(
                    "UPDATE deliveries SET status = ?, lease_owner = NULL,"
                    "       lease_expires_at = NULL, last_error = NULL, dead_reason = NULL"
                    " WHERE group_name = ? AND task_id = ?",
                    (TaskStatus.SUCCEEDED.value, group, task_id),
                )
                checkpoint = self._advance_checkpoint(conn, group)
                conn.commit()
                return checkpoint
            except Exception:
                conn.rollback()
                raise

    # 位点意义上“已处理终结”的状态：成功或进入死信。
    # retrying/inflight/pending 仍未终结，位点不得越过它们。
    _CHECKPOINT_TERMINAL = (TaskStatus.SUCCEEDED.value, TaskStatus.DEAD.value)

    @classmethod
    def _advance_checkpoint(cls, conn: sqlite3.Connection, group: str) -> int:
        """从 checkpoint+1 开始，把连续已终结（成功/死信）的投递推进过去。"""
        grow = conn.execute(
            "SELECT checkpoint FROM consumer_groups WHERE name = ?", (group,)
        ).fetchone()
        checkpoint = grow["checkpoint"]
        placeholders = ",".join("?" for _ in cls._CHECKPOINT_TERMINAL)
        while True:
            nxt = conn.execute(
                f"SELECT seq, status FROM deliveries"
                f" WHERE group_name = ? AND seq = ?"
                f"   AND status IN ({placeholders})",
                (group, checkpoint + 1, *cls._CHECKPOINT_TERMINAL),
            ).fetchone()
            if nxt is None:
                break
            checkpoint += 1
        conn.execute(
            "UPDATE consumer_groups SET checkpoint = ? WHERE name = ?",
            (checkpoint, group),
        )
        return checkpoint

    @_translate_storage_error
    def mark_retrying(
        self,
        group: str,
        task_id: str,
        consumer: str,
        attempt: int,
        visible_at: float,
        error_message: str,
        now: float,
    ) -> None:
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._guard_lease(conn, group, task_id, consumer, now)
                conn.execute(
                    "UPDATE deliveries SET status = ?, attempt = ?, visible_at = ?,"
                    "       lease_owner = NULL, lease_expires_at = NULL, last_error = ?"
                    " WHERE group_name = ? AND task_id = ?",
                    (
                        TaskStatus.RETRYING.value,
                        attempt,
                        visible_at,
                        error_message,
                        group,
                        task_id,
                    ),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @_translate_storage_error
    def mark_dead(
        self,
        group: str,
        task_id: str,
        consumer: str,
        attempt: int,
        reason: DeadLetterReason,
        error_message: str,
        now: float,
    ) -> int:
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._guard_lease(conn, group, task_id, consumer, now)
                conn.execute(
                    "UPDATE deliveries SET status = ?, attempt = ?, last_error = ?,"
                    "       dead_reason = ?, lease_owner = NULL, lease_expires_at = NULL"
                    " WHERE group_name = ? AND task_id = ?",
                    (
                        TaskStatus.DEAD.value,
                        attempt,
                        error_message,
                        reason.value,
                        group,
                        task_id,
                    ),
                )
                # 死信是终结状态，位点可连续越过它。
                checkpoint = self._advance_checkpoint(conn, group)
                conn.commit()
                return checkpoint
            except Exception:
                conn.rollback()
                raise

    @_translate_storage_error
    def reclaim_expired(self, group: str, now: float) -> list[Delivery]:
        """租约到期的 inflight 任务回收为立即可投递，不增加尝试次数。"""
        reclaimed: list[Delivery] = []
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                rows = conn.execute(
                    "SELECT * FROM deliveries"
                    " WHERE group_name = ? AND status = ?"
                    "   AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?"
                    " ORDER BY seq ASC",
                    (group, TaskStatus.INFLIGHT.value, now),
                ).fetchall()
                for row in rows:
                    conn.execute(
                        "UPDATE deliveries SET status = ?, lease_owner = NULL,"
                        "       lease_expires_at = NULL, visible_at = ?,"
                        "       last_error = ?"
                        " WHERE group_name = ? AND task_id = ?",
                        (
                            TaskStatus.RETRYING.value,
                            now,
                            "lease expired before result was reported",
                            group,
                            row["task_id"],
                        ),
                    )
                    fresh = conn.execute(
                        "SELECT * FROM deliveries WHERE group_name = ? AND task_id = ?",
                        (group, row["task_id"]),
                    ).fetchone()
                    reclaimed.append(self._row_to_delivery(fresh))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return reclaimed

    # ---- 位点 -----------------------------------------------------------
    @_translate_storage_error
    def commit_checkpoint(self, group: str, seq: int, consumer: str) -> int:
        """显式提交位点。

        仅接受 seq == checkpoint + 1 且该投递已成功：
        - seq <= checkpoint          → 回退，拒绝；
        - seq > checkpoint + 1       → 跳跃，拒绝；
        - 对应投递不存在或未成功     → 拒绝。
        """
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._ensure_group(conn, group)
                grow = conn.execute(
                    "SELECT checkpoint FROM consumer_groups WHERE name = ?", (group,)
                ).fetchone()
                current = grow["checkpoint"]
                if seq <= current:
                    raise CheckpointRejectedError(
                        seq,
                        current,
                        "rollback is not allowed: checkpoint can only move forward",
                    )
                if seq > current + 1:
                    raise CheckpointRejectedError(
                        seq,
                        current,
                        "jump is not allowed: commit seq must be checkpoint + 1",
                    )
                target = conn.execute(
                    "SELECT status FROM deliveries WHERE group_name = ? AND seq = ?",
                    (group, seq),
                ).fetchone()
                if target is None:
                    raise CheckpointRejectedError(
                        seq, current, "no delivery exists at that seq"
                    )
                if target["status"] not in self._CHECKPOINT_TERMINAL:
                    raise CheckpointRejectedError(
                        seq,
                        current,
                        f"delivery at seq is not terminal (status={target['status']})",
                    )
                conn.execute(
                    "UPDATE consumer_groups SET checkpoint = ? WHERE name = ?",
                    (seq, group),
                )
                conn.commit()
                return seq
            except Exception:
                conn.rollback()
                raise

    @_translate_storage_error
    def get_checkpoint(self, group: str) -> int:
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._ensure_group(conn, group)
                row = conn.execute(
                    "SELECT checkpoint FROM consumer_groups WHERE name = ?", (group,)
                ).fetchone()
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return row["checkpoint"]

    # ---- 查询（测试与可解释性） -----------------------------------------
    @_translate_storage_error
    def get_delivery(self, group: str, task_id: str) -> Delivery:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM deliveries WHERE group_name = ? AND task_id = ?",
                (group, task_id),
            ).fetchone()
        if row is None:
            raise TaskNotFoundError(task_id)
        return self._row_to_delivery(row)

    @_translate_storage_error
    def list_deliveries(
        self,
        group: str,
        status: TaskStatus | None = None,
    ) -> list[Delivery]:
        with self._lock:
            if status is None:
                rows = self.conn.execute(
                    "SELECT * FROM deliveries WHERE group_name = ? ORDER BY seq ASC",
                    (group,),
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT * FROM deliveries WHERE group_name = ? AND status = ?"
                    " ORDER BY seq ASC",
                    (group, status.value),
                ).fetchall()
        return [self._row_to_delivery(r) for r in rows]

    @_translate_storage_error
    def list_dead_letters(self, group: str) -> list[Delivery]:
        return self.list_deliveries(group, TaskStatus.DEAD)

    @_translate_storage_error
    def count_by_status(self, group: str) -> dict[TaskStatus, int]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT status, COUNT(*) AS n FROM deliveries"
                " WHERE group_name = ? GROUP BY status",
                (group,),
            ).fetchall()
        counts: dict[TaskStatus, int] = {s: 0 for s in TaskStatus}
        for row in rows:
            counts[TaskStatus(row["status"])] = row["n"]
        return counts
