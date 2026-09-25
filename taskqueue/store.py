"""基于本地 SQLite 的持久化存储：任务、消费组位点与租约。"""

from __future__ import annotations

import sqlite3
import threading

from taskqueue.errors import LeaseConflictError, OffsetRegressionError
from taskqueue.models import FailureReason, Task, TaskStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    failure_reason  TEXT NOT NULL DEFAULT 'NONE',
    last_error      TEXT,
    visible_at      REAL NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS leases (
    task_id     TEXT PRIMARY KEY,
    grp         TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    fence       INTEGER NOT NULL,
    expires_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS offsets (
    grp    TEXT PRIMARY KEY,
    offset INTEGER NOT NULL DEFAULT 0
);
"""


class TaskStore:
    """SQLite 单文件存储，进程重启后任务与位点不丢失。"""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        with self._lock, self._conn:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---- 内部工具 ----
    def _row_to_task(self, row: sqlite3.Row) -> Task:
        return Task(
            task_id=row["task_id"],
            idempotency_key=row["idempotency_key"],
            payload=row["payload"],
            status=TaskStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            failure_reason=FailureReason(row["failure_reason"]),
            last_error=row["last_error"],
            visible_at=row["visible_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _get_row(self, task_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()

    def _check_lease(self, task_id: str, group: str, fence: int, now: float) -> None:
        row = self._conn.execute(
            "SELECT * FROM leases WHERE task_id = ?", (task_id,)
        ).fetchone()
        if (
            row is None
            or row["grp"] != group
            or row["fence"] != fence
            or row["expires_at"] <= now
        ):
            raise LeaseConflictError(
                f"lease mismatch for task {task_id}: "
                f"expected fence={fence} group={group}"
            )

    # ---- 任务 ----
    def insert_task(self, task: Task) -> bool:
        """按幂等键去重插入；返回是否真正新建。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO tasks
                   (task_id, idempotency_key, payload, status, attempts,
                    max_attempts, failure_reason, visible_at, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    task.task_id,
                    task.idempotency_key,
                    task.payload,
                    task.status.value,
                    task.attempts,
                    task.max_attempts,
                    task.failure_reason.value,
                    task.visible_at,
                    task.created_at,
                    task.updated_at,
                ),
            )
            return cur.rowcount == 1

    def get_task(self, task_id: str) -> Task | None:
        with self._lock:
            row = self._get_row(task_id)
            return self._row_to_task(row) if row else None

    def get_task_by_key(self, idempotency_key: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            return self._row_to_task(row) if row else None

    def dead_letters(self) -> list[Task]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE status = ? ORDER BY seq",
                (TaskStatus.DEAD.value,),
            ).fetchall()
            return [self._row_to_task(r) for r in rows]

    # ---- 租约 ----
    def acquire(
        self, group: str, consumer_id: str, now: float, lease_ttl: float
    ) -> tuple[Task, int] | None:
        """为消费组领取下一个可见任务，返回任务与 fencing token。

        可见 = PENDING/FAILED_RETRYABLE 且 visible_at 到期，
        或 LEASED 但租约已过期（消费者中断后重新投递）。
        同一任务同一时刻只会被一个租约持有，fence 单调递增。
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                """SELECT * FROM tasks
                   WHERE (status IN (?, ?) AND visible_at <= ?)
                      OR (status = ? AND task_id IN
                          (SELECT task_id FROM leases WHERE expires_at <= ?))
                   ORDER BY seq LIMIT 1""",
                (
                    TaskStatus.PENDING.value,
                    TaskStatus.FAILED_RETRYABLE.value,
                    now,
                    TaskStatus.LEASED.value,
                    now,
                ),
            ).fetchone()
            if row is None:
                return None
            task_id = row["task_id"]
            old = self._conn.execute(
                "SELECT fence FROM leases WHERE task_id = ?", (task_id,)
            ).fetchone()
            fence = (old["fence"] if old else 0) + 1
            self._conn.execute(
                """INSERT INTO leases (task_id, grp, consumer_id, fence, expires_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(task_id) DO UPDATE SET
                       grp = excluded.grp,
                       consumer_id = excluded.consumer_id,
                       fence = excluded.fence,
                       expires_at = excluded.expires_at""",
                (task_id, group, consumer_id, fence, now + lease_ttl),
            )
            self._conn.execute(
                "UPDATE tasks SET status = ?, updated_at = ? WHERE task_id = ?",
                (TaskStatus.LEASED.value, now, task_id),
            )
            task = self._row_to_task(self._get_row(task_id))
            return task, fence

    def ack(self, task_id: str, group: str, fence: int, now: float) -> None:
        """确认成功：校验租约后标记成功并推进消费组位点。"""
        with self._lock, self._conn:
            self._check_lease(task_id, group, fence, now)
            self._conn.execute(
                """UPDATE tasks SET status = ?, failure_reason = ?, last_error = NULL,
                   updated_at = ? WHERE task_id = ?""",
                (TaskStatus.SUCCEEDED.value, FailureReason.NONE.value, now, task_id),
            )
            self._conn.execute("DELETE FROM leases WHERE task_id = ?", (task_id,))
            self._advance_offset_locked(group)

    def fail(
        self,
        task_id: str,
        group: str,
        fence: int,
        now: float,
        visible_at: float,
        dead: bool,
        reason: str,
        error: str,
    ) -> None:
        """确认失败：校验租约后重排可见时间或进入死信。"""
        with self._lock, self._conn:
            self._check_lease(task_id, group, fence, now)
            status = TaskStatus.DEAD if dead else TaskStatus.FAILED_RETRYABLE
            self._conn.execute(
                """UPDATE tasks SET status = ?, attempts = attempts + 1,
                   failure_reason = ?, last_error = ?, visible_at = ?, updated_at = ?
                   WHERE task_id = ?""",
                (status.value, reason, error, visible_at, now, task_id),
            )
            self._conn.execute("DELETE FROM leases WHERE task_id = ?", (task_id,))
            if dead:
                self._advance_offset_locked(group)

    # ---- 位点 ----
    def _advance_offset_locked(self, group: str) -> None:
        """推进位点到已连续终结（成功或死信）的最大 seq。"""
        row = self._conn.execute(
            "SELECT offset FROM offsets WHERE grp = ?", (group,)
        ).fetchone()
        current = row["offset"] if row else 0
        nxt = self._conn.execute(
            """SELECT MIN(seq) AS gap FROM tasks
               WHERE seq > ? AND status NOT IN (?, ?)""",
            (current, TaskStatus.SUCCEEDED.value, TaskStatus.DEAD.value),
        ).fetchone()["gap"]
        target = (nxt - 1) if nxt is not None else self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS m FROM tasks"
        ).fetchone()["m"]
        if target > current:
            self._conn.execute(
                """INSERT INTO offsets (grp, offset) VALUES (?, ?)
                   ON CONFLICT(grp) DO UPDATE SET offset = excluded.offset""",
                (group, target),
            )

    def commit_offset(self, group: str, offset: int) -> None:
        """外部提交位点：只允许严格 +1 递增，回退或跳跃明确拒绝。"""
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT offset FROM offsets WHERE grp = ?", (group,)
            ).fetchone()
            current = row["offset"] if row else 0
            if offset != current + 1:
                raise OffsetRegressionError(
                    f"offset must advance exactly +1: current={current}, got={offset}"
                )
            self._conn.execute(
                """INSERT INTO offsets (grp, offset) VALUES (?, ?)
                   ON CONFLICT(grp) DO UPDATE SET offset = excluded.offset""",
                (group, offset),
            )

    def get_offset(self, group: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT offset FROM offsets WHERE grp = ?", (group,)
            ).fetchone()
            return row["offset"] if row else 0
