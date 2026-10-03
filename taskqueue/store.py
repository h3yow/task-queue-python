"""基于本地 SQLite 的持久化存储：任务、按组处理状态、租约与位点。"""

from __future__ import annotations

import sqlite3
import threading

from taskqueue.errors import LeaseConflictError, OffsetRegressionError
from taskqueue.models import FailureReason, GroupTaskState, Task, TaskStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload         TEXT NOT NULL,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS group_task_state (
    task_id        TEXT NOT NULL,
    grp            TEXT NOT NULL,
    status         TEXT NOT NULL,
    attempts       INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 3,
    failure_reason TEXT NOT NULL DEFAULT 'NONE',
    last_error     TEXT,
    visible_at     REAL NOT NULL DEFAULT 0,
    updated_at     REAL NOT NULL,
    PRIMARY KEY (task_id, grp)
);
CREATE TABLE IF NOT EXISTS leases (
    task_id     TEXT NOT NULL,
    grp         TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    fence       INTEGER NOT NULL,
    expires_at  REAL NOT NULL,
    PRIMARY KEY (task_id, grp)
);
CREATE TABLE IF NOT EXISTS offsets (
    grp    TEXT PRIMARY KEY,
    offset INTEGER NOT NULL DEFAULT 0
);
"""


class TaskStore:
    """SQLite 单文件存储，进程重启后任务、按组状态与位点不丢失。"""

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
            max_attempts=row["max_attempts"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def _row_to_state(self, row: sqlite3.Row) -> GroupTaskState:
        return GroupTaskState(
            task_id=row["task_id"],
            group=row["grp"],
            status=TaskStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            failure_reason=FailureReason(row["failure_reason"]),
            last_error=row["last_error"],
            visible_at=row["visible_at"],
            updated_at=row["updated_at"],
        )

    def _get_task_row(self, task_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()

    def _get_state_row(self, task_id: str, group: str) -> sqlite3.Row | None:
        return self._conn.execute(
            "SELECT * FROM group_task_state WHERE task_id = ? AND grp = ?",
            (task_id, group),
        ).fetchone()

    def _check_lease(self, task_id: str, group: str, fence: int, now: float) -> None:
        row = self._conn.execute(
            "SELECT * FROM leases WHERE task_id = ? AND grp = ?",
            (task_id, group),
        ).fetchone()
        if row is None or row["fence"] != fence or row["expires_at"] <= now:
            raise LeaseConflictError(
                f"lease mismatch for task {task_id} group {group}: "
                f"expected fence={fence}"
            )

    # ---- 任务 ----
    def insert_task(self, task: Task) -> bool:
        """按幂等键去重插入；返回是否真正新建。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO tasks
                   (task_id, idempotency_key, payload, max_attempts,
                    created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    task.task_id,
                    task.idempotency_key,
                    task.payload,
                    task.max_attempts,
                    task.created_at,
                    task.updated_at,
                ),
            )
            return cur.rowcount == 1

    def get_task(self, task_id: str) -> Task | None:
        with self._lock:
            row = self._get_task_row(task_id)
            return self._row_to_task(row) if row else None

    def get_task_by_key(self, idempotency_key: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            return self._row_to_task(row) if row else None

    # ---- 按组状态 ----
    def get_state(self, task_id: str, group: str) -> GroupTaskState | None:
        with self._lock:
            row = self._get_state_row(task_id, group)
            return self._row_to_state(row) if row else None

    def dead_letters(self, group: str) -> list[GroupTaskState]:
        """返回该组判为死信的按组状态（顺序与提交顺序一致）。"""
        with self._lock:
            rows = self._conn.execute(
                """SELECT s.* FROM group_task_state s
                   JOIN tasks t ON t.task_id = s.task_id
                   WHERE s.grp = ? AND s.status = ?
                   ORDER BY t.seq""",
                (group, TaskStatus.DEAD.value),
            ).fetchall()
            return [self._row_to_state(r) for r in rows]

    # ---- 租约 ----
    def acquire(
        self, group: str, consumer_id: str, now: float, lease_ttl: float
    ) -> tuple[Task, GroupTaskState, int] | None:
        """为消费组领取下一个该组可见的任务。

        - 组状态在首次领取时惰性创建（PENDING）；各组独立一行，互不干扰。
        - 可见 = 该组 PENDING/FAILED_RETRYABLE 且 visible_at 到期，
          或该组 LEASED 但自己的租约已过期（消费者中断后重新投递）。
        - 同一 (任务, 组) 同一时刻只有一个租约，fence 按组单调递增。
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                """SELECT t.*,
                          COALESCE(s.status, ?)      AS s_status,
                          COALESCE(s.attempts, 0)    AS s_attempts,
                          COALESCE(s.max_attempts, t.max_attempts) AS s_max_attempts,
                          COALESCE(s.failure_reason, ?) AS s_failure_reason,
                          s.last_error               AS s_last_error,
                          COALESCE(s.visible_at, 0)  AS s_visible_at,
                          COALESCE(s.updated_at, 0)  AS s_updated_at
                   FROM tasks t
                   LEFT JOIN group_task_state s
                          ON s.task_id = t.task_id AND s.grp = ?
                   WHERE (
                             (COALESCE(s.status, ?) IN (?, ?)
                              AND COALESCE(s.visible_at, t.created_at) <= ?)
                          OR (COALESCE(s.status, ?) = ?
                              AND EXISTS (
                                  SELECT 1 FROM leases l
                                  WHERE l.task_id = t.task_id AND l.grp = ?
                                    AND l.expires_at <= ?))
                         )
                   ORDER BY t.seq LIMIT 1""",
                (
                    TaskStatus.PENDING.value,
                    FailureReason.NONE.value,
                    group,
                    TaskStatus.PENDING.value,
                    TaskStatus.PENDING.value,
                    TaskStatus.FAILED_RETRYABLE.value,
                    now,
                    TaskStatus.PENDING.value,
                    TaskStatus.LEASED.value,
                    group,
                    now,
                ),
            ).fetchone()
            if row is None:
                return None
            task_id = row["task_id"]
            existing = self._get_state_row(task_id, group)
            expired_redelivery = (
                existing is not None
                and existing["status"] == TaskStatus.LEASED.value
            )
            # upsert 组状态
            self._conn.execute(
                """INSERT INTO group_task_state
                       (task_id, grp, status, attempts, max_attempts,
                        failure_reason, last_error, visible_at, updated_at)
                   VALUES (?, ?, ?, 0, ?, ?, NULL, ?, ?)
                   ON CONFLICT(task_id, grp) DO UPDATE SET
                       status = excluded.status,
                       failure_reason = excluded.failure_reason,
                       updated_at = excluded.updated_at""",
                (
                    task_id,
                    group,
                    TaskStatus.LEASED.value,
                    row["s_max_attempts"],
                    (
                        FailureReason.LEASE_EXPIRED.value
                        if expired_redelivery
                        else FailureReason.NONE.value
                    ),
                    now,
                    now,
                ),
            )
            old = self._conn.execute(
                "SELECT fence FROM leases WHERE task_id = ? AND grp = ?",
                (task_id, group),
            ).fetchone()
            fence = (old["fence"] if old else 0) + 1
            self._conn.execute(
                """INSERT INTO leases (task_id, grp, consumer_id, fence, expires_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(task_id, grp) DO UPDATE SET
                       consumer_id = excluded.consumer_id,
                       fence = excluded.fence,
                       expires_at = excluded.expires_at""",
                (task_id, group, consumer_id, fence, now + lease_ttl),
            )
            task = self._row_to_task(row)
            state = self._row_to_state(self._get_state_row(task_id, group))
            return task, state, fence

    def ack(self, task_id: str, group: str, fence: int, now: float) -> None:
        """确认成功：校验该组租约后仅更新该组状态并推进该组位点。"""
        with self._lock, self._conn:
            self._check_lease(task_id, group, fence, now)
            self._conn.execute(
                """UPDATE group_task_state
                   SET status = ?, failure_reason = ?, last_error = NULL,
                       updated_at = ?
                   WHERE task_id = ? AND grp = ?""",
                (
                    TaskStatus.SUCCEEDED.value,
                    FailureReason.NONE.value,
                    now,
                    task_id,
                    group,
                ),
            )
            self._conn.execute(
                "DELETE FROM leases WHERE task_id = ? AND grp = ?",
                (task_id, group),
            )
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
        """确认业务失败：仅作用于该组，重排可见时间或进入该组死信。"""
        with self._lock, self._conn:
            self._check_lease(task_id, group, fence, now)
            status = TaskStatus.DEAD if dead else TaskStatus.FAILED_RETRYABLE
            self._conn.execute(
                """UPDATE group_task_state
                   SET status = ?, attempts = attempts + 1,
                       failure_reason = ?, last_error = ?, visible_at = ?,
                       updated_at = ?
                   WHERE task_id = ? AND grp = ?""",
                (status.value, reason, error, visible_at, now, task_id, group),
            )
            self._conn.execute(
                "DELETE FROM leases WHERE task_id = ? AND grp = ?",
                (task_id, group),
            )
            if dead:
                self._advance_offset_locked(group)

    # ---- 位点 ----
    def _advance_offset_locked(self, group: str) -> None:
        """推进该组位点到已连续终结（成功或死信）的最大任务 seq。"""
        row = self._conn.execute(
            "SELECT offset FROM offsets WHERE grp = ?", (group,)
        ).fetchone()
        current = row["offset"] if row else 0
        nxt = self._conn.execute(
            """SELECT MIN(t.seq) AS gap FROM tasks t
               LEFT JOIN group_task_state s
                      ON s.task_id = t.task_id AND s.grp = ?
               WHERE t.seq > ?
                 AND COALESCE(s.status, ?) NOT IN (?, ?)""",
            (
                group,
                current,
                TaskStatus.PENDING.value,
                TaskStatus.SUCCEEDED.value,
                TaskStatus.DEAD.value,
            ),
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
