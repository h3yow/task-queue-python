"""基于本地 SQLite 的持久化存储：任务、按组处理状态、租约与位点。"""

from __future__ import annotations

import sqlite3
import threading

from taskqueue.errors import LeaseConflictError, OffsetRegressionError
from taskqueue.models import FailureReason, GroupTask, Task, TaskStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload         TEXT NOT NULL,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    created_at      REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS group_tasks (
    task_id        TEXT NOT NULL,
    grp            TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    status         TEXT NOT NULL DEFAULT 'PENDING',
    attempts       INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 3,
    failure_reason TEXT NOT NULL DEFAULT 'NONE',
    last_error     TEXT,
    visible_at     REAL NOT NULL DEFAULT 0,
    fence          INTEGER NOT NULL DEFAULT 0,
    created_at     REAL NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_group_tasks_grp_status
    ON group_tasks (grp, status);
CREATE INDEX IF NOT EXISTS idx_leases_grp_expiry
    ON leases (grp, expires_at);
"""

_TERMINAL = (TaskStatus.SUCCEEDED.value, TaskStatus.DEAD.value)


class TaskStore:
    """SQLite 单文件存储，进程重启后任务与各组进度不丢失。"""

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
            created_at=row["created_at"],
        )

    def _row_to_group_task(self, row: sqlite3.Row, *, redelivered: bool = False) -> GroupTask:
        return GroupTask(
            task_id=row["task_id"],
            grp=row["grp"],
            seq=row["seq"],
            status=TaskStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            failure_reason=FailureReason(row["failure_reason"]),
            last_error=row["last_error"],
            visible_at=row["visible_at"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            redelivered=redelivered,
            payload=row["payload"] if "payload" in row.keys() else "",
        )

    def _check_lease(self, task_id: str, group: str, fence: int, now: float) -> None:
        row = self._conn.execute(
            "SELECT * FROM leases WHERE task_id = ? AND grp = ?",
            (task_id, group),
        ).fetchone()
        if (
            row is None
            or row["fence"] != fence
            or row["expires_at"] <= now
        ):
            raise LeaseConflictError(
                f"lease mismatch for task {task_id} group {group}: "
                f"expected fence={fence}"
            )

    # ---- 任务（共享元数据）----
    def insert_task(self, task: Task, max_attempts: int) -> bool:
        """按幂等键去重插入；返回是否真正新建。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO tasks
                   (task_id, idempotency_key, payload, max_attempts, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    task.task_id,
                    task.idempotency_key,
                    task.payload,
                    max_attempts,
                    task.created_at,
                ),
            )
            return cur.rowcount == 1

    def get_task(self, task_id: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            return self._row_to_task(row) if row else None

    def get_task_by_key(self, idempotency_key: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            return self._row_to_task(row) if row else None

    # ---- 消费组处理状态 ----
    def get_group_task(self, task_id: str, grp: str) -> GroupTask | None:
        with self._lock:
            row = self._conn.execute(
                """SELECT gt.*, t.payload FROM group_tasks gt
                   JOIN tasks t ON t.task_id = gt.task_id
                   WHERE gt.task_id = ? AND gt.grp = ?""",
                (task_id, grp),
            ).fetchone()
            return self._row_to_group_task(row) if row else None

    def dead_letters(self, grp: str) -> list[GroupTask]:
        """按组查询死信：一个组判死不影响其他组领取。"""
        with self._lock:
            rows = self._conn.execute(
                """SELECT gt.*, t.payload FROM group_tasks gt
                   JOIN tasks t ON t.task_id = gt.task_id
                   WHERE gt.grp = ? AND gt.status = ?
                   ORDER BY gt.seq""",
                (grp, TaskStatus.DEAD.value),
            ).fetchall()
            return [self._row_to_group_task(r) for r in rows]

    # ---- 租约 ----
    def acquire(
        self, group: str, consumer_id: str, now: float, lease_ttl: float
    ) -> tuple[GroupTask, int] | None:
        """为消费组领取下一个可处理任务，返回组状态（含 payload）与 fence。

        可领取：
        - 该组还没见过的任务（LEFT JOIN 缺失，领取时惰性建组状态行）；
        - PENDING / FAILED_RETRYABLE 且 visible_at 到期；
        - LEASED 但租约已过期（消费者掉线重投）。
        同一 (任务, 组) 同一时刻只有一个租约；fence 单调递增。
        租约到期重投不增加 attempts，failure_reason 记为 LEASE_EXPIRED，
        与业务失败严格区分。
        """
        with self._lock, self._conn:
            row = self._conn.execute(
                """SELECT t.seq AS seq, t.task_id AS task_id, t.payload AS payload,
                          t.max_attempts AS task_max_attempts, t.created_at AS task_created_at,
                          gt.status AS status, gt.attempts AS attempts,
                          gt.max_attempts AS max_attempts,
                          gt.failure_reason AS failure_reason, gt.last_error AS last_error,
                          gt.visible_at AS visible_at, gt.fence AS old_fence,
                          gt.created_at AS gt_created_at, gt.updated_at AS gt_updated_at,
                          l.expires_at AS lease_expires
                   FROM tasks t
                   LEFT JOIN group_tasks gt
                       ON gt.task_id = t.task_id AND gt.grp = ?
                   LEFT JOIN leases l
                       ON l.task_id = t.task_id AND l.grp = ?
                   WHERE gt.task_id IS NULL
                      OR (gt.status IN (?, ?) AND gt.visible_at <= ?)
                      OR (gt.status = ? AND l.expires_at <= ?)
                   ORDER BY t.seq LIMIT 1""",
                (
                    group,
                    group,
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
            fresh = row["status"] is None
            expired = (not fresh) and row["status"] == TaskStatus.LEASED.value
            old_fence = row["old_fence"] or 0
            fence = old_fence + 1

            if fresh:
                # 惰性为该组建立独立处理进度
                self._conn.execute(
                    """INSERT INTO group_tasks
                       (task_id, grp, seq, status, attempts, max_attempts,
                        failure_reason, last_error, visible_at, fence,
                        created_at, updated_at)
                       VALUES (?, ?, ?, ?, 0, ?, 'NONE', NULL, ?, ?, ?, ?)""",
                    (
                        task_id, group, row["seq"], TaskStatus.LEASED.value,
                        row["task_max_attempts"], now, fence, now, now,
                    ),
                )
            else:
                if expired:
                    # 租约到期重投：只改状态/原因，不动 attempts
                    self._conn.execute(
                        """UPDATE group_tasks
                           SET status = ?, failure_reason = ?, fence = ?,
                               updated_at = ?
                           WHERE task_id = ? AND grp = ?""",
                        (
                            TaskStatus.LEASED.value,
                            FailureReason.LEASE_EXPIRED.value,
                            fence, now, task_id, group,
                        ),
                    )
                else:
                    self._conn.execute(
                        """UPDATE group_tasks
                           SET status = ?, fence = ?, updated_at = ?
                           WHERE task_id = ? AND grp = ?""",
                        (TaskStatus.LEASED.value, fence, now, task_id, group),
                    )

            self._conn.execute(
                """INSERT INTO leases (task_id, grp, consumer_id, fence, expires_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(task_id, grp) DO UPDATE SET
                       consumer_id = excluded.consumer_id,
                       fence = excluded.fence,
                       expires_at = excluded.expires_at""",
                (task_id, group, consumer_id, fence, now + lease_ttl),
            )

            gt = self._conn.execute(
                """SELECT gt.*, t.payload FROM group_tasks gt
                   JOIN tasks t ON t.task_id = gt.task_id
                   WHERE gt.task_id = ? AND gt.grp = ?""",
                (task_id, group),
            ).fetchone()
            return self._row_to_group_task(gt, redelivered=expired), fence

    def ack(self, task_id: str, group: str, fence: int, now: float) -> None:
        """确认成功：校验租约后按组标记成功并推进该组位点。"""
        with self._lock, self._conn:
            self._check_lease(task_id, group, fence, now)
            self._conn.execute(
                """UPDATE group_tasks SET status = ?, failure_reason = ?,
                   last_error = NULL, updated_at = ?
                   WHERE task_id = ? AND grp = ?""",
                (
                    TaskStatus.SUCCEEDED.value, FailureReason.NONE.value,
                    now, task_id, group,
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
        """确认业务失败：校验租约后按组退避重排或进入死信。"""
        with self._lock, self._conn:
            self._check_lease(task_id, group, fence, now)
            status = TaskStatus.DEAD if dead else TaskStatus.FAILED_RETRYABLE
            self._conn.execute(
                """UPDATE group_tasks SET status = ?, attempts = attempts + 1,
                   failure_reason = ?, last_error = ?, visible_at = ?, updated_at = ?
                   WHERE task_id = ? AND grp = ?""",
                (status.value, reason, error, visible_at, now, task_id, group),
            )
            self._conn.execute(
                "DELETE FROM leases WHERE task_id = ? AND grp = ?",
                (task_id, group),
            )
            if dead:
                self._advance_offset_locked(group)

    # ---- 位点（按组）----
    def _advance_offset_locked(self, group: str) -> None:
        """推进组位点到已连续终结（成功或死信）的最大 seq。"""
        row = self._conn.execute(
            "SELECT offset FROM offsets WHERE grp = ?", (group,)
        ).fetchone()
        current = row["offset"] if row else 0
        nxt = self._conn.execute(
            """SELECT MIN(gt.seq) AS gap FROM group_tasks gt
               WHERE gt.grp = ? AND gt.seq > ?
                 AND gt.status NOT IN (?, ?)""",
            (group, current, *_TERMINAL),
        ).fetchone()["gap"]
        if nxt is None:
            target = self._conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM group_tasks WHERE grp = ?",
                (group,),
            ).fetchone()["m"]
        else:
            target = nxt - 1
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
