"""任务队列门面：生产、按组消费、按组重试与死信。"""

from __future__ import annotations

import time
import uuid

from taskqueue.errors import TaskNotFoundError, normalize_error
from taskqueue.models import FailureReason, GroupTask, Task
from taskqueue.retry import RetryPolicy
from taskqueue.store import TaskStore


class TaskQueue:
    def __init__(
        self,
        store: TaskStore,
        retry_policy: RetryPolicy | None = None,
        lease_ttl: float = 5.0,
        clock=time.time,
    ) -> None:
        self._store = store
        self._retry = retry_policy or RetryPolicy()
        self._lease_ttl = lease_ttl
        self._clock = clock

    def submit(
        self, idempotency_key: str, payload: str, max_attempts: int | None = None
    ) -> tuple[Task, bool]:
        """提交任务；重复幂等键返回既有任务且不产生副作用。

        任务本身对所有消费组共享；各组在首次领取时建立独立处理进度。
        返回 (任务, 是否新建)。
        """
        if not idempotency_key:
            raise ValueError("idempotency_key must not be empty")
        now = self._clock()
        limit = max_attempts or self._retry.max_attempts
        task = Task(
            task_id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            payload=payload,
            created_at=now,
        )
        created = self._store.insert_task(task, limit)
        if not created:
            existing = self._store.get_task_by_key(idempotency_key)
            assert existing is not None
            return existing, False
        return task, True

    def lease(self, group: str, consumer_id: str) -> tuple[GroupTask, int] | None:
        return self._store.acquire(group, consumer_id, self._clock(), self._lease_ttl)

    def ack(self, task_id: str, group: str, fence: int) -> None:
        self._store.ack(task_id, group, fence, self._clock())

    def fail(self, task_id: str, group: str, fence: int, error: Exception) -> None:
        """归一化业务异常并按该组策略重试或进入死信。

        尝试次数按组独立统计；只有业务处理抛异常才计数，
        租约到期重投不计入。
        """
        err = normalize_error(error)
        gt = self._store.get_group_task(task_id, group)
        if gt is None:
            raise TaskNotFoundError(
                f"task not found for group: task={task_id} group={group}"
            )
        attempts = gt.attempts + 1
        dead = attempts >= gt.max_attempts
        now = self._clock()
        visible_at = now if dead else now + self._retry.delay_for_attempt(attempts)
        reason = (
            FailureReason.MAX_ATTEMPTS_EXCEEDED if dead else FailureReason.HANDLER_ERROR
        )
        self._store.fail(
            task_id, group, fence, now, visible_at, dead, reason.value, err.message
        )

    # ---- 查询 ----
    def get_task(self, task_id: str) -> Task:
        task = self._store.get_task(task_id)
        if task is None:
            raise TaskNotFoundError(f"task not found: {task_id}")
        return task

    def get_group_task(self, task_id: str, group: str) -> GroupTask:
        """按组查询处理进度与失败判定（业务异常 / 重试耗尽 / 租约到期）。"""
        gt = self._store.get_group_task(task_id, group)
        if gt is None:
            raise TaskNotFoundError(
                f"task not found for group: task={task_id} group={group}"
            )
        return gt

    def dead_letters(self, group: str) -> list[GroupTask]:
        """按组列出死信。"""
        return self._store.dead_letters(group)

    def get_offset(self, group: str) -> int:
        return self._store.get_offset(group)

    def commit_offset(self, group: str, offset: int) -> None:
        self._store.commit_offset(group, offset)
