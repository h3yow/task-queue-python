"""任务队列门面：生产、消费、重试与死信。"""

from __future__ import annotations

import time
import uuid

from taskqueue.errors import TaskNotFoundError, normalize_error
from taskqueue.models import FailureReason, Task, TaskStatus
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

        返回 (任务, 是否新建)。
        """
        if not idempotency_key:
            raise ValueError("idempotency_key must not be empty")
        now = self._clock()
        task = Task(
            task_id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            payload=payload,
            max_attempts=max_attempts or self._retry.max_attempts,
            visible_at=now,
            created_at=now,
            updated_at=now,
        )
        created = self._store.insert_task(task)
        if not created:
            existing = self._store.get_task_by_key(idempotency_key)
            assert existing is not None
            return existing, False
        return task, True

    def lease(self, group: str, consumer_id: str) -> tuple[Task, int] | None:
        return self._store.acquire(group, consumer_id, self._clock(), self._lease_ttl)

    def ack(self, task_id: str, group: str, fence: int) -> None:
        self._store.ack(task_id, group, fence, self._clock())

    def fail(self, task_id: str, group: str, fence: int, error: Exception) -> None:
        """归一化异常并按策略重试或进入死信。"""
        err = normalize_error(error)
        task = self._store.get_task(task_id)
        if task is None:
            raise TaskNotFoundError(f"task not found: {task_id}")
        attempts = task.attempts + 1
        limit = task.max_attempts
        dead = attempts >= limit
        now = self._clock()
        visible_at = now if dead else now + self._retry.delay_for_attempt(attempts)
        reason = (
            FailureReason.MAX_ATTEMPTS_EXCEEDED if dead else FailureReason.HANDLER_ERROR
        )
        self._store.fail(
            task_id, group, fence, now, visible_at, dead, reason.value, err.message
        )

    def get_task(self, task_id: str) -> Task:
        task = self._store.get_task(task_id)
        if task is None:
            raise TaskNotFoundError(f"task not found: {task_id}")
        return task

    def dead_letters(self) -> list[Task]:
        return self._store.dead_letters()

    def get_offset(self, group: str) -> int:
        return self._store.get_offset(group)

    def commit_offset(self, group: str, offset: int) -> None:
        self._store.commit_offset(group, offset)
