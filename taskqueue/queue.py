"""任务队列门面：生产、按组投递、重试与死信。"""

from __future__ import annotations

import time
import uuid

from taskqueue.errors import TaskNotFoundError, normalize_error
from taskqueue.models import FailureReason, GroupTaskState, Task, TaskStatus
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

        任务对所有消费组共享，每个组首次领取时建立独立处理状态。
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
            created_at=now,
            updated_at=now,
        )
        created = self._store.insert_task(task)
        if not created:
            existing = self._store.get_task_by_key(idempotency_key)
            assert existing is not None
            return existing, False
        return task, True

    def lease(
        self, group: str, consumer_id: str
    ) -> tuple[Task, GroupTaskState, int] | None:
        """为指定消费组领取任务，返回 (任务, 该组处理状态, fencing token)。"""
        return self._store.acquire(group, consumer_id, self._clock(), self._lease_ttl)

    def ack(self, task_id: str, group: str, fence: int) -> None:
        """该组确认成功；不影响其他组。"""
        self._store.ack(task_id, group, fence, self._clock())

    def fail(self, task_id: str, group: str, fence: int, error: Exception) -> None:
        """该组业务失败：归一化异常并按该组尝试次数决定重试或进入该组死信。"""
        if self._store.get_task(task_id) is None:
            raise TaskNotFoundError(f"task not found: {task_id}")
        state = self._store.get_state(task_id, group)
        if state is None:
            raise TaskNotFoundError(
                f"task {task_id} has no state in group {group}"
            )
        err = normalize_error(error)
        attempts = state.attempts + 1
        limit = state.max_attempts
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
        """任务描述本身（组无关）。"""
        task = self._store.get_task(task_id)
        if task is None:
            raise TaskNotFoundError(f"task not found: {task_id}")
        return task

    def get_state(self, task_id: str, group: str) -> GroupTaskState:
        """查询某个组对某任务的独立处理状态（含失败原因口径）。"""
        state = self._store.get_state(task_id, group)
        if state is None:
            raise TaskNotFoundError(
                f"task {task_id} has no state in group {group}"
            )
        return state

    def dead_letters(self, group: str) -> list[GroupTaskState]:
        """该组死信；一个组判死信不影响其他组继续领取。"""
        return self._store.dead_letters(group)

    def get_offset(self, group: str) -> int:
        return self._store.get_offset(group)

    def commit_offset(self, group: str, offset: int) -> None:
        self._store.commit_offset(group, offset)
