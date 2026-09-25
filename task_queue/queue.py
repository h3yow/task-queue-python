"""任务队列门面：生产、消费、重试退避与死信处理。"""

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Self

from .errors import (
    HandlerNotFoundError,
    LeaseExpiredError,
    PoisonTaskError,
    QueueClosedError,
)
from .logging_utils import configure_logging, get_logger, log_decision
from .models import DeadLetterReason, QueueConfig, Task
from .store import SQLiteStore

# 处理器接收 (task, attempt)，返回 None；失败抛任意异常。
TaskHandler = Callable[[Task, int], None]


@dataclass(slots=True, frozen=True)
class SubmitResult:
    """提交结果：任务本体 + 是否为本次新建（False 表示幂等命中）。"""

    task: Task
    created: bool

    def __getattr__(self, name: str) -> Any:
        # 便捷代理：result.id / result.task_type 等直接转发到任务本体。
        return getattr(self.task, name)


def normalize_exception(exc: BaseException) -> str:
    """把任意底层异常归一化为可解释、可区分的错误消息（不泄露内部栈）。"""
    return f"{type(exc).__name__}: {exc}"


class TaskQueue:
    """对外的任务队列接口。"""

    def __init__(
        self,
        store: SQLiteStore,
        config: QueueConfig | None = None,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.store = store
        self.config = config or QueueConfig()
        self._clock = clock
        self._handlers: dict[str, TaskHandler] = {}
        self._closed = False
        self._close_lock = threading.Lock()
        self._logger = get_logger("task_queue.queue")

    # ---- 生命周期 -------------------------------------------------------
    def start(self) -> None:
        configure_logging()
        self.store.initialize()

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        self.store.close()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def closed(self) -> bool:
        return self._closed

    def _ensure_open(self) -> None:
        if self._closed:
            raise QueueClosedError("task queue has been closed")

    # ---- 处理器注册 ------------------------------------------------------
    def register_handler(self, task_type: str, handler: TaskHandler) -> None:
        if not callable(handler):
            raise TypeError(f"handler for {task_type!r} must be callable")
        self._handlers[task_type] = handler

    def get_handler(self, task_type: str) -> TaskHandler:
        handler = self._handlers.get(task_type)
        if handler is None:
            raise HandlerNotFoundError(task_type)
        return handler

    # ---- 生产 -----------------------------------------------------------
    def submit(
        self,
        idempotency_key: str,
        task_type: str,
        payload: dict[str, Any] | None = None,
        groups: list[str] | str | None = None,
    ) -> SubmitResult:
        """提交任务；相同幂等键返回同一任务且不产生重复投递/副作用。"""
        self._ensure_open()
        if not idempotency_key:
            raise ValueError("idempotency_key must be a non-empty string")
        if groups is None:
            group_list = ["default"]
        elif isinstance(groups, str):
            group_list = [groups]
        else:
            group_list = list(groups)
        if not group_list:
            raise ValueError("at least one consumer group is required")

        task_id = uuid.uuid4().hex
        task, created = self.store.submit_task(
            task_id=task_id,
            idempotency_key=idempotency_key,
            task_type=task_type,
            payload=payload or {},
            groups=group_list,
        )
        log_decision(
            self._logger,
            task.id,
            attempt=0,
            decision="submitted" if created else "dedup_hit",
            reason="new task accepted" if created else "idempotency key already seen",
            idempotency_key=idempotency_key,
            groups=group_list,
            task_type=task_type,
        )
        return SubmitResult(task=task, created=created)

    # ---- 消费 -----------------------------------------------------------
    def poll_and_process(
        self,
        group: str,
        consumer: str,
        max_tasks: int = 1,
    ) -> int:
        """领取并处理一批任务，返回成功处理的任务数。"""
        self._ensure_open()
        now = self._clock()
        deliveries = self.store.fetch_available(
            group=group,
            consumer=consumer,
            now=now,
            lease_seconds=self.config.lease_seconds,
            limit=max_tasks,
        )
        succeeded = 0
        for delivery in deliveries:
            if self._process_one(group, consumer, delivery):
                succeeded += 1
        return succeeded

    def _process_one(self, group: str, consumer: str, delivery: Any) -> bool:
        """处理单个投递；返回是否成功。所有判定均打印决策日志。"""
        task = self.store.get_task(delivery.task_id)
        attempt = delivery.attempt
        now = self._clock()

        handler = self._handlers.get(task.task_type)
        if handler is None:
            # 确定性永久失败：缺少处理器，直接进死信。
            message = f"HandlerNotFoundError: no handler registered for {task.task_type!r}"
            self.store.mark_dead(
                group, task.id, consumer, attempt,
                DeadLetterReason.POISON_TASK, message, now,
            )
            log_decision(
                self._logger, task.id, attempt, "dead",
                "no handler registered for task type",
                group=group, consumer=consumer, dead_reason=DeadLetterReason.POISON_TASK.value,
            )
            return False

        try:
            handler(task, attempt)
        except PoisonTaskError as exc:
            # 处理器显式声明不可重试。
            message = normalize_exception(exc)
            self.store.mark_dead(
                group, task.id, consumer, attempt,
                DeadLetterReason.POISON_TASK, message, self._clock(),
            )
            log_decision(
                self._logger, task.id, attempt, "dead",
                "handler raised PoisonTaskError; no retry",
                group=group, consumer=consumer, dead_reason=DeadLetterReason.POISON_TASK.value,
                error=message,
            )
            return False
        except Exception as exc:  # noqa: BLE001 - 边界：归一化所有底层异常
            return self._handle_failure(group, consumer, task, attempt, exc)

        # 处理器成功：只有租约仍属于本消费者才能写入结果。
        try:
            checkpoint = self.store.complete(group, task.id, consumer, self._clock())
        except LeaseExpiredError:
            log_decision(
                self._logger, task.id, attempt, "result_discarded",
                "lease expired while processing; another consumer owns it now",
                group=group, consumer=consumer,
            )
            return False
        log_decision(
            self._logger, task.id, attempt, "succeeded",
            "handler returned without error",
            group=group, consumer=consumer, checkpoint=checkpoint,
        )
        return True

    def _handle_failure(
        self,
        group: str,
        consumer: str,
        task: Task,
        attempt: int,
        exc: BaseException,
    ) -> bool:
        policy = self.config.retry_policy
        message = normalize_exception(exc)
        now = self._clock()

        if attempt >= policy.max_attempts:
            try:
                self.store.mark_dead(
                    group, task.id, consumer, attempt,
                    DeadLetterReason.MAX_RETRIES_EXCEEDED, message, now,
                )
            except LeaseExpiredError:
                log_decision(
                    self._logger, task.id, attempt, "result_discarded",
                    "lease expired before failure could be recorded",
                    group=group, consumer=consumer,
                )
                return False
            log_decision(
                self._logger, task.id, attempt, "dead",
                f"attempt {attempt}/{policy.max_attempts} failed; retries exhausted",
                group=group, consumer=consumer,
                dead_reason=DeadLetterReason.MAX_RETRIES_EXCEEDED.value, error=message,
            )
            return False

        delay = policy.delay_for_attempt(attempt)
        visible_at = now + delay
        try:
            self.store.mark_retrying(
                group, task.id, consumer, attempt, visible_at, message, now,
            )
        except LeaseExpiredError:
            log_decision(
                self._logger, task.id, attempt, "result_discarded",
                "lease expired before failure could be recorded",
                group=group, consumer=consumer,
            )
            return False
        log_decision(
            self._logger, task.id, attempt, "retry",
            f"attempt {attempt}/{policy.max_attempts} failed; backing off",
            group=group, consumer=consumer, next_attempt=attempt + 1,
            next_delay_seconds=round(delay, 6), visible_at=round(visible_at, 6),
            error=message,
        )
        return False

    def reclaim_expired(self, group: str) -> int:
        """回收租约到期任务，返回回收条数。"""
        self._ensure_open()
        reclaimed = self.store.reclaim_expired(group, self._clock())
        for delivery in reclaimed:
            log_decision(
                self._logger, delivery.task_id, delivery.attempt, "reclaimed",
                "lease expired before consumer reported a result; redelivering",
                group=group,
            )
        return len(reclaimed)

    # ---- 位点 -----------------------------------------------------------
    def commit(self, group: str, seq: int, consumer: str) -> int:
        self._ensure_open()
        return self.store.commit_checkpoint(group, seq, consumer)

    def checkpoint(self, group: str) -> int:
        self._ensure_open()
        return self.store.get_checkpoint(group)

    # ---- 观测 -----------------------------------------------------------
    def stats(self, group: str) -> dict[str, Any]:
        self._ensure_open()
        counts = self.store.count_by_status(group)
        dead = [
            {
                "task_id": d.task_id,
                "attempt": d.attempt,
                "reason": d.dead_reason.value if d.dead_reason else None,
                "error": d.last_error,
            }
            for d in self.store.list_dead_letters(group)
        ]
        return {
            "group": group,
            "checkpoint": self.store.get_checkpoint(group),
            "counts": {status.value: n for status, n in counts.items()},
            "dead_letters": dead,
        }
