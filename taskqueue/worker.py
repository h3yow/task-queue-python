"""消费者：租约内执行任务，异常归一化，租约到期可被重新投递。

支持一次性处理（run_once）与后台自动消费循环（start/stop）。
"""

from __future__ import annotations

import logging
import threading
from typing import Callable

from taskqueue.models import FailureReason, GroupTaskState, Task
from taskqueue.queue import TaskQueue

logger = logging.getLogger("taskqueue.worker")


class Worker:
    def __init__(
        self,
        queue: TaskQueue,
        group: str,
        consumer_id: str,
        handler: Callable[[str], None],
        poll_interval: float = 0.05,
    ) -> None:
        self._queue = queue
        self._group = group
        self._consumer_id = consumer_id
        self._handler = handler
        self._poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def group(self) -> str:
        return self._group

    @property
    def consumer_id(self) -> str:
        return self._consumer_id

    def run_once(self) -> bool:
        """领取并处理一个任务，返回是否有任务被处理。"""
        leased = self._queue.lease(self._group, self._consumer_id)
        if leased is None:
            return False
        task, state, fence = leased
        if state.failure_reason == FailureReason.LEASE_EXPIRED:
            # 上一个消费者掉线导致租约到期重投：尝试次数不累加，也不算业务失败
            logger.info(
                "task=%s group=%s attempt=%d decision=redeliver basis=lease_expired "
                "consumer=%s",
                task.task_id, self._group, state.attempts + 1, self._consumer_id,
            )
        try:
            self._handler(task.payload)
        except Exception as exc:  # 归一化在 queue.fail 内完成
            self._settle_fail(task, state, fence, exc)
        else:
            self._settle_ack(task, state, fence)
        return True

    def _settle_ack(
        self, task: Task, state: GroupTaskState, fence: int
    ) -> None:
        self._queue.ack(task.task_id, self._group, fence)
        logger.info(
            "task=%s group=%s attempt=%d decision=ack basis=handler_succeeded "
            "consumer=%s",
            task.task_id, self._group, state.attempts + 1, self._consumer_id,
        )

    def _settle_fail(
        self, task: Task, state: GroupTaskState, fence: int, exc: Exception
    ) -> None:
        attempt = state.attempts + 1
        dead = attempt >= state.max_attempts
        self._queue.fail(task.task_id, self._group, fence, exc)
        decision = "dead_letter" if dead else "retry"
        basis = (
            "max_attempts_exceeded"
            if dead
            else "handler_error"
        )
        logger.info(
            "task=%s group=%s attempt=%d decision=%s basis=%s error=%s consumer=%s",
            task.task_id, self._group, attempt, decision, basis, exc,
            self._consumer_id,
        )

    # ---- 后台自动消费 ----
    def start(self) -> None:
        """启动后台消费线程：服务启动后自动把提交进来的任务消费掉。"""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name=f"worker-{self._group}-{self._consumer_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float | None = None) -> None:
        """通知停止并等待消费线程退出。"""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                processed = self.run_once()
            except Exception:
                logger.exception(
                    "group=%s consumer=%s worker loop error",
                    self._group, self._consumer_id,
                )
                processed = False
            except BaseException:
                # 消费者进程级崩溃（SystemExit/KeyboardInterrupt）：本线程退出，
                # 租约到期后任务会由其他消费者重新接管，不丢任务
                logger.warning(
                    "group=%s consumer=%s crashed; lease will expire and redeliver",
                    self._group, self._consumer_id,
                )
                return
            if not processed:
                self._stop.wait(self._poll_interval)
