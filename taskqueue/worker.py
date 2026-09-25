"""消费者：租约内执行任务，异常归一化，租约到期可被重新投递。"""

from __future__ import annotations

import logging
from typing import Callable

from taskqueue.queue import TaskQueue

logger = logging.getLogger("taskqueue.worker")


class Worker:
    def __init__(
        self,
        queue: TaskQueue,
        group: str,
        consumer_id: str,
        handler: Callable[[str], None],
    ) -> None:
        self._queue = queue
        self._group = group
        self._consumer_id = consumer_id
        self._handler = handler

    def run_once(self) -> bool:
        """领取并处理一个任务，返回是否有任务被处理。"""
        leased = self._queue.lease(self._group, self._consumer_id)
        if leased is None:
            return False
        task, fence = leased
        try:
            self._handler(task.payload)
        except Exception as exc:  # 归一化在 queue.fail 内完成
            self._queue.fail(task.task_id, self._group, fence, exc)
            logger.info(
                "task=%s attempt=%d decision=fail error=%s",
                task.task_id, task.attempts + 1, exc,
            )
        else:
            self._queue.ack(task.task_id, self._group, fence)
            logger.info(
                "task=%s attempt=%d decision=ack",
                task.task_id, task.attempts + 1,
            )
        return True
