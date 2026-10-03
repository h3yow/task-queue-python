"""消费者：租约内执行任务，异常归一化，租约到期可被重新投递。"""

from __future__ import annotations

import logging
from typing import Callable

from taskqueue.errors import LeaseConflictError
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
        """领取并处理一个任务，返回是否有任务被处理（领取到即算）。"""
        leased = self._queue.lease(self._group, self._consumer_id)
        if leased is None:
            return False
        gt, fence = leased
        if gt.redelivered:
            # 租约到期重投不算业务失败，也不增加尝试次数
            logger.info(
                "task=%s group=%s attempt=%d decision=redeliver basis=LEASE_EXPIRED",
                gt.task_id, self._group, gt.attempts + 1,
            )
        else:
            logger.info(
                "task=%s group=%s attempt=%d decision=lease",
                gt.task_id, self._group, gt.attempts + 1,
            )
        try:
            self._handler(gt.payload)
        except Exception as exc:  # 归一化在 queue.fail 内完成
            try:
                self._queue.fail(gt.task_id, self._group, fence, exc)
            except LeaseConflictError:
                # 租约已失效（如处理过慢被重投），本次结算作废，不重复计数
                logger.info(
                    "task=%s group=%s attempt=%d decision=skip "
                    "basis=LEASE_CONFLICT error=%s",
                    gt.task_id, self._group, gt.attempts + 1, exc,
                )
                return True
            settled = self._queue.get_group_task(gt.task_id, self._group)
            logger.info(
                "task=%s group=%s attempt=%d decision=%s basis=%s error=%s",
                gt.task_id, self._group, gt.attempts + 1,
                "dead" if settled.status.value == "DEAD" else "retry",
                settled.failure_reason.value, exc,
            )
        else:
            try:
                self._queue.ack(gt.task_id, self._group, fence)
            except LeaseConflictError:
                logger.info(
                    "task=%s group=%s attempt=%d decision=skip "
                    "basis=LEASE_CONFLICT",
                    gt.task_id, self._group, gt.attempts + 1,
                )
                return True
            logger.info(
                "task=%s group=%s attempt=%d decision=ack basis=HANDLER_OK",
                gt.task_id, self._group, gt.attempts + 1,
            )
        return True
