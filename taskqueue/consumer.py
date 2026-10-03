"""自动消费服务：启动后自行领取并处理提交进来的任务。

每个 ConsumerService 是一个后台消费者线程；同一消费组可以起多个
实例（不同 consumer_id）并发消费。单组单任务同一时刻只会被一个
有效租约持有者执行；消费者崩溃或租约到期后任务会被其他实例重新
领取，不丢任务、不重复计数。
"""

from __future__ import annotations

import logging
import threading
from typing import Callable

from taskqueue.queue import TaskQueue
from taskqueue.worker import Worker

logger = logging.getLogger("taskqueue.consumer")


class ConsumerService:
    def __init__(
        self,
        queue: TaskQueue,
        group: str,
        handler: Callable[[str], None],
        consumer_id: str | None = None,
        poll_interval: float = 0.05,
        idle_sleep: float = 0.05,
    ) -> None:
        self._queue = queue
        self._group = group
        self._handler = handler
        self._consumer_id = consumer_id or f"consumer-{id(self):x}"
        self._poll_interval = poll_interval
        self._idle_sleep = idle_sleep
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._worker = Worker(queue, group, self._consumer_id, handler)

    @property
    def consumer_id(self) -> str:
        return self._consumer_id

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self.run_forever,
            name=f"consumer-{self._group}-{self._consumer_id}",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "consumer started group=%s consumer_id=%s",
            self._group, self._consumer_id,
        )

    def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                handled = self._worker.run_once()
            except Exception:  # 消费循环不能因单次异常退出
                logger.exception(
                    "consumer loop error group=%s consumer_id=%s",
                    self._group, self._consumer_id,
                )
                handled = False
            if not handled:
                self._stop.wait(self._idle_sleep)
            elif self._poll_interval:
                self._stop.wait(self._poll_interval)

    def stop(self, timeout: float | None = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
