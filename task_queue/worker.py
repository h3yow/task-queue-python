"""并发消费者工作线程。"""

from __future__ import annotations

import threading

from .errors import QueueClosedError
from .logging_utils import get_logger, log_decision
from .queue import TaskQueue


class ConsumerWorker:
    """持续轮询并处理任务的后台线程。

    多个 worker 使用相同 group 时共同分摊任务；不同 group 各自独立消费。
    领取动作在存储层以原子条件更新完成，因此同一任务同一时刻只会被
    一个消费者执行。每轮在处理前先回收租约到期的任务。
    """

    def __init__(
        self,
        queue: TaskQueue,
        group: str,
        consumer: str,
        *,
        poll_interval_seconds: float = 0.05,
        batch_size: int = 1,
        reclaim_interval_polls: int = 1,
    ) -> None:
        self.queue = queue
        self.group = group
        self.consumer = consumer
        self.poll_interval_seconds = poll_interval_seconds
        self.batch_size = batch_size
        self.reclaim_interval_polls = reclaim_interval_polls
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._poll_count = 0
        self.processed = 0
        self._logger = get_logger(f"task_queue.worker.{consumer}")

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run, name=f"consumer-{self.consumer}", daemon=True
        )
        self._thread.start()
        log_decision(
            self._logger, "-", 0, "worker_started",
            "background consumer thread started",
            group=self.group, consumer=self.consumer,
        )

    def stop(self, timeout: float | None = 5.0) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)

    def is_alive(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def run_once(self) -> int:
        """执行一轮：先回收过期租约，再领取并处理一批。"""
        self._poll_count += 1
        if (
            self.reclaim_interval_polls > 0
            and self._poll_count % self.reclaim_interval_polls == 0
        ):
            n = self.queue.reclaim_expired(self.group)
            if n:
                log_decision(
                    self._logger, "-", 0, "reclaim_round",
                    "expired leases reclaimed before polling",
                    group=self.group, reclaimed=n,
                )
        n = self.queue.poll_and_process(self.group, self.consumer, self.batch_size)
        self.processed += n
        return n

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                processed = self.run_once()
            except QueueClosedError:
                # 队列被正常关闭：退出循环，不当作错误。
                self._stop_event.set()
                break
            except Exception as exc:  # noqa: BLE001 - 线程不能因单条异常退出
                self._logger.error(
                    "consumer loop error group=%s consumer=%s error=%r",
                    self.group,
                    self.consumer,
                    f"{type(exc).__name__}: {exc}",
                )
                processed = 0
            # 没领到任务时退避轮询间隔；领到则立即再试。
            if processed == 0:
                self._stop_event.wait(self.poll_interval_seconds)
        log_decision(
            self._logger, "-", 0, "worker_stopped",
            "stop event set; consumer thread exiting",
            group=self.group, consumer=self.consumer, processed=self.processed,
        )
