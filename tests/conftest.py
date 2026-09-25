"""pytest 公共夹具：可控时钟与队列工厂，保证测试确定性。"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from task_queue.models import QueueConfig, RetryPolicy
from task_queue.queue import TaskQueue
from task_queue.store import SQLiteStore


class FakeClock:
    """可手动推进的时钟，避免真实 sleep。"""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_retry_policy(**overrides: Any) -> RetryPolicy:
    defaults: dict[str, Any] = {
        "max_attempts": 3,
        "base_delay_seconds": 1.0,
        "backoff_multiplier": 2.0,
        "max_delay_seconds": 100.0,
        "jitter_seconds": 0.0,
    }
    defaults.update(overrides)
    return RetryPolicy(**defaults)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def make_queue(clock: FakeClock, tmp_path: Any) -> Callable[..., TaskQueue]:
    created: list[TaskQueue] = []

    def _factory(
        config: QueueConfig | None = None,
        *,
        path: str = ":memory:",
    ) -> TaskQueue:
        store = SQLiteStore(path)
        queue = TaskQueue(store, config or QueueConfig(retry_policy=make_retry_policy()), clock=clock)
        queue.start()
        created.append(queue)
        return queue

    yield _factory
    for queue in created:
        queue.close()


@pytest.fixture
def db_path(tmp_path: Any) -> str:
    return str(tmp_path / "queue.db")
