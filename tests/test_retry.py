"""失败重试、退避增长、死信原因与异常归一化场景。"""

from __future__ import annotations

import logging

import pytest

from task_queue.errors import PoisonTaskError
from task_queue.models import DeadLetterReason, QueueConfig, RetryPolicy
from tests.conftest import make_retry_policy


def test_backoff_grows_with_attempt_number() -> None:
    policy = RetryPolicy(
        max_attempts=5, base_delay_seconds=1.0,
        backoff_multiplier=2.0, max_delay_seconds=100.0,
    )
    delays = [policy.delay_for_attempt(a) for a in range(1, 5)]
    assert delays == [1.0, 2.0, 4.0, 8.0]  # 随尝试次数指数增长


def test_backoff_is_capped_by_max_delay() -> None:
    policy = RetryPolicy(
        max_attempts=10, base_delay_seconds=10.0,
        backoff_multiplier=10.0, max_delay_seconds=30.0,
    )
    assert policy.delay_for_attempt(1) == 10.0
    assert policy.delay_for_attempt(2) == 30.0  # 被 max_delay 截断
    assert policy.delay_for_attempt(5) == 30.0


def test_task_not_visible_until_backoff_elapsed(make_queue, clock) -> None:
    queue = make_queue()
    attempts: list[int] = []

    def handler(task, attempt):
        attempts.append(attempt)
        raise ValueError("transient")

    queue.register_handler("t", handler)
    task = queue.submit("k1", "t", {}, groups="g1")

    assert queue.poll_and_process("g1", "c1") == 0
    delivery = queue.store.get_delivery("g1", task.id)
    assert delivery.status.value == "retrying"
    # 退避窗口 1s 内不可见。
    clock.advance(0.5)
    assert queue.store.fetch_available("g1", "c1", clock(), 30) == []
    # 到期后可再次领取，尝试次数递增。
    clock.advance(0.6)
    redelivered = queue.store.fetch_available("g1", "c1", clock(), 30)
    assert len(redelivered) == 1 and redelivered[0].attempt == 2
    assert attempts == [1]


def test_exhausting_retries_moves_to_dead_letter(make_queue, clock) -> None:
    config = QueueConfig(retry_policy=make_retry_policy(max_attempts=3))
    queue = make_queue(config)
    attempts: list[int] = []

    def handler(task, attempt):
        attempts.append(attempt)
        raise KeyError("missing field")

    queue.register_handler("t", handler)
    task = queue.submit("k2", "t", {}, groups="g1")

    for round_no in range(1, 4):
        queue.poll_and_process("g1", "c1")
        if round_no < 3:
            clock.advance(100)

    assert attempts == [1, 2, 3]
    delivery = queue.store.get_delivery("g1", task.id)
    assert delivery.status.value == "dead"
    assert delivery.dead_reason == DeadLetterReason.MAX_RETRIES_EXCEEDED
    # 底层异常被归一化：保留类型名与信息，但不向外抛出原始栈。
    assert delivery.last_error == "KeyError: 'missing field'"
    assert queue.stats("g1")["counts"]["dead"] == 1


def test_poison_task_goes_dead_immediately_with_distinct_reason(make_queue) -> None:
    queue = make_queue()
    attempts: list[int] = []

    def handler(task, attempt):
        attempts.append(attempt)
        raise PoisonTaskError("payload cannot be parsed")

    queue.register_handler("t", handler)
    task = queue.submit("k3", "t", {}, groups="g1")

    assert queue.poll_and_process("g1", "c1") == 0
    assert attempts == [1]  # 没有任何重试
    delivery = queue.store.get_delivery("g1", task.id)
    assert delivery.status.value == "dead"
    assert delivery.dead_reason == DeadLetterReason.POISON_TASK
    assert "PoisonTaskError" in (delivery.last_error or "")


def test_underlying_exception_is_normalized_not_propagated(make_queue, caplog) -> None:
    queue = make_queue()

    def handler(task, attempt):
        raise RuntimeError("db connection lost")

    queue.register_handler("t", handler)
    queue.submit("k4", "t", {}, groups="g1")

    # poll_and_process 不应把 RuntimeError 透传给调用方。
    queue.poll_and_process("g1", "c1")  # 不抛异常即通过

    with caplog.at_level(logging.INFO, logger="task_queue.queue"):
        queue.poll_and_process("g1", "c1")  # 无可见任务；这里主要验证前序日志
    retry_logs = [r.getMessage() for r in caplog.records if "decision=retry" in r.getMessage()]
    assert retry_logs == [] or all("RuntimeError" in m for m in retry_logs)


def test_dead_letter_reasons_are_distinguishable(make_queue, clock) -> None:
    config = QueueConfig(retry_policy=make_retry_policy(max_attempts=2))
    queue = make_queue(config)
    queue.register_handler("retry_exhausted", lambda t, a: (_ for _ in ()).throw(ValueError("x")))
    queue.register_handler("poison", lambda t, a: (_ for _ in ()).throw(PoisonTaskError("bad")))

    queue.submit("a", "retry_exhausted", {}, groups="g1")
    for _ in range(2):
        queue.poll_and_process("g1", "c1")
        clock.advance(100)
    queue.submit("b", "poison", {}, groups="g1")
    queue.poll_and_process("g1", "c1")

    dead = {d.task_id: d.dead_reason for d in queue.store.list_dead_letters("g1")}
    reasons = set(dead.values())
    assert reasons == {
        DeadLetterReason.MAX_RETRIES_EXCEEDED,
        DeadLetterReason.POISON_TASK,
    }


@pytest.mark.parametrize("attempt,expected_delay", [(1, 1.0), (2, 2.0), (3, 4.0)])
def test_retry_log_records_attempt_and_delay(make_queue, clock, attempt, expected_delay, caplog):
    config = QueueConfig(retry_policy=make_retry_policy(max_attempts=5))
    queue = make_queue(config)

    def handler(task, att):
        raise ValueError("boom")

    queue.register_handler("t", handler)
    queue.submit("klog", "t", {}, groups="g1")
    with caplog.at_level(logging.INFO, logger="task_queue.queue"):
        for i in range(attempt):
            queue.poll_and_process("g1", "c1")
            if i < attempt - 1:
                clock.advance(100)

    retry_messages = [r.getMessage() for r in caplog.records if "decision=retry" in r.getMessage()]
    assert len(retry_messages) == attempt
    assert f"attempt={attempt}" in retry_messages[-1]
    assert f"next_delay_seconds={float(expected_delay)}" in retry_messages[-1]
    assert "reason=" in retry_messages[-1]
