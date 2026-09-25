"""租约互斥、租约超时回收与进程/消费者中断场景。"""

from __future__ import annotations

import logging

from task_queue.errors import LeaseExpiredError
from task_queue.models import QueueConfig, TaskStatus
from tests.conftest import make_retry_policy


def test_same_task_cannot_be_claimed_by_two_consumers(make_queue, clock) -> None:
    config = QueueConfig(lease_seconds=30, retry_policy=make_retry_policy())
    queue = make_queue(config)
    queue.submit("k1", "t", {}, groups="g1")

    first = queue.store.fetch_available("g1", "consumer-A", clock(), 30, 1)
    assert len(first) == 1 and first[0].lease_owner == "consumer-A"

    # 租约有效期间，第二个消费者无法领取同一任务。
    second = queue.store.fetch_available("g1", "consumer-B", clock(), 30, 1)
    assert second == []


def test_lease_expired_task_is_reclaimable_and_redelivered(make_queue, clock) -> None:
    config = QueueConfig(lease_seconds=10, retry_policy=make_retry_policy())
    queue = make_queue(config)
    task = queue.submit("k2", "t", {}, groups="g1")

    claimed = queue.store.fetch_available("g1", "consumer-A", clock(), 10, 1)
    assert claimed[0].status == TaskStatus.INFLIGHT

    # 未到期：不回收。
    clock.advance(5)
    assert queue.reclaim_expired("g1") == 0
    assert queue.store.get_delivery("g1", task.id).lease_owner == "consumer-A"

    # 到期：回收后立即可被其他消费者领取，任务不丢、尝试次数不额外增加。
    clock.advance(6)
    assert queue.reclaim_expired("g1") == 1
    delivery = queue.store.get_delivery("g1", task.id)
    assert delivery.lease_owner is None
    assert delivery.status == TaskStatus.RETRYING
    assert delivery.attempt == 1  # 回收不增加尝试次数

    redelivered = queue.store.fetch_available("g1", "consumer-B", clock(), 10, 1)
    assert len(redelivered) == 1
    assert redelivered[0].lease_owner == "consumer-B"
    assert redelivered[0].attempt == 2  # 再次领取才算新的尝试


def test_late_result_from_old_owner_is_rejected(make_queue, clock) -> None:
    """原消费者在租约到期后才回报结果，结果必须被拒绝而不是覆盖新状态。"""
    config = QueueConfig(lease_seconds=10, retry_policy=make_retry_policy())
    queue = make_queue(config)
    task = queue.submit("k3", "t", {}, groups="g1")

    queue.store.fetch_available("g1", "consumer-A", clock(), 10, 1)
    clock.advance(11)  # A 的租约到期
    queue.reclaim_expired("g1")
    # B 接管并成功处理。
    queue.store.fetch_available("g1", "consumer-B", clock(), 10, 1)
    queue.store.complete("g1", task.id, "consumer-B", clock())

    # A 迟到的“成功”上报必须被拒绝。
    try:
        queue.store.complete("g1", task.id, "consumer-A", clock())
    except LeaseExpiredError as exc:
        assert exc.task_id == task.id and exc.consumer == "consumer-A"
    else:  # pragma: no cover
        raise AssertionError("expected LeaseExpiredError")

    assert queue.store.get_delivery("g1", task.id).status == TaskStatus.SUCCEEDED


def test_crashed_consumer_task_not_lost_and_finishes_once(make_queue, clock) -> None:
    """消费者在处理中崩溃（未回报）：到期回收后另一消费者完成，最终成功一次。"""
    config = QueueConfig(lease_seconds=10, retry_policy=make_retry_policy())
    queue = make_queue(config)
    task = queue.submit("k4", "t", {}, groups="g1")

    executions: list[str] = []
    queue.register_handler("t", lambda t, attempt: executions.append(f"{attempt}"))

    # consumer-A 领取（模拟崩溃：只领取、不处理回报）。
    queue.store.fetch_available("g1", "consumer-A", clock(), 10, 1)
    clock.advance(11)
    assert queue.reclaim_expired("g1") == 1

    # worker 每轮会先回收再领取；这里直接用 B 完成端到端处理。
    processed = queue.poll_and_process("g1", "consumer-B")
    assert processed == 1
    assert executions  # 副作用发生
    delivery = queue.store.get_delivery("g1", task.id)
    assert delivery.status == TaskStatus.SUCCEEDED
    assert delivery.attempt == 2
    assert queue.checkpoint("g1") == 1


def test_state_survives_reopen_with_file_backed_store(make_queue, db_path, clock) -> None:
    """进程重启：基于文件的存储重新打开后，任务与租约状态完整保留。"""
    config = QueueConfig(lease_seconds=10, retry_policy=make_retry_policy())
    queue = make_queue(config, path=db_path)
    task = queue.submit("k5", "t", {}, groups="g1")
    queue.store.fetch_available("g1", "consumer-A", clock(), 10, 1)
    queue.close()

    # 模拟重启：新 store/queue 指向同一文件。
    from task_queue.queue import TaskQueue
    from task_queue.store import SQLiteStore

    reopened_store = SQLiteStore(db_path)
    reopened = TaskQueue(reopened_store, config, clock=clock)
    reopened.start()

    delivery = reopened.store.get_delivery("g1", task.id)
    assert delivery.status == TaskStatus.INFLIGHT
    assert delivery.lease_owner == "consumer-A"

    clock.advance(11)
    assert reopened.reclaim_expired("g1") == 1  # 重启后仍能回收
    reopened.close()


def test_reclaim_decision_logged(make_queue, clock, caplog) -> None:
    config = QueueConfig(lease_seconds=10, retry_policy=make_retry_policy())
    queue = make_queue(config)
    task = queue.submit("k6", "t", {}, groups="g1")
    queue.store.fetch_available("g1", "A", clock(), 10, 1)
    clock.advance(11)

    with caplog.at_level(logging.INFO, logger="task_queue.queue"):
        queue.reclaim_expired("g1")

    logs = [r.getMessage() for r in caplog.records if "decision=reclaimed" in r.getMessage()]
    assert len(logs) == 1
    assert f"task={task.id}" in logs[0]
    assert "attempt=1" in logs[0]
    assert "reason=" in logs[0]
