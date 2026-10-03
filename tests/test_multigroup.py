"""多消费组独立投递：各组各领一次、进度/失败/死信互不干扰。"""

from __future__ import annotations

import logging
import time

from taskqueue import (
    FailureReason,
    RetryPolicy,
    TaskQueue,
    TaskStatus,
    TaskStore,
)

logger = logging.getLogger("test.multigroup")


def log_decision(task_id: str, group: str, attempts: int, basis: str) -> None:
    logger.info(
        "task=%s group=%s attempts=%d basis=%s", task_id, group, attempts, basis
    )


def make_queue(max_attempts: int = 3, lease_ttl: float = 0.05) -> TaskQueue:
    return TaskQueue(
        TaskStore(),
        retry_policy=RetryPolicy(base_delay=0.02, multiplier=2.0,
                                 max_attempts=max_attempts),
        lease_ttl=lease_ttl,
    )


def test_each_group_leases_and_succeeds_independently() -> None:
    q = make_queue()
    task, _ = q.submit("k-multi-1", "p")

    g1, s1, f1 = q.lease("g1", "c1")
    g2, s2, f2 = q.lease("g2", "c2")
    # 两个组都能各自领到同一个任务，且 fence 各自从 1 开始
    assert g1.task_id == task.task_id
    assert g2.task_id == task.task_id
    assert f1 == 1 and f2 == 1
    log_decision(task.task_id, "g1/g2", 1, "both groups leased the same task")

    # g1 先成功，不影响 g2 仍持有有效租约并成功
    q.ack(task.task_id, "g1", f1)
    st1 = q.get_state(task.task_id, "g1")
    st2 = q.get_state(task.task_id, "g2")
    assert st1.status == TaskStatus.SUCCEEDED
    assert st2.status == TaskStatus.LEASED
    log_decision(task.task_id, "g1", st1.attempts, f"ack, g2 remains {st2.status}")

    q.ack(task.task_id, "g2", f2)
    st2 = q.get_state(task.task_id, "g2")
    assert st2.status == TaskStatus.SUCCEEDED
    assert q.get_offset("g1") == 1
    assert q.get_offset("g2") == 1


def test_one_group_dead_letter_does_not_block_other_group() -> None:
    q = make_queue(max_attempts=2)
    task, _ = q.submit("k-multi-dead", "p")

    # g1 连续业务失败两次 -> g1 死信
    for attempt in (1, 2):
        leased = q.lease("g1", "c1")
        assert leased is not None
        _, state, fence = leased
        q.fail(task.task_id, "g1", fence, ValueError(f"g1-boom-{attempt}"))
        state = q.get_state(task.task_id, "g1")
        log_decision(task.task_id, "g1", state.attempts,
                     f"attempt={attempt} status={state.status} reason={state.failure_reason}")
        if attempt < 2:
            time.sleep(q._retry.delay_for_attempt(attempt))
    st1 = q.get_state(task.task_id, "g1")
    assert st1.status == TaskStatus.DEAD
    assert st1.failure_reason == FailureReason.MAX_ATTEMPTS_EXCEEDED
    assert st1.attempts == 2
    assert q.lease("g1", "c1") is None  # g1 死信后不再投递

    # g2 完全不受影响：仍可正常领取并成功，尝试次数从 0 开始
    leased2 = q.lease("g2", "c2")
    assert leased2 is not None and leased2[0].task_id == task.task_id
    _, st2, f2 = leased2
    assert st2.attempts == 0
    assert st2.status == TaskStatus.LEASED
    q.ack(task.task_id, "g2", f2)
    assert q.get_state(task.task_id, "g2").status == TaskStatus.SUCCEEDED
    log_decision(task.task_id, "g2", 0, "g1 dead but g2 acked normally")

    # 死信按组查询互不可见
    assert [s.task_id for s in q.dead_letters("g1")] == [task.task_id]
    assert q.dead_letters("g2") == []
    # 位点各算各的
    assert q.get_offset("g1") == 1
    assert q.get_offset("g2") == 1


def test_failure_reasons_are_distinguished_per_group() -> None:
    q = make_queue(max_attempts=3)
    task, _ = q.submit("k-reasons", "p")

    # g1：业务异常 -> HANDLER_ERROR
    _, _, fence = q.lease("g1", "c1")
    q.fail(task.task_id, "g1", fence, KeyError("missing"))
    st1 = q.get_state(task.task_id, "g1")
    assert st1.failure_reason == FailureReason.HANDLER_ERROR
    assert st1.status == TaskStatus.FAILED_RETRYABLE
    log_decision(task.task_id, "g1", st1.attempts, "handler error -> retryable")

    # g2：消费者掉线，租约到期被重投 -> LEASE_EXPIRED，不算业务失败
    q.lease("g2", "c2")
    assert q.lease("g2", "c3") is None
    time.sleep(0.06)
    _, st2, _ = q.lease("g2", "c3")
    assert st2.failure_reason == FailureReason.LEASE_EXPIRED
    assert st2.attempts == 0
    log_decision(task.task_id, "g2", st2.attempts, "lease expired redelivery, no attempt charged")

    # g3：业务失败打满 -> MAX_ATTEMPTS_EXCEEDED
    for attempt in (1, 2, 3):
        leased = q.lease("g3", "c3")
        assert leased is not None
        _, _, f = leased
        q.fail(task.task_id, "g3", f, RuntimeError("g3-boom"))
        state = q.get_state(task.task_id, "g3")
        if attempt < 3:
            time.sleep(q._retry.delay_for_attempt(attempt))
    st3 = q.get_state(task.task_id, "g3")
    assert st3.failure_reason == FailureReason.MAX_ATTEMPTS_EXCEEDED
    assert st3.status == TaskStatus.DEAD
    log_decision(task.task_id, "g3", st3.attempts, "max attempts exceeded -> dead")

    # 三种口径在同一次按组查询里互不混淆
    reasons = {
        g: q.get_state(task.task_id, g).failure_reason
        for g in ("g1", "g2", "g3")
    }
    assert reasons == {
        "g1": FailureReason.HANDLER_ERROR,
        "g2": FailureReason.LEASE_EXPIRED,
        "g3": FailureReason.MAX_ATTEMPTS_EXCEEDED,
    }


def test_group_offsets_advance_independently_with_gaps() -> None:
    q = make_queue(max_attempts=1)
    t1, _ = q.submit("k-a", "a")
    t2, _ = q.submit("k-b", "b")

    # g1 先处理 t2（通过直接让 t1 留在 PENDING 无法跳过，故先领 t1）
    # 顺序投递：g1 完成 t1；g2 在 t1 上死信后继续 t2
    _, _, f1 = q.lease("g1", "c1")
    q.ack(t1.task_id, "g1", f1)
    assert q.get_offset("g1") == 1
    _, _, f2 = q.lease("g1", "c1")
    q.ack(t2.task_id, "g1", f2)
    assert q.get_offset("g1") == 2

    # g2: t1 一次失败即死信
    _, _, g2f1 = q.lease("g2", "c1")
    q.fail(t1.task_id, "g2", g2f1, RuntimeError("x"))
    assert q.get_offset("g2") == 1  # 死信同样推进位点
    _, st2, g2f2 = q.lease("g2", "c1")
    assert st2.task_id == t2.task_id
    q.ack(t2.task_id, "g2", g2f2)
    assert q.get_offset("g2") == 2
    log_decision("both", "g1/g2", 2, "offsets advanced independently")
