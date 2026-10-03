"""任务队列行为测试。

覆盖：多消费组独立投递与独立进度、按组失败/死信口径（业务异常 /
重试耗尽 / 租约到期重投三分开）、自动消费下的并发与租约到期重投、
幂等提交、退避重试、位点校验与进程重启。
"""

from __future__ import annotations

import logging
import threading
import time

import pytest

from taskqueue import (
    ConsumerService,
    FailureReason,
    LeaseConflictError,
    OffsetRegressionError,
    RetryPolicy,
    TaskQueue,
    TaskQueueError,
    TaskStatus,
    TaskStore,
    Worker,
    normalize_error,
)

logger = logging.getLogger("test.taskqueue")


def log_decision(task_id: str, group: str, attempts: int, basis: str) -> None:
    """单测日志：打印任务标识、消费组、尝试次数与判定依据。"""
    logger.info("task=%s group=%s attempts=%d basis=%s", task_id, group, attempts, basis)


@pytest.fixture()
def queue() -> TaskQueue:
    return TaskQueue(
        TaskStore(),
        retry_policy=RetryPolicy(base_delay=0.02, multiplier=2.0, max_attempts=3),
        lease_ttl=0.05,
    )


def _wait_until(condition, timeout: float = 5.0, interval: float = 0.01) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return condition()


# ---- 幂等提交 ----
def test_duplicate_submit_is_deduplicated(queue: TaskQueue) -> None:
    t1, created1 = queue.submit("key-1", "payload")
    t2, created2 = queue.submit("key-1", "payload-changed")
    log_decision(t1.task_id, "-", 0, f"created1={created1} created2={created2}")
    assert created1 is True
    assert created2 is False
    assert t2.task_id == t1.task_id
    assert queue.get_task(t1.task_id).payload == "payload"  # 无重复副作用


def test_empty_idempotency_key_rejected(queue: TaskQueue) -> None:
    with pytest.raises(ValueError):
        queue.submit("", "payload")


# ---- 多消费组独立投递 ----
def test_multiple_groups_each_consume_once_independently(queue: TaskQueue) -> None:
    task, _ = queue.submit("k-multi", "p")

    g1_first, fence_g1 = queue.lease("g1", "c1")
    # g1 已领取未结算期间，g2 仍可独立领取同一任务
    g2_first, fence_g2 = queue.lease("g2", "c2")
    assert g1_first.task_id == task.task_id
    assert g2_first.task_id == task.task_id
    assert fence_g1 == fence_g2 == 1  # 各组 fence 独立计数
    log_decision(task.task_id, "g1", g1_first.attempts, "leased independently")
    log_decision(task.task_id, "g2", g2_first.attempts, "leased independently")

    queue.ack(task.task_id, "g1", fence_g1)
    # g1 成功不影响 g2：租约仍有效，可正常结算
    g2_state_before = queue.get_group_task(task.task_id, "g2")
    assert g2_state_before.status == TaskStatus.LEASED
    queue.ack(task.task_id, "g2", fence_g2)

    g1_final = queue.get_group_task(task.task_id, "g1")
    g2_final = queue.get_group_task(task.task_id, "g2")
    assert g1_final.status == TaskStatus.SUCCEEDED
    assert g2_final.status == TaskStatus.SUCCEEDED
    assert queue.get_offset("g1") == 1
    assert queue.get_offset("g2") == 1
    # 终态后各组都不再投递
    assert queue.lease("g1", "c1") is None
    assert queue.lease("g2", "c2") is None


def test_two_groups_drain_same_batch_with_independent_progress(queue: TaskQueue) -> None:
    n = 10
    ids = [queue.submit(f"k-{i}", f"p-{i}")[0].task_id for i in range(n)]

    def drain(group: str) -> None:
        w = Worker(queue, group, f"{group}-c", lambda _p: None)
        while w.run_once():
            pass

    t1 = threading.Thread(target=drain, args=("g1",))
    t2 = threading.Thread(target=drain, args=("g2",))
    t1.start(); t2.start()
    t1.join(); t2.join()

    for tid in ids:
        s1 = queue.get_group_task(tid, "g1")
        s2 = queue.get_group_task(tid, "g2")
        assert s1.status == TaskStatus.SUCCEEDED and s1.attempts == 0
        assert s2.status == TaskStatus.SUCCEEDED and s2.attempts == 0
    assert queue.get_offset("g1") == n
    assert queue.get_offset("g2") == n


# ---- 按组失败与死信：互不影响 ----
def test_group_dead_letter_does_not_block_other_group(queue: TaskQueue) -> None:
    task, _ = queue.submit("k-dead", "p", max_attempts=2)

    # g1 连续业务失败两次 -> 死信
    for attempt in (1, 2):
        leased = queue.lease("g1", "c1")
        assert leased is not None
        gt, fence = leased
        queue.fail(gt.task_id, "g1", fence, ValueError(f"boom-{attempt}"))
        state = queue.get_group_task(task.task_id, "g1")
        log_decision(task.task_id, "g1", state.attempts,
                     f"attempt={attempt} status={state.status} reason={state.failure_reason}")
        if attempt == 1:
            time.sleep(0.03)  # 等退避窗口过去

    g1_final = queue.get_group_task(task.task_id, "g1")
    assert g1_final.status == TaskStatus.DEAD
    assert g1_final.attempts == 2
    assert g1_final.failure_reason == FailureReason.MAX_ATTEMPTS_EXCEEDED

    # g1 判死后，g2 依然能正常领取并成功
    leased2 = queue.lease("g2", "c2")
    assert leased2 is not None and leased2[0].task_id == task.task_id
    queue.ack(task.task_id, "g2", leased2[1])
    g2_final = queue.get_group_task(task.task_id, "g2")
    assert g2_final.status == TaskStatus.SUCCEEDED
    assert g2_final.attempts == 0

    # 死信按组查询，互不串
    assert [t.task_id for t in queue.dead_letters("g1")] == [task.task_id]
    assert queue.dead_letters("g2") == []
    assert queue.lease("g1", "c1") is None  # 死信对该组不再投递


def test_three_failure_basis_are_distinguished(queue: TaskQueue) -> None:
    # 1) 业务处理抛异常：HANDLER_ERROR，计入尝试次数
    task, _ = queue.submit("k-basis", "p", max_attempts=3)
    gt, fence = queue.lease("g", "c1")
    queue.fail(task.task_id, "g", fence, ValueError("boom"))
    state = queue.get_group_task(task.task_id, "g")
    assert state.status == TaskStatus.FAILED_RETRYABLE
    assert state.failure_reason == FailureReason.HANDLER_ERROR
    assert state.attempts == 1
    log_decision(task.task_id, "g", state.attempts, "HANDLER_ERROR -> retryable")
    time.sleep(0.03)

    # 2) 租约到期重投：LEASE_EXPIRED，不增加尝试次数、不算业务失败
    queue.lease("g", "c2")  # c2 领取后宕机
    assert queue.lease("g", "c3") is None
    time.sleep(0.06)
    redelivered, fence3 = queue.lease("g", "c3")
    assert redelivered.redelivered is True
    state = queue.get_group_task(task.task_id, "g")
    assert state.failure_reason == FailureReason.LEASE_EXPIRED
    assert state.attempts == 1  # 租约重投没有把次数算重
    log_decision(task.task_id, "g", state.attempts, "LEASE_EXPIRED -> redelivered")

    # 3) 业务重试次数耗尽：MAX_ATTEMPTS_EXCEEDED -> 死信
    queue.fail(task.task_id, "g", fence3, RuntimeError("boom-2"))
    time.sleep(0.05)  # 第 2 次业务失败退避 0.04s
    gt4, fence4 = queue.lease("g", "c4")
    queue.fail(task.task_id, "g", fence4, RuntimeError("boom-3"))
    final = queue.get_group_task(task.task_id, "g")
    assert final.status == TaskStatus.DEAD
    assert final.failure_reason == FailureReason.MAX_ATTEMPTS_EXCEEDED
    assert final.attempts == 3
    log_decision(task.task_id, "g", final.attempts, "MAX_ATTEMPTS_EXCEEDED -> dead")


def test_lease_expiry_redelivery_attempts_not_double_counted(queue: TaskQueue) -> None:
    task, _ = queue.submit("k-nodouble", "p")
    gt, f1 = queue.lease("g", "c1")
    queue.fail(task.task_id, "g", f1, ValueError("once"))
    assert queue.get_group_task(task.task_id, "g").attempts == 1
    time.sleep(0.03)

    queue.lease("g", "c2")  # c2 宕机
    time.sleep(0.06)
    gt3, f3 = queue.lease("g", "c3")  # 到期重投
    assert gt3.redelivered is True
    queue.ack(task.task_id, "g", f3)  # 重投后处理成功

    final = queue.get_group_task(task.task_id, "g")
    assert final.status == TaskStatus.SUCCEEDED
    assert final.attempts == 1  # 只统计业务失败那一次
    assert final.failure_reason == FailureReason.NONE
    log_decision(task.task_id, "g", final.attempts, "recovered after expiry, count kept")


# ---- 重复投递最终状态一致 ----
def test_redelivery_after_lease_expiry_is_consistent(queue: TaskQueue) -> None:
    task, _ = queue.submit("k-redeliver", "p")
    first, fence1 = queue.lease("g", "c1")
    log_decision(first.task_id, "g", first.attempts, "c1 leased, then crashed without ack")
    # c1 中断未 ack；租约到期后 c2 可重新投递
    time.sleep(0.06)
    second, fence2 = queue.lease("g", "c2")
    assert second.task_id == task.task_id
    assert fence2 > fence1
    log_decision(second.task_id, "g", second.attempts, f"redelivered fence {fence1}->{fence2}")
    # 旧租约持有者的 ack 必须被拒绝，最终状态只能由有效租约决定
    with pytest.raises(LeaseConflictError):
        queue.ack(task.task_id, "g", fence1)
    queue.ack(task.task_id, "g", fence2)
    final = queue.get_group_task(task.task_id, "g")
    log_decision(final.task_id, "g", final.attempts, f"final status={final.status}")
    assert final.status == TaskStatus.SUCCEEDED
    assert final.attempts == 0


# ---- 失败重试与死信（退避口径回归）----
def test_retry_backoff_and_dead_letter(queue: TaskQueue) -> None:
    task, _ = queue.submit("k-fail", "p")
    policy = queue._retry
    for attempt in range(1, 4):
        leased = queue.lease("g", "c1")
        assert leased is not None
        t, fence = leased
        queue.fail(t.task_id, "g", fence, ValueError(f"boom-{attempt}"))
        state = queue.get_group_task(t.task_id, "g")
        log_decision(
            state.task_id, "g", state.attempts,
            f"attempt={attempt} status={state.status} reason={state.failure_reason}",
        )
        if attempt < 3:
            assert state.status == TaskStatus.FAILED_RETRYABLE
            assert state.failure_reason == FailureReason.HANDLER_ERROR
            # 退避间隔随尝试次数增长
            assert policy.delay_for_attempt(attempt + 1) > policy.delay_for_attempt(attempt)
            time.sleep(policy.delay_for_attempt(attempt))
        else:
            assert state.status == TaskStatus.DEAD
            assert state.failure_reason == FailureReason.MAX_ATTEMPTS_EXCEEDED
    dead = queue.dead_letters("g")
    assert [t.task_id for t in dead] == [task.task_id]
    assert "ValueError" in dead[0].last_error  # 错误类型可区分
    assert queue.lease("g", "c1") is None  # 死信不再投递


def test_error_normalization() -> None:
    err = normalize_error(ValueError("low-level detail"))
    assert isinstance(err, TaskQueueError)
    assert err.code == "TASK_HANDLER_ERROR"
    assert "ValueError" in err.message
    # 已归一化的错误不再重复包装
    assert normalize_error(err) is err


# ---- 自动消费：服务启动后自行处理，并发不重复 ----
def test_auto_consume_concurrent_consumers_no_double_execution(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "q.db"))
    q = TaskQueue(store, retry_policy=RetryPolicy(base_delay=0.01, max_attempts=3),
                  lease_ttl=0.2)
    n = 30
    for i in range(n):
        q.submit(f"k-{i}", f"p-{i}")

    executed: list[str] = []
    executed_lock = threading.Lock()

    def handler(payload: str) -> None:
        with executed_lock:
            executed.append(payload)
        time.sleep(0.005)

    services = [ConsumerService(q, "g", handler, consumer_id=f"c{i}") for i in range(4)]
    for svc in services:
        svc.start()
    try:
        assert _wait_until(lambda: q.get_offset("g") == n)
    finally:
        for svc in services:
            svc.stop()

    logger.info("task=all group=g attempts=%d basis=AUTO_CONSUMED unique=%d",
                len(executed), len(set(executed)))
    assert len(executed) == n
    assert len(set(executed)) == n  # 无重复执行
    assert q.get_offset("g") == n


# ---- 自动消费：两个组各自消费同一批任务 ----
def test_auto_consume_two_groups_independent_results(queue: TaskQueue) -> None:
    n = 8
    ids = [queue.submit(f"k-{i}", f"p-{i}")[0].task_id for i in range(n)]

    def ok_handler(_payload: str) -> None:
        return None

    def fail_handler(_payload: str) -> None:
        raise ValueError("always fails")

    svc_ok = ConsumerService(queue, "billing", ok_handler, consumer_id="b-1")
    svc_bad = ConsumerService(queue, "analytics", fail_handler, consumer_id="a-1")
    svc_ok.start(); svc_bad.start()
    try:
        assert _wait_until(lambda: queue.get_offset("billing") == n)
        assert _wait_until(lambda: queue.get_offset("analytics") == n)
    finally:
        svc_ok.stop(); svc_bad.stop()

    for tid in ids:
        assert queue.get_group_task(tid, "billing").status == TaskStatus.SUCCEEDED
        dead = queue.get_group_task(tid, "analytics")
        assert dead.status == TaskStatus.DEAD
        assert dead.failure_reason == FailureReason.MAX_ATTEMPTS_EXCEEDED
        assert dead.attempts == 3
        log_decision(tid, "billing|analytics", 3, "SUCCEEDED vs DEAD, independent")
    assert len(queue.dead_letters("analytics")) == n
    assert queue.dead_letters("billing") == []


# ---- 自动消费：消费者崩溃/租约到期后被别的消费者重新领走 ----
def test_auto_consume_recovers_after_lease_expiry(tmp_path, caplog) -> None:
    store = TaskStore(str(tmp_path / "recover.db"))
    q = TaskQueue(store, lease_ttl=0.08)
    task, _ = q.submit("k-crash", "p")

    q.lease("g", "crashed-1")  # 模拟消费者拿到租约后宕机，不 ack 不 fail

    done = threading.Event()

    def handler(_payload: str) -> None:
        done.set()

    svc = ConsumerService(q, "g", handler, consumer_id="c-2", idle_sleep=0.02)
    caplog.set_level(logging.INFO, logger="taskqueue.worker")
    svc.start()
    try:
        assert _wait_until(done.is_set, timeout=3.0)
        assert _wait_until(
            lambda: q.get_group_task(task.task_id, "g").status == TaskStatus.SUCCEEDED
        )
    finally:
        svc.stop()

    final = q.get_group_task(task.task_id, "g")
    assert final.attempts == 0  # 租约到期重投不计数、不算业务失败
    # 日志能看出任务标识、尝试次数与 LEASE_EXPIRED 判定依据
    assert any("LEASE_EXPIRED" in r.message and task.task_id in r.message
               for r in caplog.records)
    log_decision(task.task_id, "g", final.attempts, "recovered after lease expiry")


# ---- 并发消费：同一任务不被同时执行（Worker 直跑回归）----
def test_concurrent_consumers_no_double_execution(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "q.db"))
    q = TaskQueue(store, lease_ttl=0.2)
    n = 20
    for i in range(n):
        q.submit(f"k-{i}", f"p-{i}")

    executed: list[str] = []
    executed_lock = threading.Lock()
    barrier = threading.Barrier(4)

    def handler(payload: str) -> None:
        with executed_lock:
            executed.append(payload)
        time.sleep(0.005)

    def consumer(cid: str) -> None:
        barrier.wait()
        w = Worker(q, "g", cid, handler)
        while w.run_once():
            pass

    threads = [threading.Thread(target=consumer, args=(f"c{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    log_decision("all", "g", n, f"executed={len(executed)} unique={len(set(executed))}")
    assert len(executed) == n
    assert len(set(executed)) == n  # 无重复执行
    assert q.get_offset("g") == n


# ---- 租约到期不丢失 ----
def test_lease_expiry_redelivers_without_loss(queue: TaskQueue) -> None:
    task, _ = queue.submit("k-lease", "p")
    queue.lease("g", "c1")  # c1 拿到租约后宕机，既不 ack 也不 fail
    assert queue.lease("g", "c2") is None  # 租约未到期前不可重复投递
    time.sleep(0.06)
    leased = queue.lease("g", "c2")
    assert leased is not None and leased[0].task_id == task.task_id
    log_decision(task.task_id, "g", 0, "lease expired -> redelivered, not lost")


# ---- 位点规则 ----
def test_offset_regression_and_jump_rejected(queue: TaskQueue) -> None:
    queue.commit_offset("g", 1)
    queue.commit_offset("g", 2)
    for bad in (1, 2, 5, 0):
        with pytest.raises(OffsetRegressionError):
            queue.commit_offset("g", bad)
        log_decision("offset", "g", 0, f"rejected jump/regression to {bad}")
    queue.commit_offset("g", 3)
    assert queue.get_offset("g") == 3


# ---- 进程重启后状态保留 ----
def test_restart_preserves_state(tmp_path) -> None:
    db = str(tmp_path / "restart.db")
    q1 = TaskQueue(TaskStore(db))
    task, _ = q1.submit("k-restart", "p")
    leased, fence = q1.lease("g", "c1")
    q1.ack(leased.task_id, "g", fence)  # ack 会自动推进位点到 1

    q2 = TaskQueue(TaskStore(db))  # 模拟进程重启
    restored = q2.get_group_task(task.task_id, "g")
    log_decision(restored.task_id, "g", restored.attempts, f"restored status={restored.status}")
    assert restored.status == TaskStatus.SUCCEEDED
    assert q2.get_offset("g") == 1
    t2, created = q2.submit("k-restart", "p")  # 重启后幂等仍生效
    assert created is False and t2.task_id == task.task_id
