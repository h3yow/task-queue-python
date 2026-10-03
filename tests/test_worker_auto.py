"""自动消费：服务启动后自行消费、同组并发互斥、租约到期重投不丢不重。"""

from __future__ import annotations

import logging
import threading
import time

from taskqueue import (
    FailureReason,
    RetryPolicy,
    TaskQueue,
    TaskStatus,
    TaskStore,
    Worker,
)

logger = logging.getLogger("test.worker_auto")


def log_decision(task_id: str, group: str, attempts: int, basis: str) -> None:
    logger.info(
        "task=%s group=%s attempts=%d basis=%s", task_id, group, attempts, basis
    )


def wait_until(predicate, timeout: float = 3.0, interval: float = 0.01) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_background_worker_consumes_submitted_tasks_automatically(tmp_path) -> None:
    q = TaskQueue(TaskStore(str(tmp_path / "auto.db")), lease_ttl=2.0)
    done: list[str] = []
    done_lock = threading.Lock()

    def handler(payload: str) -> None:
        with done_lock:
            done.append(payload)

    w = Worker(q, "g", "auto-1", handler, poll_interval=0.01)
    w.start()  # 服务启动即自动消费，无需手动领取
    try:
        for i in range(10):
            q.submit(f"auto-{i}", f"payload-{i}")
        assert wait_until(lambda: len(done) == 10)
    finally:
        w.stop(timeout=2.0)

    assert sorted(done) == [f"payload-{i}" for i in range(10)]
    assert q.get_offset("g") == 10
    log_decision("10 tasks", "g", 10, "auto-consumed after startup")


def test_concurrent_workers_same_group_execute_once(tmp_path) -> None:
    q = TaskQueue(
        TaskStore(str(tmp_path / "cc.db")),
        retry_policy=RetryPolicy(base_delay=0.01, max_attempts=3),
        lease_ttl=0.3,
    )
    n = 30
    for i in range(n):
        q.submit(f"cc-{i}", f"p-{i}")

    executed: list[str] = []
    exec_lock = threading.Lock()

    def handler(payload: str) -> None:
        with exec_lock:
            executed.append(payload)
        time.sleep(0.005)

    workers = [
        Worker(q, "g", f"auto-{i}", handler, poll_interval=0.005)
        for i in range(4)
    ]
    for w in workers:
        w.start()
    try:
        assert wait_until(lambda: len(executed) == n, timeout=5.0)
        assert q.get_offset("g") == n
    finally:
        for w in workers:
            w.stop(timeout=2.0)

    log_decision("all", "g", len(executed),
                 f"executed={len(executed)} unique={len(set(executed))}")
    assert len(executed) == n
    assert len(set(executed)) == n  # 同一组同一任务同一时刻只被一个消费者执行


def test_crashed_lease_is_redelivered_to_other_worker(tmp_path) -> None:
    q = TaskQueue(
        TaskStore(str(tmp_path / "crash.db")),
        retry_policy=RetryPolicy(base_delay=0.01, max_attempts=3),
        lease_ttl=0.05,
    )
    task, _ = q.submit("crash-1", "p")

    holder_done = threading.Event()

    def crashing_handler(_: str) -> None:
        # 模拟消费者拿到任务后立刻崩溃（不 ack / fail），让线程退出
        holder_done.set()
        raise SystemExit(0)

    crashed = Worker(q, "g", "crashed", crashing_handler, poll_interval=0.01)
    crashed.start()
    assert holder_done.wait(timeout=2.0)
    crashed.stop(timeout=1.0)

    # 租约到期前别人领不到；到期后新消费者自动接管并 ack
    picked = threading.Event()

    def recovering_handler(payload: str) -> None:
        picked.set()

    survivor = Worker(q, "g", "survivor", recovering_handler, poll_interval=0.01)
    survivor.start()
    try:
        assert picked.wait(timeout=2.0), "task was lost after lease expiry"
        assert wait_until(
            lambda: q.get_state(task.task_id, "g").status == TaskStatus.SUCCEEDED
        )
        state = q.get_state(task.task_id, "g")
        # 重投不计业务尝试，也不记业务失败：最终 attempts=0、failure_reason=NONE
        assert state.attempts == 0
        assert state.failure_reason == FailureReason.NONE
        log_decision(task.task_id, "g", 0,
                     "redelivered after crash, attempts not double-counted")
    finally:
        survivor.stop(timeout=2.0)


def test_two_groups_auto_consume_same_batch_independently(tmp_path) -> None:
    q = TaskQueue(
        TaskStore(str(tmp_path / "twogroup.db")),
        retry_policy=RetryPolicy(base_delay=0.01, max_attempts=1),
        lease_ttl=0.5,
    )
    for i in range(5):
        q.submit(f"tg-{i}", "fail:boom" if i % 2 == 0 else f"ok-{i}")

    g1_seen: list[str] = []
    g2_seen: list[str] = []
    lock = threading.Lock()

    def make_handler(seen):
        def handler(payload: str) -> None:
            if payload.startswith("fail:"):
                raise ValueError(payload)
            with lock:
                seen.append(payload)
        return handler

    w1 = Worker(q, "g1", "g1-auto", make_handler(g1_seen), poll_interval=0.01)
    w2 = Worker(q, "g2", "g2-auto", make_handler(g2_seen), poll_interval=0.01)
    w1.start()
    w2.start()
    try:
        assert wait_until(lambda: q.get_offset("g1") == 5 and q.get_offset("g2") == 5)
    finally:
        w1.stop(timeout=2.0)
        w2.stop(timeout=2.0)

    for group in ("g1", "g2"):
        dead = q.dead_letters(group)
        assert len(dead) == 3  # 3 个失败任务，max_attempts=1 直接死信
        for s in dead:
            assert s.failure_reason == FailureReason.MAX_ATTEMPTS_EXCEEDED
            assert s.attempts == 1
    assert sorted(g1_seen) == sorted(g2_seen) == ["ok-1", "ok-3"]
    log_decision("5 tasks", "g1&g2", 5, "both groups independently finished the batch")
