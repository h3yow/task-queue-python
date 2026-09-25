"""多消费者并发处理场景。"""

from __future__ import annotations

import threading
import time

from task_queue.models import QueueConfig
from task_queue.worker import ConsumerWorker
from tests.conftest import make_retry_policy


def _wait_until(predicate, timeout: float = 5.0, interval: float = 0.005) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_concurrent_workers_in_same_group_do_not_double_run(make_queue) -> None:
    """同组多个 worker 并发消费，每个任务恰好被一个消费者执行一次。"""
    config = QueueConfig(
        lease_seconds=30,
        reclaim_interval_seconds=1.0,
        retry_policy=make_retry_policy(),
    )
    queue = make_queue(config)

    n_tasks = 40
    executed: list[str] = []
    inflight = {"n": 0, "max": 0}
    lock = threading.Lock()

    def handler(task, attempt):
        with lock:
            executed.append(task.id)
            inflight["n"] += 1
            inflight["max"] = max(inflight["max"], inflight["n"])
        # 处理期间让出 CPU，制造真实的并发领取与执行重叠。
        time.sleep(0.01)
        with lock:
            inflight["n"] -= 1

    queue.register_handler("t", handler)
    for i in range(n_tasks):
        queue.submit(f"k-{i}", "t", {"i": i}, groups="g1")

    workers = [
        ConsumerWorker(queue, "g1", f"w{idx}", poll_interval_seconds=0.001, batch_size=1)
        for idx in range(6)
    ]
    for w in workers:
        w.start()
    assert _wait_until(lambda: queue.stats("g1")["counts"]["succeeded"] == n_tasks)
    for w in workers:
        w.stop(timeout=5)

    # 关键不变量：每个任务恰好执行一次，没有重复副作用。
    assert len(executed) == n_tasks
    assert len(set(executed)) == n_tasks
    assert inflight["max"] >= 2  # 确实发生过并发执行
    assert queue.stats("g1")["counts"]["succeeded"] == n_tasks


def test_tasks_are_distributed_across_consumers(make_queue) -> None:
    """同组两个 worker 都应实际分到任务（分摊而非串行独占）。"""
    queue = make_queue()
    owners: list[str] = []
    lock = threading.Lock()

    def make_handler(owner):
        def _h(task, attempt):
            with lock:
                owners.append(owner)
            time.sleep(0.002)
        return _h

    queue.register_handler("a", make_handler("w0"))
    queue.register_handler("b", make_handler("w1"))
    for i in range(20):
        queue.submit(f"k-{i}", "a" if i % 2 == 0 else "b", {}, groups="g1")

    w0 = ConsumerWorker(queue, "g1", "w0", poll_interval_seconds=0.001)
    w1 = ConsumerWorker(queue, "g1", "w1", poll_interval_seconds=0.001)
    w0.start(); w1.start()
    assert _wait_until(lambda: queue.stats("g1")["counts"]["succeeded"] == 20)
    w0.stop(); w1.stop()

    assert queue.stats("g1")["counts"]["succeeded"] == 20
    assert "w0" in owners and "w1" in owners  # 分摊给了两个消费者
    assert len(owners) == 20


def test_different_groups_consume_independently(make_queue) -> None:
    """不同消费者组各自维护进度，互不影响位点与状态（同步确定性驱动）。"""
    queue = make_queue()
    queue.register_handler("t", lambda task, attempt: None)
    for i in range(5):
        queue.submit(f"k-{i}", "t", {}, groups=["g1", "g2"])

    # g1 消费全部 5 个。
    for _ in range(5):
        assert queue.poll_and_process("g1", "c1") == 1
    # g2 只消费前 2 个。
    assert queue.poll_and_process("g2", "c2") == 1
    assert queue.poll_and_process("g2", "c2") == 1

    assert queue.checkpoint("g1") == 5
    assert queue.checkpoint("g2") == 2
    stats_g1 = queue.stats("g1")
    stats_g2 = queue.stats("g2")
    assert stats_g1["counts"]["succeeded"] == 5
    assert stats_g2["counts"]["succeeded"] == 2
    # g2 剩余 3 个仍为 pending，可被后续继续消费；g1 不受影响。
    assert stats_g2["counts"]["pending"] == 3
    # 两组的死信、位点彼此独立。
    assert stats_g1["checkpoint"] == 5 and stats_g2["checkpoint"] == 2


def test_worker_reclaims_and_completes_after_lease_timeout() -> None:
    """租约超时后任务被回收并由另一消费者最终完成，且只成功一次。

    本用例必须使用真实时钟（租约按真实时间到期），因此不使用 FakeClock 夹具。
    """
    from task_queue.queue import TaskQueue
    from task_queue.store import SQLiteStore

    config = QueueConfig(
        lease_seconds=0.1,
        reclaim_interval_seconds=0.01,
        retry_policy=make_retry_policy(max_attempts=5),
    )
    queue = TaskQueue(SQLiteStore(":memory:"), config)  # 默认 time.time
    queue.start()

    successes: list[int] = []
    successes_lock = threading.Lock()

    def handler(task, attempt):
        if attempt == 1:
            # 模拟消费者卡死：阻塞到租约超时，随后"进程中断"——
            # 不记录成功副作用并抛出，complete 也因租约过期被拒绝。
            time.sleep(1.0)
            raise RuntimeError("consumer interrupted before commit")
        with successes_lock:
            successes.append(attempt)

    queue.register_handler("slow", handler)
    queue.submit("k-lease", "slow", {}, groups="g1")

    w0 = ConsumerWorker(
        queue, "g1", "w0", poll_interval_seconds=0.001, reclaim_interval_polls=10_000
    )  # w0 自身轮询间隔很大，基本不触发回收
    w1 = ConsumerWorker(
        queue, "g1", "w1", poll_interval_seconds=0.001, reclaim_interval_polls=1
    )
    w0.start(); w1.start()
    assert _wait_until(lambda: queue.stats("g1")["counts"]["succeeded"] == 1, timeout=6)
    w0.stop(); w1.stop()
    queue.close()

    # 唯一成功副作用来自回收后的第二次尝试；中断的第一次不算成功。
    assert successes == [2]
