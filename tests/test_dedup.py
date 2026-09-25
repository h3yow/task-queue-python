"""幂等提交与重复投递一致性场景。"""

from __future__ import annotations

import logging

from task_queue.errors import DuplicateIdempotencyKeyError


def test_duplicate_submit_same_key_does_not_duplicate_task(make_queue) -> None:
    queue = make_queue()
    side_effects: list[str] = []
    queue.register_handler("email", lambda task, attempt: side_effects.append(task.id))

    first = queue.submit("idem-1", "email", {"to": "a"}, groups="g1")
    second = queue.submit("idem-1", "email", {"to": "a"}, groups="g1")

    # 相同幂等键：返回同一任务，投递表只有一行。
    assert first.id == second.id
    assert queue.store.list_deliveries("g1") and len(queue.store.list_deliveries("g1")) == 1

    queue.poll_and_process("g1", "c1")
    assert side_effects == [first.id]  # 只执行一次
    stats = queue.stats("g1")
    assert stats["counts"]["succeeded"] == 1
    assert stats["counts"]["pending"] == 0


def test_duplicate_submit_with_different_payload_is_rejected(make_queue) -> None:
    queue = make_queue()
    queue.submit("idem-2", "email", {"to": "a"}, groups="g1")
    try:
        queue.submit("idem-2", "email", {"to": "DIFFERENT"}, groups="g1")
    except DuplicateIdempotencyKeyError as exc:
        assert exc.idempotency_key == "idem-2"
    else:  # pragma: no cover
        raise AssertionError("expected DuplicateIdempotencyKeyError")

    # 冲突提交没有产生新任务/新投递。
    deliveries = queue.store.list_deliveries("g1")
    assert len(deliveries) == 1


def test_submit_to_multiple_groups_then_duplicate_adds_no_extra(make_queue) -> None:
    queue = make_queue()
    t1 = queue.submit("idem-3", "email", {}, groups=["g1", "g2"])
    t2 = queue.submit("idem-3", "email", {}, groups=["g2", "g3"])

    assert t1.id == t2.id
    assert len(queue.store.list_deliveries("g1")) == 1
    assert len(queue.store.list_deliveries("g2")) == 1
    assert len(queue.store.list_deliveries("g3")) == 1  # 新组被补齐，旧组不重复


def test_redelivery_after_failures_ends_consistent(make_queue, clock) -> None:
    """同一任务被重复投递（重试）多次后，最终状态与执行计数一致、可解释。"""
    queue = make_queue()
    attempts: list[int] = []

    def flaky(task, attempt):
        attempts.append(attempt)
        if attempt < 3:
            raise RuntimeError("transient failure")

    queue.register_handler("flaky", flaky)
    task = queue.submit("idem-4", "flaky", {}, groups="g1")

    # 三轮领取：前两次失败退避，第三次成功。
    for expected_attempt in (1, 2, 3):
        processed = queue.poll_and_process("g1", "c1")
        if expected_attempt < 3:
            assert processed == 0
            clock.advance(100)  # 越过退避窗口
        else:
            assert processed == 1

    assert attempts == [1, 2, 3]
    delivery = queue.store.get_delivery("g1", task.id)
    assert delivery.status.value == "succeeded"
    assert delivery.attempt == 3
    assert queue.checkpoint("g1") == 1
    counts = queue.store.count_by_status("g1")
    assert counts  # sanity: counts queryable
    assert queue.stats("g1")["counts"]["succeeded"] == 1


def test_decision_logs_include_task_attempt_and_reason(make_queue, caplog) -> None:
    queue = make_queue()
    queue.register_handler("ok", lambda task, attempt: None)

    with caplog.at_level(logging.INFO, logger="task_queue.queue"):
        task = queue.submit("idem-5", "ok", {}, groups="g1")
        queue.poll_and_process("g1", "c1")

    messages = [r.getMessage() for r in caplog.records]
    submitted = [m for m in messages if "decision=submitted" in m]
    succeeded = [m for m in messages if "decision=succeeded" in m]
    assert submitted and f"task={task.id}" in submitted[0]
    assert succeeded
    assert "attempt=1" in succeeded[0]
    assert "reason=" in succeeded[0]  # 判定依据
