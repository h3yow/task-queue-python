"""位点推进、回退拒绝与跳跃拒绝场景。"""

from __future__ import annotations

from task_queue.errors import CheckpointRejectedError
from task_queue.models import QueueConfig
from tests.conftest import make_retry_policy


def _submit_and_succeed(queue, key, group="g1", consumer="c1") -> int:
    queue.submit(key, "t", {}, groups=group)
    queue.poll_and_process(group, consumer)
    return queue.checkpoint(group)


def test_checkpoint_advances_sequentially(make_queue) -> None:
    queue = make_queue()
    queue.register_handler("t", lambda task, attempt: None)

    assert _submit_and_succeed(queue, "k1") == 1
    assert _submit_and_succeed(queue, "k2") == 2
    assert _submit_and_succeed(queue, "k3") == 3


def test_checkpoint_rollback_is_rejected(make_queue) -> None:
    queue = make_queue()
    queue.register_handler("t", lambda task, attempt: None)
    _submit_and_succeed(queue, "k1")
    _submit_and_succeed(queue, "k2")
    assert queue.checkpoint("g1") == 2

    # 回退到 1 或 0 必须被明确拒绝，位点保持不变。
    for backward in (1, 0):
        try:
            queue.commit("g1", backward, "c1")
        except CheckpointRejectedError as exc:
            assert exc.requested == backward and exc.current == 2
            assert "rollback" in exc.reason
        else:  # pragma: no cover
            raise AssertionError(f"rollback to {backward} should be rejected")
    assert queue.checkpoint("g1") == 2


def test_checkpoint_jump_is_rejected(make_queue) -> None:
    queue = make_queue()
    queue.register_handler("t", lambda task, attempt: None)
    _submit_and_succeed(queue, "k1")  # checkpoint = 1

    # 直接跳到 3（seq=2 尚未终结）必须拒绝，且不能从中间状态继续。
    try:
        queue.commit("g1", 3, "c1")
    except CheckpointRejectedError as exc:
        assert exc.requested == 3 and exc.current == 1
        assert "jump" in exc.reason
    else:  # pragma: no cover
        raise AssertionError("jump to 3 should be rejected")
    assert queue.checkpoint("g1") == 1


def test_commit_past_non_terminal_delivery_rejected(make_queue, clock) -> None:
    config = QueueConfig(retry_policy=make_retry_policy(max_attempts=3))
    queue = make_queue(config)
    queue.register_handler("good", lambda task, attempt: None)
    queue.register_handler("bad", lambda task, attempt: (_ for _ in ()).throw(ValueError("x")))

    # seq=1 一直处于 retrying（退避中），不能用 seq=2 的成功跳过它。
    queue.submit("k-bad", "bad", {}, groups="g1")
    queue.poll_and_process("g1", "c1")  # -> retrying, visible_at 未来
    queue.submit("k-good", "good", {}, groups="g1")
    # seq=2 虽已成功领取但无法在 seq=1 未终结时完成位点推进：
    # 这里 seq=2 根本还没领取（被 seq=1 顺序领取规则约束），验证 commit(2) 被拒。
    try:
        queue.commit("g1", 2, "c1")
    except CheckpointRejectedError as exc:
        assert "jump" in exc.reason or "not terminal" in exc.reason
    else:  # pragma: no cover
        raise AssertionError("commit past retrying delivery should be rejected")
    assert queue.checkpoint("g1") == 0


def test_checkpoint_advances_past_dead_letter_once_resolved(make_queue, clock) -> None:
    """死信是终结状态：后续任务成功后位点应能连续越过死信。"""
    queue = make_queue()
    queue.register_handler("good", lambda task, attempt: None)
    queue.register_handler("bad", lambda task, attempt: (_ for _ in ()).throw(ValueError("x")))

    queue.submit("k-bad", "bad", {}, groups="g1")
    for _ in range(3):  # max_attempts=3 -> dead
        queue.poll_and_process("g1", "c1")
        clock.advance(100)
    assert queue.checkpoint("g1") == 1  # 死信终结，位点可推进到 1

    queue.submit("k-good", "good", {}, groups="g1")
    queue.poll_and_process("g1", "c1")
    assert queue.checkpoint("g1") == 2


def test_explicit_commit_accepts_next_terminal_seq(make_queue) -> None:
    queue = make_queue()
    queue.register_handler("t", lambda task, attempt: None)
    queue.submit("k1", "t", {}, groups="g1")
    # 领取后仍 inflight，此时显式 commit(1) 应被拒（未终结）。
    queue.store.fetch_available("g1", "c1", 1000.0, 30)
    try:
        queue.commit("g1", 1, "c1")
    except CheckpointRejectedError as exc:
        assert "not terminal" in exc.reason
    else:  # pragma: no cover
        raise AssertionError("commit of inflight delivery should be rejected")

    # 完成后 complete 已自动把位点推进到 1；显式提交下一个 seq=2 的
    # 正常路径由上面的顺序推进用例覆盖。这里单独验证一次显式提交
    # checkpoint+1 的成功路径：构造一个成功但位点尚未越过的独立组。
    queue.submit("k-g2", "t", {}, groups="g2")
    delivery_g2 = queue.store.fetch_available("g2", "c1", 1000.0, 30)[0]
    # 直接置为成功但不触发自动推进（模拟外部系统自行管理位点）。
    conn = queue.store.conn
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE deliveries SET status='succeeded', lease_owner=NULL,"
        " lease_expires_at=NULL WHERE group_name='g2' AND task_id=?",
        (delivery_g2.task_id,),
    )
    conn.commit()
    assert queue.checkpoint("g2") == 0
    assert queue.commit("g2", 1, "c1") == 1
    assert queue.checkpoint("g2") == 1
