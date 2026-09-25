"""归一化错误类型。

对外接口只暴露本模块定义的异常类型，底层异常（如 sqlite3.Error）
必须被捕获并包装为这里的子类，不得直接透出。
"""

from __future__ import annotations


class TaskQueueError(Exception):
    """任务队列对外错误的基类。"""


class StorageError(TaskQueueError):
    """底层存储访问失败（底层异常以 __cause__ 保留）。"""


class DuplicateIdempotencyKeyError(TaskQueueError):
    """幂等键冲突：相同幂等键对应不同的任务内容。"""

    def __init__(self, idempotency_key: str) -> None:
        self.idempotency_key = idempotency_key
        super().__init__(f"idempotency key already exists with different payload: {idempotency_key}")


class LeaseExpiredError(TaskQueueError):
    """租约已过期或已被其他消费者持有，提交结果被拒绝。"""

    def __init__(self, task_id: str, consumer: str) -> None:
        self.task_id = task_id
        self.consumer = consumer
        super().__init__(f"lease expired or not held by consumer '{consumer}': {task_id}")


class LeaseAlreadyHeldError(TaskQueueError):
    """任务正被其他消费者持有，当前无法领取。"""

    def __init__(self, task_id: str, holder: str) -> None:
        self.task_id = task_id
        self.holder = holder
        super().__init__(f"task {task_id} is already held by consumer '{holder}'")


class CheckpointRejectedError(TaskQueueError):
    """位点提交被拒绝：位点回退或非法跳跃。"""

    def __init__(self, requested: int, current: int, reason: str) -> None:
        self.requested = requested
        self.current = current
        self.reason = reason
        super().__init__(f"checkpoint {requested} rejected (current={current}): {reason}")


class TaskNotFoundError(TaskQueueError):
    """引用的任务不存在。"""

    def __init__(self, task_id: str) -> None:
        self.task_id = task_id
        super().__init__(f"task not found: {task_id}")


class HandlerNotFoundError(TaskQueueError):
    """注册的任务类型没有对应的处理器。"""

    def __init__(self, task_type: str) -> None:
        self.task_type = task_type
        super().__init__(f"no handler registered for task type: {task_type}")


class PoisonTaskError(TaskQueueError):
    """处理器主动声明该任务不可重试，应立即进入死信。"""


class QueueClosedError(TaskQueueError):
    """队列已关闭，拒绝继续提交或消费。"""
