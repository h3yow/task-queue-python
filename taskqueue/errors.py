"""归一化错误类型：底层异常不得直接透出到对外接口。"""

from __future__ import annotations


class TaskQueueError(Exception):
    """所有对外暴露的队列错误基类。"""

    code = "TASK_QUEUE_ERROR"

    def __init__(self, message: str, *, cause: Exception | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.cause = cause


class DuplicateTaskError(TaskQueueError):
    """提交重复幂等标识（非致命，用于区分判定）。"""

    code = "DUPLICATE_TASK"


class TaskNotFoundError(TaskQueueError):
    code = "TASK_NOT_FOUND"


class LeaseConflictError(TaskQueueError):
    """任务已被其他消费者持有租约。"""

    code = "LEASE_CONFLICT"


class OffsetRegressionError(TaskQueueError):
    """位点回退或跳跃，必须明确拒绝。"""

    code = "OFFSET_REGRESSION"


class TaskHandlerError(TaskQueueError):
    """处理器抛出的异常被归一化为该类型。"""

    code = "TASK_HANDLER_ERROR"


class DeadLetterError(TaskQueueError):
    """任务超过重试上限进入死信。"""

    code = "DEAD_LETTER"


def normalize_error(exc: Exception) -> TaskQueueError:
    """把任意底层异常归一化为可区分的 TaskQueueError。

    已是 TaskQueueError 的异常原样返回；其余包装为 TaskHandlerError，
    保留原始异常类型名以便区分判定，但不把底层异常对象直接透出。
    """
    if isinstance(exc, TaskQueueError):
        return exc
    return TaskHandlerError(
        f"{type(exc).__name__}: {exc}",
        cause=exc,
    )
