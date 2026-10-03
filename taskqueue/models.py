"""任务、按组处理状态与租约的数据模型。"""

from __future__ import annotations

import enum
from dataclasses import dataclass


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    SUCCEEDED = "SUCCEEDED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    DEAD = "DEAD"


class FailureReason(str, enum.Enum):
    NONE = "NONE"
    # 业务处理器抛异常（可重试失败）
    HANDLER_ERROR = "HANDLER_ERROR"
    # 消费者掉线 / 租约到期导致重新投递（不计为业务失败）
    LEASE_EXPIRED = "LEASE_EXPIRED"
    # 业务失败重试次数用尽，进入死信
    MAX_ATTEMPTS_EXCEEDED = "MAX_ATTEMPTS_EXCEEDED"


@dataclass
class Task:
    """任务描述本身：对所有消费组共享同一份。"""

    task_id: str
    idempotency_key: str
    payload: str
    max_attempts: int = 3
    created_at: float = 0.0
    updated_at: float = 0.0


@dataclass
class GroupTaskState:
    """单个消费组对单个任务的独立处理状态。

    每个 (task_id, group) 一行：尝试次数、失败原因、死信判定互不影响。
    """

    task_id: str
    group: str
    max_attempts: int = 3
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    failure_reason: FailureReason = FailureReason.NONE
    last_error: str | None = None
    visible_at: float = 0.0
    updated_at: float = 0.0


@dataclass
class Lease:
    task_id: str
    consumer_group: str
    consumer_id: str
    fence: int
    expires_at: float
