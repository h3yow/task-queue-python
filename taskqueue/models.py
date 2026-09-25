"""任务与租约的数据模型。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    LEASED = "LEASED"
    SUCCEEDED = "SUCCEEDED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    DEAD = "DEAD"


class FailureReason(str, enum.Enum):
    NONE = "NONE"
    HANDLER_ERROR = "HANDLER_ERROR"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    MAX_ATTEMPTS_EXCEEDED = "MAX_ATTEMPTS_EXCEEDED"


@dataclass
class Task:
    task_id: str
    idempotency_key: str
    payload: str
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    failure_reason: FailureReason = FailureReason.NONE
    last_error: str | None = None
    visible_at: float = 0.0
    created_at: float = 0.0
    updated_at: float = 0.0


@dataclass
class Lease:
    task_id: str
    consumer_group: str
    consumer_id: str
    fence: int
    expires_at: float
