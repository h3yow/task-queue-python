"""领域模型与可配置项（仅类型定义，无业务逻辑）。"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TaskStatus(str, Enum):
    """任务在某个消费者组内的投递状态。"""

    PENDING = "pending"      # 已提交，尚未投递
    INFLIGHT = "inflight"    # 已被消费者领取，租约有效
    SUCCEEDED = "succeeded"  # 处理成功（终态）
    RETRYING = "retrying"    # 处理失败，等待退避后重新投递
    DEAD = "dead"            # 超过重试上限或被判定为毒消息（终态）


class DeadLetterReason(str, Enum):
    """死信原因，必须可区分。"""

    MAX_RETRIES_EXCEEDED = "max_retries_exceeded"
    POISON_TASK = "poison_task"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """重试与退避配置。

    delay = min(base_delay * backoff_multiplier ** (attempt - 1), max_delay)
    attempt 从 1 开始，即第一次失败后的等待为 base_delay。
    """

    max_attempts: int = 5
    base_delay_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    max_delay_seconds: float = 60.0
    jitter_seconds: float = 0.0

    def delay_for_attempt(self, attempt: int) -> float:
        """第 attempt 次失败（从 1 起）后需要等待的秒数。"""
        if attempt < 1:
            raise ValueError(f"attempt must be >= 1, got {attempt}")
        raw = self.base_delay_seconds * self.backoff_multiplier ** (attempt - 1)
        delay = min(raw, self.max_delay_seconds)
        if self.jitter_seconds > 0:
            delay += random.uniform(0, self.jitter_seconds)
        return max(0.0, delay)


@dataclass(frozen=True, slots=True)
class QueueConfig:
    """队列全局配置。"""

    lease_seconds: float = 30.0
    reclaim_interval_seconds: float = 1.0
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)


@dataclass(slots=True)
class Task:
    """任务本体（全局唯一，与消费者组无关）。"""

    id: str
    idempotency_key: str
    task_type: str
    payload: dict[str, Any]
    created_at: float


@dataclass(slots=True)
class Delivery:
    """一次可被消费者领取的投递记录（任务 × 消费者组）。"""

    task_id: str
    group: str
    seq: int
    status: TaskStatus
    attempt: int
    visible_at: float
    lease_owner: str | None
    lease_expires_at: float | None
    last_error: str | None
    dead_reason: DeadLetterReason | None
