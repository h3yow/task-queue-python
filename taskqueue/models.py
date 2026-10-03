"""任务、消费组处理状态与租约的数据模型。

一个 Task 只保存任务本身的共享元数据；每个消费组各自持有一份
GroupTask（状态、尝试次数、失败原因、可见时间、位点进度），
多组之间互不影响。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"                    # 组内尚未投递
    LEASED = "LEASED"                      # 已被某消费者持有租约
    SUCCEEDED = "SUCCEEDED"                # 该组处理成功（终态）
    FAILED_RETRYABLE = "FAILED_RETRYABLE"  # 业务失败，等待退避重试
    DEAD = "DEAD"                          # 重试耗尽，进入死信（终态）


class FailureReason(str, enum.Enum):
    NONE = "NONE"
    HANDLER_ERROR = "HANDLER_ERROR"                # 业务处理抛异常（计入尝试次数）
    LEASE_EXPIRED = "LEASE_EXPIRED"                # 消费者掉线，租约到期被重新投递
    MAX_ATTEMPTS_EXCEEDED = "MAX_ATTEMPTS_EXCEEDED"  # 业务重试次数耗尽进入死信


@dataclass
class Task:
    """任务共享元数据，与消费组无关。"""

    task_id: str
    idempotency_key: str
    payload: str
    created_at: float = 0.0


@dataclass
class GroupTask:
    """某个消费组对某个任务的独立处理进度与判定。"""

    task_id: str
    grp: str
    seq: int                          # 任务全局序号，用于组内顺序与位点
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0                 # 业务失败尝试次数（租约到期重投不计数）
    max_attempts: int = 3
    failure_reason: FailureReason = FailureReason.NONE
    last_error: str | None = None
    visible_at: float = 0.0
    created_at: float = 0.0
    updated_at: float = 0.0
    # 非持久化：本次租约是否来自租约到期后的重新投递
    redelivered: bool = False
    # 非持久化：处理时方便使用的任务内容
    payload: str = ""


@dataclass
class Lease:
    task_id: str
    consumer_group: str
    consumer_id: str
    fence: int
    expires_at: float
