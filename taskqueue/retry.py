"""可配置的重试策略：间隔随尝试次数增长。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    base_delay: float = 0.1
    multiplier: float = 2.0
    max_delay: float = 30.0
    max_attempts: int = 3

    def delay_for_attempt(self, attempt: int) -> float:
        """第 attempt 次失败后的退避间隔（attempt 从 1 开始）。"""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        delay = self.base_delay * (self.multiplier ** (attempt - 1))
        return min(delay, self.max_delay)

    def is_dead(self, attempts: int) -> bool:
        """是否已超过重试上限。"""
        return attempts >= self.max_attempts
