"""统一日志工具：日志中必须包含任务标识、尝试次数与判定依据。"""

from __future__ import annotations

import logging

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
_configured = False


def configure_logging(level: int = logging.INFO) -> None:
    """配置根日志格式（幂等，可重复调用）。"""
    global _configured
    if _configured:
        logging.getLogger().setLevel(level)
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    _configured = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_decision(
    logger: logging.Logger,
    task_id: str,
    attempt: int,
    decision: str,
    reason: str,
    **fields: object,
) -> None:
    """打印一条带任务标识/尝试次数/判定依据的结构化决策日志。

    形如: task=... attempt=2 decision=retry reason='backoff' key=value ...
    """
    parts = [f"task={task_id}", f"attempt={attempt}", f"decision={decision}", f"reason={reason!r}"]
    for key, value in fields.items():
        parts.append(f"{key}={value!r}")
    logger.info(" ".join(parts))
