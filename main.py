"""FastAPI 应用入口：装配本地任务队列并暴露 HTTP 接口。

仅依赖本地资源：默认使用同目录下的 SQLite 文件持久化，
进程重启后任务、租约与位点状态完整恢复。
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from task_queue import (
    ConsumerWorker,
    QueueConfig,
    RetryPolicy,
    SQLiteStore,
    TaskQueue,
)
from task_queue.api import create_router, install_exception_handlers
from task_queue.logging_utils import configure_logging

# 可通过环境变量覆盖，便于本地验证与测试。
DB_PATH = os.environ.get("TASK_QUEUE_DB", "queue.db")
CONSUMER_GROUP = os.environ.get("TASK_QUEUE_GROUP", "default")


def build_queue(db_path: str = DB_PATH) -> TaskQueue:
    """构造并配置队列（注册处理器、设定重试/租约策略）。"""
    config = QueueConfig(
        lease_seconds=30.0,
        reclaim_interval_seconds=1.0,
        retry_policy=RetryPolicy(
            max_attempts=5,
            base_delay_seconds=1.0,
            backoff_multiplier=2.0,
            max_delay_seconds=60.0,
        ),
    )
    queue = TaskQueue(SQLiteStore(db_path), config)

    # 示例处理器：真实业务应替换为具体任务类型。
    def echo_handler(task, attempt):
        print(f"[handler] task={task.id} attempt={attempt} payload={task.payload}")

    queue.register_handler("echo", echo_handler)
    return queue


# 模块级创建（未 start），路由可静态绑定；生命周期在 lifespan 中管理。
queue = build_queue()
worker = ConsumerWorker(queue, CONSUMER_GROUP, consumer="main-0", batch_size=4)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    configure_logging()
    queue.start()
    worker.start()
    app.state.queue = queue
    app.state.worker = worker
    try:
        yield
    finally:
        worker.stop()
        queue.close()


app = FastAPI(title="Local Task Queue", lifespan=lifespan)
install_exception_handlers(app)
app.include_router(create_router(queue))


@app.get("/")
def read_root() -> dict:
    return {"Hello": "World"}


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "checkpoint": queue.checkpoint(CONSUMER_GROUP)}
