"""任务队列 HTTP 接口：底层异常一律归一化后透出。"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from taskqueue import (
    ConsumerService,
    RetryPolicy,
    TaskQueue,
    TaskQueueError,
    TaskStore,
)
from taskqueue.models import GroupTask, Task

_queue = TaskQueue(
    TaskStore(os.environ.get("TASK_QUEUE_DB", "taskqueue.db")),
    retry_policy=RetryPolicy(
        base_delay=float(os.environ.get("TASK_QUEUE_RETRY_BASE_DELAY", "0.5")),
        multiplier=float(os.environ.get("TASK_QUEUE_RETRY_MULTIPLIER", "2.0")),
        max_delay=float(os.environ.get("TASK_QUEUE_RETRY_MAX_DELAY", "30")),
        max_attempts=int(os.environ.get("TASK_QUEUE_MAX_ATTEMPTS", "3")),
    ),
    lease_ttl=float(os.environ.get("TASK_QUEUE_LEASE_TTL", "5")),
)

logger = logging.getLogger("taskqueue.demo")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 自动消费：TASK_QUEUE_AUTO_CONSUME_GROUPS="g1,g2" 时为每个组启动一个后台消费者。
    # 处理器仅做演示，生产代码应注入自己的业务处理器。
    groups = [
        g.strip()
        for g in os.environ.get("TASK_QUEUE_AUTO_CONSUME_GROUPS", "").split(",")
        if g.strip()
    ]
    services = [
        ConsumerService(
            _queue,
            grp,
            handler=lambda payload, grp=grp: logger.info(
                "auto handled group=%s payload=%s", grp, payload
            ),
            consumer_id=f"{grp}-auto",
        )
        for grp in groups
    ]
    for svc in services:
        svc.start()
    app.state.auto_services = services
    yield
    for svc in services:
        svc.stop()


app = FastAPI(title="task-queue", lifespan=lifespan)


def _task_view(t: Task) -> dict:
    return {
        "task_id": t.task_id,
        "idempotency_key": t.idempotency_key,
        "created_at": t.created_at,
    }


def _group_task_view(g: GroupTask) -> dict:
    return {
        "task_id": g.task_id,
        "group": g.grp,
        "status": g.status.value,
        "attempts": g.attempts,
        "max_attempts": g.max_attempts,
        "failure_reason": g.failure_reason.value,
        "last_error": g.last_error,
    }


def _guard(fn):
    try:
        return fn()
    except TaskQueueError as exc:
        # 只透出归一化错误码与消息，不透出底层异常
        raise HTTPException(status_code=409, detail={"code": exc.code, "message": exc.message})


class SubmitRequest(BaseModel):
    idempotency_key: str
    payload: str
    max_attempts: int | None = None


class LeaseRequest(BaseModel):
    group: str
    consumer_id: str


class SettleRequest(BaseModel):
    group: str
    fence: int
    error: str | None = None


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/tasks", status_code=201)
def submit_task(req: SubmitRequest):
    task, created = _guard(
        lambda: _queue.submit(req.idempotency_key, req.payload, req.max_attempts)
    )
    return {"created": created, "task": _task_view(task)}


@app.post("/leases")
def lease_task(req: LeaseRequest):
    leased = _guard(lambda: _queue.lease(req.group, req.consumer_id))
    if leased is None:
        return {"leased": False}
    gt, fence = leased
    view = _group_task_view(gt)
    view["payload"] = gt.payload
    view["redelivered"] = gt.redelivered
    return {"leased": True, "fence": fence, "group_task": view}


@app.post("/tasks/{task_id}/ack")
def ack_task(task_id: str, req: SettleRequest):
    _guard(lambda: _queue.ack(task_id, req.group, req.fence))
    return {"status": "SUCCEEDED"}


@app.post("/tasks/{task_id}/fail")
def fail_task(task_id: str, req: SettleRequest):
    _guard(lambda: _queue.fail(task_id, req.group, req.fence, RuntimeError(req.error or "failed")))
    return {"group_task": _group_task_view(_queue.get_group_task(task_id, req.group))}


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    return {"task": _task_view(_guard(lambda: _queue.get_task(task_id)))}


@app.get("/tasks/{task_id}/groups/{group}")
def get_group_task(task_id: str, group: str):
    """按组查询处理进度与失败判定。"""
    gt = _guard(lambda: _queue.get_group_task(task_id, group))
    return {"group_task": _group_task_view(gt)}


@app.get("/groups/{group}/dead-letters")
def dead_letters(group: str):
    return {"group": group, "dead_letters": [_group_task_view(g) for g in _queue.dead_letters(group)]}


@app.get("/offsets/{group}")
def get_offset(group: str):
    return {"group": group, "offset": _queue.get_offset(group)}
