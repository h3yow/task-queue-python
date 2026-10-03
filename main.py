"""任务队列 HTTP 接口：底层异常一律归一化后透出，状态按消费组区分。"""

from __future__ import annotations

import logging
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from taskqueue import RetryPolicy, TaskQueue, TaskQueueError, TaskStore, Worker
from taskqueue.models import GroupTaskState, Task

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="task-queue")

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
_poll_interval = float(os.environ.get("TASK_QUEUE_POLL_INTERVAL", "0.1"))
# 服务启动即自动消费的组，逗号分隔，例如 "order,notify"
_worker_groups = [g for g in os.environ.get("TASK_QUEUE_GROUPS", "").split(",") if g]
_workers: list[Worker] = []


def _demo_handler(payload: str) -> None:
    """本地演示处理器：payload 以 fail: 开头时抛异常，便于验证失败/死信链路。"""
    if payload.startswith("fail:"):
        raise RuntimeError(payload[len("fail:"):])


@app.on_event("startup")
def _start_workers() -> None:
    for idx, group in enumerate(_worker_groups):
        w = Worker(
            _queue,
            group=group,
            consumer_id=f"{group}-auto-{idx}",
            handler=_demo_handler,
            poll_interval=_poll_interval,
        )
        w.start()
        _workers.append(w)


@app.on_event("shutdown")
def _stop_workers() -> None:
    for w in _workers:
        w.stop(timeout=2.0)


def _task_view(t: Task) -> dict:
    return {
        "task_id": t.task_id,
        "idempotency_key": t.idempotency_key,
        "payload": t.payload,
        "max_attempts": t.max_attempts,
    }


def _state_view(s: GroupTaskState) -> dict:
    return {
        "task_id": s.task_id,
        "group": s.group,
        "status": s.status.value,
        "attempts": s.attempts,
        "max_attempts": s.max_attempts,
        "failure_reason": s.failure_reason.value,
        "last_error": s.last_error,
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


class StateRequest(BaseModel):
    group: str


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
    task, state, fence = leased
    return {
        "leased": True,
        "fence": fence,
        "task": _task_view(task) | {"payload": task.payload},
        "state": _state_view(state),
    }


@app.post("/tasks/{task_id}/ack")
def ack_task(task_id: str, req: SettleRequest):
    _guard(lambda: _queue.ack(task_id, req.group, req.fence))
    return {"status": "SUCCEEDED"}


@app.post("/tasks/{task_id}/fail")
def fail_task(task_id: str, req: SettleRequest):
    _guard(
        lambda: _queue.fail(
            task_id, req.group, req.fence, RuntimeError(req.error or "failed")
        )
    )
    return {"state": _state_view(_queue.get_state(task_id, req.group))}


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    return {"task": _task_view(_guard(lambda: _queue.get_task(task_id)))}


@app.get("/tasks/{task_id}/state")
def get_task_state(task_id: str, group: str):
    """按组查询处理状态：HANDLER_ERROR / MAX_ATTEMPTS_EXCEEDED / LEASE_EXPIRED 可区分。"""
    return {"state": _state_view(_guard(lambda: _queue.get_state(task_id, group)))}


@app.get("/groups/{group}/dead-letters")
def group_dead_letters(group: str):
    return {"group": group, "dead_letters": [_state_view(s) for s in _queue.dead_letters(group)]}


@app.get("/offsets/{group}")
def get_offset(group: str):
    return {"group": group, "offset": _queue.get_offset(group)}
