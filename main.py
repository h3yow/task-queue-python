"""任务队列 HTTP 接口：底层异常一律归一化后透出。"""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from taskqueue import RetryPolicy, TaskQueue, TaskQueueError, TaskStore
from taskqueue.models import Task

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


def _task_view(t: Task) -> dict:
    return {
        "task_id": t.task_id,
        "idempotency_key": t.idempotency_key,
        "status": t.status.value,
        "attempts": t.attempts,
        "failure_reason": t.failure_reason.value,
        "last_error": t.last_error,
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
    task, fence = leased
    return {"leased": True, "fence": fence, "task": _task_view(task) | {"payload": task.payload}}


@app.post("/tasks/{task_id}/ack")
def ack_task(task_id: str, req: SettleRequest):
    _guard(lambda: _queue.ack(task_id, req.group, req.fence))
    return {"status": "SUCCEEDED"}


@app.post("/tasks/{task_id}/fail")
def fail_task(task_id: str, req: SettleRequest):
    _guard(lambda: _queue.fail(task_id, req.group, req.fence, RuntimeError(req.error or "failed")))
    return {"task": _task_view(_queue.get_task(task_id))}


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    return {"task": _task_view(_guard(lambda: _queue.get_task(task_id)))}


@app.get("/dead-letters")
def dead_letters():
    return {"dead_letters": [_task_view(t) for t in _queue.dead_letters()]}


@app.get("/offsets/{group}")
def get_offset(group: str):
    return {"group": group, "offset": _queue.get_offset(group)}
