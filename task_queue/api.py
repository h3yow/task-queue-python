"""FastAPI 接入层：把任务队列能力暴露为本地 HTTP 接口。

所有对外错误在此归一化为稳定的 HTTP 状态码与错误体，底层异常
（sqlite3.* 等）只记录在服务端日志，不透出到响应。
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import (
    CheckpointRejectedError,
    DuplicateIdempotencyKeyError,
    HandlerNotFoundError,
    LeaseAlreadyHeldError,
    LeaseExpiredError,
    PoisonTaskError,
    QueueClosedError,
    StorageError,
    TaskNotFoundError,
    TaskQueueError,
)
from .queue import TaskQueue

logger = logging.getLogger("task_queue.api")


# ---- 请求 / 响应模型 -----------------------------------------------------
class SubmitTaskRequest(BaseModel):
    idempotency_key: str = Field(min_length=1)
    task_type: str = Field(min_length=1)
    payload: dict = Field(default_factory=dict)
    groups: list[str] | str | None = None


class TaskResponse(BaseModel):
    task_id: str
    idempotency_key: str
    task_type: str
    created: bool


class CommitCheckpointRequest(BaseModel):
    seq: int = Field(ge=1)
    consumer: str = Field(min_length=1)


class CheckpointResponse(BaseModel):
    group: str
    checkpoint: int


def _error(status: int, code: str, message: str, **extra: object) -> JSONResponse:
    body: dict[str, object] = {"error": {"code": code, "message": message}}
    body["error"].update(extra)
    return JSONResponse(status_code=status, content=body)


def create_router(queue: TaskQueue) -> APIRouter:
    """创建绑定到给定队列的路由。"""
    router = APIRouter()

    @router.post("/tasks", response_model=TaskResponse)
    def submit_task(req: SubmitTaskRequest) -> TaskResponse:
        result = queue.submit(
            idempotency_key=req.idempotency_key,
            task_type=req.task_type,
            payload=req.payload,
            groups=req.groups,
        )
        return TaskResponse(
            task_id=result.task.id,
            idempotency_key=result.task.idempotency_key,
            task_type=result.task.task_type,
            created=result.created,
        )

    @router.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict:
        task = queue.store.get_task(task_id)
        return {
            "task_id": task.id,
            "idempotency_key": task.idempotency_key,
            "task_type": task.task_type,
            "payload": task.payload,
            "created_at": task.created_at,
        }

    @router.get("/groups/{group}/stats")
    def group_stats(group: str) -> dict:
        return queue.stats(group)

    @router.get("/groups/{group}/checkpoint", response_model=CheckpointResponse)
    def get_checkpoint(group: str) -> CheckpointResponse:
        return CheckpointResponse(group=group, checkpoint=queue.checkpoint(group))

    @router.post("/groups/{group}/checkpoint", response_model=CheckpointResponse)
    def commit_checkpoint(group: str, req: CommitCheckpointRequest) -> CheckpointResponse:
        checkpoint = queue.commit(group, req.seq, req.consumer)
        return CheckpointResponse(group=group, checkpoint=checkpoint)

    @router.get("/groups/{group}/dead-letters")
    def dead_letters(group: str) -> dict:
        letters = queue.store.list_dead_letters(group)
        return {
            "group": group,
            "dead_letters": [
                {
                    "task_id": d.task_id,
                    "seq": d.seq,
                    "attempt": d.attempt,
                    "reason": d.dead_reason.value if d.dead_reason else None,
                    "error": d.last_error,
                }
                for d in letters
            ],
        }

    return router


def install_exception_handlers(app: FastAPI) -> None:
    """把归一化错误映射为稳定 HTTP 响应；不漏出底层异常。"""

    @app.exception_handler(DuplicateIdempotencyKeyError)
    async def _dup(_req: Request, exc: DuplicateIdempotencyKeyError) -> JSONResponse:
        return _error(
            409, "idempotency_conflict",
            "the idempotency key was already used with a different payload",
            idempotency_key=exc.idempotency_key,
        )

    @app.exception_handler(TaskNotFoundError)
    async def _not_found(_req: Request, exc: TaskNotFoundError) -> JSONResponse:
        return _error(404, "task_not_found", str(exc), task_id=exc.task_id)

    @app.exception_handler(CheckpointRejectedError)
    async def _checkpoint(_req: Request, exc: CheckpointRejectedError) -> JSONResponse:
        return _error(
            409, "checkpoint_rejected", exc.reason,
            requested=exc.requested, current=exc.current,
        )

    @app.exception_handler(LeaseExpiredError)
    async def _lease_expired(_req: Request, exc: LeaseExpiredError) -> JSONResponse:
        return _error(
            409, "lease_expired", str(exc),
            task_id=exc.task_id, consumer=exc.consumer,
        )

    @app.exception_handler(LeaseAlreadyHeldError)
    async def _lease_held(_req: Request, exc: LeaseAlreadyHeldError) -> JSONResponse:
        return _error(
            409, "lease_already_held", str(exc),
            task_id=exc.task_id, holder=exc.holder,
        )

    @app.exception_handler(HandlerNotFoundError)
    async def _no_handler(_req: Request, exc: HandlerNotFoundError) -> JSONResponse:
        return _error(422, "handler_not_found", str(exc), task_type=exc.task_type)

    @app.exception_handler(QueueClosedError)
    async def _closed(_req: Request, exc: QueueClosedError) -> JSONResponse:
        return _error(503, "queue_closed", str(exc))

    @app.exception_handler(StorageError)
    async def _storage(_req: Request, exc: StorageError) -> JSONResponse:
        # 底层细节不外泄，仅记录服务端日志并返回通用错误。
        logger.exception("storage failure")
        return _error(500, "storage_error", "a storage error occurred")

    @app.exception_handler(TaskQueueError)
    async def _fallback(_req: Request, exc: TaskQueueError) -> JSONResponse:
        logger.exception("unexpected task queue error")
        return _error(500, "task_queue_error", "an internal error occurred")

    # PoisonTaskError 是处理器内部控制信号，不应作为 HTTP 错误暴露。
    _ = PoisonTaskError
