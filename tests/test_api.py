"""HTTP 接口层测试：归一化错误映射与幂等语义。"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from task_queue import QueueConfig, SQLiteStore, TaskQueue
from task_queue.api import create_router, install_exception_handlers
from tests.conftest import make_retry_policy


@pytest.fixture
def client_and_queue():
    queue = TaskQueue(
        SQLiteStore(":memory:"),
        QueueConfig(retry_policy=make_retry_policy(max_attempts=2)),
    )
    queue.start()
    queue.register_handler("ok", lambda task, attempt: None)
    queue.register_handler(
        "always_fail", lambda t, a: (_ for _ in ()).throw(ValueError("boom"))
    )
    app = FastAPI()
    install_exception_handlers(app)
    app.include_router(create_router(queue))
    client = TestClient(app)
    yield client, queue
    queue.close()


def test_submit_and_dedup_over_http(client_and_queue) -> None:
    client, queue = client_and_queue
    body = {"idempotency_key": "api-1", "task_type": "ok", "payload": {"n": 1}, "groups": "g1"}
    r1 = client.post("/tasks", json=body)
    assert r1.status_code == 200 and r1.json()["created"] is True
    r2 = client.post("/tasks", json=body)
    assert r2.status_code == 200 and r2.json()["created"] is False
    assert r1.json()["task_id"] == r2.json()["task_id"]
    # 只有一条投递。
    assert len(queue.store.list_deliveries("g1")) == 1


def test_idempotency_conflict_returns_409(client_and_queue) -> None:
    client, _ = client_and_queue
    client.post("/tasks", json={"idempotency_key": "api-2", "task_type": "ok", "payload": {"n": 1}})
    r = client.post("/tasks", json={"idempotency_key": "api-2", "task_type": "ok", "payload": {"n": 999}})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_conflict"


def test_checkpoint_rollback_and_jump_rejected_over_http(client_and_queue) -> None:
    client, queue = client_and_queue
    client.post("/tasks", json={"idempotency_key": "api-3", "task_type": "ok", "groups": "g1"})
    queue.poll_and_process("g1", "c1")  # 成功 -> checkpoint=1

    # 回退到 1（current=1）被拒。
    r = client.post("/groups/g1/checkpoint", json={"seq": 1, "consumer": "c1"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "checkpoint_rejected"
    assert "rollback" in r.json()["error"]["message"]

    # 跳跃到 3 被拒。
    r = client.post("/groups/g1/checkpoint", json={"seq": 3, "consumer": "c1"})
    assert r.status_code == 409
    assert "jump" in r.json()["error"]["message"]

    # 位点未被破坏。
    assert client.get("/groups/g1/checkpoint").json()["checkpoint"] == 1


def test_dead_letters_endpoint_distinguishes_reasons(client_and_queue) -> None:
    client, queue = client_and_queue
    client.post("/tasks", json={"idempotency_key": "api-4", "task_type": "always_fail", "groups": "g1"})
    # max_attempts=2：两次失败后进死信。
    queue.poll_and_process("g1", "c1")
    # 退避窗口：直接推进存储层可见时间以便立即重投。
    conn = queue.store.conn
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("UPDATE deliveries SET visible_at=0 WHERE group_name='g1'")
    conn.commit()
    queue.poll_and_process("g1", "c1")

    r = client.get("/groups/g1/dead-letters")
    assert r.status_code == 200
    letters = r.json()["dead_letters"]
    assert len(letters) == 1
    assert letters[0]["reason"] == "max_retries_exceeded"
    assert "ValueError" in letters[0]["error"]


def test_get_missing_task_returns_404(client_and_queue) -> None:
    client, _ = client_and_queue
    r = client.get("/tasks/does-not-exist")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "task_not_found"
