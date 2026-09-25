# task-queue-python

仅依赖本地资源（SQLite 文件 / 内存）即可运行的任务队列与投递组件，
在并发消费、失败重试与进程重启等情况下保证一致且可解释的结果。

## 结构

- `taskqueue/models.py` — 任务、状态、失败原因、租约的数据模型
- `taskqueue/errors.py` — 归一化错误类型，底层异常不直接透出
- `taskqueue/retry.py` — 可配置退避重试策略
- `taskqueue/store.py` — SQLite 持久化：任务、租约（fencing）、消费组位点
- `taskqueue/queue.py` — 队列门面：提交 / 租约 / ack / fail / 死信
- `taskqueue/worker.py` — 消费者循环
- `main.py` — FastAPI HTTP 接口

## 投递语义

- **至少一次投递**：任务被确认（ack）前可能因租约到期被重新投递；
  消费者必须幂等处理。
- **幂等提交**：提交时必须携带 `idempotency_key`，重复提交返回既有任务
  （`created=false`），不产生重复任务或重复副作用。
- **消费进度按消费者组维护**：每组一个位点（offset），ack / 死信后自动推进；
  重复投递同一任务时，由 fencing token 保证只有有效租约的结算生效，
  最终状态一致。

## 去重与租约规则

- 任务表对 `idempotency_key` 唯一约束，重复插入被忽略。
- 同一任务同一时刻只允许一个租约；租约携带单调递增的 fencing token。
- 消费者中断或租约到期（`lease_ttl`）后，任务可被其他消费者重新领取，
  旧持有者的 ack/fail 会因 fence 不匹配被 `LEASE_CONFLICT` 拒绝，任务不丢失。
- 外部提交位点只允许严格 +1 递增；回退或跳跃抛出 `OFFSET_REGRESSION`，
  不会从中间继续。

## 失败重试与死信

- 失败按 `RetryPolicy` 退避重排：`delay = min(base_delay * multiplier^(attempt-1), max_delay)`。
- 达到 `max_attempts` 后进入死信，`failure_reason` 可区分：
  `HANDLER_ERROR`（处理异常，可重试）、`MAX_ATTEMPTS_EXCEEDED`（死信）、
  `LEASE_EXPIRED`、`NONE`。
- 处理器抛出的任意异常归一化为 `TASK_HANDLER_ERROR`（保留原始类型名），
  HTTP 接口只透出归一化错误码与消息。

## 可配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `TASK_QUEUE_DB` | `taskqueue.db` | SQLite 路径，`:memory:` 为纯内存 |
| `TASK_QUEUE_RETRY_BASE_DELAY` | `0.5` | 首次重试基础间隔（秒） |
| `TASK_QUEUE_RETRY_MULTIPLIER` | `2.0` | 退避倍率 |
| `TASK_QUEUE_RETRY_MAX_DELAY` | `30` | 退避上限（秒） |
| `TASK_QUEUE_MAX_ATTEMPTS` | `3` | 最大尝试次数 |
| `TASK_QUEUE_LEASE_TTL` | `5` | 租约时长（秒） |

## 本地验证

```bash
uv sync
uv run pytest tests/ -q          # 运行全部行为测试
uv run pytest tests/ -q -s --log-cli-level=INFO   # 查看任务标识/尝试次数/判定依据日志
uv run uvicorn main:app          # 启动 HTTP 服务
```

HTTP 示例：

```bash
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"idempotency_key": "k1", "payload": "hello"}'
curl -X POST localhost:8000/leases -H 'content-type: application/json' \
  -d '{"group": "g", "consumer_id": "c1"}'
curl -X POST localhost:8000/tasks/<task_id>/ack -H 'content-type: application/json' \
  -d '{"group": "g", "fence": 1}'
```

## 测试覆盖

`tests/test_taskqueue.py` 覆盖：幂等去重、重复投递最终一致、退避重试与死信、
错误归一化、多消费者并发不重复执行、租约到期重投不丢失、位点回退/跳跃拒绝、
进程重启后状态与幂等保持。
