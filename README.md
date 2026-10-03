# task-queue-python

仅依赖本地资源（SQLite 文件 / 内存）即可运行的任务队列与投递组件，
在多消费组、并发消费、失败重试与进程重启等情况下保证一致且可解释的结果。

## 结构

- `taskqueue/models.py` — `Task`（任务共享元数据）、`GroupTask`（按组处理状态）、状态、失败原因、租约
- `taskqueue/errors.py` — 归一化错误类型，底层异常不直接透出
- `taskqueue/retry.py` — 可配置退避重试策略
- `taskqueue/store.py` — SQLite 持久化：任务、按组处理状态、租约（fencing）、按组位点
- `taskqueue/queue.py` — 队列门面：提交 / 按组租约 / ack / fail / 按组死信与状态查询
- `taskqueue/worker.py` — 单次消费循环（领取 → 执行 → 结算），输出判定日志
- `taskqueue/consumer.py` — `ConsumerService`：启动后自动消费的后台消费者
- `main.py` — FastAPI HTTP 接口

## 多消费组投递语义

- **一个任务，多组各处理一次**：`tasks` 表只存任务共享内容（标识、幂等键、
  payload）；每个消费组首次领取该任务时，在 `group_tasks` 中惰性建立一份
  **独立处理进度**（状态、尝试次数、可见时间、fencing 计数）。
- **组间互不干扰**：A 组 ack 成功或判成失败/死信，完全不影响 B 组能否领取
  同一任务；各组各自推进自己的状态与位点。
- **组内并发安全**：同一 (任务, 组) 同一时刻只有一个有效租约；同组可以起多个
  消费者并发抢任务，一个任务只会被其中一个执行。消费者崩溃或租约到期后，
  任务被其他消费者重新领取，不丢任务。
- **至少一次投递**：ack 前任务可能因租约到期被重投，业务处理器必须幂等。
- **幂等提交**：提交必须携带 `idempotency_key`，重复提交返回既有任务
  （`created=false`），不产生重复任务或副作用。

## 各组进度与失败口径

按组查询 `GroupTask` 时，`failure_reason` 严格区分三种情况，不会混淆：

| failure_reason | 含义 | attempts 是否 +1 | 后续状态 |
| --- | --- | --- | --- |
| `HANDLER_ERROR` | 业务处理器抛异常 | 是 | 退避后 `FAILED_RETRYABLE` 重试 |
| `MAX_ATTEMPTS_EXCEEDED` | 业务重试次数耗尽 | 是（最后一次） | `DEAD` 死信，该组不再投递 |
| `LEASE_EXPIRED` | 消费者掉线，租约到期被重新投递 | **否** | 仍可被领取，成功后清回 `NONE` |
| `NONE` | 无失败（待处理 / 已成功） | — | — |

- 重试次数、死信判定**按组独立统计**：A 组把任务判死后，B 组仍能正常领取。
- 租约到期重投只更新组状态为 `LEASED` 并把原因记为 `LEASE_EXPIRED`，
  **不增加尝试次数、不算业务失败**；重投后若处理成功，原因清为 `NONE`。
- 死信按组查询：`dead_letters(group)` / `GET /groups/{group}/dead-letters`。

## 租约与位点规则

- 租约维度是 (task_id, grp)，携带单调递增的 fencing token（计数保存在
  `group_tasks.fence`，跨 ack/fail 结算周期也不回退）。旧持有者的
  ack/fail 一律以 `LEASE_CONFLICT` 拒绝，结算不会算重。
- 租约到期后任务可被其他消费者重新领取，返回的 `GroupTask.redelivered=True`。
- 位点按组维护（`offsets` 表）：ack / 死信后自动推进到已连续终结的最大序号；
  外部提交位点只允许严格 +1，回退或跳跃抛 `OFFSET_REGRESSION`。

## 自动消费

`ConsumerService(queue, group, handler, consumer_id=...)` 是一个后台线程：

- 服务 `start()` 后自行 `lease → handler → ack/fail` 循环，无需手动领任务；
- 同一组起多个实例（不同 `consumer_id`）即并发消费，靠租约保证不重复执行；
- 实例崩溃后，其手上的任务在 `lease_ttl` 到期后被其他实例重新领走；
- `stop()` 优雅退出。

HTTP 服务可用 `TASK_QUEUE_AUTO_CONSUME_GROUPS=g1,g2` 在启动时为每个组
自动挂一个演示消费者（实际接入请注入业务 handler）。

## 失败重试与退避

- 业务失败按 `RetryPolicy` 退避重排：
  `delay = min(base_delay * multiplier^(attempt-1), max_delay)`。
- 处理器抛出的任意异常归一化为 `TASK_HANDLER_ERROR`（保留原始类型名），
  HTTP 接口只透出归一化错误码与消息。

## 可配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `TASK_QUEUE_DB` | `taskqueue.db` | SQLite 路径，`:memory:` 为纯内存 |
| `TASK_QUEUE_RETRY_BASE_DELAY` | `0.5` | 首次重试基础间隔（秒） |
| `TASK_QUEUE_RETRY_MULTIPLIER` | `2.0` | 退避倍率 |
| `TASK_QUEUE_RETRY_MAX_DELAY` | `30` | 退避上限（秒） |
| `TASK_QUEUE_MAX_ATTEMPTS` | `3` | 默认最大业务尝试次数（可被单次提交覆盖） |
| `TASK_QUEUE_LEASE_TTL` | `5` | 租约时长（秒），到期未结算可被重投 |
| `TASK_QUEUE_AUTO_CONSUME_GROUPS` | 空 | 逗号分隔的组名，启动后为这些组自动消费 |

代码内 `ConsumerService(poll_interval=..., idle_sleep=...)` 还可调节轮询节奏。

## 本地验证

```bash
uv sync
uv run pytest tests/ -q                                # 运行全部行为测试
uv run pytest tests/ -q --log-cli-level=INFO           # 查看任务标识/组/尝试次数/判定依据日志
TASK_QUEUE_AUTO_CONSUME_GROUPS=g1 uv run uvicorn main:app   # 带自动消费启动 HTTP 服务
```

HTTP 示例（两个组各领一次同一任务）：

```bash
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"idempotency_key": "k1", "payload": "hello"}'
# 组 g1 领取并成功
curl -X POST localhost:8000/leases -H 'content-type: application/json' \
  -d '{"group": "g1", "consumer_id": "c1"}'
curl -X POST localhost:8000/tasks/<task_id>/ack -H 'content-type: application/json' \
  -d '{"group": "g1", "fence": 1}'
# 组 g2 仍可独立领取同一任务
curl -X POST localhost:8000/leases -H 'content-type: application/json' \
  -d '{"group": "g2", "consumer_id": "c2"}'
# 按组查看进度与失败原因（HANDLER_ERROR / MAX_ATTEMPTS_EXCEEDED / LEASE_EXPIRED）
curl localhost:8000/tasks/<task_id>/groups/g1
# 按组查看死信
curl localhost:8000/groups/g1/dead-letters
```

## 测试覆盖

`tests/test_taskqueue.py`（17 个用例）覆盖：

- 多组独立投递：两组各领取并结算同一任务、两组并发消费同一批任务进度独立；
- 按组失败/死信：A 组判死后 B 组仍能成功、死信按组查询不串组；
- 三种失败口径区分：`HANDLER_ERROR` / `LEASE_EXPIRED` /
  `MAX_ATTEMPTS_EXCEEDED`，租约到期重投不增加尝试次数；
- 自动消费：服务启动后自行处理、同组 4 消费者并发不重复执行、
  两组同一批任务结果各自独立、崩溃后租约到期被重新领走且日志可查；
- 回归：幂等去重、退避重试与死信、错误归一化、租约 fencing 最终一致、
  位点回退/跳跃拒绝、进程重启后状态与幂等保持。
