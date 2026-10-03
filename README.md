# task-queue-python

仅依赖本地资源（SQLite 文件 / 内存）即可运行的任务队列与投递组件，
在并发消费、失败重试、多消费组与进程重启等情况下保证一致且可解释的结果。

## 结构

- `taskqueue/models.py` — 任务、按组处理状态、失败原因、租约的数据模型
- `taskqueue/errors.py` — 归一化错误类型，底层异常不直接透出
- `taskqueue/retry.py` — 可配置退避重试策略
- `taskqueue/store.py` — SQLite 持久化：任务、按组状态、租约（fencing）、按组位点
- `taskqueue/queue.py` — 队列门面：提交 / 按组租约 / ack / fail / 按组死信
- `taskqueue/worker.py` — 消费者：一次性处理与后台自动消费循环
- `main.py` — FastAPI HTTP 接口（可选启动自动消费 worker）

## 多消费组投递语义

任务描述（`task_id`、`idempotency_key`、`payload`）全局只有一份，
但**每个消费组对每个任务各自持有一行独立的处理状态**
（`group_task_state`，主键 `(task_id, grp)`）：

- 同一个任务提交后，每个组都能各自领取并独立处理一次，互不干扰；
  组之间不共享处理结果：A 组 ack 成功或判死信，B 组照样能领到、处理它。
- 租约也按 `(task_id, grp)` 维度持有：不同组可以同时持有同一个任务，
  但**同一组内**同一任务同一时刻只允许一个消费者执行；
  fencing token 按 `(task_id, grp)` 各自单调递增。
- 组状态在该组首次领取任务时惰性创建，因此后续新加入的组也能领到全部历史任务。
- 位点（offset）按组维护，只统计本组已连续终结（成功 / 死信）的进度。

## 各组进度与失败口径

每组独立维护 `attempts`（业务尝试次数）、`status`、`failure_reason`，
通过 `queue.get_state(task_id, group)`（HTTP `GET /tasks/{id}/state?group=`）
可以按组分清三种失败/重投依据，绝不混成一种：

| failure_reason | 含义 | 是否累加 attempts |
| --- | --- | --- |
| `HANDLER_ERROR` | 业务处理器抛异常，任务进入可重试 | 是（+1） |
| `MAX_ATTEMPTS_EXCEEDED` | 业务失败重试次数用尽，进入该组死信 | 是（最后一次 +1） |
| `LEASE_EXPIRED` | 消费者中途掉线 / 租约到期，任务被重新投递 | **否，也不算业务失败** |
| `NONE` | 尚未失败或已成功 | 否 |

- 退避重试与死信判定各算各的：一个组把任务判成死信后，
  其他组仍可正常领取和处理；死信按组查询（`dead_letters(group)`）。
- 租约到期重投只是换个消费者继续执行，不推进该组尝试次数；
  待任务真正被 ack/fail 结算后，`failure_reason` 会被结算结果覆盖。
- 旧租约持有者迟到的 ack/fail 会因 fence 不匹配被 `LEASE_CONFLICT` 拒绝。

## 自动消费

- `Worker.start()` 启动后台守护线程：服务启动后自动轮询领取并处理提交进来的任务，
  无需手动一个个去领；`Worker.stop()` 优雅停止。
- 同组起多个 `Worker`（不同 `consumer_id`）即并发消费：
  租约互斥保证同一任务不会被两个消费者同时执行。
- 消费者进程崩溃（处理器抛出 `SystemExit` 等）或租约到期后，
  任务在 `lease_ttl` 到期后被其他消费者重新领走，不丢任务，也不重复计尝试次数。
- HTTP 服务可通过环境变量 `TASK_QUEUE_GROUPS=g1,g2` 在启动时为每个组
  各起一个自动消费 worker（内置演示处理器：payload 以 `fail:` 开头即抛异常）。

## 去重与租约规则

- 任务表对 `idempotency_key` 唯一约束，重复提交返回既有任务（`created=false`），
  不产生重复任务或重复副作用。
- 同一 `(任务, 组)` 同一时刻只允许一个租约；租约携带单调递增的 fencing token。
- 外部提交位点只允许严格 +1 递增；回退或跳跃抛出 `OFFSET_REGRESSION`。

## 失败重试与死信

- 失败按 `RetryPolicy` 退避重排：`delay = min(base_delay * multiplier^(attempt-1), max_delay)`。
- `max_attempts` 可在提交任务时单独指定，缺省取队列策略值；该值在各组首次领取时
  复制到该组状态，之后按组独立计数。
- 处理器抛出的任意异常归一化为 `TASK_HANDLER_ERROR`（保留原始类型名），
  HTTP 接口只透出归一化错误码与消息。

## 可配置项

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `TASK_QUEUE_DB` | `taskqueue.db` | SQLite 路径，`:memory:` 为纯内存 |
| `TASK_QUEUE_RETRY_BASE_DELAY` | `0.5` | 首次重试基础间隔（秒） |
| `TASK_QUEUE_RETRY_MULTIPLIER` | `2.0` | 退避倍率 |
| `TASK_QUEUE_RETRY_MAX_DELAY` | `30` | 退避上限（秒） |
| `TASK_QUEUE_MAX_ATTEMPTS` | `3` | 默认最大业务尝试次数（每组独立计数） |
| `TASK_QUEUE_LEASE_TTL` | `5` | 租约时长（秒），到期未结算则重新投递 |
| `TASK_QUEUE_GROUPS` | （空） | 服务启动即自动消费的组，逗号分隔，如 `g1,g2` |
| `TASK_QUEUE_POLL_INTERVAL` | `0.1` | 自动消费 worker 空轮询间隔（秒） |

> 注意：旧版本数据库是单组表结构，与本版本不兼容；删除旧 `taskqueue.db`
> 后重启即可按新结构自动建库。

## 本地验证

```bash
uv sync
uv run pytest tests/ -q          # 运行全部行为测试（17 个）
uv run pytest tests/ -q -s --log-cli-level=INFO   # 查看任务标识/尝试次数/判定依据日志
TASK_QUEUE_GROUPS=g1,g2 uv run uvicorn main:app   # 两个组自动消费
```

HTTP 示例：

```bash
curl -X POST localhost:8000/tasks -H 'content-type: application/json' \
  -d '{"idempotency_key": "k1", "payload": "hello"}'
curl -X POST localhost:8000/leases -H 'content-type: application/json' \
  -d '{"group": "g1", "consumer_id": "c1"}'
curl -X POST localhost:8000/tasks/<task_id>/ack -H 'content-type: application/json' \
  -d '{"group": "g1", "fence": 1}'
# 按组查询处理状态（三种失败口径可区分）
curl 'localhost:8000/tasks/<task_id>/state?group=g1'
# 按组查询死信
curl localhost:8000/groups/g1/dead-letters
```

库内直接使用：

```python
q = TaskQueue(TaskStore(), lease_ttl=5.0)
q.submit("k1", "payload")
w1 = Worker(q, group="g1", consumer_id="c1", handler=my_handler)
w2 = Worker(q, group="g2", consumer_id="c2", handler=another_handler)
w1.start(); w2.start()   # 两个组各自自动消费同一批任务
```

## 测试覆盖

- `tests/test_taskqueue.py`（原有用例，适配按组接口，无回归）：幂等去重、
  重复投递最终一致、退避重试与死信、错误归一化、多消费者并发不重复执行、
  租约到期重投不丢失、位点回退/跳跃拒绝、进程重启后状态与幂等保持。
- `tests/test_multigroup.py`（新增）：多组各自领取并独立 ack、一组死信不阻塞
  另一组、三种失败原因按组可区分、各组位点独立推进。
- `tests/test_worker_auto.py`（新增）：启动后自动消费、同组 4 消费者并发不重复
  执行、消费者崩溃后租约到期被接管且不丢不重计、两个组自动消费同一批结果独立。
