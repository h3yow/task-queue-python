# 本地任务队列与投递组件

一套**仅依赖本地资源**（Python 标准库 `sqlite3` + FastAPI）即可运行的任务队列，
在并发消费、失败重试与进程重启等情况下保证一致且可解释的结果。

- 持久化：本地 SQLite 文件（默认 `queue.db`），进程重启后任务、租约与位点完整恢复。
- 并发：多消费者通过原子条件更新抢占租约，同一任务同一时刻只被一个消费者执行。
- 可靠：失败按可配置退避重试，超限或毒消息进入死信，失败原因可区分。
- 可解释：每个决策（提交/去重/重试/死信/回收/位点）都打印带任务标识、尝试次数与判定依据的日志。

## 快速开始

```bash
# 安装依赖（uv）
uv sync

# 运行全部测试
uv run pytest -q

# 启动 HTTP 服务（默认监听 8000，使用 ./queue.db）
uv run uvicorn main:app --reload
```

## 核心概念与投递语义

| 概念 | 说明 |
| --- | --- |
| **Task（任务）** | 全局唯一，由幂等键标识，包含 `task_type` 与 `payload`。 |
| **Consumer Group（消费者组）** | 同一任务可被多个组各自独立消费；进度按组维护。 |
| **Delivery（投递）** | 任务 × 组 的一条可领取记录，携带状态、尝试次数、租约。 |
| **Lease（租约）** | 消费者领取任务时获得的排他所有权，带过期时间。 |
| **Checkpoint（位点）** | 组内已“连续终结”的最大序号，单调递增。 |

投递语义为 **at-least-once（至少一次）**：任务可能被重复投递（重试、租约回收），
因此处理器应具备幂等性；组件保证**同一任务同一时刻只被一个消费者执行**，
且**成功副作用在一致状态下只被确认一次**（迟到的重复结果会被租约守卫拒绝）。

## 幂等与去重规则

- 提交任务必须携带 `idempotency_key`。
- **相同幂等键 + 相同内容**（`task_type` 与 `payload` 一致）：返回同一个任务，
  不产生重复任务、重复投递或重复副作用（HTTP 返回 `created=false`）。
- **相同幂等键 + 不同内容**：明确拒绝，抛出 `DuplicateIdempotencyKeyError`
  （HTTP `409 idempotency_conflict`），不会静默覆盖。
- 去重基于数据库唯一约束，进程重启后依然有效。

## 租约规则

- 消费者通过 `fetch_available` 领取任务，获得 `lease_seconds` 的排他租约。
- 租约有效期内，其他消费者**无法**领取同一任务。
- 处理结果（成功/重试/死信）只能由**当前租约持有者**提交；租约过期后，
  原持有者的迟到结果会被拒绝（`LeaseExpiredError`），不会覆盖新状态。
- 租约到期且未收到结果时，任务被**回收**为可投递状态（`reclaim_expired`），
  由其他消费者重新执行——任务不丢失。回收**不增加**尝试次数，重新领取才计入新尝试。

## 重试与死信

- 处理器抛出普通异常 → 进入 `retrying`，按退避策略延迟后重新投递。
- 退避间隔随尝试次数增长：`delay = min(base_delay * multiplier^(attempt-1), max_delay)`，
  可选叠加随机抖动 `jitter`。
- 尝试次数达到 `max_attempts` → 进入死信，原因 `max_retries_exceeded`。
- 处理器抛出 `PoisonTaskError`（或缺少对应处理器）→ 立即进入死信，原因 `poison_task`。
- 所有底层异常被**归一化**为 `TypeName: message` 形式记录，不会把原始异常栈透出到对外接口；
  对外只暴露 `task_queue.errors` 中定义的稳定错误类型。

## 位点（Checkpoint）规则

- 位点表示组内已**连续终结**（成功或死信）的最大序号，只能单调前进。
- 任务成功或进入死信时，位点自动连续推进。
- 显式提交 `commit(group, seq)` 仅接受 `seq == checkpoint + 1` 且该投递已终结：
  - `seq <= checkpoint` → **回退**，拒绝（`CheckpointRejectedError`，原因含 `rollback`）；
  - `seq > checkpoint + 1` → **跳跃**，拒绝（原因含 `jump`）；
  - 对应投递未终结（pending/inflight/retrying）→ 拒绝（原因含 `not terminal`）。
- 位点**不会**从中间状态继续，也不会因回退/跳跃而被破坏。

## 可配置项

通过 `QueueConfig` 与 `RetryPolicy` 配置（见 `task_queue/models.py`）：

| 配置项 | 默认 | 说明 |
| --- | --- | --- |
| `lease_seconds` | 30.0 | 租约有效期（秒） |
| `reclaim_interval_seconds` | 1.0 | 后台回收过期租约的间隔 |
| `retry_policy.max_attempts` | 5 | 最大尝试次数（含首次） |
| `retry_policy.base_delay_seconds` | 1.0 | 首次失败后的基础退避 |
| `retry_policy.backoff_multiplier` | 2.0 | 退避增长倍数 |
| `retry_policy.max_delay_seconds` | 60.0 | 退避上限 |
| `retry_policy.jitter_seconds` | 0.0 | 随机抖动上限 |

服务级环境变量（`main.py`）：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `TASK_QUEUE_DB` | `queue.db` | SQLite 数据库文件路径 |
| `TASK_QUEUE_GROUP` | `default` | 主消费者组名 |

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/tasks` | 提交任务（幂等），返回 `task_id` 与 `created` |
| `GET` | `/tasks/{task_id}` | 查询任务 |
| `GET` | `/groups/{group}/stats` | 组内各状态计数、位点与死信列表 |
| `GET` | `/groups/{group}/checkpoint` | 查询位点 |
| `POST` | `/groups/{group}/checkpoint` | 显式提交位点（回退/跳跃返回 409） |
| `GET` | `/groups/{group}/dead-letters` | 死信列表（含可区分原因） |
| `GET` | `/health` | 健康检查 |

错误统一为 `{"error": {"code": ..., "message": ...}}`，状态码语义：
`404` 任务不存在、`409` 幂等冲突/位点拒绝/租约冲突、`422` 缺少处理器、
`503` 队列已关闭、`500` 存储错误（细节不外泄）。

## 本地验证方法

```bash
# 1. 运行全部单元/集成测试（覆盖去重、重试、租约超时、并发、位点、HTTP）
uv run pytest -q

# 2. 查看带判定依据的决策日志（测试会打印 task/attempt/decision/reason）
uv run pytest -q -s

# 3. 启动服务并手动验证
TASK_QUEUE_DB=/tmp/demo.db uv run uvicorn main:app --port 8000 &
curl -s -X POST localhost:8000/tasks \
  -H 'Content-Type: application/json' \
  -d '{"idempotency_key":"demo-1","task_type":"echo","payload":{"msg":"hi"},"groups":"default"}'
curl -s localhost:8000/groups/default/stats
```

## 项目结构

```
task_queue/
  models.py         # 领域模型与可配置项（RetryPolicy/QueueConfig/Task/Delivery）
  errors.py         # 归一化错误类型（对外唯一暴露的异常）
  store.py          # SQLite 持久化：任务、投递、租约、位点（唯一事实来源）
  queue.py          # 门面：提交去重、消费、退避重试、死信判定、位点
  worker.py         # 后台消费者线程（并发消费 + 租约回收）
  api.py            # FastAPI 路由与归一化错误 -> HTTP 映射
  logging_utils.py  # 决策日志（task/attempt/decision/reason）
main.py             # 应用装配与生命周期
tests/              # 去重/重试/租约/并发/位点/HTTP 场景测试
```
