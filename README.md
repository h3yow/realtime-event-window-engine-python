# realtime-event-window-engine

进程内的实时事件接入 + 按事件时间的滚动窗口聚合服务（FastAPI），不依赖任何外部中间件，本地可直接运行验证。

## 语义说明

### 事件时间（event_time）

每条事件必须携带 `event_time`（毫秒 epoch），聚合**只按事件时间**进行，与到达顺序无关：

- 事件允许**乱序**到达：落在同一窗口 `[start, start+size)` 内的事件会被正确聚合，无论上报顺序。
- `event_id` 是幂等键：同一 `event_id` 无论重复上报多少次（哪怕载荷不同），**最终只计入一次**，其余以 `duplicate` 拒绝。

### 水位线（watermark）

水位线 `W` 表示"事件时间早于 `W` 的数据被认为已经到齐"。推进规则：

```
W = max(已观察到的 event_time) - allowed_lateness_ms
```

- 通过 `POST /watermark/observe` 按观察到的最大事件时间自动推进，或用 `POST /watermark` 显式指定。
- 水位线**单调不减**，回退会被拒绝（`invalid`）。
- `event_time < W` 的事件视为**迟到太久（过期）**，以 `expired` 明确拒绝并计数——不会被悄悄丢掉。

### 窗口何时出结果

滚动窗口 `[start, start+size)` 在 **`W >= start+size`** 时关闭并产出结果：

- 结果一旦产出即**不可变**：之后到达的旧事件只会被 `expired` 拒绝，不会修改已出结果。
- 水位线尚未越过的窗口继续累积新事件。
- 只产出有事件的窗口（空窗口不产出）。
- 并发接入时引擎内部以锁保证同一窗口**不重算、不漏算**（有并发测试覆盖）。

### 拒绝原因分类

| 原因 | HTTP 状态码 | 含义 |
|---|---|---|
| `invalid` | 400 | 格式非法（缺字段 / 类型错 / 负时间戳等） |
| `duplicate` | 409 | `event_id` 已处理过 |
| `expired` | 410 | 事件时间早于水位线，迟到太久 |
| `backpressure` | 429 | 积压窗口达到上限，或单次拉取超过上限 |

判定顺序：非法 → 过期 → 重复 → 背压（因此"已过期窗口里的老事件重复上报"判为 `expired`）。

### 背压保护

- 已产出但未被拉走的窗口结果积压达到 `max_pending_windows` 时，新事件以 429 `backpressure` 拒绝，拉取后自动恢复。
- `GET /results?limit=N` 中 `N > max_pull_batch` 时以 429 拒绝，防止一次拉太多拖垮进程。
- 背压拒绝与其他错误用独立的 `reason` 和状态码区分。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/events` | 上报事件：`{"event_id": "...", "event_time": <ms>, "value": <number>}` |
| POST | `/watermark` | 显式推进水位线：`{"watermark": <ms>}`，返回本次关闭的窗口 |
| POST | `/watermark/observe` | 按事件时间推进：`{"event_time": <ms>}`，W = max(event_time) - lateness |
| GET | `/results?limit=N` | 拉取至多 N 条已产出结果（N 超上限 → 429） |
| GET | `/stats` | 内部状态：水位线、积压数、各类计数器 |
| POST | `/reset` | 重置全部状态（本地验证用） |

默认参数（见 `app/api.py` 的 `create_app`）：窗口 10s，允许迟到 2s，积压上限 1024，单次拉取上限 100。

## 本地验证

```bash
# 安装依赖
uv sync

# 跑测试（日志会逐条打印 event_time / watermark / WINDOW EMITTED，
# 可直接核对 duplicate / expired 判定）
uv run pytest -s

# 启动服务
uv run uvicorn app.api:app --port 8000
# 或：uv run python main.py
```

手动验证示例：

```bash
# 上报（乱序）
curl -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","event_time":1000,"value":2.5}'

# 重复上报 → 409 duplicate
curl -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"e1","event_time":1000,"value":2.5}'

# 推进水位线（W = 13000 - 2000 = 11000），[0,10000) 窗口出结果
curl -X POST localhost:8000/watermark/observe -H 'Content-Type: application/json' \
  -d '{"event_time":13000}'

# 迟到事件 → 410 expired，不会修改已出结果
curl -X POST localhost:8000/events -H 'Content-Type: application/json' \
  -d '{"event_id":"late","event_time":500,"value":9}'

# 拉取结果 / 查看状态
curl "localhost:8000/results?limit=10"
curl localhost:8000/stats
```

## 结构

```
app/
  models.py   # Event / WindowResult / RejectReason
  errors.py   # EventRejectedError / BackpressureError
  parsing.py  # 上报载荷校验（格式问题一律 invalid）
  engine.py   # 窗口引擎：水位线推进、去重、过期判定、背压（线程安全）
  api.py      # FastAPI 路由与错误码映射
tests/
  test_engine.py  # 乱序 / 重复 / 迟到 / 不可变 / 背压 / 并发
  test_api.py     # HTTP 状态码区分 / 背压 429 / 并发接入
```
