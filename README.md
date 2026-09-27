# 实时事件接入与事件时间窗口聚合（进程内版）

一个零外部中间件的实时事件窗口聚合服务：事件带**事件时间**上报，允许乱序、
重复；按**水位线（watermark）**推进滚动窗口，窗口一旦出结果就不再被旧事件修改；
结果积压与拉取有背压保护。全部状态都在单进程内存里，本地直接跑即可验证。

- 语言/框架：Python 3.14 + FastAPI（uvicorn）
- 并发安全：引擎内置一把可重入锁，多个接入请求并发时同一窗口不多算、不漏算
- 无数据库、无消息队列、无外部进程

## 快速开始

```bash
uv sync                       # 安装依赖（已带 fastapi[standard]）
uv run python main.py         # 启动 HTTP 服务 http://127.0.0.1:8000
```

另开终端：

```bash
# 上报事件（时间为整数毫秒 epoch）
curl -s -X POST http://127.0.0.1:8000/events \
  -H 'content-type: application/json' \
  -d '{"event_id":"e1","event_time":11000,"key":"k"}'

# 拉取已关闭窗口的聚合结果
curl -s 'http://127.0.0.1:8000/results'
curl -s 'http://127.0.0.1:8000/stats'
```

交互式文档：http://127.0.0.1:8000/docs

## 事件时间与水位线语义

- **事件时间（event time）**：事件实际发生的时间，由上报方在
  `event_time` 字段给出，**整数毫秒 epoch**。到达顺序可以乱序。
- **处理时间**：服务收到请求的墙上时间，本系统不用它做窗口归属，只用于日志。
- **水位线**：系统对"事件时间已经推进到哪里"的保守估计：

  ```
  watermark = 已接收事件的最大 event_time - allowed_lateness_ms（默认 2000ms）
  ```

  含义是："我们认为事件时间 ≤ watermark 的数据已经到齐了"。
  允许迟到量（默认 2 秒）给了乱序事件一个宽限：**event_time > watermark**
  的迟到事件仍然可以补进它所属的、尚未关闭的窗口。
- 水位线**单调不减**，只随被成功接收（计数）的事件推进；重复事件和过期事件
  不会推进水位线，避免旁路数据影响已定稿结果。

## 窗口什么时候出结果

- 窗口为固定大小滚动窗口（默认 10 秒）：`[N*size, (N+1)*size)`，左闭右开。
- 当窗口结束时间满足 `end <= watermark` 时，窗口**关闭并定稿**，聚合结果进入
  积压队列（`GET /results` 可拉取）。
- **定稿即不可变**：关闭之后到达、且 `event_time <= watermark` 的事件判定为
  "迟到太久"（`late`），被丢弃且单独计数，绝不会改动已经产出的窗口结果；
  而更靠后、尚未关闭的窗口继续正常累积。
- 边界约定：`event_time == watermark` 即视为迟到（水位线是"已到齐"的上界）。
- 空窗口（整窗没有任何事件）不产生结果，避免无意义输出。
- 聚合口径：窗口内按 `key` 分组计数，结果形如 `{"k": 3, "x": 1}`，另给 `total`。

## 重复 / 过期 / 非法如何分辨

同一个 `event_id` 重复到达，最终只计一次。四类判定互斥，HTTP 响应明确区分：

| 情况 | 判定 | HTTP 状态 | 响应体 |
|---|---|---|---|
| 正常接收 | accepted | 200 | `{"status":"accepted", ...}` |
| event_id 已处理过 | duplicate | 200 | `{"status":"duplicate", ...}` |
| event_time ≤ 水位线，窗口已定稿 | late | 200 | `{"status":"late", ...}` |
| 不是 JSON 对象 / 缺字段 / 类型错 / 时间非非负整数毫秒 | invalid | **422** | `{"error":"invalid_event", ...}` |
| 积压将超上限，或单次拉取超限 | backpressure | **503** | `{"error":"backpressure", ...}` |

说明：
- 重复优先于迟到判定——只要 id 见过，无论它带的时间多旧，都是 duplicate。
- 非法事件不参与任何状态，`stats.invalid` 单独计数。
- 背压是独立错误码 `backpressure` + 503，便于客户端退避重试，不和 422 混淆。

## 背压保护

- `max_backlog`（默认 128）：已关闭、未被消费的窗口结果积压上限。接入新事件前
  会预判"本次将关闭几个窗口"，若 `现有积压 + 将关闭数 > 上限`，该事件被整体
  拒绝（503），不会出现半写入；客户端消费 `/results` 后自动恢复。
- `max_pull`（默认 64）：单次拉取结果数上限。`GET /results?limit=N` 中
  `N > max_pull` 直接 503 限流；不传 limit 时按 `max_pull` 返回。
- 拉取是非阻塞的：没有结果时返回空数组，绝不挂住连接。

## 并发正确性

`WindowEngine.ingest` 在一把可重入锁内原子完成
"去重检查 → 迟到检查 → 背压预判 → 计数 → 推进水位线 → 关窗"，
因此并发接入时：

- 同一个 `event_id` 只有一个请求 accepted，其余全部 duplicate；
- 每个窗口只关闭一次，结果只入队一次，不会多算；
- 每个被接收的事件都恰好计数一次，不会漏算。

## 本地验证

### 1) 一键看判定全过程（推荐先看）

```bash
uv run python demo.py
```

脚本按乱序、重复、合法迟到、过期迟到、非法格式的顺序喂事件，逐条打印
事件时间、所属窗口、水位线、窗口定稿结果与最终统计，可直接人工核对。

### 2) 跑全部单测

```bash
uv run python -m unittest discover -s tests -v
```

测试覆盖：

- 乱序到达且仍在水位线之上 → 补进旧窗口；
- 重复事件（含并发重复）→ 只算一次；
- 迟到事件（含 `event_time == watermark` 边界）→ 拒绝且不改已定稿结果；
- 非法 JSON / 缺字段 / 类型错误 → 422；
- 8 线程并发接入（引擎直调 + 真实 uvicorn/httpx 两种方式）→
  窗口总数精确、重复判定一致；
- 积压上限拒绝接入、超限拉取限流、消费后恢复、错误码区分。

单测日志会打印每个事件的 `event_time`、当前 `watermark` 以及
`closed` 窗口结果，方便核对重复与过期判定。

## 代码结构

```
main.py            # uvicorn 启动入口（日志配置）
demo.py            # 进程内演示脚本
app/
  models.py        # Event / WindowResult / EngineConfig / EngineStats
  errors.py        # Invalid / Duplicate / Late / Backpressure 四类错误
  engine.py        # 解析、去重、水位线、关窗、背压（线程安全）
  api.py           # FastAPI 路由与错误码映射
tests/
  test_engine.py   # 引擎单测（乱序/重复/迟到/并发/背压）
  test_api.py      # HTTP 协议、错误码、真实服务并发测试
```
