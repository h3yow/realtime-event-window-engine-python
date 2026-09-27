"""进程内窗口聚合引擎。

线程安全：一把可重入锁保护全部内部状态；HTTP 多个接入请求并发时，
同一窗口既不会多算（event_id 去重 + 窗口只关闭一次）也不会漏算。

语义（时间均为整数毫秒 epoch）：
- 窗口为固定大小滚动窗口 [N*size, (N+1)*size)。
- 水位线 watermark = 观察到的最大事件时间 - allowed_lateness。
- 当窗口结束时间 end <= watermark 时窗口关闭，结果进入积压队列；
  关闭之后到达、且 event_time <= watermark 的事件属于"迟到太久"，
  不再计入任何窗口，也不会改变已出结果。
"""
from __future__ import annotations

import threading
from collections import deque

from .errors import (
    BackpressureError,
    DuplicateEventError,
    InvalidEventError,
    LateEventError,
)
from .models import EngineConfig, EngineStats, Event, IngestOutcome, WindowResult


def parse_event(raw: object) -> Event:
    """把外部 JSON 数据解析成 Event；格式不对抛 InvalidEventError。

    判定非法的情况：
    - 不是 JSON 对象；
    - event_id 缺失/不是字符串/为空；
    - event_time 缺失/不是整数/为布尔/为负数；
    - key 缺失/不是字符串/为空。
    """
    if not isinstance(raw, dict):
        raise InvalidEventError("事件必须是 JSON 对象")

    event_id = raw.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        raise InvalidEventError("event_id 必须是非空字符串")

    event_time = raw.get("event_time")
    # 必须是真正的整数：bool 是 int 的子类，显式排除
    if not isinstance(event_time, int) or isinstance(event_time, bool) or event_time < 0:
        raise InvalidEventError("event_time 必须是非负整数（毫秒 epoch）", event_id=event_id)

    key = raw.get("key")
    if not isinstance(key, str) or not key:
        raise InvalidEventError("key 必须是非空字符串", event_id=event_id)

    return Event(event_id=event_id, event_time=event_time, key=key)


class WindowEngine:
    """按事件时间、水位线推进的滚动窗口聚合引擎。"""

    def __init__(self, config: EngineConfig | None = None):
        self.config = config or EngineConfig()
        if self.config.window_size_ms <= 0:
            raise ValueError("window_size_ms 必须为正数")
        if self.config.allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms 不能为负数")
        if self.config.max_backlog <= 0 or self.config.max_pull <= 0:
            raise ValueError("max_backlog / max_pull 必须为正数")
        self._lock = threading.RLock()
        # 已处理过的事件 id，用于跨窗口去重（重复最终只算一次）
        self._seen_ids: set[str] = set()
        # 打开中的窗口：window_start -> {key: count}
        self._windows: dict[int, dict[str, int]] = {}
        # 已关闭、等待被拉取的窗口结果
        self._backlog: deque[WindowResult] = deque()
        self._max_event_time = 0
        self._stats = EngineStats()

    # ---- 内部工具 ----
    def _window_start(self, event_time: int) -> int:
        return (event_time // self.config.window_size_ms) * self.config.window_size_ms

    def _watermark_locked(self) -> int:
        """当前水位线 = 最大事件时间 - 允许迟到量（不小于 0）。"""
        return max(0, self._max_event_time - self.config.allowed_lateness_ms)

    def _close_windows_locked(self, watermark: int) -> list[WindowResult]:
        """关闭所有 end <= watermark 的窗口，结果按时间顺序进入积压队列。

        返回本次关闭的窗口结果；调用方需保证积压队列有足够空位。
        """
        closed: list[WindowResult] = []
        for start in sorted(self._windows):
            end = start + self.config.window_size_ms
            if end > watermark:
                break  # 后面的窗口更新，无需再看
            counts = self._windows.pop(start)
            result = WindowResult(start=start, end=end, counts=dict(counts))
            self._backlog.append(result)
            closed.append(result)
        return closed

    # ---- 对外 API ----
    def mark_invalid(self) -> None:
        """登记一个非法输入（如 HTTP 层的非法 JSON），不计入 received。"""
        with self._lock:
            self._stats.invalid += 1

    def ingest(self, raw: object) -> IngestOutcome:
        """接入一条事件。

        判定顺序（互斥，便于分辨）：
        1. 格式非法 -> InvalidEventError
        2. event_id 已处理 -> DuplicateEventError（重复，只算一次）
        3. event_time <= 当前水位线 -> LateEventError（迟到太久，丢弃但登记）
        4. 积压已满且本次会关窗 -> BackpressureError（明确拒绝，事件不生效）
        5. 其余接收并累积，随后推进水位线、关闭到期窗口。
        """
        try:
            event = parse_event(raw)  # 非法直接抛
        except InvalidEventError:
            with self._lock:
                self._stats.received += 1
                self._stats.invalid += 1
            raise

        with self._lock:
            self._stats.received += 1

            # 1) 重复：无论事件时间早晚，重复永远是重复，绝不二次计数
            if event.event_id in self._seen_ids:
                self._stats.duplicates += 1
                raise DuplicateEventError(
                    f"事件 {event.event_id} 重复到达，已忽略",
                    event_id=event.event_id,
                )

            # 2) 迟到太久：水位线之前的窗口已定稿
            watermark = self._watermark_locked()
            if event.event_time <= watermark:
                self._seen_ids.add(event.event_id)
                self._stats.late += 1
                raise LateEventError(
                    f"事件时间 {event.event_time} <= 水位线 {watermark}，"
                    f"所属窗口已定稿，事件丢弃",
                    event_id=event.event_id,
                )

            # 3) 背压预判：本次接收后将要关闭的窗口数 + 现有积压
            #    若会超出上限则明确拒绝（事件完全不生效，不会半写入）
            prospective_max = max(self._max_event_time, event.event_time)
            prospective_wm = max(0, prospective_max - self.config.allowed_lateness_ms)
            will_close = sum(
                1
                for start in self._windows
                if start + self.config.window_size_ms <= prospective_wm
            )
            if len(self._backlog) + will_close > self.config.max_backlog:
                self._stats.rejected_backpressure += 1
                raise BackpressureError(
                    f"结果积压将达上限 {self.config.max_backlog}"
                    f"（现有 {len(self._backlog)}，本次将关窗 {will_close}），"
                    f"请先消费 /results 后再上报",
                    event_id=event.event_id,
                )

            # 4) 正常接收并累积
            self._seen_ids.add(event.event_id)
            self._stats.accepted += 1
            start = self._window_start(event.event_time)
            bucket = self._windows.setdefault(start, {})
            bucket[event.key] = bucket.get(event.key, 0) + 1

            # 5) 推进水位线，关闭到期窗口（每个窗口只关一次 -> 不会多算）
            self._max_event_time = prospective_max
            closed = self._close_windows_locked(prospective_wm)

            return IngestOutcome(
                status="accepted",
                event=event,
                watermark=self._watermark_locked(),
                window_start=start,
                closed_windows=closed,
            )

    def pull(self, max_items: int | None = None) -> list[WindowResult]:
        """非阻塞拉取已关闭窗口结果（按窗口时间从旧到新）。

        - max_items 超过配置上限 -> BackpressureError（限流，原因可分辨）；
        - 队列为空返回空列表，不阻塞。
        """
        with self._lock:
            if max_items is None:
                max_items = self.config.max_pull
            if max_items <= 0:
                raise InvalidEventError("limit 必须是正整数")
            if max_items > self.config.max_pull:
                self._stats.rejected_backpressure += 1
                raise BackpressureError(
                    f"单次最多拉取 {self.config.max_pull} 个结果，"
                    f"请求 {max_items} 个被限流"
                )
            out: list[WindowResult] = []
            while max_items > 0 and self._backlog:
                out.append(self._backlog.popleft())
                max_items -= 1
            return out

    def stats(self) -> EngineStats:
        with self._lock:
            snap = EngineStats(
                received=self._stats.received,
                accepted=self._stats.accepted,
                duplicates=self._stats.duplicates,
                late=self._stats.late,
                invalid=self._stats.invalid,
                rejected_backpressure=self._stats.rejected_backpressure,
                watermark=self._watermark_locked(),
                open_windows=len(self._windows),
                backlog=len(self._backlog),
            )
        return snap
