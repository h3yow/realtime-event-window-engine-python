"""进程内事件时间窗口聚合引擎（线程安全）。

语义：
- 水位线 W = max(已观察事件时间) - allowed_lateness_ms。
- event_time < W 的事件视为过期（迟到太久），拒绝且计入 expired。
- 窗口 [s, s+size) 在 W >= s+size 时关闭，结果不可再变。
- event_id 为幂等键，重复到达只计一次。
- 已出结果未被拉走的窗口积压达到上限时，新事件以背压拒绝。
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field

from .errors import BackpressureError, EventRejectedError
from .models import Event, RejectReason, WindowResult

logger = logging.getLogger("window_engine")


@dataclass(slots=True)
class _MutableWindow:
    """累积中的窗口内部状态。"""

    start: int
    end: int
    count: int = 0
    total: float = 0.0
    min_value: float = 0.0
    max_value: float = 0.0
    first_event_time: int = 0
    last_event_time: int = 0
    ids: set[str] = field(default_factory=set)

    def add(self, event: Event) -> None:
        if self.count == 0:
            self.total = event.value
            self.min_value = event.value
            self.max_value = event.value
            self.first_event_time = event.event_time
            self.last_event_time = event.event_time
        else:
            self.total += event.value
            self.min_value = min(self.min_value, event.value)
            self.max_value = max(self.max_value, event.value)
            self.first_event_time = min(self.first_event_time, event.event_time)
            self.last_event_time = max(self.last_event_time, event.event_time)
        self.count += 1
        self.ids.add(event.event_id)

    def freeze(self, emitted_at: int) -> WindowResult:
        return WindowResult(
            window_start=self.start,
            window_end=self.end,
            count=self.count,
            sum=self.total,
            min_value=self.min_value,
            max_value=self.max_value,
            first_event_time=self.first_event_time,
            last_event_time=self.last_event_time,
            emitted_at=emitted_at,
        )


class WindowEngine:
    """以水位线推进的滚动窗口聚合引擎。"""

    def __init__(
        self,
        window_size_ms: int,
        allowed_lateness_ms: int,
        max_pending_windows: int,
        max_pull_batch: int,
    ) -> None:
        if window_size_ms <= 0:
            raise ValueError("window_size_ms must be positive")
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms must be non-negative")
        if max_pending_windows <= 0:
            raise ValueError("max_pending_windows must be positive")
        if max_pull_batch <= 0:
            raise ValueError("max_pull_batch must be positive")
        self.window_size_ms = window_size_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        self.max_pending_windows = max_pending_windows
        self.max_pull_batch = max_pull_batch

        self._lock = threading.RLock()
        self._watermark: int | None = None
        self._max_event_time: int | None = None
        self._windows: dict[int, _MutableWindow] = {}
        self._emitted: set[int] = set()
        self._seen: set[str] = set()
        self._pending: deque[WindowResult] = deque()
        self._counters = {
            "accepted": 0,
            "duplicate": 0,
            "expired": 0,
            "invalid": 0,
            "backpressure": 0,
        }

    # ------------------------------------------------------------------ #
    def _window_start(self, event_time: int) -> int:
        return (event_time // self.window_size_ms) * self.window_size_ms

    def ingest(self, event: Event) -> None:
        """接入一条事件。

        拒绝判定顺序：非法 -> 过期 -> 重复 -> 背压。
        """
        with self._lock:
            if (
                not isinstance(event.event_id, str)
                or not event.event_id
                or not isinstance(event.event_time, int)
                or isinstance(event.event_time, bool)
                or event.event_time < 0
                or not isinstance(event.value, (int, float))
                or isinstance(event.value, bool)
            ):
                self._counters["invalid"] += 1
                logger.warning("reject invalid event id=%s time=%s", event.event_id, event.event_time)
                raise EventRejectedError(RejectReason.INVALID, "invalid event", event_id=event.event_id)

            if self._watermark is not None and event.event_time < self._watermark:
                self._counters["expired"] += 1
                logger.info(
                    "reject EXPIRED id=%s event_time=%d watermark=%d",
                    event.event_id, event.event_time, self._watermark,
                )
                raise EventRejectedError(
                    RejectReason.EXPIRED,
                    f"event_time {event.event_time} is behind watermark {self._watermark}",
                    event_id=event.event_id,
                )

            if event.event_id in self._seen:
                self._counters["duplicate"] += 1
                logger.info(
                    "reject DUPLICATE id=%s event_time=%d watermark=%s",
                    event.event_id, event.event_time, self._watermark,
                )
                raise EventRejectedError(
                    RejectReason.DUPLICATE,
                    f"event_id {event.event_id} already ingested",
                    event_id=event.event_id,
                )

            if len(self._pending) >= self.max_pending_windows:
                self._counters["backpressure"] += 1
                logger.warning(
                    "reject BACKPRESSURE id=%s pending=%d cap=%d",
                    event.event_id, len(self._pending), self.max_pending_windows,
                )
                raise BackpressureError(
                    f"pending windows {len(self._pending)} >= {self.max_pending_windows}, drain results first",
                    pending=len(self._pending),
                )

            start = self._window_start(event.event_time)
            window = self._windows.get(start)
            if window is None:
                window = _MutableWindow(start=start, end=start + self.window_size_ms)
                self._windows[start] = window
            window.add(event)
            self._seen.add(event.event_id)
            self._counters["accepted"] += 1
            logger.info(
                "accept id=%s event_time=%d window=[%d,%d) watermark=%s",
                event.event_id, event.event_time, start, start + self.window_size_ms, self._watermark,
            )

    def advance_watermark(self, new_watermark: int) -> list[WindowResult]:
        """显式推进水位线（单调不减），返回本次新关闭的窗口，按时间升序。"""
        with self._lock:
            if self._watermark is not None and new_watermark < self._watermark:
                raise EventRejectedError(
                    RejectReason.INVALID,
                    f"watermark cannot go backwards: {new_watermark} < {self._watermark}",
                )
            if self._watermark is None:
                self._watermark = new_watermark
            else:
                self._watermark = max(self._watermark, new_watermark)
            return self._close_due_windows()

    def observe_and_advance(self, event_time: int) -> list[WindowResult]:
        """根据观察到的事件时间推进水位线：W = max_event_time - allowed_lateness。"""
        with self._lock:
            if self._max_event_time is None or event_time > self._max_event_time:
                self._max_event_time = event_time
            candidate = self._max_event_time - self.allowed_lateness_ms
            if self._watermark is None or candidate > self._watermark:
                self._watermark = candidate
                logger.info("watermark -> %d (max_event_time=%d)", candidate, self._max_event_time)
                return self._close_due_windows()
            return []

    def _close_due_windows(self) -> list[WindowResult]:
        """关闭所有 end <= watermark 的窗口（持锁调用）。"""
        assert self._watermark is not None
        due = sorted(s for s, w in self._windows.items() if w.end <= self._watermark)
        closed: list[WindowResult] = []
        now_ms = time.time_ns() // 1_000_000
        for start in due:
            window = self._windows.pop(start)
            result = window.freeze(now_ms)
            self._emitted.add(start)
            self._pending.append(result)
            self._seen.difference_update(window.ids)
            closed.append(result)
            logger.info(
                "WINDOW EMITTED [%d,%d) count=%d sum=%.4f watermark=%d",
                start, window.end, result.count, result.sum, self._watermark,
            )
        return closed

    def pull_results(self, max_items: int) -> list[WindowResult]:
        """从积压结果队列头部拉取至多 max_items 条。

        max_items > max_pull_batch 时以背压拒绝（防止一次拉太多）。
        """
        with self._lock:
            if not isinstance(max_items, int) or isinstance(max_items, bool) or max_items <= 0:
                raise EventRejectedError(RejectReason.INVALID, "max_items must be a positive integer")
            if max_items > self.max_pull_batch:
                self._counters["backpressure"] += 1
                logger.warning(
                    "pull BACKPRESSURE requested=%d cap=%d pending=%d",
                    max_items, self.max_pull_batch, len(self._pending),
                )
                raise BackpressureError(
                    f"pull size {max_items} exceeds per-request cap {self.max_pull_batch}",
                    pending=len(self._pending),
                )
            pulled: list[WindowResult] = []
            while max_items > 0 and self._pending:
                pulled.append(self._pending.popleft())
                max_items -= 1
            return pulled

    def stats(self) -> dict:
        """返回当前内部状态快照（监控 / 测试核对用）。"""
        with self._lock:
            return {
                "watermark": self._watermark,
                "max_event_time": self._max_event_time,
                "open_windows": len(self._windows),
                "emitted_windows": len(self._emitted),
                "pending_results": len(self._pending),
                "tracked_event_ids": len(self._seen),
                "counters": dict(self._counters),
            }

    def reset(self) -> None:
        """重置全部状态（测试用）。"""
        with self._lock:
            self._watermark = None
            self._max_event_time = None
            self._windows = {}
            self._emitted = set()
            self._seen = set()
            self._pending.clear()
            for key in self._counters:
                self._counters[key] = 0
