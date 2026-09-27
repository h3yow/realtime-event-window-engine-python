"""数据模型：事件、窗口结果、引擎配置与统计。

时间约定：所有事件时间 / 水位线 / 窗口起止都是 **整数毫秒 epoch**，
水位线语义为 watermark = 已观察最大事件时间 - allowed_lateness，
"事件时间 t <= watermark" 即视为迟到。
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Event:
    """一条上报事件。

    event_id  全局唯一，用于去重。
    event_time 事件发生时间（整数毫秒 epoch），允许乱序到达。
    key       事件分组键（窗口内按 key 分别聚合计数）。
    """

    event_id: str
    event_time: int
    key: str


@dataclass(frozen=True, slots=True)
class WindowResult:
    """一个 [start, end) 滚动窗口的最终聚合结果。"""

    start: int
    end: int
    counts: dict[str, int]

    @property
    def total(self) -> int:
        return sum(self.counts.values())


@dataclass(slots=True)
class IngestOutcome:
    """单次接入的判定结果，重复 / 过期 / 接收 三种互斥情况。"""

    status: str            # "accepted" | "duplicate" | "late"
    event: Event
    watermark: int
    window_start: int | None = None
    closed_windows: list[WindowResult] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class EngineConfig:
    """引擎参数（后面再填默认值与校验）。"""

    window_size_ms: int = 10_000
    allowed_lateness_ms: int = 2_000
    max_backlog: int = 128
    max_pull: int = 64


@dataclass(slots=True)
class EngineStats:
    """运行期统计，便于观测与测试断言。"""

    received: int = 0
    accepted: int = 0
    duplicates: int = 0
    late: int = 0
    invalid: int = 0
    rejected_backpressure: int = 0
    watermark: int = 0
    open_windows: int = 0
    backlog: int = 0
