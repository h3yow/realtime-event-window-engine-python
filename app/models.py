"""数据模型：事件、窗口结果、拒绝原因。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class RejectReason(str, Enum):
    """事件接入被拒绝的原因分类。"""

    DUPLICATE = "duplicate"          # 重复事件（event_id 已处理过）
    EXPIRED = "expired"              # 迟到太久：事件时间早于水位线
    INVALID = "invalid"              # 格式非法
    BACKPRESSURE = "backpressure"    # 背压：积压过多


@dataclass(frozen=True, slots=True)
class Event:
    """一条上报事件。

    event_time 为事件时间（毫秒 epoch），event_id 为幂等键，
    value 为参与窗口聚合的数值。
    """

    event_id: str
    event_time: int
    value: float


@dataclass(frozen=True, slots=True)
class WindowResult:
    """一个已关闭窗口的聚合结果。"""

    window_start: int
    window_end: int
    count: int
    sum: float
    min_value: float
    max_value: float
    first_event_time: int
    last_event_time: int
    emitted_at: int = field(default=0)
