"""异常类型：区分非法 / 过期 / 重复 / 背压。"""
from __future__ import annotations

from .models import RejectReason


class EventRejectedError(Exception):
    """事件接入被拒绝。reason 标识具体拒绝原因。"""

    def __init__(self, reason: RejectReason, message: str, *, event_id: str | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.event_id = event_id


class BackpressureError(EventRejectedError):
    """背压拒绝：积压窗口过多或一次拉取过多。"""

    def __init__(self, message: str, *, pending: int | None = None) -> None:
        super().__init__(RejectReason.BACKPRESSURE, message)
        self.pending = pending
