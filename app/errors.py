"""错误类型：非法 / 重复 / 过期 / 背压，互相可以分辨。"""
from __future__ import annotations


class EventError(Exception):
    """所有事件处理错误的基类。"""

    code = "event_error"
    http_status = 400

    def __init__(self, message: str, *, event_id: str | None = None):
        super().__init__(message)
        self.message = message
        self.event_id = event_id

    def to_dict(self) -> dict:
        return {
            "error": self.code,
            "message": self.message,
            "event_id": self.event_id,
        }


class InvalidEventError(EventError):
    """事件格式非法：不是对象、缺字段、字段类型不对、时间不是整数毫秒等。"""

    code = "invalid_event"
    http_status = 422


class DuplicateEventError(EventError):
    """同一个 event_id 已经处理过（重复到达，最终只算一次）。"""

    code = "duplicate_event"
    http_status = 200


class LateEventError(EventError):
    """事件迟到太久：所属窗口已经关闭并出过结果。"""

    code = "late_event"
    http_status = 200


class BackpressureError(EventError):
    """结果积压太多或一次拉取太多，被明确拒绝/限流。"""

    code = "backpressure"
    http_status = 503
