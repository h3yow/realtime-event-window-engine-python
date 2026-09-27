"""把外部 JSON 载荷解析为领域对象；格式问题一律归为 invalid。"""
from __future__ import annotations

import math
from typing import Any

from .errors import EventRejectedError
from .models import Event, RejectReason


def parse_event(payload: Any) -> Event:
    """校验并解析事件上报载荷。

    要求：event_id 非空字符串；event_time 为非负整数（毫秒 epoch）；
    value 为有限数值。任何不符合的情况抛 INVALID。
    """
    if not isinstance(payload, dict):
        raise EventRejectedError(RejectReason.INVALID, "payload must be a JSON object")

    event_id = payload.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        raise EventRejectedError(RejectReason.INVALID, "event_id must be a non-empty string")

    event_time = payload.get("event_time")
    if isinstance(event_time, bool) or not isinstance(event_time, int) or event_time < 0:
        raise EventRejectedError(
            RejectReason.INVALID, "event_time must be a non-negative integer (ms epoch)"
        )

    value = payload.get("value")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EventRejectedError(RejectReason.INVALID, "value must be a number")
    value = float(value)
    if not math.isfinite(value):
        raise EventRejectedError(RejectReason.INVALID, "value must be finite")

    return Event(event_id=event_id, event_time=event_time, value=value)
