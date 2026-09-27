"""FastAPI 接入层：事件上报、水位线推进、结果拉取（带背压）。"""
from __future__ import annotations

from typing import Any

from fastapi import Body, FastAPI, Request
from fastapi.responses import JSONResponse

from .engine import WindowEngine
from .errors import EventRejectedError
from .models import RejectReason, WindowResult
from .parsing import parse_event

# 拒绝原因 -> HTTP 状态码：与其他错误明确区分
_STATUS_BY_REASON = {
    RejectReason.INVALID: 400,       # 格式非法
    RejectReason.DUPLICATE: 409,     # 重复事件
    RejectReason.EXPIRED: 410,       # 迟到太久（窗口已关闭）
    RejectReason.BACKPRESSURE: 429,  # 背压
}


def _window_to_dict(w: WindowResult) -> dict[str, Any]:
    return {
        "window_start": w.window_start,
        "window_end": w.window_end,
        "count": w.count,
        "sum": w.sum,
        "min_value": w.min_value,
        "max_value": w.max_value,
        "first_event_time": w.first_event_time,
        "last_event_time": w.last_event_time,
        "emitted_at": w.emitted_at,
    }


def create_app(engine: WindowEngine | None = None) -> FastAPI:
    app = FastAPI(title="realtime-event-window-engine")
    app.state.engine = engine or WindowEngine(
        window_size_ms=10_000,
        allowed_lateness_ms=2_000,
        max_pending_windows=1024,
        max_pull_batch=100,
    )

    @app.exception_handler(EventRejectedError)
    async def rejected_handler(_: Request, exc: EventRejectedError) -> JSONResponse:
        return JSONResponse(
            status_code=_STATUS_BY_REASON[exc.reason],
            content={"status": "rejected", "reason": exc.reason.value, "message": exc.message},
        )

    @app.get("/")
    def root() -> dict[str, str]:
        return {"service": "realtime-event-window-engine"}

    @app.post("/events")
    def ingest_event(payload: Any = Body(...)) -> dict[str, Any]:
        """上报一条事件（允许乱序；重复 / 过期 / 非法会被分类拒绝）。"""
        event = parse_event(payload)
        app.state.engine.ingest(event)
        return {"status": "accepted", "event_id": event.event_id}

    @app.post("/watermark")
    def advance_watermark(payload: dict[str, Any]) -> dict[str, Any]:
        """显式推进水位线，返回本次关闭的窗口结果。"""
        watermark = payload.get("watermark") if isinstance(payload, dict) else None
        if isinstance(watermark, bool) or not isinstance(watermark, int):
            raise EventRejectedError(RejectReason.INVALID, "watermark must be an integer (ms epoch)")
        emitted = app.state.engine.advance_watermark(watermark)
        return {
            "watermark": app.state.engine.stats()["watermark"],
            "emitted": [_window_to_dict(w) for w in emitted],
        }

    @app.post("/watermark/observe")
    def observe_watermark(payload: dict[str, Any]) -> dict[str, Any]:
        """按观察到的事件时间推进水位线：W = max(event_time) - allowed_lateness。"""
        event_time = payload.get("event_time") if isinstance(payload, dict) else None
        if isinstance(event_time, bool) or not isinstance(event_time, int):
            raise EventRejectedError(RejectReason.INVALID, "event_time must be an integer (ms epoch)")
        emitted = app.state.engine.observe_and_advance(event_time)
        return {
            "watermark": app.state.engine.stats()["watermark"],
            "emitted": [_window_to_dict(w) for w in emitted],
        }

    @app.get("/results")
    def pull_results(limit: int = 100) -> dict[str, Any]:
        """拉取积压的窗口结果；limit 超过单次上限时以 429 背压拒绝。"""
        pulled = app.state.engine.pull_results(limit)
        return {
            "pulled": len(pulled),
            "results": [_window_to_dict(w) for w in pulled],
            "pending": app.state.engine.stats()["pending_results"],
        }

    @app.get("/stats")
    def stats() -> dict[str, Any]:
        return app.state.engine.stats()

    @app.post("/reset")
    def reset() -> dict[str, str]:
        """重置引擎状态（本地验证 / 测试用）。"""
        app.state.engine.reset()
        return {"status": "reset"}

    return app


app = create_app()
