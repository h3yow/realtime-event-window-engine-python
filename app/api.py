"""HTTP 接入层（FastAPI）。

响应约定（三类情况可分辨）：
- 正常接收：HTTP 200，status="accepted"，body 带 watermark 与本次关闭窗口；
- 重复 / 迟到：HTTP 200，status="duplicate"/"late"，body 带原因；
- 格式非法（含非法 JSON）：HTTP 422，error="invalid_event"；
- 背压 / 限流：HTTP 503，error="backpressure"，与其它错误明确区分。
"""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .engine import WindowEngine
from .errors import (
    BackpressureError,
    DuplicateEventError,
    InvalidEventError,
    LateEventError,
)
from .models import WindowResult

logger = logging.getLogger("window_engine")


def _result_dict(r: WindowResult) -> dict[str, Any]:
    return {"start": r.start, "end": r.end, "counts": r.counts, "total": r.total}


def create_app(engine: WindowEngine | None = None) -> FastAPI:
    eng = engine or WindowEngine()
    app = FastAPI(title="realtime-event-window-engine")

    @app.post("/events")
    async def post_event(request: Request) -> JSONResponse:
        """上报单条事件；读取原始 JSON，非法 JSON 也算 invalid_event。"""
        raw: Any
        try:
            body = await request.body()
            raw = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning("REJECT invalid json body")
            err = InvalidEventError("请求体不是合法 JSON")
            eng.mark_invalid()
            return JSONResponse(err.to_dict(), status_code=err.http_status)

        try:
            outcome = eng.ingest(raw)
        except InvalidEventError as exc:
            logger.warning(
                "REJECT invalid event_id=%s: %s", exc.event_id, exc.message
            )
            return JSONResponse(exc.to_dict(), status_code=exc.http_status)
        except DuplicateEventError as exc:
            s = eng.stats()
            logger.info(
                "DUP event_id=%s watermark=%d", exc.event_id, s.watermark
            )
            return JSONResponse(
                {
                    "status": "duplicate",
                    "event_id": exc.event_id,
                    "message": exc.message,
                    "watermark": s.watermark,
                },
                status_code=exc.http_status,
            )
        except LateEventError as exc:
            s = eng.stats()
            logger.info(
                "LATE event_id=%s event_time=%d watermark=%d",
                exc.event_id,
                raw.get("event_time"),
                s.watermark,
            )
            return JSONResponse(
                {
                    "status": "late",
                    "event_id": exc.event_id,
                    "message": exc.message,
                    "watermark": s.watermark,
                },
                status_code=exc.http_status,
            )
        except BackpressureError as exc:
            logger.warning("REJECT backpressure event_id=%s", exc.event_id)
            return JSONResponse(exc.to_dict(), status_code=exc.http_status)

        closed = [_result_dict(r) for r in outcome.closed_windows]
        logger.info(
            "ACCEPT event_id=%s event_time=%d window=[%d,%d) watermark=%d "
            "closed=%s",
            outcome.event.event_id,
            outcome.event.event_time,
            outcome.window_start,
            outcome.window_start + eng.config.window_size_ms,
            outcome.watermark,
            closed,
        )
        return JSONResponse(
            {
                "status": "accepted",
                "event_id": outcome.event.event_id,
                "event_time": outcome.event.event_time,
                "window_start": outcome.window_start,
                "watermark": outcome.watermark,
                "closed_windows": closed,
            }
        )

    @app.get("/results")
    async def get_results(limit: int | None = None) -> JSONResponse:
        """非阻塞拉取已关闭窗口结果，按窗口时间从旧到新。

        不传 limit 时最多返回引擎配置的 max_pull 个；超过上限返回 503。
        """
        try:
            results = eng.pull(limit)
        except BackpressureError as exc:
            logger.warning("REJECT pull backpressure: %s", exc.message)
            return JSONResponse(exc.to_dict(), status_code=exc.http_status)
        except InvalidEventError as exc:
            return JSONResponse(exc.to_dict(), status_code=exc.http_status)
        data = [_result_dict(r) for r in results]
        logger.info("PULL limit=%d returned=%d", limit, len(data))
        return JSONResponse({"results": data, "returned": len(data)})

    @app.get("/stats")
    async def get_stats() -> JSONResponse:
        s = eng.stats()
        return JSONResponse(
            {
                "received": s.received,
                "accepted": s.accepted,
                "duplicates": s.duplicates,
                "late": s.late,
                "invalid": s.invalid,
                "rejected_backpressure": s.rejected_backpressure,
                "watermark": s.watermark,
                "open_windows": s.open_windows,
                "backlog": s.backlog,
            }
        )

    return app
