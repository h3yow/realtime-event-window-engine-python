"""本地演示：进程内直接跑引擎，打印乱序/重复/迟到/关窗全过程。

运行：uv run python demo.py
窗口大小 10s，允许迟到 2s（时间单位毫秒）。
"""
from __future__ import annotations

import logging

from app.engine import WindowEngine
from app.errors import (
    BackpressureError,
    DuplicateEventError,
    InvalidEventError,
    LateEventError,
)
from app.models import EngineConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-7s | %(message)s",
)
log = logging.getLogger("demo")

# 事件序列：乱序、重复、合法迟到、过期迟到、非法格式
SEQUENCE: list[tuple[str, int, str]] = [
    ("e1", 11_000, "k"),   # 先到窗口 [10000,20000)，水位线 9000
    ("e2", 9_500, "k"),    # 乱序但 9500 > 水位线 9000 -> 补进 [0,10000)
    ("e3", 1_000, "x"),    # 更早的乱序事件，仍在水位线之上
    ("e1", 11_000, "k"),   # 重复 -> 只算一次
    ("e4", 22_000, "k"),   # 推进水位线到 20000，[0,10000) 定稿出结果
    ("e5", 5_000, "k"),    # 5000 <= 水位线 20000 -> 迟到太久，拒绝
]

INVALID_PAYLOADS: list[object] = [
    "{not json",
    {"event_id": "e6"},
    {"event_id": "e6", "event_time": "1000", "key": "k"},
]


def main() -> None:
    eng = WindowEngine(
        EngineConfig(
            window_size_ms=10_000,
            allowed_lateness_ms=2_000,
            max_backlog=128,
            max_pull=64,
        )
    )

    log.info("==== 正常事件序列（乱序 / 重复 / 迟到）====")
    for eid, t, key in SEQUENCE:
        try:
            out = eng.ingest({"event_id": eid, "event_time": t, "key": key})
            for w in out.closed_windows:
                log.info(
                    ">>> 窗口定稿 [%d,%d) counts=%s total=%d",
                    w.start, w.end, w.counts, w.total,
                )
            log.info(
                "ACCEPT id=%s time=%d window_start=%d watermark=%d",
                eid, t, out.window_start, out.watermark,
            )
        except DuplicateEventError:
            log.warning("DUP     id=%s time=%d（重复，只算一次）", eid, t)
        except LateEventError as e:
            log.warning("LATE    id=%s time=%d（%s）", eid, t, e.message)

    log.info("==== 非法格式 ====")
    for raw in INVALID_PAYLOADS:
        try:
            eng.ingest(raw)
        except InvalidEventError as e:
            log.warning("INVALID payload=%r -> %s", raw, e.message)

    log.info("==== 拉取已关闭窗口结果 ====")
    results = eng.pull()
    for w in results:
        log.info("RESULT [%d,%d) counts=%s total=%d", w.start, w.end, w.counts, w.total)

    log.info("==== 统计 ====")
    log.info("%s", eng.stats())


if __name__ == "__main__":
    main()
