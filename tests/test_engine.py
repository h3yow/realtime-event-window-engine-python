"""窗口引擎单测：乱序、重复、迟到、不可变、背压、并发。

运行：uv run pytest -s
日志会逐条打印 event_time / watermark / WINDOW EMITTED，便于人工核对
重复（DUPLICATE）和过期（EXPIRED）判定是否正确。
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.engine import WindowEngine
from app.errors import BackpressureError, EventRejectedError
from app.models import Event, RejectReason

logger = logging.getLogger("test_engine")

SIZE = 1_000  # 窗口长度 1000ms
LATE = 200    # 允许迟到 200ms


def make_engine() -> WindowEngine:
    return WindowEngine(
        window_size_ms=SIZE,
        allowed_lateness_ms=LATE,
        max_pending_windows=4,
        max_pull_batch=8,
    )


def test_out_of_order_aggregation() -> None:
    """乱序到达的事件在同一窗口内应得到正确聚合。"""
    engine = make_engine()
    # 故意倒序、穿插地报 [0,1000) 窗口的事件
    events = [
        Event("c", 900, 3.0),
        Event("a", 100, 2.0),
        Event("d", 950, -1.0),
        Event("b", 500, 10.0),
    ]
    for e in events:
        logger.info("ingest event_time=%d value=%s", e.event_time, e.value)
        engine.ingest(e)

    emitted = engine.advance_watermark(1_000)
    assert len(emitted) == 1
    w = emitted[0]
    logger.info("window result [%d,%d) count=%d sum=%.1f", w.window_start, w.window_end, w.count, w.sum)
    assert (w.window_start, w.window_end) == (0, 1_000)
    assert w.count == 4
    assert w.sum == pytest.approx(14.0)
    assert (w.min_value, w.max_value) == (-1.0, 10.0)
    assert (w.first_event_time, w.last_event_time) == (100, 950)


def test_duplicate_counted_once() -> None:
    """同一 event_id 重复到达，窗口内只能算一次。"""
    engine = make_engine()
    engine.ingest(Event("x", 100, 5.0))
    # 完全重复的载荷
    with pytest.raises(EventRejectedError) as ei:
        engine.ingest(Event("x", 100, 5.0))
    assert ei.value.reason is RejectReason.DUPLICATE
    # 同一 id 但带不同时间/值，仍然是重复（幂等键只有一个）
    with pytest.raises(EventRejectedError) as ei:
        engine.ingest(Event("x", 200, 99.0))
    assert ei.value.reason is RejectReason.DUPLICATE

    emitted = engine.advance_watermark(1_000)
    w = emitted[0]
    logger.info("duplicate check window count=%d sum=%.1f", w.count, w.sum)
    assert w.count == 1
    assert w.sum == pytest.approx(5.0)
    assert engine.stats()["counters"]["duplicate"] == 2


def test_late_event_expired_after_window_close() -> None:
    """水位线越过窗口结束时间后，旧窗口出结果且不可再被后到事件修改。"""
    engine = make_engine()
    engine.ingest(Event("a", 100, 1.0))
    engine.ingest(Event("b", 200, 2.0))
    # 水位线到 1000：[0,1000) 关闭出结果
    emitted = engine.advance_watermark(1_000)
    assert len(emitted) == 1
    frozen = emitted[0]
    logger.info("emitted watermark=%d window=[%d,%d) count=%d", 1000, frozen.window_start,
                frozen.window_end, frozen.count)

    # 事件时间 50 < 水位线 1000：迟到太久，必须明确拒绝（不是悄悄丢掉）
    with pytest.raises(EventRejectedError) as ei:
        engine.ingest(Event("late1", 50, 100.0))
    assert ei.value.reason is RejectReason.EXPIRED
    logger.info("late event_time=50 rejected against watermark=%d", engine.stats()["watermark"])

    # 重复报一条已经过期窗口里的老事件：过期优先于重复判定
    with pytest.raises(EventRejectedError) as ei:
        engine.ingest(Event("a", 100, 1.0))
    assert ei.value.reason is RejectReason.EXPIRED

    # 已拉取的结果不可变
    pulled = engine.pull_results(8)
    assert len(pulled) == 1
    assert pulled[0] == frozen
    assert pulled[0].sum == pytest.approx(3.0)
    assert engine.stats()["counters"]["expired"] == 2


def test_late_but_within_allowed_lateness_still_accepted() -> None:
    """轻微乱序（未跌破水位线）的事件仍计入，窗口在 end<=watermark 时才关闭。"""
    engine = make_engine()
    engine.ingest(Event("old", 100, 1.0))  # [0,1000) 内先有一条
    # 观察到 1300 后水位线 = 1300-200 = 1100，[0,1000) 关闭
    engine.ingest(Event("a", 1_300, 1.0))
    emitted = engine.observe_and_advance(1_300)
    assert len(emitted) == 1
    assert emitted[0].window_start == 0
    # 事件时间 1050 >= 水位线 1100? 1050 < 1100 -> 过期
    with pytest.raises(EventRejectedError) as ei:
        engine.ingest(Event("late", 1_050, 1.0))
    assert ei.value.reason is RejectReason.EXPIRED
    # 但 1150 属于 [1000,2000) 且 >= 1100，仍然接受
    engine.ingest(Event("ok", 1_150, 2.0))
    logger.info("event_time=1150 accepted at watermark=%s", engine.stats()["watermark"])
    assert engine.stats()["counters"]["accepted"] == 3


def test_watermark_is_monotonic_and_closes_in_order() -> None:
    """水位线只能单调推进；多个到期窗口按时间升序出结果。"""
    engine = make_engine()
    for t, i in [(100, 0), (1_100, 1), (2_100, 2)]:
        engine.ingest(Event(f"e{i}", t, 1.0))
    emitted = engine.advance_watermark(2_000)
    assert [w.window_start for w in emitted] == [0, 1_000]
    with pytest.raises(EventRejectedError) as ei:
        engine.advance_watermark(1_999)
    assert ei.value.reason is RejectReason.INVALID
    # 之后的窗口继续累积，不受已关闭窗口影响
    emitted = engine.advance_watermark(3_000)
    assert [w.window_start for w in emitted] == [2_000]


def test_backpressure_pending_windows() -> None:
    """积压窗口达到上限后，新事件以背压拒绝；拉走后恢复。"""
    engine = make_engine()  # max_pending_windows=4
    # 造出 4 个已关闭但不拉取的窗口
    for i in range(4):
        engine.ingest(Event(f"e{i}", i * 1_000 + 100, 1.0))
    engine.advance_watermark(4_000)
    assert engine.stats()["pending_results"] == 4

    with pytest.raises(BackpressureError):
        engine.ingest(Event("new", 5_000, 1.0))
    logger.info("ingest rejected by backpressure, pending=%d", engine.stats()["pending_results"])

    pulled = engine.pull_results(8)
    assert len(pulled) == 4
    # 队列腾出空间后恢复接入
    engine.ingest(Event("new", 5_000, 1.0))
    assert engine.stats()["counters"]["accepted"] == 5


def test_pull_batch_cap_enforced() -> None:
    """一次拉取条数超过上限时明确以背压拒绝，与非法参数区分。"""
    engine = make_engine()  # max_pull_batch=8
    engine.ingest(Event("a", 100, 1.0))
    engine.advance_watermark(1_000)
    with pytest.raises(BackpressureError):
        engine.pull_results(9)
    with pytest.raises(EventRejectedError) as ei:
        engine.pull_results(0)
    assert ei.value.reason is RejectReason.INVALID
    assert len(engine.pull_results(8)) == 1
    assert engine.pull_results(8) == []  # 拉完即空，不重复


def test_invalid_event_fields() -> None:
    """格式非法事件以 INVALID 拒绝，且不计入任何窗口。"""
    engine = make_engine()
    for e in (
        Event("", 100, 1.0),
        Event("t1", -1, 1.0),
    ):
        with pytest.raises(EventRejectedError) as ei:
            engine.ingest(e)
        assert ei.value.reason is RejectReason.INVALID
    assert engine.stats()["open_windows"] == 0


def test_concurrent_ingest_no_double_count_and_no_loss() -> None:
    """多线程同时接入：每个 event_id 恰好计入一次，窗口计数不重不漏。"""
    engine = make_engine()
    ids = [f"id-{i}" for i in range(300)]

    def ingest_worker(event_id: str) -> None:
        # 每个 id 报两遍（且事件时间在窗口间分散），并发执行
        idx = int(event_id.split("-")[1])
        t = (idx % 50) * 100 + 50  # 全部落在 [0,5000)
        e = Event(event_id, t, 1.0)
        try:
            engine.ingest(e)
        except EventRejectedError as ex:
            assert ex.reason is RejectReason.DUPLICATE
        try:
            engine.ingest(e)
        except EventRejectedError as ex:
            assert ex.reason is RejectReason.DUPLICATE

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(ingest_worker, ids))

    emitted = engine.advance_watermark(5_000)
    total_count = sum(w.count for w in emitted)
    total_sum = sum(w.sum for w in emitted)
    logger.info("concurrent result windows=%d total_count=%d", len(emitted), total_count)
    for w in emitted:
        logger.info("window [%d,%d) count=%d", w.window_start, w.window_end, w.count)

    assert total_count == 300          # 不重不漏
    assert total_sum == pytest.approx(300.0)
    counters = engine.stats()["counters"]
    assert counters["accepted"] == 300
    assert counters["duplicate"] == 300
    assert counters["expired"] == 0
