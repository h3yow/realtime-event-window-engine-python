"""引擎单测：乱序、重复、迟到、并发接入、背压。

运行：uv run python -m unittest tests.test_engine -v
日志会逐条打印 事件时间 / 水位线 / 关闭窗口结果，方便人工核对。
"""
from __future__ import annotations

import logging
import threading
import unittest

from app.engine import WindowEngine, parse_event
from app.errors import (
    BackpressureError,
    DuplicateEventError,
    InvalidEventError,
    LateEventError,
)
from app.models import EngineConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s | %(message)s",
)
log = logging.getLogger("test")

SIZE = 10_000
LATENESS = 2_000


def send(eng: WindowEngine, eid: str, t: int, key: str = "k") -> str:
    """发送并记录日志，返回判定状态。"""
    try:
        out = eng.ingest({"event_id": eid, "event_time": t, "key": key})
    except DuplicateEventError:
        wm = eng.stats().watermark
        log.info("event=%s time=%-6d -> DUP     watermark=%d", eid, t, wm)
        return "duplicate"
    except LateEventError:
        wm = eng.stats().watermark
        log.info("event=%s time=%-6d -> LATE    watermark=%d", eid, t, wm)
        return "late"
    closed = [(r.start, r.end, dict(r.counts)) for r in out.closed_windows]
    log.info(
        "event=%s time=%-6d -> ACCEPT  window=[%d,%d) watermark=%d closed=%s",
        eid,
        t,
        out.window_start,
        out.window_start + SIZE,
        out.watermark,
        closed,
    )
    return "accepted"


class OutOfOrderTests(unittest.TestCase):
    """乱序到达：只要还在水位线之上，旧窗口仍能补进事件。"""

    def test_out_of_order_within_lateness(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, 100, 50))
        # 先来窗口 [10000,20000) 的事件 -> 水位线 9000，不关任何窗
        self.assertEqual(send(eng, "a", 11_000), "accepted")
        # 9500 > 水位线 9000，补进窗口 [0,10000)
        self.assertEqual(send(eng, "b", 9_500), "accepted")
        self.assertEqual(eng.stats().watermark, 9_000)
        # 再来 20000 -> 水位线 18000，窗口 [0,10000) 关闭，含 a? 不含：
        # a 在 [10000,20000)，b 在 [0,10000)
        send(eng, "c", 20_000)
        results = eng.pull()
        log.info("窗口结果: %s", [(r.start, r.end, r.counts) for r in results])
        self.assertEqual(len(results), 1)
        self.assertEqual((results[0].start, results[0].end), (0, 10_000))
        self.assertEqual(results[0].counts, {"k": 1})

    def test_aggregation_groups_by_key(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, 100, 50))
        for eid, t, key in [
            ("a", 1_000, "x"),
            ("b", 2_000, "y"),
            ("c", 3_000, "x"),
            ("d", 15_000, "z"),
        ]:
            send(eng, eid, t, key)
        results = eng.pull()
        log.info("分组聚合结果: %s", [(r.counts) for r in results])
        self.assertEqual(results[0].counts, {"x": 2, "y": 1})
        self.assertEqual(results[0].total, 3)


class DuplicateTests(unittest.TestCase):
    """重复事件最终只算一次，且与非法/过期可分辨。"""

    def test_duplicate_counted_once(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, 100, 50))
        send(eng, "a", 1_000)
        with self.assertRaises(DuplicateEventError):
            eng.ingest({"event_id": "a", "event_time": 1_000, "key": "k"})
        # 第三次重复同样拒绝
        with self.assertRaises(DuplicateEventError):
            eng.ingest({"event_id": "a", "event_time": 9_999, "key": "k"})
        send(eng, "z", 20_000)  # 关窗
        (r,) = eng.pull()
        self.assertEqual(r.total, 1)
        self.assertEqual(eng.stats().duplicates, 2)

    def test_duplicate_distinct_from_late_and_invalid(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, 100, 50))
        send(eng, "a", 1_000)
        send(eng, "b", 20_000)  # 水位线 18000，[0,10000) 已关闭
        with self.assertRaises(DuplicateEventError):
            eng.ingest({"event_id": "a", "event_time": 1_000, "key": "k"})
        with self.assertRaises(LateEventError):
            eng.ingest({"event_id": "c", "event_time": 5_000, "key": "k"})
        with self.assertRaises(InvalidEventError):
            eng.ingest({"event_id": "d"})
        s = eng.stats()
        self.assertEqual((s.duplicates, s.late, s.invalid), (1, 1, 1))


class LateEventTests(unittest.TestCase):
    """迟到太久：水位线之前的事件不得改动已定稿窗口。"""

    def test_late_rejected_after_window_closed(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, 100, 50))
        send(eng, "a", 1_000)
        send(eng, "b", 12_000)  # wm=10000，窗口 [0,10000) 关闭
        (r,) = eng.pull()
        self.assertEqual(r.counts, {"k": 1})
        # 迟到事件必须拒绝，且不能改已出结果
        with self.assertRaises(LateEventError):
            eng.ingest({"event_id": "c", "event_time": 5_000, "key": "k"})
        self.assertEqual(eng.pull(), [])  # 没有新结果被"补"出来
        self.assertEqual(eng.stats().late, 1)

    def test_boundary_event_time_equal_watermark_is_late(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, 100, 50))
        send(eng, "a", 1_000)
        send(eng, "b", 12_000)  # wm=10000
        with self.assertRaises(LateEventError):
            eng.ingest({"event_id": "c", "event_time": 10_000, "key": "k"})

    def test_empty_windows_are_not_emitted(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, 100, 50))
        send(eng, "a", 25_000)  # 跳过前两个窗口，空窗不产生结果
        results = eng.pull()
        self.assertEqual(results, [])


class ParseTests(unittest.TestCase):
    """格式非法的各种形态。"""

    def test_valid(self):
        e = parse_event({"event_id": "x", "event_time": 1, "key": "y"})
        self.assertEqual((e.event_id, e.event_time, e.key), ("x", 1, "y"))

    def test_invalid_forms(self):
        bad = [
            None,
            "nope",
            [],
            {"event_time": 1, "key": "k"},
            {"event_id": "", "event_time": 1, "key": "k"},
            {"event_id": "x", "event_time": -1, "key": "k"},
            {"event_id": "x", "event_time": True, "key": "k"},
            {"event_id": "x", "event_time": 1.5, "key": "k"},
            {"event_id": "x", "event_time": "1", "key": "k"},
            {"event_id": "x", "event_time": 1, "key": ""},
            {"event_id": "x", "event_time": 1, "key": 2},
        ]
        for raw in bad:
            with self.assertRaises(InvalidEventError):
                parse_event(raw)


class ConcurrentIngestionTests(unittest.TestCase):
    """多线程并发接入：同一窗口不多算也不漏算；重复判定一致。"""

    def test_concurrent_unique_events_no_loss_no_double(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, max_backlog=100_000))
        n_thread = 8
        per_thread = 200

        def worker(tid: int) -> None:
            for i in range(per_thread):
                eid = f"t{tid}-{i}"
                # 全部落在窗口 [0,10000)
                send(eng, eid, 1_000 + (i % 8_000), f"g{tid}")

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_thread)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 再发一个远事件关窗
        send(eng, "close", 30_000)
        (r,) = eng.pull()
        expected = n_thread * per_thread
        log.info(
            "并发结果窗口=[%d,%d) total=%d expected=%d counts_per_group=%s",
            r.start,
            r.end,
            r.total,
            expected,
            {k: v for k, v in list(r.counts.items())[:3]},
        )
        self.assertEqual(r.total, expected)  # 不漏算
        self.assertEqual(eng.stats().accepted, expected + 1)  # +1 为关窗事件
        self.assertEqual(eng.stats().duplicates, 0)

    def test_concurrent_duplicates(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, max_backlog=100_000))
        barrier = threading.Barrier(8)

        def worker() -> None:
            barrier.wait()
            send(eng, "same-id", 2_000)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        send(eng, "close", 30_000)
        (r,) = eng.pull()
        self.assertEqual(r.total, 1)  # 同一事件只算一次
        self.assertEqual(eng.stats().duplicates, 7)


class BackpressureTests(unittest.TestCase):
    """积压保护：积压太多拒绝接入；一次拉太多限流；原因区别于其它错误。"""

    def test_backlog_cap_rejects_ingest(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, max_backlog=2, max_pull=2))
        # 造 2 个已关闭窗口积压（两个窗口都要有事件才会生成结果）
        send(eng, "a", 1_000)
        send(eng, "b", 11_000)  # wm=9000
        send(eng, "c", 22_000)  # wm=20000，关 [0,10000) 与 [10000,20000)
        self.assertEqual(eng.stats().backlog, 2)
        # 达到上限后再来会再关窗的事件 -> 明确拒绝
        with self.assertRaises(BackpressureError):
            eng.ingest({"event_id": "d", "event_time": 32_000, "key": "k"})
        # 消费后恢复
        pulled = eng.pull()
        self.assertEqual(len(pulled), 2)
        out = eng.ingest({"event_id": "e", "event_time": 32_000, "key": "k"})
        self.assertEqual(out.status, "accepted")

    def test_oversized_pull_rate_limited(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, max_backlog=10, max_pull=3))
        with self.assertRaises(BackpressureError):
            eng.pull(4)
        with self.assertRaises(InvalidEventError):
            eng.pull(0)

    def test_pull_is_non_blocking(self):
        eng = WindowEngine(EngineConfig(SIZE, LATENESS, 10, 3))
        self.assertEqual(eng.pull(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
