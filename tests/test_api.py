"""HTTP 接入层测试：状态码区分、背压、并发接入。"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from app.api import create_app
from app.engine import WindowEngine

logger = logging.getLogger("test_api")


def make_client() -> TestClient:
    engine = WindowEngine(
        window_size_ms=1_000,
        allowed_lateness_ms=200,
        max_pending_windows=4,
        max_pull_batch=8,
    )
    return TestClient(create_app(engine))


def test_post_event_and_results_roundtrip() -> None:
    c = make_client()
    r = c.post("/events", json={"event_id": "a", "event_time": 100, "value": 1.5})
    assert r.status_code == 200
    r = c.post("/watermark", json={"watermark": 1000})
    assert r.status_code == 200
    emitted = r.json()["emitted"]
    assert len(emitted) == 1
    logger.info("emitted via API: %s", emitted[0])
    r = c.get("/results", params={"limit": 8})
    assert r.status_code == 200
    assert r.json()["pulled"] == 1
    assert r.json()["results"][0]["sum"] == 1.5


def test_rejection_reasons_use_distinct_status_codes() -> None:
    """重复 409 / 过期 410 / 非法 400，拒绝原因清晰可辨。"""
    c = make_client()
    assert c.post("/events", json={"event_id": "a", "event_time": 100, "value": 1}).status_code == 200

    r = c.post("/events", json={"event_id": "a", "event_time": 100, "value": 1})
    assert r.status_code == 409 and r.json()["reason"] == "duplicate"

    r = c.post("/watermark", json={"watermark": 1000})
    assert r.status_code == 200

    r = c.post("/events", json={"event_id": "late", "event_time": 50, "value": 1})
    assert r.status_code == 410 and r.json()["reason"] == "expired"

    r = c.post("/events", json={"event_id": "bad", "event_time": "now", "value": 1})
    assert r.status_code == 400 and r.json()["reason"] == "invalid"

    r = c.post("/events", json={"event_id": "bad2", "event_time": 1})
    assert r.status_code == 400 and r.json()["reason"] == "invalid"


def test_backpressure_status_429() -> None:
    """积压过多（429）与普通非法请求（400）区分。"""
    c = make_client()
    for i in range(4):
        assert c.post("/events", json={"event_id": f"e{i}", "event_time": i * 1000 + 100,
                                       "value": 1}).status_code == 200
    assert c.post("/watermark", json={"watermark": 4000}).status_code == 200

    r = c.post("/events", json={"event_id": "new", "event_time": 5000, "value": 1})
    assert r.status_code == 429 and r.json()["reason"] == "backpressure"
    logger.info("ingest backpressure response: %s", r.json())

    # 一次拉太多同样 429
    r = c.get("/results", params={"limit": 9})
    assert r.status_code == 429 and r.json()["reason"] == "backpressure"
    logger.info("pull backpressure response: %s", r.json())

    # 正常拉取后背压解除
    assert c.get("/results", params={"limit": 8}).json()["pulled"] == 4
    assert c.post("/events", json={"event_id": "new", "event_time": 5000,
                                   "value": 1}).status_code == 200


def test_watermark_cannot_go_backwards() -> None:
    c = make_client()
    assert c.post("/watermark", json={"watermark": 1000}).status_code == 200
    r = c.post("/watermark", json={"watermark": 999})
    assert r.status_code == 400 and r.json()["reason"] == "invalid"


def test_concurrent_posts_count_exactly_once() -> None:
    """并发上报同一批 id（每个两遍）：窗口聚合不重不漏。"""
    c = make_client()

    def post(i: int) -> tuple[int, str]:
        body = {"event_id": f"id-{i}", "event_time": (i % 50) * 100 + 50, "value": 1}
        r = c.post("/events", json=body)
        return r.status_code, r.json().get("reason", "")

    ids = list(range(200))
    calls = ids + ids  # 每个 id 报两遍
    with ThreadPoolExecutor(max_workers=16) as pool:
        outcomes = list(pool.map(post, calls))

    accepted = sum(1 for code, _ in outcomes if code == 200)
    duplicate = sum(1 for code, reason in outcomes if code == 409 and reason == "duplicate")
    other = [o for o in outcomes if o[0] not in (200, 409)]
    assert other == []
    assert accepted == 200 and duplicate == 200

    r = c.post("/watermark", json={"watermark": 5000})
    windows = r.json()["emitted"]
    total = sum(w["count"] for w in windows)
    for w in windows:
        logger.info("concurrent window [%d,%d) count=%d", w["window_start"], w["window_end"], w["count"])
    assert total == 200
