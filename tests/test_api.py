"""HTTP 层测试：状态码/错误码分辨、背压、结果拉取、真实 HTTP 并发。

运行：uv run python -m unittest tests.test_api -v
并发用例启动一个真实 uvicorn 服务（端口随机），用 httpx 多线程打流量。
"""
from __future__ import annotations

import contextlib
import socket
import threading
import time
import unittest

import httpx
import uvicorn
from fastapi.testclient import TestClient

from app.api import create_app
from app.engine import WindowEngine
from app.models import EngineConfig

SIZE = 10_000
LATENESS = 2_000


def make_client(**cfg) -> tuple[TestClient, WindowEngine]:
    eng = WindowEngine(EngineConfig(SIZE, LATENESS, **cfg))
    return TestClient(create_app(eng)), eng


@contextlib.contextmanager
def live_server(engine: WindowEngine):
    """在后台线程跑真实 uvicorn，yield base_url。"""
    app = create_app(engine)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, args=([sock],), daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    # 等待服务就绪
    for _ in range(100):
        try:
            if httpx.get(base_url + "/stats", timeout=0.5).status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.05)
    else:  # pragma: no cover
        raise RuntimeError("测试服务未启动")
    try:
        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        sock.close()


class HttpClassificationTests(unittest.TestCase):
    """重复 / 过期 / 非法 / 背压必须互相可分辨。"""

    def setUp(self):
        self.client, self.eng = make_client(max_backlog=100, max_pull=50)

    def test_accepted_then_duplicate_then_late(self):
        r = self.client.post(
            "/events", json={"event_id": "a", "event_time": 1_000, "key": "k"}
        )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "accepted")

        r = self.client.post(
            "/events", json={"event_id": "a", "event_time": 1_000, "key": "k"}
        )
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "duplicate")

        # 推进水位线关闭 [0,10000)
        self.client.post(
            "/events", json={"event_id": "b", "event_time": 22_000, "key": "k"}
        )
        r = self.client.post(
            "/events", json={"event_id": "c", "event_time": 5_000, "key": "k"}
        )
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["status"], "late")
        self.assertIn("水位线", body["message"])

    def test_invalid_json_and_invalid_fields_are_422(self):
        r = self.client.post(
            "/events",
            content=b"{not-json",
            headers={"content-type": "application/json"},
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json()["error"], "invalid_event")

        r = self.client.post(
            "/events", json={"event_id": "x", "event_time": "1", "key": "k"}
        )
        self.assertEqual(r.status_code, 422)
        self.assertEqual(r.json()["error"], "invalid_event")

        r = self.client.post("/events", json=[1, 2, 3])
        self.assertEqual(r.status_code, 422)

    def test_results_and_stats(self):
        self.client.post(
            "/events", json={"event_id": "a", "event_time": 1_000, "key": "x"}
        )
        self.client.post(
            "/events", json={"event_id": "b", "event_time": 1_500, "key": "y"}
        )
        self.client.post(
            "/events", json={"event_id": "c", "event_time": 22_000, "key": "z"}
        )
        r = self.client.get("/results")
        self.assertEqual(r.status_code, 200)
        results = r.json()["results"]
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["counts"], {"x": 1, "y": 1})
        self.assertEqual(results[0]["total"], 2)
        # 再次拉取为空（结果只消费一次）
        self.assertEqual(self.client.get("/results").json()["results"], [])

        stats = self.client.get("/stats").json()
        self.assertEqual(stats["accepted"], 3)
        self.assertEqual(stats["watermark"], 20_000)


class HttpBackpressureTests(unittest.TestCase):
    def test_pull_over_limit_is_503(self):
        client, _ = make_client(max_backlog=10, max_pull=2)
        r = client.get("/results?limit=3")
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.json()["error"], "backpressure")

    def test_backlog_full_ingest_is_503_and_recovers(self):
        client, _ = make_client(max_backlog=1, max_pull=1)
        client.post("/events", json={"event_id": "a", "event_time": 1_000, "key": "k"})
        client.post("/events", json={"event_id": "b", "event_time": 12_000, "key": "k"})
        # wm=10000，[0,10000) 关闭进积压，backlog=1
        # 再来会关闭 [10000,20000) 的事件 -> 503
        r = client.post(
            "/events", json={"event_id": "d", "event_time": 32_000, "key": "k"}
        )
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.json()["error"], "backpressure")
        # 非法事件仍是 422，与背压区分
        r = client.post("/events", json={"event_id": "bad"})
        self.assertEqual(r.status_code, 422)
        # 消费后恢复接收
        self.assertEqual(client.get("/results").status_code, 200)
        r = client.post(
            "/events", json={"event_id": "e", "event_time": 32_000, "key": "k"}
        )
        self.assertEqual(r.status_code, 200)


class HttpConcurrencyTests(unittest.TestCase):
    """并发 HTTP 上报：同一窗口总数精确，重复 id 只生效一次。"""

    def test_concurrent_unique_events_exact_counts(self):
        eng = WindowEngine(
            EngineConfig(SIZE, LATENESS, max_backlog=100_000, max_pull=100_000)
        )
        n_thread = 8
        per_thread = 100
        errors: list[Exception] = []

        with live_server(eng) as base_url:
            client = httpx.Client(base_url=base_url, timeout=5)

            def worker(tid: int):
                try:
                    for i in range(per_thread):
                        r = client.post(
                            "/events",
                            json={
                                "event_id": f"t{tid}-{i}",
                                "event_time": 1_000 + (i % 8_000),
                                "key": "k",
                            },
                        )
                        if r.status_code != 200:
                            errors.append(AssertionError(r.text))
                except Exception as exc:  # pragma: no cover
                    errors.append(exc)

            threads = [
                threading.Thread(target=worker, args=(t,)) for t in range(n_thread)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            client.close()

        self.assertEqual(errors, [])
        # 关窗并拉取
        with TestClient(create_app(eng)) as c:
            c.post(
                "/events",
                json={"event_id": "close", "event_time": 30_000, "key": "k"},
            )
            results = c.get("/results").json()["results"]
        self.assertEqual(results[0]["total"], n_thread * per_thread)
        self.assertEqual(eng.stats().duplicates, 0)

    def test_concurrent_same_id_only_one_accepted(self):
        eng = WindowEngine(
            EngineConfig(SIZE, LATENESS, max_backlog=100_000, max_pull=100_000)
        )
        n = 8
        barrier = threading.Barrier(n)
        statuses: list[str] = []
        status_lock = threading.Lock()

        with live_server(eng) as base_url:
            client = httpx.Client(base_url=base_url, timeout=5)

            def worker():
                barrier.wait()
                r = client.post(
                    "/events",
                    json={"event_id": "same", "event_time": 2_000, "key": "k"},
                )
                with status_lock:
                    statuses.append(r.json()["status"])

            threads = [threading.Thread(target=worker) for _ in range(n)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            client.close()

        self.assertEqual(statuses.count("accepted"), 1)
        self.assertEqual(statuses.count("duplicate"), n - 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
