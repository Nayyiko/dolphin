#!/usr/bin/env python3
"""
Dolphin 压测框架 — 负载生成器

用线程池 + requests.Session（共享连接池）做 HTTP 压测。
相比 loadgen (Go) 的优势：可以嵌入 Python 统计流程，方便参数调优。

用法（作为库被 bench.py 调用）:
    from loadgen import load_test, LoadResult
    result = load_test(url, concurrency=50, duration=10, qps_limit=None)
    print(result.summary())
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import numpy as np
import requests


@dataclass
class LoadResult:
    url: str
    concurrency: int
    duration: float
    total: int = 0
    success: int = 0
    errors: int = 0
    status5xx: int = 0
    status_counts: dict = field(default_factory=dict)
    latencies_ms: list = field(default_factory=list)
    qps_limit: int = 0

    def qps(self) -> float:
        return self.total / self.duration if self.duration else 0

    def error_rate(self) -> float:
        return self.errors / self.total if self.total else 0

    def percentiles(self) -> dict:
        if not self.latencies_ms:
            return {"p50": 0, "p95": 0, "p99": 0, "avg": 0, "max": 0}
        arr = np.array(self.latencies_ms)
        return {
            "p50": round(float(np.percentile(arr, 50)), 2),
            "p95": round(float(np.percentile(arr, 95)), 2),
            "p99": round(float(np.percentile(arr, 99)), 2),
            "avg": round(float(arr.mean()), 2),
            "max": round(float(arr.max()), 2),
            "std": round(float(arr.std()), 2),
        }

    def summary(self) -> str:
        p = self.percentiles()
        return (
            f"url={self.url} conc={self.concurrency} dur={self.duration}s "
            f"total={self.total} success={self.success} errors={self.errors} "
            f"5xx={self.status5xx} QPS={self.qps():.0f} "
            f"err%={self.error_rate()*100:.2f} "
            f"P50={p['p50']}ms P95={p['p95']}ms P99={p['p99']}ms "
            f"avg={p['avg']}ms max={p['max']}ms"
        )


class QPSLimiter:
    """简单的 QPS 限制器：通过最小间隔控制请求速率。"""

    def __init__(self, qps: int):
        self.min_interval = 1.0 / qps if qps > 0 else 0
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait = self._last + self.min_interval - now
            if wait > 0:
                time.sleep(wait)
                self._last = time.monotonic()
            else:
                self._last = now


def _worker(url, session, stop_event, limiter, result, method="GET", body=None):
    while not stop_event.is_set():
        if limiter:
            limiter.wait()
        start = time.monotonic()
        try:
            if method == "GET":
                resp = session.get(url, timeout=5)
            else:
                resp = session.request(method, url, json=body if body else None, timeout=5)
            status = resp.status_code
            result.total += 1
            if status < 500:
                result.success += 1
            else:
                result.status5xx += 1
            result.status_counts[status] = result.status_counts.get(status, 0) + 1
            resp.close()
        except Exception:
            result.total += 1
            result.errors += 1
        result.latencies_ms.append((time.monotonic() - start) * 1000)


def load_test(
    url: str,
    concurrency: int = 50,
    duration: float = 10,
    qps_limit: int = 0,
    method: str = "GET",
    body: dict = None,
    verify_proxy: bool = True,
) -> LoadResult:
    """
    执行压测。

    verify_proxy=False 时禁用 requests 的环境代理（避免 Windows 代理导致 localhost 超时）。
    """
    result = LoadResult(url=url, concurrency=concurrency, duration=duration, qps_limit=qps_limit)
    stop_event = threading.Event()
    limiter = QPSLimiter(qps_limit) if qps_limit else None

    session = requests.Session()
    # 禁用代理访问 localhost：与 bench_full.ps1 改 curl.exe 同理
    if not verify_proxy:
        session.trust_env = False
    # 连接池：容量足够，复用连接
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=concurrency,
        pool_maxsize=concurrency * 2,
        max_retries=0,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    pool = ThreadPoolExecutor(max_workers=concurrency)
    start = time.monotonic()
    futures = [pool.submit(_worker, url, session, stop_event, limiter, result, method, body)
               for _ in range(concurrency)]

    try:
        time.sleep(duration)
    finally:
        stop_event.set()
        for f in futures:
            f.result(timeout=5)
        pool.shutdown(wait=True)
        result.duration = time.monotonic() - start

    return result
