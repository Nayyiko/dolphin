#!/usr/bin/env python3
"""
Dolphin 压测框架 — Prometheus 指标采集器

从各组件的 /metrics 端点抓取指标，解析成可用结构。
支持 histograms（调度延迟、执行耗时）和 counters/gauges。
"""

import re
import time

import requests


class MetricsClient:
    """抓取并解析 Prometheus 文本格式指标。"""

    def __init__(self, base_urls: dict, verify_proxy: bool = False):
        """
        base_urls: {"scheduler": "http://localhost:9090", "worker": "http://localhost:9091", "gateway": "http://localhost:8080"}
        """
        self.base_urls = base_urls
        self.session = requests.Session()
        self.session.trust_env = verify_proxy  # 默认 False，绕开 Windows 代理

    def fetch(self, component: str) -> str:
        url = self.base_urls[component] + "/metrics"
        resp = self.session.get(url, timeout=5)
        resp.raise_for_status()
        return resp.text

    def fetch_all(self) -> dict:
        return {name: self.fetch(name) for name in self.base_urls}

    # ── 解析 ──

    @staticmethod
    def _parse_counter(text: str, metric: str, label_filter: dict = None) -> int:
        """解析 counter/gauge 的值。返回总和（按 label_filter 过滤，若给的话）。"""
        pattern = re.compile(re.escape(metric) + r"\{(.*?)\} ([0-9.e+-]+)")
        total = 0
        for m in pattern.finditer(text):
            labels = m.group(1)
            val = float(m.group(2))
            if label_filter:
                ok = all(f'{k}="{v}"' in labels for k, v in label_filter.items())
                if not ok:
                    continue
            total += val
        return int(total)

    @staticmethod
    def _parse_simple(text: str, metric: str) -> float:
        """解析无 label 的 metric 值（如 xxx_count / xxx_sum）。"""
        m = re.search(re.escape(metric) + r" ([0-9.e+-]+)", text)
        return float(m.group(1)) if m else 0.0

    # ── 高级查询 ──

    def histogram(self, component: str, metric: str, labels: dict = None) -> dict:
        """
        解析 histogram，返回 {count, sum, buckets}。
        labels 用于过滤特定 label（如 {"handler_type":"http"}）。
        """
        text = self.fetch(component)
        label_filter = labels or {}

        # buckets
        buckets = []
        pattern = re.compile(re.escape(metric) + r"_bucket\{(.*?)\} ([0-9.e+-]+)")
        for m in pattern.finditer(text):
            label_str, val = m.group(1), float(m.group(2))
            ok = all(f'{k}="{v}"' in label_str for k, v in label_filter.items())
            if not ok:
                continue
            le = re.search(r'le="([^"]+)"', label_str)
            if le:
                buckets.append((le.group(1), val))
        buckets.sort(key=lambda x: float("inf") if x[0] == "+Inf" else float(x[0]))

        # count & sum: 兼容有 label 和无 label 两种情况
        count, ssum = 0.0, 0.0
        for m in re.finditer(re.escape(metric) + r"_count(?:\{(.*?)\})? ([0-9.e+-]+)", text):
            label_str, val = m.group(1), float(m.group(2))
            ok = all(f'{k}="{v}"' in (label_str or "") for k, v in label_filter.items())
            if ok:
                count += val
        for m in re.finditer(re.escape(metric) + r"_sum(?:\{(.*?)\})? ([0-9.e+-]+)", text):
            label_str, val = m.group(1), float(m.group(2))
            ok = all(f'{k}="{v}"' in (label_str or "") for k, v in label_filter.items())
            if ok:
                ssum += val

        return {"count": count, "sum": ssum, "buckets": buckets}

    def histogram_percentiles(self, component: str, metric: str, labels: dict = None) -> dict:
        """从 histogram 估算 P50/P95/P99。"""
        h = self.histogram(component, metric, labels)
        count, buckets = h["count"], h["buckets"]
        if count < 10:
            return {"count": int(count), "p50": None, "p95": None, "p99": None, "avg": None}

        def pct(p):
            target = count * p
            prev_le = 0.0
            prev_cum = 0.0
            for le, cum_val in buckets:
                if le == "+Inf":
                    return float(prev_le)
                le_f = float(le)
                if cum_val >= target:
                    # 线性插值：在 (prev_le, le_f] 区间内定位
                    if cum_val <= prev_cum:
                        return le_f
                    ratio = (target - prev_cum) / (cum_val - prev_cum)
                    return prev_le + (le_f - prev_le) * ratio
                prev_le = le_f
                prev_cum = cum_val
            return float(buckets[-1][0]) if buckets else None

        avg = h["sum"] / count if count else None
        return {
            "count": int(count),
            "p50": round(pct(0.50), 3) if count >= 10 else None,
            "p95": round(pct(0.95), 3) if count >= 10 else None,
            "p99": round(pct(0.99), 3) if count >= 10 else None,
            "avg": round(avg, 3) if avg is not None else None,
        }

    def counter_delta(self, component: str, metric: str, wait_seconds: float) -> dict:
        """抓取 counter 在 wait_seconds 内的增量（用于速率统计）。"""
        v1 = self._parse_simple(self.fetch(component), metric)
        time.sleep(wait_seconds)
        v2 = self._parse_simple(self.fetch(component), metric)
        return {"delta": v2 - v1, "rate_per_sec": (v2 - v1) / wait_seconds if wait_seconds else 0}
