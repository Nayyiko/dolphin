#!/usr/bin/env python3
"""
Dolphin 参数调优工具

自动化扫描关键参数，找最优配置，并解释每个参数对性能的影响。

用法:
    python3 hack/bench/tune.py --concurrency          # 扫描最优并发（不需要重启）
    python3 hack/bench/tune.py --rate                 # 扫描限流 rate/capacity（自动重启 gateway）
    python3 hack/bench/tune.py --all                  # 全量

参数扫描会修改 configs/gateway.yaml，完成后恢复原配置。
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import yaml
from loadgen import load_test

GATEWAY = os.environ.get("DOLPHIN_GATEWAY", "http://localhost:8080")
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
CONFIG = os.path.join(PROJECT_DIR, "configs", "gateway.yaml")
RESULTS_DIR = os.path.join(PROJECT_DIR, "results")
GW_BIN = os.path.join(PROJECT_DIR, "bin", "gateway.exe" if os.name == "nt" else "gateway")

tune_result = {}


def log(msg=""):
    print(msg)


def ensure_gateway_binary():
    if not os.path.exists(GW_BIN):
        log(f"▶ 构建 gateway 二进制...")
        subprocess.run(["go", "build", "-o", GW_BIN, "./cmd/gateway"], cwd=PROJECT_DIR, check=True)


def is_gateway_up(url: str = GATEWAY) -> bool:
    try:
        import requests
        r = requests.get(url + "/health", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


def scan_concurrency(urls: list, durations: list):
    """扫描并发数，找吞吐/延迟的拐点。"""
    log("═" * 60)
    log("  并发扫描：找 QPS 和 P99 的平衡点")
    log("═" * 60)

    results = []
    for url in urls:
        for conc in [1, 5, 10, 20, 30, 50, 80, 100]:
            r = load_test(url, concurrency=conc, duration=10, verify_proxy=False)
            p = r.percentiles()
            results.append({
                "url": url, "concurrency": conc,
                "qps": round(r.qps(), 0),
                "p50": p["p50"], "p95": p["p95"], "p99": p["p99"],
                "error_rate": round(r.error_rate() * 100, 2),
            })
            log(f"  conc={conc:>3} → QPS={r.qps():6.0f}  P50={p['p50']:6.1f}ms  "
                f"P95={p['p95']:6.1f}ms  P99={p['p99']:6.1f}ms  err={r.error_rate()*100:.2f}%")

    # 分析最优并发
    best = min(results, key=lambda x: (x["error_rate"] > 1, -x["qps"]))
    log()
    log(f"  ▸ 最优并发（误差 <1% 时 QPS 最高）: {best['concurrency']} → "
        f"{best['qps']} QPS, P99={best['p99']}ms")

    tune_result["concurrency"] = results
    tune_result["best_concurrency"] = best
    return results


def scan_rate_limit():
    """扫描限流 rate/capacity，自动重启 gateway。"""
    log("═" * 60)
    log("  限流参数扫描：找吞吐与延迟的平衡（自动重启 gateway）")
    log("═" * 60)

    ensure_gateway_binary()
    if not is_gateway_up():
        log("  ⚠️ gateway 未运行，无法扫描限流参数（需要重启 gateway）")
        log("  请先启动 gateway 后再试 --rate")
        return None

    # 备份原配置
    with open(CONFIG, "r", encoding="utf-8") as f:
        original = yaml.safe_load(f)

    results = []
    try:
        # 扫描 rate/capacity 组合
        # rate 是每秒令牌数（决定稳态吞吐），capacity 是突发容量（决定瞬时爆发能力）
        for rate, cap in [(50, 100), (100, 200), (200, 400), (500, 1000), (1000, 2000)]:
            log(f"\n  ▶ 测试 rate={rate}, capacity={cap}")
            # 修改配置
            cfg = yaml.safe_load(open(CONFIG, encoding="utf-8"))
            cfg["rate_limit"]["default_rate"] = rate
            cfg["rate_limit"]["default_capacity"] = cap
            with open(CONFIG, "w", encoding="utf-8") as f:
                yaml.safe_dump(cfg, f, allow_unicode=True)

            # 重启 gateway
            restart_gateway()

            # 压测
            r = load_test(f"{GATEWAY}/health", concurrency=10, duration=8, verify_proxy=False)
            p = r.percentiles()
            results.append({
                "rate": rate, "capacity": cap,
                "qps": round(r.qps(), 0),
                "p50": p["p50"], "p95": p["p95"], "p99": p["p99"],
                "error_rate": round(r.error_rate() * 100, 2),
            })
            log(f"    → QPS={r.qps():6.0f}  P50={p['p50']:6.1f}ms  P95={p['p95']:6.1f}ms  "
                f"P99={p['p99']:6.1f}ms  err={r.error_rate()*100:.2f}%")
    finally:
        # 恢复原配置并重启
        with open(CONFIG, "w", encoding="utf-8") as f:
            yaml.safe_dump(original, f, allow_unicode=True)
        restart_gateway()

    # 分析
    log()
    log("  ▸ 限流参数分析:")
    for r in results:
        if r["error_rate"] > 0:
            log(f"    rate={r['rate']}: 错误率 {r['error_rate']}% — 限流拒绝导致（rate 低于请求速率）")
        else:
            log(f"    rate={r['rate']}: QPS={r['qps']}, P99={r['p99']}ms — 限流未触发，瓶颈在其他层")

    tune_result["rate_limit"] = results
    return results


def restart_gateway():
    """重启 gateway（Windows 下杀进程再启动，Unix 下 kill + 起新进程）。"""
    # 杀掉旧 gateway
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/IM", "gateway.exe"], capture_output=True)
    else:
        subprocess.run(["pkill", "-f", "bin/gateway"], capture_output=True)

    time.sleep(1)

    # 启动新 gateway（后台）
    logfile = open(os.path.join(PROJECT_DIR, "results", "gateway_tune.log"), "w")
    subprocess.Popen([GW_BIN, "-config", CONFIG], cwd=PROJECT_DIR,
                     stdout=logfile, stderr=logfile)

    # 等待就绪
    for _ in range(30):
        time.sleep(1)
        if is_gateway_up():
            log("  ✅ gateway 重启完成")
            return
    log("  ⚠️ gateway 重启超时")


def main():
    parser = argparse.ArgumentParser(description="Dolphin 参数调优")
    parser.add_argument("--concurrency", action="store_true", help="扫描并发")
    parser.add_argument("--rate", action="store_true", help="扫描限流参数")
    parser.add_argument("--all", action="store_true", help="全量")
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)

    if args.all or args.concurrency:
        scan_concurrency([f"{GATEWAY}/health"], [10])
    if args.all or args.rate:
        scan_rate_limit()

    if tune_result:
        json_path = os.path.join(RESULTS_DIR, "tune_report.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(tune_result, f, ensure_ascii=False, indent=2)
        log(f"\n✅ 调优报告: {json_path}")


if __name__ == "__main__":
    main()
