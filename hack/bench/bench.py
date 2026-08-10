#!/usr/bin/env python3
"""
Dolphin 完整压测框架 — 主程序

编排所有压测阶段，输出可信数字 + 瓶颈分析。

用法:
    python3 hack/bench/bench.py                    # 完整压测
    python3 hack/bench/bench.py --gateway-only     # 只测网关
    python3 hack/bench/bench.py --schedule-only    # 只测调度链路
    python3 hack/bench/bench.py --tune             # 参数调优（限流 rate/capacity 扫描）

输出:
    results/bench_report.txt   完整报告
    results/bench_report.json  结构化结果
"""

import argparse
import json
import os
import subprocess
import sys
import time

# 让 import 能找到同目录模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from loadgen import load_test
from metrics import MetricsClient

# 组件地址
SCHED_METRICS = os.environ.get("DOLPHIN_SCHED_METRICS", "http://localhost:9090")
WORKER_METRICS = os.environ.get("DOLPHIN_WORKER_METRICS", "http://localhost:9091")
GATEWAY = os.environ.get("DOLPHIN_GATEWAY", "http://localhost:8080")

# 项目路径
PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
RESULTS_DIR = os.path.join(PROJECT_DIR, "results")
CTL = os.path.join(PROJECT_DIR, "bin", "dolphinctl.exe" if os.name == "nt" else "dolphinctl")

report_lines = []
report_data = {}


def log(msg: str = ""):
    print(msg)
    report_lines.append(msg)


def section(title: str):
    log()
    log("═" * 60)
    log(f"  {title}")
    log("═" * 60)


def require_components(mc: MetricsClient):
    """检查三个组件是否可达。"""
    section("环境检查")
    ok = True
    for name, url in [("gateway", GATEWAY), ("scheduler", SCHED_METRICS), ("worker", WORKER_METRICS)]:
        try:
            r = mc.session.get(url + "/metrics" if url != GATEWAY else url + "/health", timeout=3)
            log(f"  ✅ {name} 可达 ({r.status_code})")
        except Exception as e:
            log(f"  ❌ {name} 不可达: {e}")
            ok = False
    if not ok:
        log("环境未就绪，请先启动组件")
        sys.exit(1)
    return ok


def ensure_tools():
    """确保 dolphinctl 存在。"""
    if not os.path.exists(CTL):
        log(f"▶ 构建 dolphinctl...")
        subprocess.run(["go", "build", "-o", CTL, "./cmd/dolphinctl"], cwd=PROJECT_DIR, check=True)


def run_ctl(args: list) -> str:
    """运行 dolphinctl 并返回输出。"""
    proc = subprocess.run([CTL] + args, capture_output=True, text=True, timeout=120)
    return proc.stdout


def bench_gateway(mc: MetricsClient, concurrency: int = 10, duration: float = 15):
    """网关压测：不同并发下测 QPS/P99，定位瓶颈。"""
    section(f"网关压测 (并发梯度)")
    log(f"目标: {GATEWAY}/health")
    results = {}

    # 并发梯度：10 / 30 / 50 / 100
    for conc in [10, 30, 50, 100]:
        r = load_test(f"{GATEWAY}/health", concurrency=conc, duration=duration, verify_proxy=False)
        results[str(conc)] = {
            "qps": round(r.qps(), 0),
            "p50": r.percentiles()["p50"],
            "p95": r.percentiles()["p95"],
            "p99": r.percentiles()["p99"],
            "error_rate": round(r.error_rate() * 100, 2),
            "total": r.total,
        }
        log(f"  concurrency={conc:>3}: {r.summary()}")

    # 瓶颈分析
    log()
    log("  ▸ 瓶颈分析:")
    base = results["10"]
    log(f"    并发 10: {base['qps']:.0f} QPS, P99={base['p99']}ms, 错误率 {base['error_rate']}%")
    for conc in ["30", "50", "100"]:
        r = results[conc]
        if r["error_rate"] > 1:
            log(f"    并发 {conc}: 错误率 {r['error_rate']}% — 连接层瓶颈"
                f"（Windows 临时端口/TIME_WAIT 或连接池不足）")
        elif r["qps"] < base["qps"] * 0.8:
            log(f"    并发 {conc}: QPS 降至 {r['qps']:.0f}（并发 {base['qps']:.0f} 的 "
                f"{r['qps']/base['qps']*100:.0f}%），P99 升至 {r['p99']}ms — 限流 Redis 往返成为瓶颈")
        else:
            log(f"    并发 {conc}: QPS {r['qps']:.0f} 保持稳定, P99={r['p99']}ms — 吞吐受限流上限约束")

    report_data["gateway"] = results
    return results


def bench_schedule(mc: MetricsClient, count: int = 100, wait_minutes: int = 3):
    """调度链路压测：批量建任务，等自然触发，采样调度延迟/执行耗时/成功率。"""
    section(f"调度链路压测 (创建 {count} 个任务, 等待 {wait_minutes} 分钟自然触发)")

    # 1. 记录基线
    sched_before = mc.histogram("scheduler", "dolphin_scheduler_task_lag_seconds")
    worker_before = mc.histogram("worker", "dolphin_worker_task_duration_seconds")
    completed_before = mc._parse_simple(mc.fetch("worker"), "dolphin_worker_task_completed_total")
    dispatch_before = mc._parse_simple(mc.fetch("scheduler"), "dolphin_scheduler_dispatch_total")
    log(f"  基线: dispatch={int(dispatch_before)}, 调度样本={int(sched_before['count'])}, "
        f"执行样本={int(worker_before['count'])}, 完成={int(completed_before)}")

    # 2. 批量创建任务
    log(f"▶ 批量创建 {count} 个 */1 任务...")
    t0 = time.time()
    out = run_ctl(["stress", "create", "--count", str(count), "--prefix", "pybench",
                   "--cron", "*/1 * * * *", "--handler", f"{SCHED_METRICS}/healthz",
                   "--type", "http", "--timeout", "5"])
    elapsed = time.time() - t0
    # 解析创建数
    import re
    m = re.search(r"created (\d+)/(\d+) tasks", out)
    created = int(m.group(1)) if m else count
    log(f"  创建 {created}/{count} 个任务, 耗时 {elapsed:.1f}s, "
        f"吞吐 {created/elapsed:.1f} tasks/sec")
    report_data["create"] = {"count": created, "elapsed": round(elapsed, 2),
                             "throughput": round(created / elapsed, 1)}

    # 3. 等待自然触发
    log(f"▶ 等待任务自然触发 (最多 {wait_minutes*60}s)...")
    deadline = time.time() + wait_minutes * 60
    while time.time() < deadline:
        time.sleep(10)
        sched_now = mc.histogram("scheduler", "dolphin_scheduler_task_lag_seconds")
        new_samples = sched_now["count"] - sched_before["count"]
        if new_samples >= count * 0.9:
            log(f"  已采集到 {int(new_samples)} 个调度样本")
            break
    else:
        sched_now = mc.histogram("scheduler", "dolphin_scheduler_task_lag_seconds")

    # 4. 再等执行完成
    log("▶ 等待执行完成上报 (30s)...")
    time.sleep(30)

    # 5. 采集最终指标
    sched_after = mc.histogram("scheduler", "dolphin_scheduler_task_lag_seconds")
    worker_after = mc.histogram("worker", "dolphin_worker_task_duration_seconds")
    completed_after = mc._parse_simple(mc.fetch("worker"), "dolphin_worker_task_completed_total")
    dispatch_after = mc._parse_simple(mc.fetch("scheduler"), "dolphin_scheduler_dispatch_total")

    # 6. 计算
    lag_samples = sched_after["count"] - sched_before["count"]
    lag_sum = sched_after["sum"] - sched_before["sum"]
    dur_samples = worker_after["count"] - worker_before["count"]
    dur_sum = worker_after["sum"] - worker_before["sum"]
    new_completed = completed_after - completed_before
    new_dispatch = dispatch_after - dispatch_before

    log()
    log("  调度链路结果:")
    log(f"    总调度分发: {int(new_dispatch)}")
    log(f"    调度延迟样本: {int(lag_samples)}")
    if lag_samples >= 10:
        lag_avg = lag_sum / lag_samples
        log(f"    调度延迟均值: {lag_avg*1000:.1f} ms")
        # 用 histogram buckets 估算 P99
        h_pct = mc.histogram_percentiles("scheduler", "dolphin_scheduler_task_lag_seconds")
        log(f"    调度延迟 P50/P95/P99: {h_pct['p50']} / {h_pct['p95']} / {h_pct['p99']} ms"
            if h_pct.get("p99") else f"    调度延迟 P99: 样本不足")
        report_data["schedule_lag"] = {"count": lag_samples, "avg_ms": round(lag_avg*1000, 1),
                                       **{k: v for k, v in h_pct.items() if k in ("p50","p95","p99")}}
    else:
        log(f"    调度延迟样本不足 (<10)，无法给出可信 P99")

    log(f"    执行耗时样本: {int(dur_samples)}")
    if dur_samples >= 10:
        dur_avg = dur_sum / dur_samples
        log(f"    执行耗时均值: {dur_avg*1000:.1f} ms")
        dur_pct = mc.histogram_percentiles("worker", "dolphin_worker_task_duration_seconds",
                                           labels={"handler_type": "http"})
        log(f"    执行耗时 P50/P95/P99: {dur_pct['p50']} / {dur_pct['p95']} / {dur_pct['p99']} ms")
        report_data["exec_duration"] = {"count": dur_samples, "avg_ms": round(dur_avg*1000, 1),
                                        **{k: v for k, v in dur_pct.items() if k in ("p50","p95","p99")}}
    else:
        log(f"    执行耗时样本不足 (<10)")

    log(f"    任务完成总数: {int(new_completed)}")
    if new_completed >= 10:
        # 尝试按状态拆分
        for status in ["success", "failed", "timeout"]:
            c = mc._parse_counter(mc.fetch("worker"), "dolphin_worker_task_completed_total",
                                  label_filter={"status": status})
            if c:
                log(f"      {status}: {c}")
        success = mc._parse_counter(mc.fetch("worker"), "dolphin_worker_task_completed_total",
                                    label_filter={"status": "success"})
        log(f"    成功率: {success/new_completed*100:.2f}%")
        report_data["success_rate"] = round(success / new_completed * 100, 2)
    else:
        log(f"    完成样本不足 (<10)")

    return report_data


def main():
    parser = argparse.ArgumentParser(description="Dolphin 压测框架")
    parser.add_argument("--gateway-only", action="store_true", help="只测网关")
    parser.add_argument("--schedule-only", action="store_true", help="只测调度链路")
    parser.add_argument("--count", type=int, default=100, help="调度压测任务数")
    parser.add_argument("--wait-minutes", type=int, default=3, help="等待自然触发分钟数")
    parser.add_argument("--conc", type=int, default=10, help="网关压测基准并发")
    args = parser.parse_args()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    ensure_tools()

    mc = MetricsClient({"scheduler": SCHED_METRICS, "worker": WORKER_METRICS, "gateway": GATEWAY})

    log(f"Dolphin 压测报告 {time.strftime('%Y-%m-%d %H:%M:%S')}")

    if not args.gateway_only and not args.schedule_only:
        require_components(mc)
        bench_gateway(mc, concurrency=args.conc)
        bench_schedule(mc, count=args.count, wait_minutes=args.wait_minutes)
    elif args.gateway_only:
        require_components(mc)
        bench_gateway(mc, concurrency=args.conc)
    elif args.schedule_only:
        require_components(mc)
        bench_schedule(mc, count=args.count, wait_minutes=args.wait_minutes)

    # 写报告
    report_path = os.path.join(RESULTS_DIR, "bench_report.txt")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    json_path = os.path.join(RESULTS_DIR, "bench_report.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, ensure_ascii=False, indent=2)

    log()
    log(f"✅ 报告已保存: {report_path}")
    log(f"✅ 结构化数据: {json_path}")


if __name__ == "__main__":
    main()
