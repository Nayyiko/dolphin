# bench_gateway_repro.ps1 — 网关裸吞吐复跑（替代无源的 2093 QPS）
# ============================================================
# 背景：evidence-plan 写的"2093 QPS / P99<10ms"在仓库里没有任何原始报告；
#       results/bench-report.txt 实测是 1569 QPS + P99 204ms + 490 连接错误，
#       但那是默认配置（限流 rate=100/s）下测得，吞吐被限流器拖累。
# 本脚本：用 gateway_bench.yaml（关限流）测"裸网关吞吐"，产出可信报告，
#         并把结果自动回填 docs/benchmark.md 的网关压测表。
#
# 前置：
#   1) 用压测专用配置起一个关限流的 gateway：
#        go run ./cmd/gateway -config configs/gateway_bench.yaml
#   2) bin/loadgen.exe 已构建
# 用法：.\hack\bench_gateway_repro.ps1 [-Concurrency 100] [-DurationSec 30] [-Port 8080]
# ============================================================
param(
    [int]$Concurrency = 100,
    [int]$DurationSec = 30,
    [int]$Port = 8080
)

$ErrorActionPreference = "Stop"
$PROJECT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECT_DIR = Split-Path -Parent $PROJECT_DIR   # hack -> 项目根
Set-Location $PROJECT_DIR

$LOADGEN = Join-Path $PROJECT_DIR "bin\loadgen.exe"
$REPORT  = Join-Path $PROJECT_DIR "results\gateway_bench_report.txt"
$GW = "http://localhost:$Port"
$TARGET = "$GW/health"

# ── 0. 环境检查 ──
if (-not (Test-Path $LOADGEN)) {
    Write-Host "未找到 bin\loadgen.exe，先构建:" -ForegroundColor Yellow
    Write-Host "  go build -o bin\loadgen.exe ./cmd/loadgen" -ForegroundColor Gray
    exit 1
}
$probe = curl.exe -s -o NUL -w "%{http_code}" "$GW/health"
if ($probe -ne "200") {
    Write-Host "❌ gateway 未就绪（/health=$probe）。" -ForegroundColor Red
    Write-Host "请用关限流的压测配置起 gateway:" -ForegroundColor Yellow
    Write-Host "  go run ./cmd/gateway -config configs/gateway_bench.yaml" -ForegroundColor Gray
    exit 1
}
# 提醒：若当前 gateway 是正常配置（限流开启），吞吐会被限流拖累，结果仅作参考
$limited = curl.exe -s "$GW/metrics" | Select-String -Pattern 'rate_limit_enabled|ratelimit_rejected_total'
Write-Host "⚠️  请确认当前 gateway 用的是 gateway_bench.yaml（关限流），否则测到的是受限流拖累的吞吐。" -ForegroundColor Yellow

Write-Host "════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  网关裸吞吐复跑  Concurrency=$Concurrency  Duration=${DurationSec}s  $TARGET"
Write-Host "════════════════════════════════════════════" -ForegroundColor Cyan

# ── 打流 ──
$output = & $LOADGEN -url $TARGET -concurrency $Concurrency -duration "${DurationSec}s" 2>&1
$output | Out-Host

# ── 解析 loadgen 输出 ──
function Parse-Field([string]$Text, [string]$Pattern) {
    $m = [regex]::Match($Text, $Pattern)
    if ($m.Success) { return $m.Groups[1].Value.Trim() }
    return "N/A"
}
$all = ($output -join "`n")
$qps     = Parse-Field $all 'QPS:\s*([\d.]+)'
$p99     = Parse-Field $all 'P99 延迟:\s*([\d.]+) ms'
$p95     = Parse-Field $all 'P95 延迟:\s*([\d.]+) ms'
$total   = Parse-Field $all '总请求数:\s*(\d+)'
$success = Parse-Field $all '成功请求:\s*(\d+)'
$err5xx  = Parse-Field $all '5xx 错误:\s*(\d+)'
$connErr = Parse-Field $all '连接错误:\s*(\d+)'

# 错误率 = (总请求 - 成功) / 总请求
$errRate = "N/A"
if ($total -match '^\d+$' -and $success -match '^\d+$' -and [int]$total -gt 0) {
    $errRate = "{0:P2}" -f (([int]$total - [int]$success) / [int]$total)
}

Write-Host ""
Write-Host "── 结果 ──" -ForegroundColor Cyan
Write-Host ("网关 QPS:   {0}" -f $qps)
Write-Host ("P95 延迟:   {0} ms" -f $p95)
Write-Host ("P99 延迟:   {0} ms" -f $p99)
Write-Host ("错误率:     {0}  (总{1} 成功{2} 5xx{3} 连接错误{4})" -f $errRate, $total, $success, $err5xx, $connErr)

# ── 存档 ──
$report = @"
Dolphin 网关裸吞吐测试报告
时间: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
配置: Concurrency=$Concurrency Duration=${DurationSec}s Target=$TARGET (限流关闭)
总请求: $total
成功: $success
5xx: $err5xx
连接错误: $connErr
QPS: $qps
P95: $p95 ms
P99: $p99 ms
错误率: $errRate
"@
[System.IO.File]::WriteAllText($REPORT, $report)
Write-Host "报告已保存: $REPORT" -ForegroundColor Gray

# ── 自动回填 docs/benchmark.md 网关压测表 ──
$benchFile = Join-Path $PROJECT_DIR "docs\benchmark.md"
$bench = [System.IO.File]::ReadAllText($benchFile)
$updated = $bench
$updated = $updated.Replace("| 网关 QPS | 10,000+ | ____ |", "| 网关 QPS | 10,000+ | $qps |")
$updated = $updated.Replace("| P99 延迟 | < 5ms | ____ |", "| P99 延迟 | < 5ms | $p99 ms |")
$updated = $updated.Replace("| P95 延迟 | < 2ms | ____ |", "| P95 延迟 | < 2ms | $p95 ms |")
$updated = $updated.Replace("| 错误率 | < 0.1% | ____ |", "| 错误率 | < 0.1% | $errRate |")
if ($updated -ne $bench) {
    [System.IO.File]::WriteAllText($benchFile, $updated)
    Write-Host "✅ docs/benchmark.md 网关压测表已回填实际值" -ForegroundColor Green
} else {
    Write-Host "⚠️  未能匹配 benchmark.md 表格行（格式可能变了），未回填。请手动把上面数值填进 docs/benchmark.md。" -ForegroundColor Yellow
}
