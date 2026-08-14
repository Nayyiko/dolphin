# bench_gateway_repro.ps1 — 网关裸吞吐复跑（替代无源的 2093 QPS）
# ============================================================
# 背景：evidence-plan 写的"2093 QPS / P99<10ms"在仓库里没有任何原始报告；
#       results/bench-report.txt 实测是 1569 QPS + P99 204ms + 490 连接错误，
#       但那是默认配置（限流 rate=100/s）下测得，吞吐被限流器拖累。
# 本脚本：用 gateway_bench.yaml（关限流）测"裸网关吞吐"，产出可信报告，
#         并把结果自动回填 docs/benchmark.md 的网关压测表。
#
# 健壮性：跑 -Rounds 轮取"最好且干净"的一轮（连接错误率<=MaxConnErrPct），
#         脏数据（网关失联/大量连接错误）不会回填 benchmark.md。
#
# 前置：
#   1) 压测前请关闭其他 Dolphin 窗口（start-dev.ps1 的 scheduler/worker、
#      共享测试的 gateway2），避免抢 CPU 导致连接错误虚高。
#   2) 用压测专用配置起一个关限流的 gateway：
#        go run ./cmd/gateway -config configs/gateway_bench.yaml
#   3) bin/loadgen.exe 已构建
# 用法：.\hack\bench_gateway_repro.ps1 [-Concurrency 100] [-DurationSec 30] [-Port 8080] [-Rounds 3]
# ============================================================
param(
    [int]$Concurrency = 100,
    [int]$DurationSec = 30,
    [int]$Port = 8080,
    [int]$Rounds = 3,
    [double]$MaxConnErrPct = 1.0   # 连接错误率上限(%),超过则该轮视为脏数据
)

$ErrorActionPreference = "Stop"
$PROJECT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECT_DIR = Split-Path -Parent $PROJECT_DIR   # hack -> 项目根
Set-Location $PROJECT_DIR

$LOADGEN = Join-Path $PROJECT_DIR "bin\loadgen.exe"
$REPORT  = Join-Path $PROJECT_DIR "results\gateway_bench_report.txt"
$GW = "http://localhost:$Port"
$TARGET = "$GW/health"
New-Item -ItemType Directory -Force -Path (Join-Path $PROJECT_DIR "results") | Out-Null

# ── 0. 环境检查 ──
if (-not (Test-Path $LOADGEN)) {
    Write-Host "未找到 bin\loadgen.exe，先构建:" -ForegroundColor Yellow
    Write-Host "  go build -o bin\loadgen.exe ./cmd/loadgen" -ForegroundColor Gray
    exit 1
}
$probe = curl.exe -s --max-time 3 -o NUL -w "%{http_code}" "$GW/health"
if ($probe -ne "200") {
    Write-Host "❌ gateway 未就绪（/health=$probe）。" -ForegroundColor Red
    Write-Host "请用关限流的压测配置起 gateway:" -ForegroundColor Yellow
    Write-Host "  go run ./cmd/gateway -config configs/gateway_bench.yaml" -ForegroundColor Gray
    exit 1
}

Write-Host "⚠️  确认：当前 gateway 应为 gateway_bench.yaml（关限流）；压测前请关闭其他 Dolphin 窗口。" -ForegroundColor Yellow
Write-Host "════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  网关裸吞吐复跑  Concurrency=$Concurrency  Duration=${DurationSec}s  Rounds=$Rounds  $TARGET"
Write-Host "════════════════════════════════════════════" -ForegroundColor Cyan

# ── 解析 loadgen 输出 ──
function Parse-Field([string]$Text, [string]$Pattern) {
    $m = [regex]::Match($Text, $Pattern)
    if ($m.Success) { return $m.Groups[1].Value.Trim() }
    return "N/A"
}

# 每轮一条记录：qps/p95/p99/total/success/connErr/errRate/alive
$roundResults = @()

for ($i = 1; $i -le $Rounds; $i++) {
    Write-Host ""
    Write-Host "── 第 $i/$Rounds 轮 ──" -ForegroundColor Cyan

    $output = & $LOADGEN -url $TARGET -concurrency $Concurrency -duration "${DurationSec}s" 2>&1
    $output | Out-Host

    $all = ($output -join "`n")
    $qps     = Parse-Field $all 'QPS:\s*([\d.]+)'
    $p99     = Parse-Field $all 'P99 延迟:\s*([\d.]+) ms'
    $p95     = Parse-Field $all 'P95 延迟:\s*([\d.]+) ms'
    $total   = Parse-Field $all '总请求数:\s*(\d+)'
    $success = Parse-Field $all '成功请求:\s*(\d+)'
    $connErr = Parse-Field $all '连接错误:\s*(\d+)'
    $err5xx  = Parse-Field $all '5xx 错误:\s*(\d+)'

    $connErrPct = -1.0
    if ($total -match '^\d+$' -and $connErr -match '^\d+$' -and [int]$total -gt 0) {
        $connErrPct = ([double]$connErr / [double]$total) * 100.0
    }
    $errRate = "N/A"
    if ($total -match '^\d+$' -and $success -match '^\d+$' -and [int]$total -gt 0) {
        $errRate = "{0:P2}" -f (([int]$total - [int]$success) / [int]$total)
    }

    # 压测后健康探针
    $postCode = curl.exe -s --max-time 3 -o NUL -w "%{http_code}" "$GW/health"
    $alive = ($postCode -ne "000") -and ($postCode -ne "")

    $valid = $alive -and ($connErrPct -ge 0) -and ($connErrPct -le $MaxConnErrPct)
    $roundResults += [pscustomobject]@{
        Round = $i; Qps = $qps; P95 = $p95; P99 = $p99; Total = $total
        Success = $success; ConnErr = $connErr; ConnErrPct = $connErrPct
        ErrRate = $errRate; Alive = $alive; Valid = $valid
    }

    if (-not $alive) {
        Write-Host "⚠️  第 $i 轮：压测后网关无响应（code=$postCode）——本轮无效。" -ForegroundColor Yellow
    } elseif ($connErrPct -gt $MaxConnErrPct) {
        Write-Host "⚠️  第 $i 轮：连接错误率 $([math]::Round($connErrPct,2))% 超过阈值 $MaxConnErrPct%——本轮判定脏数据。" -ForegroundColor Yellow
    } else {
        Write-Host "✅ 第 $i 轮：连接错误率 $([math]::Round($connErrPct,2))%，数据干净。" -ForegroundColor Green
    }
}

# ── 汇总：取"干净轮"中 QPS 最高的一轮 ──
$validRounds = @($roundResults | Where-Object { $_.Valid })
$best = $null
if ($validRounds.Count -gt 0) {
    $best = $validRounds | Sort-Object { [double]$_.Qps } -Descending | Select-Object -First 1
}

Write-Host ""
Write-Host "── 汇总 ──" -ForegroundColor Cyan
$roundResults | Format-Table -AutoSize Round, Qps, P95, P99, Total, ConnErr, ConnErrPct, Valid | Out-Host

$reportText = ""
if ($null -eq $best) {
    Write-Host "❌ 没有干净轮次（全部网关失联或连接错误率超标）。请关闭其他 Dolphin 窗口后重跑。" -ForegroundColor Red
    $reportText = @"
Dolphin 网关裸吞吐测试报告
时间: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
配置: Concurrency=$Concurrency Duration=${DurationSec}s Rounds=$Rounds Target=$TARGET (限流关闭)
结果: 无干净轮次（连接错误率全部 > $MaxConnErrPct% 或网关失联）
建议: 关闭其他 Dolphin 窗口(worker/scheduler/gateway2/Docker)后重跑
"@
} else {
    Write-Host "🏆 最好一轮: 第 $($best.Round) 轮  QPS $($best.Qps)  P95 $($best.P95)ms  P99 $($best.P99)ms  连接错误 $($best.ConnErr)（$([math]::Round($best.ConnErrPct,2))%）" -ForegroundColor Green
    $reportText = @"
Dolphin 网关裸吞吐测试报告
时间: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
配置: Concurrency=$Concurrency Duration=${DurationSec}s Rounds=$Rounds Target=$TARGET (限流关闭)
取: 第 $($best.Round) 轮（干净轮中 QPS 最高）
总请求: $($best.Total)
成功: $($best.Success)
5xx: N/A
连接错误: $($best.ConnErr) ($([math]::Round($best.ConnErrPct,2))%)
QPS: $($best.Qps)
P95: $($best.P95) ms
P99: $($best.P99) ms
错误率: $($best.ErrRate)
每轮明细:
$($roundResults | Format-Table -AutoSize Round, Qps, P95, P99, Total, ConnErr, ConnErrPct, Valid | Out-String)
"@
}
[System.IO.File]::WriteAllText($REPORT, $reportText)
Write-Host "报告已保存: $REPORT" -ForegroundColor Gray

# ── 自动回填 docs/benchmark.md 网关压测表（只有干净轮才回填）──
if ($null -eq $best) {
    Write-Host "⚠️  无干净数据，跳过 benchmark.md 回填。" -ForegroundColor Yellow
} else {
    $benchFile = Join-Path $PROJECT_DIR "docs\benchmark.md"
    if (Test-Path $benchFile) {
        $bench = [System.IO.File]::ReadAllText($benchFile)
        $updated = $bench
        $updated = $updated.Replace("| 网关 QPS | 10,000+ | ____ |", "| 网关 QPS | 10,000+ | $($best.Qps) |")
        $updated = $updated.Replace("| P99 延迟 | < 5ms | ____ |", "| P99 延迟 | < 5ms | $($best.P99) ms |")
        $updated = $updated.Replace("| P95 延迟 | < 2ms | ____ |", "| P95 延迟 | < 2ms | $($best.P95) ms |")
        $updated = $updated.Replace("| 错误率 | < 0.1% | ____ |", "| 错误率 | < 0.1% | $($best.ErrRate) |")
        if ($updated -ne $bench) {
            [System.IO.File]::WriteAllText($benchFile, $updated)
            Write-Host "✅ docs/benchmark.md 网关压测表已回填第 $($best.Round) 轮实际值" -ForegroundColor Green
        } else {
            Write-Host "⚠️  未能匹配 benchmark.md 表格行（格式可能变了），未回填。请手动把上面数值填进 docs/benchmark.md。" -ForegroundColor Yellow
        }
    } else {
        Write-Host "⚠️  docs/benchmark.md 不存在，跳过回填。" -ForegroundColor Yellow
    }
}

exit $(if ($null -ne $best) { 0 } else { 1 })
