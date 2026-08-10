# bench_full.ps1 — Dolphin 完整闭环压测脚本
#
# 功能：一键完成整套压测，输出完整闭环指标报告。
#
# 阶段：
#   0. 环境检查（组件是否在运行）
#   1. 网关压测（QPS / P50 / P95 / P99 / 错误率）
#   2. 批量创建任务（测任务创建吞吐）
#   3. 等待一轮自然调度
#   4. 抓取调度延迟 + worker 执行指标
#   5. 输出完整报告到 results/bench-report.txt
#
# 用法： .\hack\bench_full.ps1
# 前置：三个组件已启动，dolphinctl.exe 已编译到 bin/

$ErrorActionPreference = "Stop"
$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ProjectDir   # hack/ -> 项目根
Set-Location $ProjectDir

$ResultsDir = Join-Path $ProjectDir "results"
New-Item -ItemType Directory -Force -Path $ResultsDir | Out-Null

$Report = Join-Path $ResultsDir "bench-report.txt"
$Ctl = Join-Path $ProjectDir "bin\dolphinctl.exe"
$Loadgen = Join-Path $ProjectDir "bin\loadgen.exe"

# 组件地址
$Gateway = "http://localhost:8080"
$SchedMetrics = "http://localhost:9090"
$WorkerMetrics = "http://localhost:9091"

# 压测参数
$GatewayDuration = "10s"
$GatewayConcurrency = 50
$BatchCount = 100          # 批量创建的任务数
$CronExpr = "*/1 * * * *"

function Write-Step([string]$title) {
    Write-Host ""
    Write-Host "════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  $title" -ForegroundColor Cyan
    Write-Host "════════════════════════════════════════" -ForegroundColor Cyan
    Add-Content $Report ""
    Add-Content $Report "=== $title ==="
}

function Write-Result([string]$line) {
    Write-Host "  $line"
    Add-Content $Report $line
}

# 清空旧报告
"" | Set-Content $Report
Write-Host "Dolphin 完整闭环压测报告 $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" | Out-File $Report -Append

# ═══════════════ 阶段 0：环境检查 ═══════════════
Write-Step "阶段 0：环境检查"

$checks = @(
    @{ Name = "Gateway (8080)";     Url = "$Gateway/health" },
    @{ Name = "Scheduler metrics";  Url = "$SchedMetrics/metrics" },
    @{ Name = "Worker metrics";     Url = "$WorkerMetrics/metrics" }
)

$envOK = $true
foreach ($c in $checks) {
    try {
        $r = Invoke-WebRequest -Uri $c.Url -UseBasicParsing -TimeoutSec 3
        if ($r.StatusCode -eq 200) {
            Write-Result "✅ $($c.Name) 可达"
        } else {
            Write-Result "❌ $($c.Name) 状态码 $($r.StatusCode)"
            $envOK = $false
        }
    } catch {
        Write-Result "❌ $($c.Name) 不可达: $($_.Exception.Message)"
        $envOK = $false
    }
}

if (-not $envOK) {
    Write-Host ""
    Write-Host "❌ 环境未就绪，请先启动三个组件和基础设施。" -ForegroundColor Red
    Write-Host "   docker compose -f deployments/docker-compose.yaml up -d etcd mysql redis" -ForegroundColor Gray
    Write-Host "   .\start-dev.ps1" -ForegroundColor Gray
    exit 1
}

# 构建工具
if (-not (Test-Path $Ctl)) {
    Write-Host "▶ 构建 dolphinctl..."
    go build -o bin\dolphinctl.exe ./cmd/dolphinctl
}
if (-not (Test-Path $Loadgen)) {
    Write-Host "▶ 构建 loadgen..."
    go build -o bin\loadgen.exe ./cmd/loadgen
}

# ═══════════════ 阶段 1：网关压测 ═══════════════
Write-Step "阶段 1：网关压测 ($GatewayDuration, 并发 $GatewayConcurrency)"
Write-Result "目标: $Gateway/health"

$gatewayOut = & $Loadgen -url "$Gateway/health" -duration $GatewayDuration -concurrency $GatewayConcurrency 2>&1 | Out-String
Write-Host $gatewayOut
Add-Content $Report $gatewayOut

# ═══════════════ 阶段 2：批量创建任务 ═══════════════
Write-Step "阶段 2：批量创建 $BatchCount 个任务（测创建吞吐）"

$createStart = Get-Date
$created = 0
for ($i = 0; $i -lt $BatchCount; $i++) {
    $name = "bench-$((Get-Date -Format 'HHmmss'))-$i"
    $out = & $Ctl task create --name $name --cron $CronExpr --handler "$SchedMetrics/healthz" --type http 2>&1
    if ($LASTEXITCODE -eq 0 -and $out -match "id=") {
        $created++
    }
    if (($i + 1) % 25 -eq 0) {
        Write-Result "  已创建 $($i+1)/$BatchCount"
    }
}
$createElapsed = ((Get-Date) - $createStart).TotalSeconds
Write-Result "✅ 创建成功: $created/$BatchCount"
Write-Result "⏱ 创建耗时: $([math]::Round($createElapsed, 2))s"
if ($createElapsed -gt 0) {
    Write-Result "📈 创建吞吐: $([math]::Round($created / $createElapsed, 1)) tasks/sec"
}

# ═══════════════ 阶段 3：等待调度 ═══════════════
Write-Step "阶段 3：等待一轮自然调度（最多 90s）"
Write-Result "等待 cron 到期触发...（$CronExpr）"

$dispatchBefore = 0
try {
    $m = Invoke-WebRequest -Uri "$SchedMetrics/metrics" -UseBasicParsing -TimeoutSec 3
    $dispatchBefore = ([regex]::Matches($m.Content, 'dolphin_scheduler_dispatch_total\{(.*?)\} ([0-9]+)') | ForEach-Object { [int]$_.Groups[2].Value } | Measure-Object -Sum).Sum
} catch { }

$waited = 0
while ($waited -lt 90) {
    Start-Sleep -Seconds 5
    $waited += 5

    try {
        $m = Invoke-WebRequest -Uri "$SchedMetrics/metrics" -UseBasicParsing -TimeoutSec 3
        $dispatchAfter = ([regex]::Matches($m.Content, 'dolphin_scheduler_dispatch_total\{(.*?)\} ([0-9]+)') | ForEach-Object { [int]$_.Groups[2].Value } | Measure-Object -Sum).Sum
        if ($dispatchAfter -gt $dispatchBefore) {
            Write-Result "✅ 检测到调度发生（等待 $waited s）"
            break
        }
    } catch { }

    if ($waited -eq 90) {
        Write-Result "⚠️ 90s 内未检测到调度（可能 cron 尚未触发，或已错过窗口）"
    }
}

# 再等 15s 让任务执行完上报
Write-Result "等待任务执行完成（15s）..."
Start-Sleep -Seconds 15

# ═══════════════ 阶段 4：抓取完整指标 ═══════════════
Write-Step "阶段 4：完整闭环指标"

# 4.1 调度器指标
Write-Result "── 调度器指标 (9090) ──"
try {
    $sm = (Invoke-WebRequest -Uri "$SchedMetrics/metrics" -UseBasicParsing -TimeoutSec 3).Content

    # dispatch total
    $dispatchTotal = ([regex]::Matches($sm, 'dolphin_scheduler_dispatch_total\{(.*?)\} ([0-9]+)') | ForEach-Object { [int]$_.Groups[2].Value } | Measure-Object -Sum).Sum
    Write-Result "总调度分发数 (dispatch_total): $dispatchTotal"

    # task lag
    $lagCount = 0; $lagSum = 0.0
    $lc = [regex]::Match($sm, 'dolphin_scheduler_task_lag_seconds_count ([0-9]+)')
    $ls = [regex]::Match($sm, 'dolphin_scheduler_task_lag_seconds_sum ([0-9.e+-]+)')
    if ($lc.Success) { $lagCount = [int]$lc.Groups[1].Value }
    if ($ls.Success) { $lagSum = [double]$ls.Groups[1].Value }
    Write-Result "调度延迟样本数: $lagCount"
    if ($lagCount -gt 0) {
        Write-Result "调度延迟 P 均值: $([math]::Round($lagSum / $lagCount * 1000, 1)) ms"
        Write-Result "调度延迟累计: $([math]::Round($lagSum, 3)) s"
    }

    # reconcile
    $rc = [regex]::Match($sm, 'dolphin_scheduler_reconcile_duration_seconds_count ([0-9]+)')
    if ($rc.Success) { Write-Result "reconcile 次数: $($rc.Groups[1].Value)" }

    # missed
    $ms = [regex]::Match($sm, 'dolphin_scheduler_missed_schedules_total ([0-9]+)')
    if ($ms.Success) { Write-Result "漏调度次数: $($ms.Groups[1].Value)" }
} catch {
    Write-Result "⚠️ 无法读取调度器指标: $($_.Exception.Message)"
}

# 4.2 Worker 指标
Write-Result ""
Write-Result "── Worker 指标 (9091) ──"
try {
    $wm = (Invoke-WebRequest -Uri "$WorkerMetrics/metrics" -UseBasicParsing -TimeoutSec 3).Content

    $completed = ([regex]::Matches($wm, 'dolphin_worker_task_completed_total\{(.*?)\} ([0-9]+)') | ForEach-Object { [int]$_.Groups[2].Value } | Measure-Object -Sum).Sum
    Write-Result "总完成执行数: $completed"

    $dc = [regex]::Match($wm, 'dolphin_worker_task_duration_seconds_count\{[^}]*\} ([0-9]+)')
    $ds = [regex]::Match($wm, 'dolphin_worker_task_duration_seconds_sum\{[^}]*\} ([0-9.e+-]+)')
    if ($dc.Success) {
        $durCount = [int]$dc.Groups[1].Value
        $durSum = [double]$ds.Groups[1].Value
        Write-Result "执行耗时样本数: $durCount"
        if ($durCount -gt 0) {
            Write-Result "平均执行耗时: $([math]::Round($durSum / $durCount * 1000, 1)) ms"
        }
    }

    # 按状态拆分
    foreach ($st in @("success", "failed", "timeout")) {
        $pattern = 'dolphin_worker_task_completed_total\{status="' + $st + '"\} ([0-9]+)'
        $sc = [regex]::Match($wm, $pattern)
        if ($sc.Success) { Write-Result "  状态 $st : $($sc.Groups[1].Value)" }
    }
} catch {
    Write-Result "⚠️ 无法读取 worker 指标: $($_.Exception.Message)"
}

# 4.3 网关指标
Write-Result ""
Write-Result "── 网关指标 (8080) ──"
try {
    $gm = (Invoke-WebRequest -Uri "$Gateway/metrics" -UseBasicParsing -TimeoutSec 3).Content
    $reqTotal = ([regex]::Matches($gm, 'dolphin_gateway_requests_total\{(.*?)\} ([0-9]+)') | ForEach-Object { [int]$_.Groups[2].Value } | Measure-Object -Sum).Sum
    Write-Result "网关总请求数: $reqTotal"
    $errTotal = ([regex]::Matches($gm, 'dolphin_gateway_errors_total\{(.*?)\} ([0-9]+)') | ForEach-Object { [int]$_.Groups[2].Value } | Measure-Object -Sum).Sum
    Write-Result "网关错误数: $errTotal"
} catch {
    Write-Result "⚠️ 无法读取网关指标: $($_.Exception.Message)"
}

# ═══════════════ 阶段 5：报告输出 ═══════════════
Write-Step "阶段 5：完成"

Write-Host ""
Write-Host "════════════════════════════════════════" -ForegroundColor Green
Write-Host "  ✅ 压测完成！完整报告已保存:" -ForegroundColor Green
Write-Host "  $Report" -ForegroundColor Green
Write-Host "════════════════════════════════════════" -ForegroundColor Green

# 清理批量创建的测试任务（可选，保持环境干净）
$cleanup = Read-Host "是否删除批量创建的压测任务? (y/n)"
if ($cleanup -eq "y") {
    Write-Host "清理压测任务..."
    $taskList = & $Ctl task list --limit 200 2>&1 | Out-String
    $ids = [regex]::Matches($taskList, 'id=([0-9a-f-]{36})') | ForEach-Object { $_.Groups[1].Value } | Select-Object -Unique
    $deleted = 0
    foreach ($id in $ids) {
        $out = & $Ctl task delete --id $id 2>&1
        if ($LASTEXITCODE -eq 0) { $deleted++ }
    }
    Write-Result "已删除 $deleted 个任务"
}
