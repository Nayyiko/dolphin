# schedule_concurrency_test.ps1 — 调度/执行并发压测
#
# 回答面试问题："同时能扛多少并发任务执行？"
#   - 单 worker 并发上限 = 协程池 capacity（configs/worker.yaml 默认 50）
#   - 在途上限 = capacity + 队列(capacity*2) = 150；超过立即拒绝（主动背压，
#     failed: worker pool full），不阻塞堆积，防止雪崩
#
# 本脚本测三个硬数据：
#   1. 峰值并发执行数（dolphin_worker_tasks_executing 峰值，应 ≈ pool capacity）
#   2. 峰值池利用率（dolphin_worker_pool_capacity_utilization，应接近 1.0）
#   3. 执行延迟分布（task_logs end-start 的 mean/p50/p95/p99）+ 全批成功率
#   4. 调度分发速率（dispatch_total 增量 / 分发耗时）
#
# 用法:
#   powershell -ExecutionPolicy Bypass -File hack\schedule_concurrency_test.ps1            # 100 个任务
#   powershell -ExecutionPolicy Bypass -File hack\schedule_concurrency_test.ps1 -TaskCount 150
#
# 前置（与 dag_test.ps1 相同）:
#   docker compose -f deployments/docker-compose.yaml up -d etcd mysql redis
#   go build -o bin\scheduler.exe ./cmd/scheduler && go build -o bin\worker.exe ./cmd/worker && go build -o bin\dolphinctl.exe ./cmd/dolphinctl
#   start-dev.ps1 启动 1 个 scheduler + 1 个 worker
#
# 输出: results\schedule_concurrency_report.txt

param(
    [int]$TaskCount = 100,    # 批量创建并同时触发的任务数（默认 100 < 150 在途上限，干净测并发）
    [int]$SleepSeconds = 3,   # 每个任务的执行耗时（秒），制造执行窗口让并发顶上去
    [int]$WaitMaxSec = 180
)

$ErrorActionPreference = "Continue"
$ProjectDir = "C:\Users\30641\Desktop\dolphin"
$Bins = "$ProjectDir\bin"
$Results = "$ProjectDir\results"
New-Item -ItemType Directory -Force -Path $Results | Out-Null
$OutFile = "$Results\schedule_concurrency_report.txt"
"" | Set-Content $OutFile
$NamePrefix = "sc-"

function Write-Step { param($msg) Write-Host "`n══════════ $msg ══════════" -ForegroundColor Green; Add-Content $OutFile "`n== $msg ==" }

# 读指标（按端口）
function Get-Metric {
    param([int]$Port, [string]$Name)
    try {
        $m = (Invoke-WebRequest -Uri "http://localhost:$Port/metrics" -UseBasicParsing -TimeoutSec 3).Content
        $line = ($m -split "`n") | Where-Object { $_ -match "^$Name " } | Select-Object -First 1
        if ($line) { return [double](($line -split " ")[1]) }
        return 0
    } catch { return -1 }
}

# MySQL 单值查询
function SqlScalar {
    param([string]$Q)
    $r = docker exec dolphin-mysql mysql -u root -pdolphin -N -e $Q 2>$null
    return $r
}

# ---------- 0. 清理 ----------
Write-Step "0. 清理旧任务"
docker exec dolphin-mysql mysql -u root -pdolphin -e "USE dolphin; DELETE FROM task_conditions; DELETE FROM task_logs; DELETE FROM tasks;" 2>$null
Write-Host "已清理 tasks / task_logs / task_conditions"

# ---------- 1. 批量创建（stress create 进程内批量，比逐条 create 快 ~10x） ----------
Write-Step "1. 批量创建 $TaskCount 个任务（handler sleep $SleepSeconds s）"
$startCreate = Get-Date
& "$Bins\dolphinctl.exe" --addr "localhost:50051" stress create --count $TaskCount --prefix $NamePrefix --cron "0 0 1 1 *" --handler "http://localhost:9090/debug/sleep?seconds=$SleepSeconds" --timeout ($SleepSeconds + 5) --retries 0 2>&1 | Out-Null
$createSec = ((Get-Date) - $startCreate).TotalSeconds
# 收集本批任务 id（步骤 0 已清空 tasks，剩余都是本批）
$idsRaw = SqlScalar "SELECT GROUP_CONCAT(id) FROM dolphin.tasks WHERE name LIKE '$NamePrefix%';"
$ids = @()
if ($idsRaw) { $ids = $idsRaw -split "," }
$createRate = if ($createSec -gt 0) { $ids.Count / $createSec } else { 0 }
Write-Host "  创建完成: $($ids.Count)/$TaskCount 个, 速率 $([math]::Round($createRate,1)) tasks/sec"
Add-Content $OutFile "created=$($ids.Count) create_rate=$([math]::Round($createRate,1))"
if ($ids.Count -eq 0) { Write-Host "❌ 创建失败，终止" -ForegroundColor Red; exit 1 }

# ---------- 2. 基线指标 ----------
Write-Step "2. 基线指标"
$dispatchBefore = Get-Metric -Port 9090 -Name "dolphin_scheduler_dispatch_total"
Write-Host "  dispatch_total 基线 = $dispatchBefore"

# ---------- 3. 同时触发全部（trigger-batch 单进程突发） ----------
Write-Step "3. 同时触发 $($ids.Count) 个任务"
$startTrig = Get-Date
& "$Bins\dolphinctl.exe" --addr "localhost:50051" task trigger-batch --ids ($ids -join ",") 2>&1
$trigSec = ((Get-Date) - $startTrig).TotalSeconds
Write-Host "  全部触发完成，耗时 $([math]::Round($trigSec,2))s"
Add-Content $OutFile "trigger_span_sec=$([math]::Round($trigSec,2))"

# ---------- 4. 峰值监控 + 等待完成 ----------
Write-Step "4. 监控并发峰值直到全部执行完成（最多 $WaitMaxSec s）"
$peakExec = 0; $peakUtil = 0.0; $peakQueue = 0
$done = $false
$dispatchDoneAt = $null   # 全部任务下发完成的时刻（用于算真实分发速率）
$pollStart = Get-Date
for ($i = 0; $i -lt $WaitMaxSec * 2; $i++) {
    $e = Get-Metric -Port 9091 -Name "dolphin_worker_tasks_executing"
    if ($e -gt $peakExec) { $peakExec = $e }
    $u = Get-Metric -Port 9091 -Name "dolphin_worker_pool_capacity_utilization"
    if ($u -gt $peakUtil) { $peakUtil = $u }
    $q = Get-Metric -Port 9090 -Name "dolphin_scheduler_queue_depth"
    if ($q -gt $peakQueue) { $peakQueue = $q }

    $dispatchAfter = Get-Metric -Port 9090 -Name "dolphin_scheduler_dispatch_total"
    if ($dispatchAfter - $dispatchBefore -ge $ids.Count -and $null -eq $dispatchDoneAt) {
        $dispatchDoneAt = Get-Date
    }

    $term = SqlScalar "SELECT COUNT(*) FROM dolphin.task_logs WHERE status IN ('success','failed','timeout');"
    if ([int]$term -ge $ids.Count) { $done = $true; break }
    $elapsedSec = ((Get-Date) - $pollStart).TotalSeconds
    if ($i % 2 -eq 0) { Write-Host ("  [{0}s] 已完成 {1}/{2}  执行中峰值={3}" -f [math]::Round($elapsedSec), $term, $ids.Count, $peakExec) }
    Start-Sleep -Milliseconds 500
}
if ($null -eq $dispatchDoneAt) { $dispatchDoneAt = Get-Date }
$dispatchDelta = $dispatchAfter - $dispatchBefore

Write-Host "  峰值并发执行 = $peakExec"
Write-Host "  峰值池利用率 = $([math]::Round($peakUtil*100,1))%"
Write-Host "  峰值队列深度 = $peakQueue"
if (-not $done) { Write-Host "  ⚠️ 超时未全部完成" -ForegroundColor Yellow }
Add-Content $OutFile "peak_executing=$peakExec"
Add-Content $OutFile "peak_pool_utilization=$([math]::Round($peakUtil*100,1))"
Add-Content $OutFile "peak_queue_depth=$peakQueue"
Add-Content $OutFile "dispatch_delta=$dispatchDelta"

# ---------- 5. 执行结果统计（task_logs） ----------
Write-Step "5. 执行统计"
$rows = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT status, TIMESTAMPDIFF(MICROSECOND, start_time, COALESCE(end_time,start_time))/1000000.0 FROM dolphin.task_logs;" 2>$null
$success = 0; $failed = 0; $timeout = 0; $running = 0
$durs = @()
foreach ($r in $rows) {
    if (-not $r) { continue }
    $p = $r -split "`t"
    $st = $p[0].Trim(); $dur = [double]$p[1]
    switch ($st) {
        "success" { $success++; $durs += $dur }
        "failed"  { $failed++ }
        "timeout" { $timeout++ }
        default   { $running++ }
    }
}
Write-Host "  success=$success failed=$failed timeout=$timeout running=$running"
$successRate = if ($rows.Count -gt 0) { [math]::Round(100.0 * $success / $rows.Count, 2) } else { 0 }
Add-Content $OutFile "success=$success failed=$failed timeout=$timeout running=$running success_rate=$successRate"

if ($durs.Count -gt 0) {
    $sorted = $durs | Sort-Object
    $n = $sorted.Count
    $mean = ($sorted | Measure-Object -Average).Average
    $p50 = $sorted[[math]::Floor($n*0.50)]
    $p95 = $sorted[[math]::Min($n-1, [math]::Floor($n*0.95))]
    $p99 = $sorted[[math]::Min($n-1, [math]::Floor($n*0.99))]
    Write-Host ("  执行耗时(s): mean={0:N2} p50={1:N2} p95={2:N2} p99={3:N2} (样本 {4})" -f $mean, $p50, $p95, $p99, $n)
    Add-Content $OutFile ("exec_dur_mean_s={0:N2} p50={1:N2} p95={2:N2} p99={3:N2} n={4}" -f $mean, $p50, $p95, $p99, $n)
}

# 分发速率：从触发开始到全部下发完成（不含执行耗时，反映调度器真实吞吐）
$dispatchSpan = ($dispatchDoneAt - $startTrig).TotalSeconds
$dispatchRate = if ($dispatchSpan -gt 0 -and $dispatchDelta -gt 0) { [math]::Round($dispatchDelta / $dispatchSpan, 1) } else { 0 }
Write-Host "  调度分发速率 = $dispatchRate tasks/sec（$dispatchDelta 次分发 / $([math]::Round($dispatchSpan,2))s）"
Add-Content $OutFile "dispatch_rate=$dispatchRate"

# ---------- 6. 汇总 ----------
Write-Step "结果已写入 $OutFile"
Get-Content $OutFile
