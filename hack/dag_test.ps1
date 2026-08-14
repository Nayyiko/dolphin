# dag_test.ps1 — DAG 依赖编排验证脚本
#
# 验证三个可测行为（每个 = 面试硬数据）：
#   1. 环检测：让任务依赖关系成环 → 创建/更新被拒绝（InvalidArgument）+ 指标计数
#   2. 依赖门控 + 事件驱动：a(慢) → b → c 链式执行，下游在上游完成后才启动；
#      且 b.start - a.end 应在毫秒级（事件推送唤醒，而非等 1s 扫描器）
#   3. 新鲜度语义：链式完成后单独触发 b（a 没有新 run）→ b 保持阻塞，
#      证明依赖不会串到上一次运行的旧结果
#
# 用法:
#   powershell -ExecutionPolicy Bypass -File hack\dag_test.ps1            # 默认 SleepSeconds=4
#   powershell -ExecutionPolicy Bypass -File hack\dag_test.ps1 -SleepSeconds 8
#
# 前置:
#   docker compose -f deployments/docker-compose.yaml up -d etcd mysql redis
#   go build -o bin\scheduler.exe ./cmd/scheduler && go build -o bin\worker.exe ./cmd/worker && go build -o bin\dolphinctl.exe ./cmd/dolphinctl
#   start-dev.ps1 启动 1 个 scheduler + 1 个 worker（scheduler 需带本次 DAG 代码）
#
# 输出: results\dag_test.txt

param(
    [int]$SleepSeconds = 4,   # 根任务 a 的执行耗时（秒），用于观测下游事件驱动延迟
    [int]$WaitMaxSec = 60
)

$ErrorActionPreference = "Continue"
$ProjectDir = "C:\Users\30641\Desktop\dolphin"
$Bins = "$ProjectDir\bin"
$Results = "$ProjectDir\results"
New-Item -ItemType Directory -Force -Path $Results | Out-Null
$GrpcPort = 50051
$MetricsPort = 9090
$OutFile = "$Results\dag_test.txt"
"" | Set-Content $OutFile

function Write-Step { param($msg) Write-Host "`n══════════ $msg ══════════" -ForegroundColor Green; Add-Content $OutFile "`n== $msg ==" }

# 从 dolphinctl 输出解析任务 id
function Get-TaskId {
    param([string]$Out)
    if ($Out -match 'id=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})') { return $Matches[1] }
    return $null
}

# 读取 scheduler 指标（只匹配"以指标名开头"的采样行）
function Get-Metric {
    param([string]$Name)
    try {
        $m = (Invoke-WebRequest -Uri "http://localhost:$MetricsPort/metrics" -UseBasicParsing -TimeoutSec 3).Content
        $line = ($m -split "`n") | Where-Object { $_ -match "^$Name " } | Select-Object -First 1
        if ($line) { return [double](($line -split " ")[1]) }
        return 0
    } catch { return -1 }
}

# 读取任务最近一条执行日志 (status | start | end)，unix 秒，3 位小数
function Get-LogRow {
    param([string]$TaskId)
    $r = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT status, ROUND(UNIX_TIMESTAMP(start_time),3), ROUND(UNIX_TIMESTAMP(COALESCE(end_time,start_time)),3) FROM dolphin.task_logs WHERE task_id='$TaskId' ORDER BY start_time ASC LIMIT 1;" 2>$null
    if (-not $r) { return $null }
    $p = $r -split "`t"
    if ($p.Count -lt 3) { return $null }
    return @{ status = $p[0].Trim(); start = [double]$p[1].Trim(); end = [double]$p[2].Trim() }
}

# 任务日志数
function Get-LogCount {
    param([string]$TaskId)
    $r = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT COUNT(*) FROM dolphin.task_logs WHERE task_id='$TaskId';" 2>$null
    return ([int]$r)
}

# ---------- 0. 清理 ----------
Write-Step "0. 清理旧任务"
docker exec dolphin-mysql mysql -u root -pdolphin -e "USE dolphin; DELETE FROM task_conditions; DELETE FROM task_logs; DELETE FROM tasks;" 2>$null
Write-Host "已清理 tasks / task_logs / task_conditions"

# ---------- 1. 环检测 ----------
Write-Step "1. 环检测：b 依赖 a，再让 a 依赖 b → 应被拒绝"
$aOut = & "$Bins\dolphinctl.exe" --addr "localhost:$GrpcPort" task create --name dag-test-a --cron "0 0 1 1 *" --handler "http://localhost:$MetricsPort/healthz" 2>&1
$aId = Get-TaskId $aOut
Write-Host "a: $aOut"
$bOut = & "$Bins\dolphinctl.exe" --addr "localhost:$GrpcPort" task create --name dag-test-b --cron "0 0 1 1 *" --handler "http://localhost:$MetricsPort/healthz" --depend-on $aId 2>&1
$bId = Get-TaskId $bOut
Write-Host "b: $bOut"
if (-not $aId -or -not $bId) { Write-Host "❌ 创建失败，终止" -ForegroundColor Red; exit 1 }

$cycleOut = & "$Bins\dolphinctl.exe" --addr "localhost:$GrpcPort" task update --id $aId --name dag-test-a --cron "0 0 1 1 *" --handler "http://localhost:$MetricsPort/healthz" --depend-on $bId 2>&1
Write-Host "update a -> depend b: $cycleOut"
if ($cycleOut -match "cycle") {
    Write-Host "  ✅ 环被拒绝，错误含 cycle" -ForegroundColor Green
    Add-Content $OutFile "cycle rejection: PASS"
} else {
    Write-Host "  ❌ 期望环被拒绝" -ForegroundColor Red
    Add-Content $OutFile "cycle rejection: FAIL"
}
$reject = Get-Metric "dolphin_scheduler_dag_cycle_reject_total"
Write-Host "  dolphin_scheduler_dag_cycle_reject_total = $reject (期望 >= 1)"
Add-Content $OutFile "cycle_reject_total=$reject"

# ---------- 2. 链式 DAG + 阻塞 + 事件驱动 ----------
Write-Step "2. 链式 DAG: a(--sleep $SleepSeconds s--) → b → c"
# a 换成慢端点，制造时间窗口观测 b/c 阻塞与事件驱动延迟
& "$Bins\dolphinctl.exe" --addr "localhost:$GrpcPort" task update --id $aId --name dag-test-a --cron "0 0 1 1 *" --handler "http://localhost:$MetricsPort/debug/sleep?seconds=$SleepSeconds" 2>&1 | Out-Null
$cOut = & "$Bins\dolphinctl.exe" --addr "localhost:$GrpcPort" task create --name dag-test-c --cron "0 0 1 1 *" --handler "http://localhost:$MetricsPort/healthz" --depend-on "$aId,$bId" 2>&1
$cId = Get-TaskId $cOut
Write-Host "c: $cOut"

# 同时触发 a/b/c：b、c 因依赖未满足被门控挂起
& "$Bins\dolphinctl.exe" --addr "localhost:$GrpcPort" task trigger --id $aId 2>&1 | Out-Null
& "$Bins\dolphinctl.exe" --addr "localhost:$GrpcPort" task trigger --id $bId 2>&1 | Out-Null
& "$Bins\dolphinctl.exe" --addr "localhost:$GrpcPort" task trigger --id $cId 2>&1 | Out-Null

# a 执行期间（2s 后），b/c 应处于阻塞状态
Start-Sleep 2
$blocked = Get-Metric "dolphin_scheduler_dag_blocked_tasks"
Write-Host "  a 执行中 blocked_tasks = $blocked (期望 >= 1: b、c 被门控)"
Add-Content $OutFile "blocked_during_a=$blocked"

# 等待 c 成功（链式全部跑完）
$cDone = $false
for ($i = 0; $i -lt $WaitMaxSec; $i++) {
    $st = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT status FROM dolphin.task_logs WHERE task_id='$cId' ORDER BY start_time DESC LIMIT 1;" 2>$null
    if ($st -match "success") { $cDone = $true; break }
    Start-Sleep 1
}
if ($cDone) { Write-Host "  ✅ c 链式执行成功" -ForegroundColor Green } else { Write-Host "  ❌ c 未在超时内成功" -ForegroundColor Red }

$aRow = Get-LogRow $aId
$bRow = Get-LogRow $bId
$cRow = Get-LogRow $cId
Write-Host ("  a: status={0} start={1} end={2}" -f $aRow.status, $aRow.start, $aRow.end)
Write-Host ("  b: status={0} start={1} end={2}" -f $bRow.status, $bRow.start, $bRow.end)
Write-Host ("  c: status={0} start={1} end={2}" -f $cRow.status, $cRow.start, $cRow.end)

$orderOk = ($null -ne $aRow) -and ($null -ne $bRow) -and ($null -ne $cRow) -and
           ($aRow.end -le $bRow.start) -and ($bRow.end -le $cRow.start)
$allOk = $orderOk -and ($aRow.status -eq "success") -and ($bRow.status -eq "success") -and ($cRow.status -eq "success")
if ($orderOk) { Write-Host "  ✅ 执行顺序 a → b → c 正确" -ForegroundColor Green } else { Write-Host "  ❌ 执行顺序错误" -ForegroundColor Red }
if ($allOk) { Write-Host "  ✅ 三个任务全部 success" -ForegroundColor Green } else { Write-Host "  ❌ 存在失败" -ForegroundColor Red }
Add-Content $OutFile ("chain_order={0} a_end={1} b_start={2} b_end={3} c_start={4}" -f $orderOk, $aRow.end, $bRow.start, $bRow.end, $cRow.start)

$bWaitMs = ($bRow.start - $aRow.end) * 1000
$cWaitMs = ($cRow.start - $bRow.end) * 1000
Write-Host ("  b 在 a 完成后 {0:N1} ms 启动（事件驱动延迟，扫描器兜底上限 ~1000ms）" -f $bWaitMs)
Write-Host ("  c 在 b 完成后 {0:N1} ms 启动（事件驱动延迟）" -f $cWaitMs)
Add-Content $OutFile ("b_start_after_a_ms={0:N1}" -f $bWaitMs)
Add-Content $OutFile ("c_start_after_b_ms={0:N1}" -f $cWaitMs)
$eventDriven = ($bWaitMs -lt 500) -and ($cWaitMs -lt 500)
if ($eventDriven) { Write-Host "  ✅ 事件驱动唤醒生效（<500ms）" -ForegroundColor Green } else { Write-Host "  ⚠️ 延迟 >500ms，可能走了 1s 扫描器兜底而非事件推送" -ForegroundColor Yellow }

$gate = Get-Metric "dolphin_scheduler_dag_gate_total"
Write-Host "  dolphin_scheduler_dag_gate_total = $gate (期望 >= 1: b/c 曾被门控)"
Add-Content $OutFile "gate_total=$gate"

# ---------- 3. 新鲜度语义：不串旧结果 ----------
Write-Step "3. 新鲜度：链式完成后单独触发 b（a 无新 run）→ b 应保持阻塞"
$bLogsBefore = Get-LogCount $bId
& "$Bins\dolphinctl.exe" --addr "localhost:$GrpcPort" task trigger --id $bId 2>&1 | Out-Null
Start-Sleep 3
$bLogsAfter = Get-LogCount $bId
Write-Host "  b 日志数: 触发前=$bLogsBefore, 3s 后=$bLogsAfter (期望不变)"
if ($bLogsAfter -eq $bLogsBefore) {
    Write-Host "  ✅ b 未复用 a 的旧成功结果（保持阻塞，无新执行）" -ForegroundColor Green
    Add-Content $OutFile "freshness: PASS"
} else {
    Write-Host "  ❌ b 产生了新执行，依赖串到了旧结果" -ForegroundColor Red
    Add-Content $OutFile "freshness: FAIL"
}
$blocked3 = Get-Metric "dolphin_scheduler_dag_blocked_tasks"
Write-Host "  此时 blocked_tasks = $blocked3 (期望 >= 1: b 被门控)"
Add-Content $OutFile "blocked_after_trigger_b=$blocked3"

# ---------- 汇总 ----------
Write-Step "结果已写入 $OutFile"
Get-Content $OutFile
