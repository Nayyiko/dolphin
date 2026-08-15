# retry_test.ps1 — 执行可靠性增强验证脚本
#
# 回答面试问题："不用 MQ 这类中间件，会不会丢消息？"
# 本脚本给出四个可复现的硬数据证据，验证执行可靠性三件套：
#   A. 执行级重试：失败 → 指数退避自动重试 → 重试耗尽标记 RetriesExhausted
#   B. 重试后最终成功：前 N 次失败、之后成功（验证重试不是"无限重试"）
#   C. 池满背压自动重试：超出 worker 在途上限的任务不再等下一个 cron，
#      自动重试直到成功（对比之前：只能等 cron 或手动触发）
#   D. stale-running 兜底救援：结果上报丢失/worker 挂死 → 扫描器救援 + 重试
#
# 用法:
#   powershell -ExecutionPolicy Bypass -File hack\retry_test.ps1            # 全跑
#   powershell -ExecutionPolicy Bypass -File hack\retry_test.ps1 -SkipPoolFull  # 跳过最耗时的 C
#
# 前置（与 schedule_concurrency_test.ps1 相同）:
#   docker compose -f deployments/docker-compose.yaml up -d etcd mysql redis
#   go build -o bin\scheduler.exe ./cmd/scheduler && go build -o bin\worker.exe ./cmd/worker && go build -o bin\dolphinctl.exe ./cmd/dolphinctl
#   start-dev.ps1 启动 1 个 scheduler（metrics 9090 / grpc 50051）+ 1 个 worker（metrics 9091）
#
# 输出: results\retry_report.txt

param(
    [switch]$SkipPoolFull,      # 跳过场景 C（池满自动重试）
    [int]$WaitMaxSec = 90       # 单场景最长等待
)

$ErrorActionPreference = "Continue"
$ProjectDir = "C:\Users\30641\Desktop\dolphin"
$Bins = "$ProjectDir\bin"
$Results = "$ProjectDir\results"
New-Item -ItemType Directory -Force -Path $Results | Out-Null
$OutFile = "$Results\retry_report.txt"
"" | Set-Content $OutFile

$SCHED = "localhost:50051"   # gRPC
$M9090 = 9090                # scheduler metrics
$M9091 = 9091                # worker metrics

function Write-Step { param($msg) Write-Host "`n══════════ $msg ══════════" -ForegroundColor Green; Add-Content $OutFile "`n== $msg ==" }
function Write-Result { param($ok, $msg) if ($ok) { Write-Host "  ✅ $msg" -ForegroundColor Green; Add-Content $OutFile "PASS: $msg" } else { Write-Host "  ❌ $msg" -ForegroundColor Red; Add-Content $OutFile "FAIL: $msg" } }

# 读无标签指标（只匹配"指标名 + 空格 + 值"的采样行，避开 # HELP/# TYPE）
function Get-Metric {
    param([int]$Port, [string]$Name)
    try {
        $m = (Invoke-WebRequest -Uri "http://localhost:$Port/metrics" -UseBasicParsing -TimeoutSec 3).Content
        $line = ($m -split "`n") | Where-Object { $_ -match "^$Name " } | Select-Object -First 1
        if ($line) { return [double](($line -split " ")[1]) }
        return 0
    } catch { return -1 }
}

# 读带 result 标签的指标，如 dolphin_scheduler_retry_total{result="scheduled"}
function Get-RetryMetric {
    param([int]$Port, [string]$Result)
    try {
        $m = (Invoke-WebRequest -Uri "http://localhost:$Port/metrics" -UseBasicParsing -TimeoutSec 3).Content
        $pat = '^dolphin_scheduler_retry_total{result="' + $Result + '"} '
        $line = ($m -split "`n") | Where-Object { $_ -match $pat } | Select-Object -First 1
        if ($line) { return [double](($line -split " ")[1]) }
        return 0
    } catch { return -1 }
}

# 读带 handler_type/result 标签的下发计数器（CounterVec），跨 result 求和。
# 不能用 Get-Metric：它只匹配"指标名 + 空格"的采样行，而标签化行长这样：
#   dolphin_scheduler_dispatch_total{handler_type="http",result="success"} 170
# 裸指标名行不存在 → 之前"下发增量 = 0/170"是脚本读取 bug，不是真实下发为零。
function Get-DispatchMetric {
    param([int]$Port)
    try {
        $m = (Invoke-WebRequest -Uri "http://localhost:$Port/metrics" -UseBasicParsing -TimeoutSec 3).Content
        $total = 0.0
        $lines = $m -split "`n" | Where-Object { $_ -match '^dolphin_scheduler_dispatch_total\{' }
        foreach ($l in $lines) { $total += [double](($l -split " ")[1]) }
        return $total
    } catch { return -1 }
}

# MySQL 单值查询（去掉末尾换行）
function SqlScalar {
    param([string]$Q)
    $r = docker exec dolphin-mysql mysql -u root -pdolphin -N -e $Q 2>$null
    if ($null -eq $r) { return "" }
    return $r.ToString().Trim()
}

# 一次性抓取某端口的 metrics 全文（轮询循环里多次取指标时避免重复 Invoke-WebRequest，
# Windows 上每次 IWR ~200-300ms，4 次/轮会把 180 轮轮询撑到 ~240s，掩盖真实耗时）。
function Get-MetricsRaw {
    param([int]$Port)
    try { return (Invoke-WebRequest -Uri "http://localhost:$Port/metrics" -UseBasicParsing -TimeoutSec 3).Content }
    catch { return "" }
}

# 从 metrics 全文中解析无标签指标值（指标名 + 空格 + 值）。
function MetricValue {
    param([string]$Raw, [string]$Name)
    $line = ($Raw -split "`n") | Where-Object { $_ -match "^$Name " } | Select-Object -First 1
    if ($line) { return [double](($line -split " ")[1]) }
    return 0
}

# MySQL 多行查询（每行按 Tab 拆列）
function SqlRows {
    param([string]$Q)
    $r = docker exec dolphin-mysql mysql -u root -pdolphin -N -e $Q 2>$null
    return $r
}

function Wait-Healthz($port, $maxSec) {
    for ($i = 0; $i -lt $maxSec; $i++) {
        try { $null = Invoke-WebRequest -Uri "http://localhost:$port/healthz" -UseBasicParsing -TimeoutSec 2; return $true } catch {}
        Start-Sleep 1
    }
    return $false
}

# 创建任务并返回 id（name 唯一）
function New-Task {
    param([string]$Name, [string]$Handler, [int]$Timeout, [int]$Retries)
    & "$Bins\dolphinctl.exe" --addr $SCHED task create --name $Name --cron "0 0 1 1 *" --handler $Handler --timeout $Timeout --retries $Retries 2>&1 | Out-Null
    return (SqlScalar "SELECT id FROM dolphin.tasks WHERE name='$Name' LIMIT 1;")
}

# ---------- 0. 前置检查 ----------
Write-Step "0. 前置检查"
if (-not (Wait-Healthz $M9090 5)) { Write-Host "❌ scheduler 未就绪（9090 无 /healthz）" -ForegroundColor Red; exit 1 }
if (-not (Wait-Healthz $M9091 5)) { Write-Host "❌ worker 未就绪（9091 无 /healthz）" -ForegroundColor Red; exit 1 }

# 版本自检：/debug/fail 是新代码才有的调试端点。旧 scheduler.exe 无此路由 → 404，
# 会让场景 A/B 拿到 "http 404" 而非重试链路（重试逻辑也在旧二进制里没有）。
# 这里快速失败给出明确提示，而不是跑出一堆 404 假数据。
try {
    $null = Invoke-WebRequest -Uri "http://localhost:9090/debug/fail" -UseBasicParsing -TimeoutSec 3
    Write-Host "  ⚠️ /debug/fail 返回 200（预期失败），行为异常，继续但请留意" -ForegroundColor Yellow
} catch {
    $probeCode = 0
    try { $probeCode = [int]$_.Exception.Response.StatusCode } catch {}
    if ($probeCode -eq 404) {
        Write-Host "  ❌ /debug/fail 返回 404 —— scheduler.exe 是旧版本，未包含可靠性增强代码。" -ForegroundColor Red
        Write-Host "  请先重新编译并重启 scheduler：" -ForegroundColor Yellow
        Write-Host "    go build -o bin\scheduler.exe ./cmd/scheduler" -ForegroundColor Yellow
        Write-Host "  （建议同时重编 worker/dolphinctl，随后重跑本脚本）" -ForegroundColor Yellow
        exit 1
    }
    # 500 是预期响应（新代码合成失败），继续。
}

Write-Step "0. 清理旧数据"
docker exec dolphin-mysql mysql -u root -pdolphin -e "USE dolphin; DELETE FROM task_conditions; DELETE FROM task_logs; DELETE FROM tasks;" 2>$null
Write-Host "已清理 tasks / task_logs / task_conditions"
# 重置 /debug/fail 失败计数器（进程内累计，不清会串轮次：
# 场景 B 用 ?times=2 语义，上一轮已消耗 2 次失败，重跑首请求就直接 recovered）
try { $null = Invoke-WebRequest -Uri "http://localhost:9090/debug/reset-fail" -UseBasicParsing -TimeoutSec 3 } catch { Write-Host "  ⚠️ /debug/reset-fail 调用失败（旧 scheduler？）" -ForegroundColor Yellow }
Write-Host "已重置 /debug/fail 计数器（本轮失败编号从 #1 起）"

# ---------- 场景 A：失败 → 自动重试 → 耗尽 ----------
Write-Step "A. 执行级重试：持续失败 → 指数退避重试 → RetriesExhausted"
$retryBase = Get-RetryMetric $M9090 "scheduled"
$exhBase = Get-RetryMetric $M9090 "exhausted"
$idA = New-Task "rt-A" "http://localhost:9090/debug/fail" 30 2
if (-not $idA) { Write-Host "❌ rt-A 创建失败" -ForegroundColor Red; exit 1 }
& "$Bins\dolphinctl.exe" --addr $SCHED task trigger --id $idA | Out-Null
Write-Host "  rt-A id=$idA 已触发（max_retries=2，期望 3 次尝试全部失败后耗尽）"

$termA = 0
for ($i = 0; $i -lt $WaitMaxSec * 2; $i++) {
    $termA = [int](SqlScalar "SELECT COUNT(*) FROM dolphin.task_logs WHERE task_id='$idA' AND status IN ('success','failed','timeout');")
    if ($termA -ge 3) { break }
    Start-Sleep -Milliseconds 500
}
Start-Sleep 1  # 等耗尽条件落库
$logsA = SqlRows "SELECT retry_count, status, error_msg FROM dolphin.task_logs WHERE task_id='$idA' ORDER BY created_at ASC;"
Write-Host "  task_logs (retry_count,status,error):"
foreach ($l in $logsA) { if ($l) { Write-Host "    $($l -replace "`t", "  ")"; Add-Content $OutFile "  log: $($l -replace "`t", "  ")" } }
$condA = SqlScalar "SELECT CONCAT(status,'/',reason) FROM dolphin.task_conditions WHERE task_id='$idA' AND type='Retries';"
Write-Host "  Condition Retries = $condA"

$retryDeltaA = (Get-RetryMetric $M9090 "scheduled") - $retryBase
$exhDeltaA = (Get-RetryMetric $M9090 "exhausted") - $exhBase
Write-Result ($termA -ge 3) "共 3 次尝试（首次 + 2 次重试）"
Write-Result ($retryDeltaA -ge 2) "retry_total{result=scheduled} 增量 = $retryDeltaA（期望 >= 2）"
Write-Result ($exhDeltaA -ge 1) "retry_total{result=exhausted} 增量 = $exhDeltaA（期望 >= 1）"
Write-Result ($condA -match "False/RetriesExhausted") "Condition Retries = $condA（期望 False/RetriesExhausted）"

# ---------- 场景 B：前 N 次失败、之后成功 ----------
Write-Step "B. 执行级重试：前 2 次失败、之后成功（重试是『有限且能恢复』的）"
$retryBaseB = Get-RetryMetric $M9090 "scheduled"
$idB = New-Task "rt-B" "http://localhost:9090/debug/fail?times=2" 30 3
if (-not $idB) { Write-Host "❌ rt-B 创建失败" -ForegroundColor Red; exit 1 }
& "$Bins\dolphinctl.exe" --addr $SCHED task trigger --id $idB | Out-Null
Write-Host "  rt-B id=$idB 已触发（times=2：前两次失败，第 3 次成功）"

$termB = 0
for ($i = 0; $i -lt $WaitMaxSec * 2; $i++) {
    $termB = [int](SqlScalar "SELECT COUNT(*) FROM dolphin.task_logs WHERE task_id='$idB' AND status IN ('success','failed','timeout');")
    if ($termB -ge 3) { break }
    Start-Sleep -Milliseconds 500
}
$logsB = SqlRows "SELECT retry_count, status FROM dolphin.task_logs WHERE task_id='$idB' ORDER BY created_at ASC;"
foreach ($l in $logsB) { if ($l) { Write-Host "    $($l -replace "`t", "  ")"; Add-Content $OutFile "  log: $($l -replace "`t", "  ")" } }
$succB = [int](SqlScalar "SELECT COUNT(*) FROM dolphin.task_logs WHERE task_id='$idB' AND status='success';")
$exhB = [int](SqlScalar "SELECT COUNT(*) FROM dolphin.task_conditions WHERE task_id='$idB' AND type='Retries';")
$retryDeltaB = (Get-RetryMetric $M9090 "scheduled") - $retryBaseB
Write-Result ($termB -ge 3 -and $succB -eq 1) "3 次尝试，最终 1 次成功"
Write-Result ($exhB -eq 0) "未触发 RetriesExhausted（期望 0 条耗尽条件）"
Write-Result ($retryDeltaB -ge 1) "retry_total{result=scheduled} 增量 = $retryDeltaB（期望 >= 1）"

# ---------- 场景 C：池满背压 → 自动重试直到成功 ----------
if (-not $SkipPoolFull) {
    Write-Step "C. 池满背压自动重试：170 任务 > 在途上限 150，超出部分自动重试直到成功"
    $retryBaseC = Get-RetryMetric $M9090 "scheduled"
    $retryDispBaseC = Get-RetryMetric $M9090 "dispatched"
    $retryFailDispBaseC = Get-RetryMetric $M9090 "dispatch_failed"
    $dispatchBaseC = Get-DispatchMetric $M9090
    $poolRejBaseC = Get-Metric $M9091 "dolphin_worker_pool_rejected_total"
    $staleBaseC = Get-Metric $M9090 "dolphin_scheduler_stale_running_rescued_total"
    $startC = Get-Date
    & "$Bins\dolphinctl.exe" --addr $SCHED stress create --count 170 --prefix "rt-C" --cron "0 0 1 1 *" --handler "http://localhost:9090/debug/sleep?seconds=2" --timeout 10 --retries 3 2>&1 | Out-Null
    # 收集 id：不能用 GROUP_CONCAT（默认 group_concat_max_len=1024，170 个 UUID
    # ~6300 字符会被截断，导致只触发前 ~27 个，多任务场景全变假）。逐行取再 join。
    $idsC = ((SqlRows "SELECT id FROM dolphin.tasks WHERE name LIKE 'rt-C-%';") -join ",")
    Write-Host "  创建 170 个 sleep(2s) 任务，触发全部（实际 id 数 = $(($idsC -split ',').Count)）"
    $trigOut = & "$Bins\dolphinctl.exe" --addr $SCHED task trigger-batch --ids $idsC 2>&1
    Write-Host "  trigger-batch: $($trigOut -join ' ')"

    $okC = 0
    $peakExecC = 0
    $peakQueueC = 0
    $peakInflightC = 0
    $peakUtilC = 0
    $peakWorkersC = 0
    for ($i = 0; $i -lt $WaitMaxSec * 2; $i++) {
        $okC = [int](SqlScalar "SELECT COUNT(DISTINCT l.task_id) FROM dolphin.task_logs l JOIN tasks t ON t.id=l.task_id WHERE t.name LIKE 'rt-C-%' AND l.status='success';")
        # 每个端口只抓一次 metrics，再本地解析各指标（省 3 次 IWR/轮）
        $raw9091 = Get-MetricsRaw $M9091
        $raw9090 = Get-MetricsRaw $M9090
        $eC = MetricValue $raw9091 "dolphin_worker_tasks_executing"
        if ($eC -gt $peakExecC) { $peakExecC = $eC }
        # 调度 WorkQueue 深度：dolphin_scheduler_queue_depth 现在由 Reconciler 真实更新
        $qC = MetricValue $raw9090 "dolphin_scheduler_queue_depth"
        if ($qC -gt $peakQueueC) { $peakQueueC = $qC }
        # worker 池在途数（排队+执行）与利用率：inflight ≈ executing → 下发是涓流，
        # 瓶颈在调度器；inflight 远大于 executing → 任务堵在 worker 队列（突发下发、worker 消化慢）。
        $iC = MetricValue $raw9091 "dolphin_worker_pool_inflight"
        if ($iC -gt $peakInflightC) { $peakInflightC = $iC }
        $uC = MetricValue $raw9091 "dolphin_worker_pool_capacity_utilization"
        if ($uC -gt $peakUtilC) { $peakUtilC = $uC }
        # 在线 worker 数：>1 说明有残留 worker 进程分流了任务（背压打不满的真凶之一）
        $wC = MetricValue $raw9090 "dolphin_scheduler_workers_online"
        if ($wC -gt $peakWorkersC) { $peakWorkersC = $wC }
        if ($okC -ge 170) { break }
        Start-Sleep -Milliseconds 500
    }
    $elapsedC = [math]::Round(((Get-Date) - $startC).TotalSeconds, 1)
    $multiC = [int](SqlScalar "SELECT COUNT(*) FROM tasks t WHERE t.name LIKE 'rt-C-%' AND (SELECT COUNT(*) FROM task_logs WHERE task_id=t.id) > 1;")
    $retryDeltaC = (Get-RetryMetric $M9090 "scheduled") - $retryBaseC
    $retryDispDeltaC = (Get-RetryMetric $M9090 "dispatched") - $retryDispBaseC
    $retryFailDispDeltaC = (Get-RetryMetric $M9090 "dispatch_failed") - $retryFailDispBaseC
    $dispatchDeltaC = (Get-DispatchMetric $M9090) - $dispatchBaseC
    $poolRejDeltaC = (Get-Metric $M9091 "dolphin_worker_pool_rejected_total") - $poolRejBaseC
    $staleDeltaC = (Get-Metric $M9090 "dolphin_scheduler_stale_running_rescued_total") - $staleBaseC
    # 没有任何 task_log 的任务数：reconcile 没下发成功（之前 workqueue Done 泄漏时
    # reconcile 出错会让任务永久卡死、连日志都不建），应为 0。
    $noLogC = [int](SqlScalar "SELECT COUNT(*) FROM tasks t WHERE t.name LIKE 'rt-C-%' AND NOT EXISTS (SELECT 1 FROM task_logs l WHERE l.task_id = t.id);")
    # 失败的任务（最新日志非 success）应为 0
    $notDoneC = [int](SqlScalar "SELECT COUNT(*) FROM tasks t WHERE t.name LIKE 'rt-C-%' AND (SELECT status FROM task_logs WHERE task_id=t.id ORDER BY created_at DESC LIMIT 1) <> 'success';")
    # 失败/超时日志总数 + 重试执行日志数（retry_count>=1）
    $failedLogC = [int](SqlScalar "SELECT COUNT(*) FROM dolphin.task_logs l JOIN tasks t ON t.id=l.task_id WHERE t.name LIKE 'rt-C-%' AND l.status IN ('failed','timeout');")
    $retryLogC = [int](SqlScalar "SELECT COUNT(*) FROM dolphin.task_logs l JOIN tasks t ON t.id=l.task_id WHERE t.name LIKE 'rt-C-%' AND l.retry_count >= 1;")
    # ── 关键判据：下发时间跨度（首次 vs 末次 task_log 创建时间差）。
    #    spread ~= 1s → 170 个任务几乎同时下发（突发）→ worker 侧瓶颈/池满背压应触发；
    #    spread ~= elapsed → 下发本身涓流（调度器侧瓶颈），这就是"池满背压够不到"的直接证据。
    $spreadC = [int](SqlScalar "SELECT COALESCE(TIMESTAMPDIFF(SECOND, MIN(start_time), MAX(start_time)), 0) FROM dolphin.task_logs l JOIN tasks t ON t.id=l.task_id WHERE t.name LIKE 'rt-C-%';")
    # ── 决定性判据：每个任务真实执行时长（end_time - start_time）。
    #    avg_dur ~= 2s → 执行正常，243s 是轮询循环慢/多 worker 分流的假象；
    #    avg_dur 几十秒 → 执行本身被拖慢（sleep 端点被争抢/worker 串行），才是真瓶颈。
    $avgDurC = [int](SqlScalar "SELECT COALESCE(ROUND(AVG(TIMESTAMPDIFF(MILLISECOND, start_time, end_time))/1000, 2), -1) FROM dolphin.task_logs l JOIN tasks t ON t.id=l.task_id WHERE t.name LIKE 'rt-C-%' AND l.end_time IS NOT NULL;")
    $maxDurC = [int](SqlScalar "SELECT COALESCE(ROUND(MAX(TIMESTAMPDIFF(MILLISECOND, start_time, end_time))/1000, 2), -1) FROM dolphin.task_logs l JOIN tasks t ON t.id=l.task_id WHERE t.name LIKE 'rt-C-%' AND l.end_time IS NOT NULL;")
    # 结果落库跨度：首个到最后一个结果写入的时差（真实执行时间线，而非下发时间线）
    $endSpanC = [int](SqlScalar "SELECT COALESCE(TIMESTAMPDIFF(SECOND, MIN(end_time), MAX(end_time)), 0) FROM dolphin.task_logs l JOIN tasks t ON t.id=l.task_id WHERE t.name LIKE 'rt-C-%' AND l.end_time IS NOT NULL;")
    # 有几个 worker 实际执行了任务（>1 = 残留 worker 进程分流）
    $distinctWorkerC = [int](SqlScalar "SELECT COUNT(DISTINCT worker_id) FROM dolphin.task_logs l JOIN tasks t ON t.id=l.task_id WHERE t.name LIKE 'rt-C-%';")
    # reconcile 平均耗时：dolphin_scheduler_reconcile_duration_seconds 直方图的 sum/count。
    # 注意是全进程累计（含 A/B/D），但场景 C 占大头；若平均 ~1s+ → DB 查询慢是主因。
    $reconcileSumC = Get-Metric $M9090 "dolphin_scheduler_reconcile_duration_seconds_sum"
    $reconcileCountC = Get-Metric $M9090 "dolphin_scheduler_reconcile_duration_seconds_count"
    $reconcileAvgC = "n/a"
    if ($reconcileCountC -gt 0) { $reconcileAvgC = [math]::Round($reconcileSumC / $reconcileCountC, 3) }
    # dispatch 直方图：dolphin_scheduler_dispatch_latency_seconds（stream.Send 耗时）。
    $dispLatSumC = Get-Metric $M9090 "dolphin_scheduler_dispatch_latency_seconds_sum"
    $dispLatCountC = Get-Metric $M9090 "dolphin_scheduler_dispatch_latency_seconds_count"
    $dispLatAvgC = "n/a"
    if ($dispLatCountC -gt 0) { $dispLatAvgC = [math]::Round($dispLatSumC / $dispLatCountC, 4) }
    $peakQueuedC = $peakInflightC - $peakExecC
    $dispatchRateC = "n/a"
    if ($elapsedC -gt 0) { $dispatchRateC = [math]::Round($dispatchDeltaC / $elapsedC, 2) }
    Write-Host "  全部成功用时 ${elapsedC}s；多日志任务数 = $multiC；重试执行日志 = $retryLogC"
    Write-Host "  诊断：下发计数增量 = $dispatchDeltaC（速率 ${dispatchRateC}/s）；worker 执行峰值 = $peakExecC；worker 在途峰值 = $peakInflightC（排队峰值 ≈ $peakQueuedC）；池利用率峰值 = $peakUtilC；调度队列峰值 = $peakQueueC；在线 worker = $peakWorkersC；无日志任务 = $noLogC"
    Write-Host "  时间线：下发跨度 = ${spreadC}s；结果落库跨度 = ${endSpanC}s；任务执行时长 平均=${avgDurC}s 最大=${maxDurC}s；reconcile 平均 = ${reconcileAvgC}s；dispatch Send 平均 = ${dispLatAvgC}s；实际执行 worker 数 = $distinctWorkerC"
    Add-Content $OutFile "  C 时间线: elapsed=${elapsedC}s spread=${spreadC}s end_span=${endSpanC}s task_dur_avg=${avgDurC}s task_dur_max=${maxDurC}s reconcile_avg=${reconcileAvgC}s dispatch_send_avg=${dispLatAvgC}s distinct_workers=$distinctWorkerC"
    Add-Content $OutFile "  C 诊断: dispatch=$dispatchDeltaC(${dispatchRateC}/s) peak_exec=$peakExecC peak_inflight=$peakInflightC peak_queued≈$peakQueuedC peak_util=$peakUtilC peak_queue_depth=$peakQueueC online_workers=$peakWorkersC noLog=$noLogC"
    Add-Content $OutFile "  C 背压: pool_rejected=$poolRejDeltaC retry_scheduled=$retryDeltaC retry_dispatched=$retryDispDeltaC retry_dispatch_failed=$retryFailDispDeltaC stale=$staleDeltaC multi=$multiC"
    Write-Host "  背压：池满拒绝增量 = $poolRejDeltaC；retry_total scheduled=$retryDeltaC dispatched=$retryDispDeltaC dispatch_failed=$retryFailDispDeltaC；stale 救援 = $staleDeltaC"
    Write-Host "  失败/超时日志 = $failedLogC，按 error_msg 分组："
    $errRows = SqlRows "SELECT l.error_msg, COUNT(*) FROM dolphin.task_logs l JOIN tasks t ON t.id=l.task_id WHERE t.name LIKE 'rt-C-%' AND l.status IN ('failed','timeout') GROUP BY l.error_msg;"
    foreach ($er in $errRows) { if ($er) { Write-Host "    $($er -replace "`t", "  ")"; Add-Content $OutFile "  err: $($er -replace "`t", "  ")" } }
    Write-Result ($okC -ge 170) "170 个任务全部最终 success"
    Write-Result ($notDoneC -eq 0) "最新日志全部为 success（无残留失败）"
    Write-Result ($noLogC -eq 0) "无日志任务 = $noLogC（期望 0，无任务卡死在调度队列）"
    Write-Result ($poolRejDeltaC -ge 1) "池满拒绝增量 = $poolRejDeltaC（期望 >= 1，背压真实发生）"
    Write-Result ($retryDeltaC -ge $poolRejDeltaC) "retry_total{scheduled} 增量 = $retryDeltaC（期望 >= 拒绝数 $poolRejDeltaC，每个被拒任务都安排重试）"
    Write-Result ($retryDispDeltaC -ge $poolRejDeltaC) "retry_total{dispatched} 增量 = $retryDispDeltaC（期望 >= 拒绝数 $poolRejDeltaC，重试真正下发执行）"
    Write-Result ($multiC -ge $poolRejDeltaC) "$multiC 个任务有过重试日志（期望 >= 拒绝数 $poolRejDeltaC）"
} else {
    Write-Step "C. 池满背压自动重试（已跳过 -SkipPoolFull）"
    Add-Content $OutFile "SKIP: pool full scenario"
}

# ---------- 场景 D：stale-running 兜底救援 ----------
Write-Step "D. stale-running 兜底救援：结果上报丢失 → 扫描器标记失败并重试"
$rescueBase = Get-Metric $M9090 "dolphin_scheduler_stale_running_rescued_total"
$idD = New-Task "rt-D" "http://localhost:9090/debug/sleep?seconds=1" 30 2
if (-not $idD) { Write-Host "❌ rt-D 创建失败" -ForegroundColor Red; exit 1 }
# 手工插入一条"结果上报丢失"的 running 日志：start_time 已超 timeout(30)+grace(30)+1s
docker exec dolphin-mysql mysql -u root -pdolphin -e "USE dolphin; INSERT INTO task_logs (id, task_id, instance_id, worker_id, status, start_time, created_at, retry_count) VALUES ('stale-0001', '$idD', 'stale-inst-0001', 'ghost-worker', 'running', NOW() - INTERVAL 61 SECOND, NOW() - INTERVAL 61 SECOND, 0);" 2>$null
Write-Host "  已插入 fake running 日志（start_time = now - 61s > timeout 30 + grace 30），等扫描器（5s 周期）救援"

$rescuedD = 0
$fakeFailed = 0
$retryLogD = 0
$succD = 0
for ($i = 0; $i -lt $WaitMaxSec * 2; $i++) {
    $rescuedD = [int]((Get-Metric $M9090 "dolphin_scheduler_stale_running_rescued_total") - $rescueBase)
    $fakeFailed = [int](SqlScalar "SELECT COUNT(*) FROM dolphin.task_logs WHERE id='stale-0001' AND status='failed' AND error_msg LIKE 'stale running%';")
    $retryLogD = [int](SqlScalar "SELECT COUNT(*) FROM dolphin.task_logs WHERE task_id='$idD' AND retry_count >= 1;")
    $succD = [int](SqlScalar "SELECT COUNT(*) FROM dolphin.task_logs WHERE task_id='$idD' AND status='success';")
    # 必须等 success 也落库（重试日志刚建时仍是 running，光等 retryLogD>=1 会过早 break）
    if ($rescuedD -ge 1 -and $fakeFailed -ge 1 -and $retryLogD -ge 1 -and $succD -ge 1) { break }
    Start-Sleep -Milliseconds 500
}
$fakeErrD = SqlScalar "SELECT error_msg FROM dolphin.task_logs WHERE id='stale-0001';"
Write-Host "  救援计数增量 = $rescuedD；fake log = failed($fakeFailed)；重试日志 = $retryLogD；成功 = $succD"
Write-Host "  fake log error: $fakeErrD"
Write-Result ($rescuedD -ge 1) "stale_running_rescued_total 增量 = $rescuedD（期望 >= 1）"
Write-Result ($fakeFailed -ge 1) "幻影 running 日志被置为 failed"
Write-Result ($retryLogD -ge 1 -and $succD -ge 1) "救援后自动重试并成功（重试日志 $retryLogD，成功 $succD）"

# ---------- 汇总 ----------
Write-Step "结果已写入 $OutFile"
Get-Content $OutFile
