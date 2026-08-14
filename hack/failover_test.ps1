# failover_test.ps1 — 故障转移测试脚本（支持重复跑取分布）
#
# 验证两个创新点：
#   1. Leader 故障转移（etcd 选主）：kill Leader，测另一个 scheduler 接管时间
#   2. Worker 故障转移（任务不丢不重）：kill 1 个 worker，测任务在另一个 worker 恢复 + 幂等
#
# 用法:
#   powershell -ExecutionPolicy Bypass -File hack\failover_test.ps1                  # 默认 10 次接管 / 5 次恢复
#   powershell -ExecutionPolicy Bypass -File hack\failover_test.ps1 -Runs 3 -WorkerRuns 2   # 快速冒烟
#
# 前置: docker compose 已启动 etcd/mysql/redis
#       编译最新 scheduler/worker/dolphinctl 到 bin/
#
# 预计耗时: 约 7 分钟（Runs=10 / WorkerRuns=5）

param(
    [int]$Runs = 10,       # Leader 接管重复次数
    [int]$WorkerRuns = 5   # Worker 恢复重复次数
)

$ErrorActionPreference = "Continue"
$ProjectDir = "C:\Users\30641\Desktop\dolphin"
$Bins = "$ProjectDir\bin"
$Results = "$ProjectDir\results"
New-Item -ItemType Directory -Force -Path $Results | Out-Null

# 端口映射：scheduler.yaml -> metrics 9090 / grpc 50051；scheduler2.yaml -> metrics 9092 / grpc 50052
function Get-GrpcPort($metricsPort) { if ($metricsPort -eq 9090) { return 50051 } else { return 50052 } }

function Write-Step { param($msg) Write-Host "`n══════════ $msg ══════════" -ForegroundColor Green }

# 注意：不能用 PowerShell 函数包装 dolphinctl——函数调用会把 "--addr" 当作命名参数解析并吞掉，
# 导致 "unknown command: localhost:50052"。必须直接 & 调用 exe（diagnose.ps1 同款写法）。

# ---------- 进程管理 ----------
function Start-Scheduler($cfg) {
    $null = Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd $ProjectDir; .\bin\scheduler.exe --config configs\$cfg" -WindowStyle Minimized
}
function Start-Worker($cfg) {
    $null = Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd $ProjectDir; .\bin\worker.exe --config configs\$cfg" -WindowStyle Minimized
}
function Get-SchedulerProc($cfg) {
    Get-CimInstance Win32_Process -Filter "Name='scheduler.exe'" | Where-Object { $_.CommandLine -like "*$cfg*" }
}
function Wait-Healthz($port, $maxSec) {
    for ($i = 0; $i -lt $maxSec; $i++) {
        try { $null = Invoke-WebRequest -Uri "http://localhost:$port/healthz" -UseBasicParsing -TimeoutSec 2; return $true } catch {}
        Start-Sleep 1
    }
    return $false
}

# ---------- 指标读取 ----------
# 只匹配"以指标名开头"的采样行，绝不子串匹配整段——Prometheus 的 # HELP 行是
# "# HELP dolphin_scheduler_is_leader 1 if this instance..."，子串匹配会命中帮助行
# 导致 is_leader 永远 True，制造"假双主 / 1s 接管"的幻觉（本脚本踩过的真实坑）。
function Get-IsLeader($port) {
    try {
        $m = (Invoke-WebRequest -Uri "http://localhost:$port/metrics" -UseBasicParsing -TimeoutSec 3).Content
        return [bool]((($m -split "`n") | Where-Object { $_ -match "^dolphin_scheduler_is_leader 1$" }))
    } catch { return $false }
}
function Get-WorkersOnline($port) {
    try {
        $m = (Invoke-WebRequest -Uri "http://localhost:$port/metrics" -UseBasicParsing -TimeoutSec 3).Content
        $line = ($m -split "`n") | Where-Object { $_ -match "^dolphin_scheduler_workers_online " } | Select-Object -First 1
        if ($line) { return [int](($line -split " ")[1]) }
        return -1
    } catch { return -1 }
}

# 探测当前 leader/follower（返回 hashtable）
function Get-LeaderFollower {
    $s1 = Get-IsLeader 9090
    $s2 = Get-IsLeader 9092
    if ($s1 -and -not $s2) { return @{ Leader = 9090; Follower = 9092; LeaderCfg = "scheduler.yaml"; FollowerCfg = "scheduler2.yaml" } }
    if ($s2 -and -not $s1) { return @{ Leader = 9092; Follower = 9090; LeaderCfg = "scheduler2.yaml"; FollowerCfg = "scheduler.yaml" } }
    if ($s1 -and $s2)      { Write-Host "  ⚠️ 双主瞬态，按 scheduler1 继续" -ForegroundColor Yellow; return @{ Leader = 9090; Follower = 9092; LeaderCfg = "scheduler.yaml"; FollowerCfg = "scheduler2.yaml"; Dual = $true } }
    return $null
}

# ---------- 统计 ----------
function Get-Stats($values) {
    if (-not $values -or @($values).Count -eq 0) { return $null }
    $sorted = @($values | Sort-Object)
    $n = $sorted.Count
    if ($n % 2 -eq 1) { $median = $sorted[[math]::Floor($n/2)] } else { $median = ($sorted[$n/2 - 1] + $sorted[$n/2]) / 2 }
    $mean = ($values | Measure-Object -Average).Average
    return [PSCustomObject]@{ N = $n; Min = [math]::Round($sorted[0],2); Median = [math]::Round($median,2); Mean = [math]::Round($mean,2); Max = [math]::Round($sorted[$n-1],2) }
}

# ---------- Leader 接管单次 ----------
function Test-LeaderFailoverOnce {
    $lf = Get-LeaderFollower
    if (-not $lf) { Write-Host "  ❌ 未检测到 Leader，跳过本次"; return $null }

    $leaderProc = Get-SchedulerProc $lf.LeaderCfg
    if (-not $leaderProc) { Write-Host "  ⚠️ 未找到 Leader 进程（$($lf.LeaderCfg)），跳过"; return $null }

    $killTime = Get-Date
    Write-Host "  kill Leader: port=$($lf.Leader) cfg=$($lf.LeaderCfg) PID=$($leaderProc.ProcessId)"
    Stop-Process -Id $leaderProc.ProcessId -Force

    $elapsed = $null
    for ($i = 0; $i -lt 40; $i++) {
        Start-Sleep 1
        if (Get-IsLeader $lf.Follower) {
            $elapsed = ((Get-Date) - $killTime).TotalSeconds
            Write-Host "  ✅ follower(port $($lf.Follower)) 接管，耗时 $([math]::Round($elapsed,2))s"
            break
        }
    }
    if ($null -eq $elapsed) { Write-Host "  ❌ 40s 内 follower 未接管" }

    # 无论接管成败都重启被杀掉的 scheduler，恢复 2 节点拓扑，保证下一轮还能测
    Write-Host "  重启被杀 scheduler ($($lf.LeaderCfg))..."
    Start-Scheduler $lf.LeaderCfg
    $null = Wait-Healthz $lf.Leader 30
    Start-Sleep 2   # 让它完成竞选（会成为 follower）

    return $elapsed
}

# ---------- Worker 恢复单次 ----------
function Test-WorkerFailoverOnce($leaderPort, $grpcPort, $batch, $idx) {
    # 慢任务用一年一次的 cron（0 0 1 1 *），避免 */1 每 1 分钟重跑干扰后续轮次；
    # 靠 task trigger 立即触发（next_run_at=now），不依赖 cron。
    $slowOut = & "$Bins\dolphinctl.exe" --addr "localhost:$grpcPort" task create --name "failover-slow-$batch-$idx" --cron "0 0 1 1 *" --handler "http://localhost:$leaderPort/debug/sleep?seconds=45" --timeout 60 --retries 1 2>&1
    $slowIdLine = $slowOut | Select-String "id=" | Select-Object -First 1
    if (-not $slowIdLine) { Write-Host "  ❌ 慢任务创建失败: $slowOut"; return $null }
    $slowId = (($slowIdLine.ToString() -split " ") | Where-Object { $_ -match "^id=" }) -replace "id=",""
    & "$Bins\dolphinctl.exe" --addr "localhost:$grpcPort" task trigger --id $slowId 2>&1 | Out-Null
    Write-Host "  慢任务 id=$slowId，等待进入 running..."

    $runningWorker = $null
    for ($i = 0; $i -lt 15; $i++) {
        Start-Sleep 2
        $row = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT CONCAT(worker_id,'|',instance_id) FROM dolphin.task_logs WHERE task_id='$slowId' AND status='running' LIMIT 1;" 2>$null
        if ($row) { $runningWorker = ($row -split '\|')[0]; break }
    }
    if (-not $runningWorker) { Write-Host "  ⚠️ 慢任务 30s 内未 running，跳过"; return $null }

    $killPid = $null
    if ($runningWorker -match '-(\d+)$') { $killPid = [int]$matches[1] }
    if (-not $killPid) { Write-Host "  ⚠️ 无法从 worker_id 提取 PID（$runningWorker），跳过"; return $null }

    # 记录被 kill worker 的配置，用于重启
    $wp = Get-CimInstance Win32_Process -Filter "ProcessId=$killPid"
    $killedCfg = if ($wp -and $wp.CommandLine -like "*worker2*") { "worker2.yaml" } else { "worker.yaml" }

    $killTime = Get-Date
    $killWid = "%-$killPid"   # MySQL 通配符是 %，不是 *
    Write-Host "  kill Worker: PID=$killPid cfg=$killedCfg（慢任务在其上 running）"
    Stop-Process -Id $killPid -Force

    # 检测故障转移：kill 之后、存活 worker 上同一 task 产生新执行记录即视为恢复。
    # 注意 1：task_logs.start_time 存 UTC，比较时间必须转 UTC。
    # 注意 2：按 task_id 过滤，避免被其它任务干扰。
    $killTimeUtc = $killTime.ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')
    $elapsed = $null
    for ($i = 0; $i -lt 45; $i++) {
        Start-Sleep 2
        $sql = "SELECT COUNT(*) FROM dolphin.task_logs WHERE task_id='$slowId' AND start_time > '$killTimeUtc' AND worker_id NOT LIKE '$killWid';"
        $newLogs = docker exec dolphin-mysql mysql -u root -pdolphin -N -e $sql 2>$null
        if ($newLogs -and [int]$newLogs -gt 0) {
            $elapsed = ((Get-Date) - $killTime).TotalSeconds
            break
        }
    }
    if ($null -eq $elapsed) { Write-Host "  ❌ 90s 内未检测到任务恢复" }
    else { Write-Host "  ✅ 存活 worker 接管，恢复耗时 $([math]::Round($elapsed,2))s" }

    # 重启被 kill 的 worker，恢复 2 worker 拓扑
    Write-Host "  重启被 kill worker ($killedCfg)..."
    Start-Worker $killedCfg
    return $elapsed
}

# ============ Step 0: 环境清理 ============
Write-Step "Step 0/6: 环境清理"
Get-Process -Name "scheduler","worker","gateway" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep 2
docker exec dolphin-mysql mysql -u root -pdolphin -e "USE dolphin; TRUNCATE TABLE tasks; TRUNCATE TABLE task_logs; TRUNCATE TABLE task_conditions; TRUNCATE TABLE workers;" 2>$null
Write-Host "已停止进程，已清空数据"

# ============ Step 1: 编译 ============
Write-Step "Step 1/6: 编译"
Push-Location $ProjectDir
go build -o bin/scheduler.exe ./cmd/scheduler/ 2>&1 | Where-Object { $_ -match "error" }
go build -o bin/worker.exe ./cmd/worker/ 2>&1 | Where-Object { $_ -match "error" }
go build -o bin/dolphinctl.exe ./cmd/dolphinctl/ 2>&1 | Where-Object { $_ -match "error" }
Pop-Location
Write-Host "编译完成"

# ============ Step 2: 启动 2 scheduler（不含 worker，隔离两类故障） ============
Write-Step "Step 2/6: 启动 2 scheduler"
Start-Scheduler "scheduler.yaml"
Start-Sleep 1
Start-Scheduler "scheduler2.yaml"
Start-Sleep 4

if (-not (Wait-Healthz 9090 15)) { Write-Host "⚠️ scheduler1 (9090) 未就绪" }
if (-not (Wait-Healthz 9092 15)) { Write-Host "⚠️ scheduler2 (9092) 未就绪" }

$lf = Get-LeaderFollower
if (-not $lf) { Write-Host "❌ 未检测到 Leader，中止"; exit 1 }
Write-Host "初始 Leader: port $($lf.Leader) ($($lf.LeaderCfg))"

# ============ Step 3: Leader 接管 × Runs ============
Write-Step "Step 3/6: Leader 故障转移（重复 $Runs 次）"
$leaderTimes = @()
for ($r = 1; $r -le $Runs; $r++) {
    Write-Host "`n--- Leader 接管 第 $r/$Runs 次 ---" -ForegroundColor Cyan
    $t = Test-LeaderFailoverOnce
    if ($null -ne $t) { $leaderTimes += $t }
}

# ============ Step 4: Worker 接管 × WorkerRuns ============
Write-Step "Step 4/6: Worker 故障转移（重复 $WorkerRuns 次）"

Start-Worker "worker.yaml"
Start-Sleep 1
Start-Worker "worker2.yaml"

$lf = Get-LeaderFollower
if (-not $lf) { Write-Host "❌ 无 Leader，中止"; exit 1 }
$leaderPort = $lf.Leader
$grpcPort = Get-GrpcPort $leaderPort
Write-Host "当前 Leader: port $leaderPort (grpc $grpcPort)"

# 等待 2 个 worker 都连上当前 leader。
# 关键：看内存注册表指标 workers_online，不是 MySQL workers 表（那是残留状态）。
Write-Host "等待 2 个 worker 连上 leader..."
$ready = $false
for ($i = 0; $i -lt 40; $i++) {
    Start-Sleep 2
    $online = Get-WorkersOnline $leaderPort
    if ($online -ge 2) { Write-Host "  ✅ $online 个 worker 已连接（${i}x2s）"; $ready = $true; break }
    if ($i % 5 -eq 0) { Write-Host "  ...等待中 ($($i*2)s/80s), 在线 worker=$online" }
}
if (-not $ready) { Write-Host "  ⚠️ 80s 内 worker 未全部连接（当前 $online），继续但结果可能受影响" }

$batch = Get-Date -Format "HHmmss"
$workerRecoveryTimes = @()
for ($r = 1; $r -le $WorkerRuns; $r++) {
    Write-Host "`n--- Worker 恢复 第 $r/$WorkerRuns 次 ---" -ForegroundColor Cyan

    # 每轮前确保 2 个 worker 在线（上一轮 kill 的那个已重启，等它重连）
    $ready = $false
    for ($i = 0; $i -lt 20; $i++) {
        Start-Sleep 2
        if ((Get-WorkersOnline $leaderPort) -ge 2) { $ready = $true; break }
    }
    if (-not $ready) { Write-Host "  ⚠️ 本轮开始前在线 worker < 2，可能影响结果" }

    $t = Test-WorkerFailoverOnce $leaderPort $grpcPort $batch $r
    if ($null -ne $t) { $workerRecoveryTimes += $t }
}

# ============ Step 5: 幂等验证 ============
Write-Step "Step 5/6: 幂等验证（不重复执行）"
$dupCheck = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "
  SELECT l1.instance_id, COUNT(*) AS cnt
  FROM dolphin.task_logs l1
  JOIN dolphin.task_logs l2 ON l1.instance_id = l2.instance_id
  WHERE l1.status != 'cancelled'
  GROUP BY l1.instance_id HAVING cnt > 1 LIMIT 5;" 2>$null
if ($dupCheck) {
    Write-Host "❌ 发现重复 instance_id（同一个执行被执行了多次）:"
    Write-Host $dupCheck
} else {
    Write-Host "✅ 无重复 instance_id，幂等验证通过：Worker 故障转移不会导致同一任务重复执行。"
}

# ============ Step 6: 汇总 + 报告 ============
Write-Step "Step 6/6: 数据汇总"
$leaderStats = Get-Stats $leaderTimes
$workerStats = Get-Stats $workerRecoveryTimes

$leaderVals = ($leaderTimes | ForEach-Object { [math]::Round($_,2) }) -join ', '
$workerVals = ($workerRecoveryTimes | ForEach-Object { [math]::Round($_,2) }) -join ', '

if ($leaderStats) {
    Write-Host "Leader 接管: N=$($leaderStats.N)  min=$($leaderStats.Min)s  median=$($leaderStats.Median)s  mean=$($leaderStats.Mean)s  max=$($leaderStats.Max)s"
    Write-Host "  每次值: $leaderVals"
    Write-Host "  机制对照: Lease TTL=15s，接管应落在 10–15s 波动区间（Lease 每 5s 续约）"
} else { Write-Host "Leader 接管: 无有效样本" }

if ($workerStats) {
    Write-Host "Worker 恢复: N=$($workerStats.N)  min=$($workerStats.Min)s  median=$($workerStats.Median)s  mean=$($workerStats.Mean)s  max=$($workerStats.Max)s"
    Write-Host "  每次值: $workerVals"
    Write-Host "  机制对照: 心跳超时 30s − kill 前已流逝 + 检测周期 5s，应落在 ~20–35s"
} else { Write-Host "Worker 恢复: 无有效样本" }

$statusDist = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT status, COUNT(*) FROM dolphin.task_logs GROUP BY status;" 2>$null
Write-Host "task_log 状态分布:"
$statusDist

$leaderLine = if ($leaderStats) { "Leader 接管: N=$($leaderStats.N), min=$($leaderStats.Min)s, median=$($leaderStats.Median)s, mean=$($leaderStats.Mean)s, max=$($leaderStats.Max)s`n  每次: $leaderVals" } else { "Leader 接管: 无有效样本" }
$workerLine = if ($workerStats) { "Worker 恢复: N=$($workerStats.N), min=$($workerStats.Min)s, median=$($workerStats.Median)s, mean=$($workerStats.Mean)s, max=$($workerStats.Max)s`n  每次: $workerVals" } else { "Worker 恢复: 无有效样本" }

$report = @"
Dolphin 故障转移测试报告
时间: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
$leaderLine
$workerLine
幂等验证: $(if ($dupCheck) {'FAIL'} else {'PASS'})
"@
$report | Out-File "$Results\failover_report.txt" -Encoding utf8
Write-Host "`n报告已保存: $Results\failover_report.txt"
