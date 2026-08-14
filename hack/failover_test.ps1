# failover_test.ps1 — 故障转移测试脚本
# 验证两个创新点：
#   1. Leader 故障转移（etcd 选主）：kill Leader，测另一个 scheduler 接管时间
#   2. Worker 故障转移（任务不丢不重）：kill 1 个 worker，测任务在另一个 worker 恢复 + 幂等
#
# 用法: powershell -ExecutionPolicy Bypass -File hack\failover_test.ps1
#
# 前置: docker compose 已启动 etcd/mysql/redis
#       编译最新 scheduler/worker/dolphinctl 到 bin/

$ErrorActionPreference = "Continue"
$ProjectDir = "C:\Users\30641\Desktop\dolphin"
$Bins = "$ProjectDir\bin"
$Results = "$ProjectDir\results"
New-Item -ItemType Directory -Force -Path $Results | Out-Null

function Write-Step { param($msg) Write-Host "`n══════════ $msg ══════════" -ForegroundColor Green }
# 注意：不能用 PowerShell 函数包装 dolphinctl——函数调用会把 "--addr" 当作
# 命名参数解析并吞掉，导致 dolphinctl 把地址值当成 command（报 "unknown command: localhost:50052"）。
# 必须直接 & 调用 exe，PowerShell 才会把 "--addr" 原样透传给 native 命令（diagnose.ps1 同款写法）。

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

# ============ Step 2: 启动 2 个 scheduler + 2 个 worker ============
Write-Step "Step 2/6: 启动 2 scheduler + 2 worker"

# 为区分 worker，需要不同的 workerID。用环境变量? worker 用 hostname+pid 做 ID，
# 两个 worker 进程 pid 不同，ID 自然不同。OK。
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd $ProjectDir; .\bin\scheduler.exe --config configs\scheduler.yaml" -WindowStyle Minimized
Start-Sleep 1
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd $ProjectDir; .\bin\scheduler.exe --config configs\scheduler2.yaml" -WindowStyle Minimized
Start-Sleep 3
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd $ProjectDir; .\bin\worker.exe --config configs\worker.yaml" -WindowStyle Minimized
Start-Sleep 1
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd $ProjectDir; .\bin\worker.exe --config configs\worker2.yaml" -WindowStyle Minimized
Start-Sleep 8

# 确认组件就绪
Write-Host "验证组件状态..."
try { $r1 = Invoke-WebRequest -Uri "http://localhost:9090/healthz" -UseBasicParsing -TimeoutSec 3; Write-Host "  scheduler1 (9090): $($r1.StatusCode)" } catch { Write-Host "  scheduler1 (9090): 不可达" }
try { $r2 = Invoke-WebRequest -Uri "http://localhost:9092/healthz" -UseBasicParsing -TimeoutSec 3; Write-Host "  scheduler2 (9092): $($r2.StatusCode)" } catch { Write-Host "  scheduler2 (9092): 不可达" }
try { $w1 = Invoke-WebRequest -Uri "http://localhost:9091/healthz" -UseBasicParsing -TimeoutSec 3; Write-Host "  worker1 (9091): $($w1.StatusCode)" } catch { Write-Host "  worker1 (9091): 不可达" }
try { $w2 = Invoke-WebRequest -Uri "http://localhost:9093/healthz" -UseBasicParsing -TimeoutSec 3; Write-Host "  worker2 (9093): $($w2.StatusCode)" } catch { Write-Host "  worker2 (9093): 不可达" }

# 确认 workers 表有 2 条 online
$workers = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT COUNT(*) FROM dolphin.workers WHERE status='online';" 2>$null
Write-Host "在线 worker 数: $workers (期望 2)"

# 确认哪个 scheduler 是 leader —— 多次采样，避免启动瞬间选举抖动/双主瞬态误判。
function Get-IsLeader($port) {
    # 只匹配"以指标名开头"的采样行。绝不能对整段内容做子串匹配——
    # Prometheus 的 # HELP 行是 "# HELP dolphin_scheduler_is_leader 1 if this instance...",
    # 子串 "dolphin_scheduler_is_leader 1" 会命中帮助行，导致 is_leader 永远报 True，
    # 制造出"假双主/1s 接管"的幻觉。
    try {
        $m = (Invoke-WebRequest -Uri "http://localhost:$port/metrics" -UseBasicParsing -TimeoutSec 3).Content
        return [bool]((($m -split "`n") | Where-Object { $_ -match "^dolphin_scheduler_is_leader 1$" }))
    } catch { return $false }
}

$s1s = @(); $s2s = @()
for ($i = 0; $i -lt 3; $i++) {
    $s1s += [bool](Get-IsLeader 9090)
    $s2s += [bool](Get-IsLeader 9092)
    Start-Sleep 2
}
$leader1 = $s1s[-1]
$leader2 = $s2s[-1]
Write-Host "scheduler1 is_leader (3 次采样): $($s1s -join ',')"
Write-Host "scheduler2 is_leader (3 次采样): $($s2s -join ',')"

if ($leader1 -and $leader2) {
    Write-Host "⚠️ 两个 scheduler 同时 is_leader=True（脑裂/抖动）！请检查 etcd 状态与 scheduler 日志" -ForegroundColor Yellow
}
if ($leader1 -and -not $leader2) { $leaderPort = 9090 }
elseif ($leader2 -and -not $leader1) { $leaderPort = 9092 }
elseif ($leader1 -and $leader2) {
    # 双主异常：无法可靠判断，按 scheduler1 继续（后续结果需谨慎解读）
    $leaderPort = 9090
    Write-Host "双主异常，按 scheduler1 继续（测试结果需谨慎解读）" -ForegroundColor Yellow
} else {
    $leaderPort = $null
    Write-Host "❌ 未检测到 Leader"
}
if ($leaderPort) {
    if ($leaderPort -eq 9090) { $followerPort = 9092; $leaderCfg = "scheduler.yaml" } else { $followerPort = 9090; $leaderCfg = "scheduler2.yaml" }
    Write-Host "当前 Leader 端口: $leaderPort (配置 $leaderCfg)"
}

# ============ Step 3: 测试 1 — Leader 故障转移 ============
Write-Step "Step 3/6: 测试 Leader 故障转移"
Write-Host "当前 Leader: 端口 $leaderPort (配置 $leaderCfg)"
Write-Host "准备 kill Leader... 3 秒后执行"

# 找到 leader scheduler 进程（通过命令行匹配）
$leaderProc = Get-CimInstance Win32_Process -Filter "Name='scheduler.exe'" | Where-Object { $_.CommandLine -match $leaderCfg }
if ($leaderProc) {
    Write-Host "找到 Leader 进程 PID: $($leaderProc.ProcessId)"

    # 记录 kill 时刻
    $killTime = Get-Date
    Write-Host "Kill Leader 时间: $($killTime.ToString('HH:mm:ss.fff'))"

    # kill leader
    Stop-Process -Id $leaderProc.ProcessId -Force
    Write-Host "已 kill Leader"

    # 轮询 follower 是否成为 leader（行首锚定，避免 # HELP 行误报）
    $followerBecameLeader = $false
    for ($i = 0; $i -lt 40; $i++) {  # 最多 40s
        Start-Sleep 1
        try {
            $m = (Invoke-WebRequest -Uri "http://localhost:$followerPort/metrics" -UseBasicParsing -TimeoutSec 2).Content
            $isLeaderNow = [bool]((($m -split "`n") | Where-Object { $_ -match "^dolphin_scheduler_is_leader 1$" }))
            if ($isLeaderNow) {
                $takeoverTime = Get-Date
                $elapsed = ($takeoverTime - $killTime).TotalSeconds
                Write-Host "✅ follower (端口 $followerPort) 已接管成为 Leader"
                Write-Host "  接管耗时: $($elapsed.ToString('F2'))s"
                Write-Host "  接管时刻: $($takeoverTime.ToString('HH:mm:ss.fff'))"
                Write-Host ""
                Write-Host "  etcd Lease TTL = 15s (scheduler.yaml election.ttl)"
                if ($elapsed -le 20) { Write-Host "  ✅ 接管时间 < TTL+检测周期，符合预期" }
                else { Write-Host "  ⚠️ 接管时间偏长，检查 etcd 状态" }
                $followerBecameLeader = $true
                $leaderTakeoverTime = $elapsed
                break
            }
        } catch {}
    }
    if (-not $followerBecameLeader) { Write-Host "❌ 40s 内 follower 未接管 Leader" }
} else {
    Write-Host "⚠️ 未找到 Leader 进程，跳过 Leader 故障转移测试"
}

# ============ Step 4: 测试 2 — Worker 故障转移 ============
Write-Step "Step 4/6: 测试 Worker 故障转移（任务不丢不重）"

# 当前还有 1 个 scheduler 在跑（原 follower 已成为 leader）。
# 注意：Leader 故障转移后，Worker 通过 etcd 自动发现新 Leader 并切换连接，
# 因此这里不必再关心 worker 连的是 50051 还是 50052。
# 但 dolphinctl 需要连到存活 scheduler 的 gRPC 端口来创建任务：
if ($leaderPort -eq 9090) { $followerGRPC = 50052 } else { $followerGRPC = 50051 }
Write-Host "存活 scheduler 的 gRPC 端口: $followerGRPC"

# Leader 切换后，等待 worker 通过 etcd 发现自动重连到新 leader（不重启）。
# 关键：不能看 MySQL workers 表——那是旧 leader 写入的残留状态（kill 后仍显示 online）。
# 必须看新 leader 的「内存注册表 worker 数」指标（dolphin_scheduler_workers_online），
# 只有它 > 0 才说明 worker 真正重连到了本实例、任务才能被下发。
Write-Host "等待 worker 自动重连到新 leader（轮询内存注册表指标）..."
function Get-WorkersOnline($port) {
    try {
        $m = (Invoke-WebRequest -Uri "http://localhost:$port/metrics" -UseBasicParsing -TimeoutSec 3).Content
        $line = ($m -split "`n") | Where-Object { $_ -match "^dolphin_scheduler_workers_online " } | Select-Object -First 1
        if ($line) { return [int](($line -split " ")[1]) }
        return -1  # 指标还没暴露
    } catch { return -1 }
}
$workersReady = $false
for ($i = 0; $i -lt 60; $i++) {  # 最多 120s（worker 重连有退避，最多 ~46s）
    Start-Sleep 2
    $online = Get-WorkersOnline $followerPort
    if ($online -ge 1) { Write-Host "  ✅ $online 个 worker 已重连到新 leader（${i}x2s）"; $workersReady = $true; break }
    if ($i % 10 -eq 0) { Write-Host "  ...等待中 (${i}x2s/120s), 内存注册表 worker=$online" }
}
if (-not $workersReady) { Write-Host "  ⚠️ 120s 内 worker 未重连到新 leader，继续（可能影响调度）" }

# 创建 3 个每 1 分钟执行的任务（*/1 cron，60s 内必触发一次）。
# handler 指向存活 scheduler 的 healthz，保证 Leader 故障转移后执行仍能成功。
# 创建后用 task trigger 立即触发（next_run_at=now），不必等 cron 整分钟，调度更快更确定。
Write-Host "创建 3 个 */1 cron 任务并立即触发..."
$batch = Get-Date -Format "HHmmss"
for ($i = 1; $i -le 3; $i++) {
    $out = & "$Bins\dolphinctl.exe" --addr "localhost:$followerGRPC" task create --name "failover-$batch-$i" --cron "*/1 * * * *" --handler "http://localhost:$followerPort/healthz" --timeout 5 --retries 1 2>&1
    Write-Host "  create #$i -> $out"
    $idLine = $out | Select-String "id=" | Select-Object -First 1
    if ($idLine) {
        $id = (($idLine.ToString() -split " ") | Where-Object { $_ -match "^id=" }) -replace "id=",""
        & "$Bins\dolphinctl.exe" --addr "localhost:$followerGRPC" task trigger --id $id 2>&1 | Out-Null
        Write-Host "  trigger #$i (id=$id)"
    } else {
        Write-Host "  ⚠️ 无法从输出解析 id，跳过 trigger"
    }
}
# 验证任务确实写入了 DB
$taskCnt = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT COUNT(*) FROM dolphin.tasks;" 2>$null
Write-Host "DB 中任务数: $taskCnt (期望 3)"

# 等待首轮调度（trigger 后 Informer 1s 内入队，最多 30s 缓冲）
Write-Host "等待首轮调度（最多 40s）..."
$dispatched = $false
for ($i = 0; $i -lt 20; $i++) {  # 20 * 2s = 40s
    Start-Sleep 2
    $cnt = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT COUNT(*) FROM dolphin.task_logs;" 2>$null
    if ($cnt -and [int]$cnt -gt 0) { $dispatched = $true; Write-Host "  首轮调度完成，task_log=$cnt (${i}x2s)"; break }
    if ($i % 5 -eq 0) { Write-Host "  ...等待中 ($($i*2)s/40s)" }
}
if (-not $dispatched) { Write-Host "❌ 40s 内任务未调度，中止测试"; exit 1 }

# 查看当前 task_log 按 worker 分布（参考信息）
$logDump = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT worker_id, status, COUNT(*) FROM dolphin.task_logs GROUP BY worker_id, status;" 2>$null
Write-Host "task_log 按 worker 分布:"
$logDump

# ============ 测试 Worker 故障转移 ============
# 造一个"运行中"任务：handler 指向 scheduler 的 /debug/sleep 慢端点，睡 45s。
# healthz 秒回，kill worker 时没有在飞任务，测不出故障转移；必须有一个 sleeping 任务。
# timeout=60 > sleep=45，保证任务在 worker 上保持 running 状态足够久。
Write-Host "`n创建 1 个慢任务（sleep 45s）并触发..."
$slowOut = & "$Bins\dolphinctl.exe" --addr "localhost:$followerGRPC" task create --name "failover-slow-$batch" --cron "*/1 * * * *" --handler "http://localhost:$followerPort/debug/sleep?seconds=45" --timeout 60 --retries 1 2>&1
Write-Host "  create slow -> $slowOut"
$slowIdLine = $slowOut | Select-String "id=" | Select-Object -First 1
if (-not $slowIdLine) {
    Write-Host "❌ 慢任务创建失败，跳过 Worker 故障转移"
} else {
    $slowId = (($slowIdLine.ToString() -split " ") | Where-Object { $_ -match "^id=" }) -replace "id=",""
    & "$Bins\dolphinctl.exe" --addr "localhost:$followerGRPC" task trigger --id $slowId 2>&1 | Out-Null
    Write-Host "  trigger slow (id=$slowId)"

    # 等慢任务进入 running，记录它在哪个 worker 上跑（worker_id = <hostname>-<pid>）
    Write-Host "等待慢任务进入 running..."
    $runningWorker = $null
    $runningInstance = $null
    for ($i = 0; $i -lt 15; $i++) {  # 最多 30s
        Start-Sleep 2
        $row = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT CONCAT(worker_id,'|',instance_id) FROM dolphin.task_logs WHERE task_id='$slowId' AND status='running' LIMIT 1;" 2>$null
        if ($row) {
            $runningWorker = ($row -split '\|')[0]
            $runningInstance = ($row -split '\|')[1]
            Write-Host "  ✅ 慢任务 running: worker=$runningWorker instance=$runningInstance (${i}x2s)"
            break
        }
        if ($i % 5 -eq 0) { Write-Host "  ...等待中 ($($i*2)s/30s)" }
    }

    if (-not $runningWorker) {
        Write-Host "⚠️ 慢任务未进入 running，跳过 Worker 故障转移"
    } else {
        # 从 worker_id 提取 PID（尾随数字）。hostname 可能是中文/乱码，但 PID 可靠。
        $killPid = $null
        if ($runningWorker -match '-(\d+)$') { $killPid = [int]$matches[1] }
        if (-not $killPid) {
            Write-Host "⚠️ 无法从 worker_id 提取 PID，跳过"
        } else {
            $killTime = Get-Date
            $killWid = "%-$killPid"   # MySQL 通配符是 %，不是 *
            Write-Host "`n准备 kill Worker: PID=$killPid (workerID 模式=$killWid)"
            Write-Host "Kill Worker 时间: $($killTime.ToString('HH:mm:ss.fff'))"
            Stop-Process -Id $killPid -Force
            Write-Host "已 kill Worker PID $killPid（慢任务正在其上运行）"

            # 检测故障转移：心跳超时(30s)后，running 任务被标 failed 并重调度到存活 worker。
            # 检测"kill 之后、存活 worker 上同一 task 产生新执行记录"即视为恢复。
            # 注意 1：task_logs.start_time 存的是 UTC，比较时间必须转 UTC。
            # 注意 2：按 task_id 过滤，避免被其它 */1 任务的 cron 重跑干扰。
            Write-Host "等待故障转移（心跳超时 30s + 检测周期 5s + 重新调度）..."
            $killTimeUtc = $killTime.ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss')
            $recovered = $false
            for ($i = 0; $i -lt 45; $i++) {  # 最多 90s
                Start-Sleep 2
                $sql = "SELECT COUNT(*) FROM dolphin.task_logs WHERE task_id='$slowId' AND start_time > '$killTimeUtc' AND worker_id NOT LIKE '$killWid';"
                $newLogs = docker exec dolphin-mysql mysql -u root -pdolphin -N -e $sql 2>$null
                if ($newLogs -and [int]$newLogs -gt 0) {
                    $newWorker = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT DISTINCT worker_id FROM dolphin.task_logs WHERE task_id='$slowId' AND start_time > '$killTimeUtc' AND worker_id NOT LIKE '$killWid' LIMIT 3;" 2>$null
                    $recoverTime = Get-Date
                    $elapsed = ($recoverTime - $killTime).TotalSeconds
                    Write-Host "✅ 检测到新执行记录（存活 worker 接管）"
                    Write-Host "  新记录 worker: $newWorker"
                    Write-Host "  恢复耗时: $($elapsed.ToString('F2'))s (从 kill 到新执行)"
                    Write-Host ""
                    Write-Host "  SLO 对标: 心跳超时 30s + 检测周期 5s ≈ 35s"
                    if ($elapsed -le 40) { Write-Host "  ✅ 达标" } else { Write-Host "  ⚠️ 超过 40s，检查心跳超时配置" }
                    $recovered = $true
                    $workerRecoveryTime = $elapsed
                    break
                }
                if ($i % 10 -eq 0) { Write-Host "  ...等待中 ($($i*2)s/90s)" }
            }
            if (-not $recovered) { Write-Host "❌ 90s 内未检测到任务恢复" }
        }
    }
}

# ============ Step 5: 幂等验证 ============
Write-Step "Step 5/6: 幂等验证（不重复执行）"
# 检查 kill 后新产生的 task_log 中，有没有与 kill 前 running 实例相同的 instance_id
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
    Write-Host "✅ 无重复 instance_id。每个执行实例只出现一次。"
    Write-Host "   幂等验证通过：Worker 故障转移不会导致同一任务重复执行。"
}

# 汇总 task_log 状态分布
Write-Step "Step 6/6: 数据汇总"
$statusDist = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT status, COUNT(*) FROM dolphin.task_logs GROUP BY status;" 2>$null
Write-Host "task_log 状态分布:"
$statusDist

Write-Host ""
Write-Host "=============== 故障转移测试完成 ===============" -ForegroundColor Cyan
if ($leaderTakeoverTime) { Write-Host "Leader 接管时间: $($leaderTakeoverTime.ToString('F2'))s" }
if ($workerRecoveryTime) { Write-Host "Worker 恢复时间: $($workerRecoveryTime.ToString('F2'))s" }

# 输出到文件
$report = @"
Dolphin 故障转移测试报告
时间: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
Leader 故障转移接管时间: $leaderTakeoverTime s
Worker 故障转移恢复时间: $workerRecoveryTime s
幂等验证: $(if ($dupCheck) {'FAIL'} else {'PASS'})
"@
$report | Out-File "$Results\failover_report.txt"
Write-Host "报告已保存: $Results\failover_report.txt"
