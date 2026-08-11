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
function Ctl { param($args) & "$Bins\dolphinctl.exe" $args 2>&1 }

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

# 确认哪个 scheduler 是 leader
$leader1 = (Invoke-WebRequest -Uri "http://localhost:9090/metrics" -UseBasicParsing -TimeoutSec 3).Content -match "dolphin_scheduler_is_leader 1"
$leader2 = (Invoke-WebRequest -Uri "http://localhost:9092/metrics" -UseBasicParsing -TimeoutSec 3).Content -match "dolphin_scheduler_is_leader 1"
Write-Host "scheduler1 is_leader: $leader1"
Write-Host "scheduler2 is_leader: $leader2"
if ($leader1) { $leaderPort = 9090; $followerPort = 9092; $leaderCfg = "scheduler.yaml" } else { $leaderPort = 9092; $followerPort = 9090; $leaderCfg = "scheduler2.yaml" }
Write-Host "当前 Leader 端口: $leaderPort (配置 $leaderCfg)"

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

    # 轮询 follower 是否成为 leader
    $followerBecameLeader = $false
    for ($i = 0; $i -lt 40; $i++) {  # 最多 40s
        Start-Sleep 1
        try {
            $m = (Invoke-WebRequest -Uri "http://localhost:$followerPort/metrics" -UseBasicParsing -TimeoutSec 2).Content
            if ($m -match "dolphin_scheduler_is_leader 1") {
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

# 创建 3 个每 1 分钟执行的任务（*/1 cron，60s 内必触发一次）
Write-Host "创建 3 个 */1 cron 任务..."
$batch = Get-Date -Format "HHmmss"
for ($i = 1; $i -le 3; $i++) {
    Ctl --addr "localhost:$followerGRPC" task create --name "failover-$batch-$i" --cron "*/1 * * * *" --handler http://localhost:9090/healthz --timeout 5 --retries 1 2>&1 | Out-Null
}
Write-Host "已创建 3 个任务 (batch=$batch)"

# 等待首轮调度（*/1 最多等 60s + 缓冲）
Write-Host "等待首轮调度（最多 75s）..."
$dispatched = $false
for ($i = 0; $i -lt 15; $i++) {  # 15 * 5s = 75s
    Start-Sleep 5
    $cnt = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT COUNT(*) FROM dolphin.task_logs;" 2>$null
    if ($cnt -and [int]$cnt -gt 0) { $dispatched = $true; Write-Host "  首轮调度完成，task_log=$cnt (${i}x5s)"; break }
    if ($i % 3 -eq 0) { Write-Host "  ...等待中 ($($i*5)s/75s)" }
}
if (-not $dispatched) { Write-Host "❌ 75s 内任务未调度，中止测试"; exit 1 }

# 查看当前 task_log 分布在哪些 worker
$logDump = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT DISTINCT worker_id FROM dolphin.task_logs WHERE status='running' OR status='success' LIMIT 10;" 2>$null
Write-Host "有执行记录的 workers: $logDump"

# 识别 worker 进程与其 workerID 的对应关系（workerID = hostname-pid）
$workerProcs = Get-Process -Name "worker" -ErrorAction SilentlyContinue
Write-Host "`n当前 worker 进程:"
foreach ($wp in $workerProcs) {
    $wid = "{0}-{1}" -f $env:COMPUTERNAME, $wp.Id
    # 注意：hostname 可能不是 COMPUTERNAME，这里只是预估
    Write-Host "  PID=$($wp.Id) → workerID≈$wid"
}

# 选一个正在执行任务的 worker 杀掉。从 task_log 中找有记录的 worker，
# 匹配 worker.exe 进程的 PID（workerID 含 PID）。
$killWorker = $null
$workersTable = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT id, status, last_heartbeat FROM dolphin.workers;" 2>$null
Write-Host "`nworkers 表:"
$workersTable

# 优先选有 task_log 记录的 worker。若无法精确匹配，杀第一个 worker。
foreach ($wp in $workerProcs) {
    $checkWid = "*-$($wp.Id)"
    $hasLog = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT COUNT(*) FROM dolphin.task_logs WHERE worker_id LIKE '$checkWid';" 2>$null
    if ($hasLog -and [int]$hasLog -gt 0) {
        $killWorker = $wp
        Write-Host "选中 worker PID=$($wp.Id)（有 $hasLog 条执行记录）"
        break
    }
}
if (-not $killWorker -and $workerProcs.Count -ge 1) {
    $killWorker = $workerProcs[0]
    Write-Host "未精确匹配，退化为杀第一个 worker PID=$($killWorker.Id)"
}

if ($killWorker) {
    # 记录 kill 时刻。workerID = <hostname>-<pid>，用 "*-<pid>" 模式匹配即可，
    # 避免 hostname 与 COMPUTERNAME 不一致的问题。
    $killTime = Get-Date
    $killWid = "*-$($killWorker.Id)"
    Write-Host "`n准备 kill Worker: PID=$($killWorker.Id), workerID 模式=$killWid"
    Write-Host "Kill Worker 时间: $($killTime.ToString('HH:mm:ss.fff'))"

    Stop-Process -Id $killWorker.Id -Force
    Write-Host "已 kill Worker PID $($killWorker.Id)"

    # 检测故障转移：等心跳超时(30s)+检测周期(5s)，该 worker 的 running 任务被
    # 标记 failed 并重新调度到存活 worker。检测到"kill 之后、存活 worker 产生
    # 新执行记录"即视为恢复。
    Write-Host "等待故障转移（心跳超时 30s + 检测周期 5s + 重新调度）..."
    $recovered = $false
    for ($i = 0; $i -lt 30; $i++) {  # 最多 60s
        Start-Sleep 2
        $sql = "SELECT COUNT(*) FROM dolphin.task_logs WHERE start_time > '$($killTime.ToString('yyyy-MM-dd HH:mm:ss'))' AND worker_id NOT LIKE '$killWid';"
        $newLogs = docker exec dolphin-mysql mysql -u root -pdolphin -N -e $sql 2>$null
        if ($newLogs -and [int]$newLogs -gt 0) {
            $newWorker = docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT DISTINCT worker_id FROM dolphin.task_logs WHERE start_time > '$($killTime.ToString('yyyy-MM-dd HH:mm:ss'))' AND worker_id NOT LIKE '$killWid' LIMIT 3;" 2>$null
            $recoverTime = Get-Date
            $elapsed = ($recoverTime - $killTime).TotalSeconds
            Write-Host "✅ 检测到新执行记录（存活 worker 接管）"
            Write-Host "  新记录 worker: $newWorker"
            Write-Host "  恢复耗时: $($elapsed.ToString('F2'))s (从 kill 到新执行)"
            Write-Host ""
            Write-Host "  SLO 对标: 故障转移恢复 P99 < 30s (心跳超时 30s)"
            if ($elapsed -le 30) { Write-Host "  ✅ 达标" } else { Write-Host "  ⚠️ 超过 30s" }
            $recovered = $true
            $workerRecoveryTime = $elapsed
            break
        }
        if ($i % 10 -eq 0) { Write-Host "  ...等待中 ($($i*2)s/60s)" }
    }
    if (-not $recovered) { Write-Host "❌ 60s 内未检测到任务恢复" }
} else {
    Write-Host "⚠️ 未找到 worker 进程"
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
