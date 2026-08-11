# Dolphin 端到端调度测试脚本
# 清除旧数据 → 启动组件 → 建任务 → 等自然触发 → 采集指标
#
# 用法: powershell -ExecutionPolicy Bypass -File hack\full_test.ps1

param(
    [int]$TaskCount = 30,
    [string]$Cron = "*/1 * * * *",
    [int]$WaitMinutes = 2
)

$ErrorActionPreference = "Continue"  # 不让 MySQL warning / docker stderr 中断脚本
$ProjectDir = "C:\Users\30641\Desktop\dolphin"
$Bins = "$ProjectDir\bin"

function Write-Step { param($msg) Write-Host "`n══════ $msg ══════" -ForegroundColor Green }

# ============ Step 1: 停止所有组件 ============
Write-Step "Step 1/7: 停止旧进程"
Get-Process -Name "scheduler","worker","gateway" -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep 2
Write-Host "已停止所有 Dolphin 进程"

# ============ Step 2: 清空 MySQL 数据 ============
Write-Step "Step 2/7: 清空 MySQL 旧数据"
$result = docker exec dolphin-mysql mysql -u root -pdolphin -e "USE dolphin; TRUNCATE TABLE task_logs; TRUNCATE TABLE task_conditions; TRUNCATE TABLE tasks; TRUNCATE TABLE workers; SELECT 'ok' AS result;" 2>&1
if ($LASTEXITCODE -ne 0) { Write-Host "清空失败: $result"; exit 1 }
Write-Host "MySQL 旧数据已清空"

# ============ Step 3: 编译 ============
Write-Step "Step 3/7: 编译最新代码"
Push-Location $ProjectDir
go build -o bin/scheduler.exe ./cmd/scheduler/ 2>&1 | Where-Object { $_ -match "error|fail" }
go build -o bin/worker.exe   ./cmd/worker/   2>&1 | Where-Object { $_ -match "error|fail" }
go build -o bin/dolphinctl.exe ./cmd/dolphinctl/ 2>&1 | Where-Object { $_ -match "error|fail" }
go build -o bin/loadgen.exe  ./cmd/loadgen/   2>&1 | Where-Object { $_ -match "error|fail" }
Pop-Location
Write-Host "编译完成"

# ============ Step 4: 启动组件 ============
Write-Step "Step 4/7: 启动 scheduler + worker"
# 先确保基础设施运行
docker compose -f $ProjectDir\deployments\docker-compose.yaml up -d 2>&1 | Out-Null
Start-Sleep 2

# 启动 scheduler（后台）
$schedLog = "$ProjectDir\results\scheduler_test.log"
Start-Process -WindowStyle Minimized -FilePath "$Bins\scheduler.exe" `
    -ArgumentList "--config $ProjectDir\configs\scheduler.yaml" `
    -RedirectStandardOutput $schedLog

# 启动 worker（后台）
$workerLog = "$ProjectDir\results\worker_test.log"
Start-Process -WindowStyle Minimized -FilePath "$Bins\worker.exe" `
    -ArgumentList "--config $ProjectDir\configs\worker.yaml" `
    -RedirectStandardOutput $workerLog

# 等待 leader 选举 + worker 注册
Write-Host "等待 Leader 选举和 Worker 注册..."
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep 2
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:9090/healthz" -UseBasicParsing -TimeoutSec 2
        if ($resp.StatusCode -eq 200) {
            # 确认 worker 注册: 检查 metrics
            $metrics = Invoke-WebRequest -Uri "http://localhost:9091/metrics" -UseBasicParsing -TimeoutSec 2
            if ($metrics.Content -match "dolphin_worker_task_completed_total") {
                $ready = $true
                Write-Host "scheduler + worker 就绪"
                break
            }
        }
    } catch {}
}
if (-not $ready) { Write-Host "WARN: 组件可能未完全就绪，继续..." }

# ============ Step 5: 记录基线指标 ============
Write-Step "Step 5/7: 记录基线"
& "$Bins\loadgen.exe" -histogram "dolphin_scheduler_task_lag_seconds" -url http://localhost:9090/metrics 2>&1
Write-Host "---"
& "$Bins\loadgen.exe" -histogram "dolphin_worker_task_duration_seconds" -url http://localhost:9091/metrics -label "handler_type=http" 2>&1

# ============ Step 6: 创建任务 + 等待自然触发 ============
Write-Step "Step 6/7: 创建 $TaskCount 个任务，cron=$Cron，等待 $WaitMinutes 分钟自然触发"
$createStart = Get-Date
& "$Bins\dolphinctl.exe" stress create --count $TaskCount --cron $Cron --handler http://localhost:9090/healthz --timeout 10 2>&1
$createElapsed = (Get-Date) - $createStart

# 等待 cron 触发
$waitSeconds = $WaitMinutes * 60 + 30  # 多加 30s 缓冲
Write-Host "等待 $WaitMinutes 分 30 秒供 cron 自然触发..."
$progress = 0
while ($progress -lt $waitSeconds) {
    Start-Sleep 15
    $progress += 15
    $remaining = $waitSeconds - $progress
    # 每 30s 检查一次调度样本
    if ($progress % 30 -eq 0) {
        $metrics = Invoke-WebRequest -Uri "http://localhost:9090/metrics" -UseBasicParsing -TimeoutSec 3
        $countMatch = [regex]::Match($metrics.Content, 'dolphin_scheduler_task_lag_seconds_count\s+(\d+)')
        if ($countMatch.Success) {
            $lagCount = [int]$countMatch.Groups[1].Value
            Write-Host "  [${progress}s / ${waitSeconds}s] 调度样本: $lagCount | 剩余约 $remaining 秒"
        }
    }
}

# ============ Step 7: 采集最终指标 ============
Write-Step "Step 7/7: 采集调度指标"

Write-Host ""
Write-Host "========== 调度延迟 (P50/P95/P99) =========="
& "$Bins\loadgen.exe" -histogram "dolphin_scheduler_task_lag_seconds" -url http://localhost:9090/metrics 2>&1

Write-Host ""
Write-Host "========== 任务执行耗时 =========="
& "$Bins\loadgen.exe" -histogram "dolphin_worker_task_duration_seconds" -url http://localhost:9091/metrics -label "handler_type=http" 2>&1

Write-Host ""
Write-Host "========== 调度分发计数 =========="
try {
    $schedMetrics = Invoke-WebRequest -Uri "http://localhost:9090/metrics" -UseBasicParsing -TimeoutSec 3
    $dispatchMatch = [regex]::Match($schedMetrics.Content, 'dolphin_scheduler_dispatch_total.*')
    Write-Host ($dispatchMatch.Value)
} catch {}
Write-Host ""
try {
    $workerMetrics = Invoke-WebRequest -Uri "http://localhost:9091/metrics" -UseBasicParsing -TimeoutSec 3
    $completedMatch = [regex]::Matches($workerMetrics.Content, 'dolphin_worker_task_completed_total.*')
    foreach ($m in $completedMatch) { Write-Host $m.Value }
} catch {}
Write-Host ""

# ============ 汇总结论 ============
Write-Host "========== 测试完成 ==========" -ForegroundColor Cyan
Write-Host "如调度延迟 P50 < 5s 且样本数 >= $TaskCount，说明自动 cron 调度正常工作" -ForegroundColor Cyan

# 清理：停止进程
Write-Host ""
$stop = Read-Host "是否停止 scheduler/worker 进程? (y/N)"
if ($stop -eq "y") {
    Get-Process -Name "scheduler","worker" -ErrorAction SilentlyContinue | Stop-Process -Force
    Write-Host "已停止"
}
