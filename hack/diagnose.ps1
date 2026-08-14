# diagnose.ps1 — 诊断"双主 + 任务不调度"
#
# 用法: 在 failover_test.ps1 失败后、scheduler/worker 进程还存活时运行
#       powershell -ExecutionPolicy Bypass -File hack\diagnose.ps1
#
# 收集 6 类决定性证据：
#   1. Docker 容器/端口映射（确认 scheduler 是否连到同一个 etcd）
#   2. etcd 里的选举 key（几个 leader key = 真/假双主的分水岭）
#   3. Scheduler/Worker 进程
#   4. 两个 scheduler 的 Prometheus 指标
#   5. MySQL tasks / task_logs / workers
#   6. 创建+trigger 一个任务，观察是否真的调度

$ErrorActionPreference = "Continue"
$ProjectDir = "C:\Users\30641\Desktop\dolphin"
$Bins = "$ProjectDir\bin"

function Write-Step { param($msg) Write-Host "`n══════════ $msg ══════════" -ForegroundColor Green }

Write-Step "1/6: Docker 容器与端口映射"
docker ps --filter "name=dolphin" --format "table {{.Names}}`t{{.Status}}`t{{.Ports}}"

Write-Step "2/6: etcd 选举 key（决定性）"
Write-Host "--- /dolphin/scheduler 前缀下所有 key ---"
docker exec dolphin-etcd etcdctl get /dolphin/scheduler --prefix -w json
Write-Host ""
Write-Host "--- 活跃 lease ---"
docker exec dolphin-etcd etcdctl lease list

Write-Step "3/6: 进程状态"
Get-Process -Name "scheduler","worker","gateway" -ErrorAction SilentlyContinue |
    Select-Object Id,ProcessName,StartTime | Format-Table -AutoSize

Write-Step "4/6: 两个 scheduler 的指标"
foreach ($port in 9090,9092) {
    Write-Host "--- scheduler http_port=$port ---"
    try {
        $m = (Invoke-WebRequest -Uri "http://localhost:$port/metrics" -UseBasicParsing -TimeoutSec 3).Content
        # 行首锚定，只打印采样行，不打印 # HELP 帮助行
        ($m -split "`n") | Where-Object { $_ -match "^dolphin_scheduler_(is_leader|leader_elections_total|dispatch_total|queue_depth|workers_online)" } | ForEach-Object { Write-Host "  $_" }
    } catch { Write-Host "  不可达" }
}

Write-Step "5/6: MySQL 状态"
Write-Host "--- tasks ---"
docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT id,name,status,next_run_at FROM dolphin.tasks;" 2>$null
Write-Host "--- task_logs 计数 ---"
docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT COUNT(*) FROM dolphin.task_logs;" 2>$null
Write-Host "--- workers ---"
docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT id,status,current_load,max_concurrency,last_heartbeat FROM dolphin.workers;" 2>$null

Write-Step "6/6: 创建+trigger 任务观察调度"
# 探测存活 scheduler 的 gRPC 端口
$grpc = $null
foreach ($p in 50051,50052) {
    try {
        $c = New-Object System.Net.Sockets.TcpClient
        $iar = $c.BeginConnect("localhost", $p, $null, $null)
        if ($iar.AsyncWaitHandle.WaitOne(1000)) { $grpc = $p; $c.Close(); break }
        $c.Close()
    } catch {}
}
Write-Host "存活 scheduler gRPC 端口: $grpc"
if ($grpc) {
    $httpPort = if ($grpc -eq 50051) { 9090 } else { 9092 }
    $batch = Get-Date -Format "HHmmss"
    Write-Host "创建任务 diag-$batch (handler=localhost:$httpPort/healthz)"
    $createOut = & "$Bins\dolphinctl.exe" --addr "localhost:$grpc" task create --name "diag-$batch" --cron "*/1 * * * *" --handler "http://localhost:$httpPort/healthz" --timeout 5 --retries 1 2>&1
    $createOut
    $idLine = ($createOut | Select-String "id=").ToString()
    if ($idLine) {
        $id = ($idLine -split " " | Where-Object { $_ -match "^id=" }) -replace "id=",""
        Write-Host "触发任务 id=$id (next_run_at=now)"
        & "$Bins\dolphinctl.exe" --addr "localhost:$grpc" task trigger --id $id 2>&1
        Start-Sleep 8
        Write-Host "--- 8s 后 task_logs ---"
        docker exec dolphin-mysql mysql -u root -pdolphin -N -e "SELECT id,task_id,instance_id,worker_id,status,start_time FROM dolphin.task_logs;" 2>$null
        Write-Host "--- 触发后两个 scheduler 的调度指标 ---"
        foreach ($port in 9090,9092) {
            try {
                $m = (Invoke-WebRequest -Uri "http://localhost:$port/metrics" -UseBasicParsing -TimeoutSec 3).Content
                ($m -split "`n") | Where-Object { $_ -match "dispatch_total|queue_depth" } | ForEach-Object { Write-Host "  [$port] $_" }
            } catch {}
        }
    } else {
        Write-Host "任务创建失败，见上方输出"
    }
} else {
    Write-Host "两个 gRPC 端口都不可达——进程可能已被清理"
}

Write-Host "`n诊断完成。请把完整输出贴给 Claude。"
