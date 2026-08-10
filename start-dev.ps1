# start-dev.ps1 — 一键启动 Dolphin 三个组件（各开一个新窗口）
# 用法: 在项目目录执行  .\start-dev.ps1
#
# 前置: docker compose 已启动 etcd/mysql/redis 且 mysql 已 healthy
# 停止: .\stop-dev.ps1  或直接关闭各窗口

$ErrorActionPreference = "Stop"

$PROJECT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
if ($PWD.Path -ne $PROJECT_DIR) {
    Write-Host "切换目录到 $PROJECT_DIR" -ForegroundColor Yellow
    Set-Location $PROJECT_DIR
}

$COMPONENTS = @(
    @{ Name = "scheduler"; Cmd = "go run ./cmd/scheduler -config configs/scheduler.yaml" },
    @{ Name = "worker";    Cmd = "go run ./cmd/worker -config configs/worker.yaml" },
    @{ Name = "gateway";   Cmd = "go run ./cmd/gateway -config configs/gateway.yaml" }
)

Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  启动 Dolphin 三个组件" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════" -ForegroundColor Cyan

foreach ($c in $COMPONENTS) {
    Write-Host "▶ 启动 $($c.Name) ..." -ForegroundColor Green
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $c.Cmd -WorkingDirectory $PROJECT_DIR
    Start-Sleep -Seconds 1
}

Write-Host ""
Write-Host "✅ 三个组件已在新窗口启动。" -ForegroundColor Green
Write-Host ""
Write-Host "检查要点:" -ForegroundColor Yellow
Write-Host "  scheduler 窗口: 应看到 became leader" -ForegroundColor Gray
Write-Host "  worker    窗口: 应看到 registered with scheduler accepted=true" -ForegroundColor Gray
Write-Host "  gateway   窗口: 应看到 gateway listening port=8080" -ForegroundColor Gray
Write-Host ""
Write-Host "验证命令（可在本窗口执行）:" -ForegroundColor Cyan
Write-Host "  go build -o bin/dolphinctl ./cmd/dolphinctl" -ForegroundColor Gray
Write-Host "  .\hack\smoke_test.sh   # 如果用的是 Git Bash" -ForegroundColor Gray
Write-Host "  curl http://localhost:8080/health" -ForegroundColor Gray
