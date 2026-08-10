# stop-dev.ps1 — 停止 Dolphin 三个组件
# 用法: .\stop-dev.ps1

Write-Host "正在关闭 Dolphin 组件窗口..." -ForegroundColor Yellow

# 找到运行 go run ./cmd/ 的窗口并关闭
Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq "powershell.exe" -and $_.CommandLine -match "dolphin"
} | ForEach-Object {
    try {
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "  已关闭 PID $($_.ProcessId)" -ForegroundColor Gray
    } catch {}
}

# 也停掉残留的 go 编译进程
Get-CimInstance Win32_Process | Where-Object {
    $_.Name -match "^(go|gateway|scheduler|worker)\.exe$" -and $_.CommandLine -match "dolphin"
} | ForEach-Object {
    try {
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        Write-Host "  已关闭 PID $($_.ProcessId)" -ForegroundColor Gray
    } catch {}
}

Write-Host ""
Write-Host "✅ 已停止。如需停止基础设施容器:" -ForegroundColor Green
Write-Host "  docker compose -f deployments/docker-compose.yaml down" -ForegroundColor Gray
