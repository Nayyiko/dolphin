# ratelimit_test.ps1 — 限流精确性验证（单实例，Redis 令牌桶）
# ============================================================
# 验证：Redis Lua 令牌桶按 rate 精确拒绝超限请求。
# 方法：打满 Qps 持续 DurationSec 秒，读 gateway /metrics 的
#       dolphin_gateway_ratelimit_rejected_total 增量，与理论超限对比。
# 理论：放行 ≈ Capacity + Rate×DurationSec（突发容量 + 持续速率）
#      拒绝 ≈ 总请求 - 放行
#
# 前置：docker compose 已起 etcd/mysql/redis；gateway 已在 8080 运行。
# 用法：.\hack\ratelimit_test.ps1 [-Qps 1000] [-DurationSec 15] [-Rate 100] [-Capacity 200]
# ============================================================
param(
    [int]$Qps = 1000,
    [int]$DurationSec = 15,
    [int]$Rate = 100,
    [int]$Capacity = 200,
    [int]$GatewayPort = 8080,
    [double]$Tolerance = 0.05
)

$ErrorActionPreference = "Stop"
$PROJECT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECT_DIR = Split-Path -Parent $PROJECT_DIR   # hack -> 项目根
Set-Location $PROJECT_DIR

$LOADGEN = Join-Path $PROJECT_DIR "bin\loadgen.exe"
$REPORT  = Join-Path $PROJECT_DIR "results\ratelimit_report.txt"
$GW = "http://localhost:$GatewayPort"
$TARGET = "$GW/health"
New-Item -ItemType Directory -Force -Path (Join-Path $PROJECT_DIR "results") | Out-Null

# ── 0. 环境检查 ──
if (-not (Test-Path $LOADGEN)) {
    Write-Host "未找到 bin\loadgen.exe，先构建:" -ForegroundColor Yellow
    Write-Host "  go build -o bin\loadgen.exe ./cmd/loadgen" -ForegroundColor Gray
    exit 1
}
$probe = curl.exe -s --max-time 3 -o NUL -w "%{http_code}" "$GW/health"
if ($probe -ne "200") {
    if ($probe -eq "429") {
        Write-Host "ℹ️  gateway 存活但桶已空（/health=429）。建议重启 gateway（等桶 key 过期 60s）保证空桶起点，否则理论放行会偏小。" -ForegroundColor Yellow
    } else {
        Write-Host "❌ gateway 未就绪（/health=$probe）。请先起服务：.\start-dev.ps1" -ForegroundColor Red
        exit 1
    }
}

# ── 读 rejected counter 增量（按 path label 过滤）──
function Get-RejectedTotal {
    param([string]$Base, [string]$PathLabel)
    for ($i = 0; $i -lt 5; $i++) {
        $raw = curl.exe -s --max-time 3 "$Base/metrics"
        $lines = $raw | Select-String -Pattern 'dolphin_gateway_ratelimit_rejected_total'
        if ($lines) {
            $total = [long]0
            foreach ($l in $lines) {
                $txt = $l.Line
                if ($txt -match ('endpoint="' + [regex]::Escape($PathLabel) + '"')) {
                    $tok = ($txt -split '\s+')[-1]
                    $v = [long]0
                    if ([long]::TryParse($tok, [ref]$v)) { $total += $v }
                }
            }
            return $total
        }
        Start-Sleep -Milliseconds 500
    }
    return 0
}

Write-Host "════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  限流精确性验证  Qps=$Qps  Duration=${DurationSec}s  Rate=$Rate  Cap=$Capacity"
Write-Host "════════════════════════════════════════════" -ForegroundColor Cyan

$R0 = Get-RejectedTotal -Base $GW -PathLabel "/health"
Write-Host "R0(rejected_total before) = $R0"

# ── 打流 ──
$totalReq = $Qps * $DurationSec
$theoreticalPass = $Capacity + $Rate * $DurationSec
$theoreticalReject = $totalReq - $theoreticalPass

Write-Host "▶ loadgen -qps $Qps -duration ${DurationSec}s → $TARGET"
$lgOut = & $LOADGEN -url $TARGET -qps $Qps -duration "${DurationSec}s" -concurrency 50 2>&1
$lgOut | Out-Host

# 用 loadgen 实际发送数校准（QPS 未必达标称，理论按实际请求数算，误差更小）
$m = [regex]::Match(($lgOut -join "`n"), '总请求数:\s*(\d+)')
if ($m.Success -and [int]$m.Groups[1].Value -gt 0) {
    $totalReq = [int]$m.Groups[1].Value
}
$theoreticalPass = $Capacity + $Rate * $DurationSec
$theoreticalReject = [math]::Max(0, $totalReq - $theoreticalPass)

# ── 压测后健康探针：区分"网关被压垮"与"限流不精确" ──
$postCode = curl.exe -s --max-time 3 -o NUL -w "%{http_code}" "$GW/health"
$gatewayAlive = ($postCode -ne "000") -and ($postCode -ne "")
if (-not $gatewayAlive) {
    Write-Host "⚠️  压测后 /health 无响应（code=$postCode）——网关疑似被压垮/失联，本组数据无效。" -ForegroundColor Yellow
    Write-Host "   请重启 gateway（关闭 start-dev.ps1 的 gateway 窗口后重跑），再重新执行本测试。" -ForegroundColor Yellow
}
if ($postCode -eq "429") {
    Write-Host "ℹ️  压测后探活返回 429——说明桶已空、限流器仍在工作，网关存活，本组数据有效。" -ForegroundColor Gray
}

# ── 读 R1 ──
$R1 = Get-RejectedTotal -Base $GW -PathLabel "/health"
Write-Host "R1(rejected_total after)  = $R1"
$deltaReject = $R1 - $R0
$actualPass = $totalReq - $deltaReject

# ── 断言 ──
$lower = [math]::Floor($theoreticalReject * (1 - $Tolerance))
$upper = [math]::Ceiling($theoreticalReject * (1 + $Tolerance))
if (-not $gatewayAlive) {
    $pass = $false
} else {
    $pass = ($deltaReject -ge $lower) -and ($deltaReject -le $upper)
}

Write-Host ""
Write-Host "── 结果 ──" -ForegroundColor Cyan
Write-Host ("总请求:      {0}" -f $totalReq)
Write-Host ("理论放行:    {0}  (=Cap {1} + Rate×T {2}×{3})" -f $theoreticalPass, $Capacity, $Rate, $DurationSec)
Write-Host ("实际放行:    {0}  (=总请求 - 拒绝增量)" -f $actualPass)
Write-Host ("理论拒绝:    {0}" -f $theoreticalReject)
Write-Host ("实际拒绝ΔR:  {0}" -f $deltaReject)
Write-Host ("容差:        ±{0}%  →  [{1}, {2}]" -f ($Tolerance*100), $lower, $upper)
if (-not $gatewayAlive) {
    Write-Host "❌ 结果无效：压测后网关无响应。请重启 gateway 后重跑。" -ForegroundColor Red
} elseif ($pass) {
    Write-Host "✅ PASS：429 数量与理论超限吻合，令牌桶算法精确" -ForegroundColor Green
} else {
    Write-Host "❌ FAIL：拒绝数落在理论窗口外，需检查限流配置或测量方法" -ForegroundColor Red
}

# ── 报告存档 ──
$reportText = @"
Dolphin 限流精确性测试报告
时间: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
配置: Qps=$Qps Duration=${DurationSec}s Rate=$Rate Capacity=$Capacity Gateway=$GatewayPort
网关状态: $(if ($gatewayAlive) { "压测后存活" } else { "压测后无响应，结果无效" })
总请求: $totalReq
理论放行: $theoreticalPass (Cap $Capacity + Rate×T $Rate×${DurationSec})
实际放行: $actualPass
理论拒绝: $theoreticalReject
实际拒绝(ΔR): $deltaReject
结果: $(if ($pass) { "PASS" } else { "FAIL" })
"@
[System.IO.File]::WriteAllText($REPORT, $reportText)
Write-Host "报告已保存: $REPORT" -ForegroundColor Gray

# ── 诊断：打印原始 ratelimit 指标行（确认 label 名与数值）──
Write-Host ""
Write-Host "── 原始 ratelimit 指标行（诊断用）──" -ForegroundColor Gray
$rawMetrics = curl.exe -s "$GW/metrics"
$rawMetrics | Select-String -Pattern 'ratelimit' | ForEach-Object { $_.Line }

exit $(if ($pass) { 0 } else { 1 })
