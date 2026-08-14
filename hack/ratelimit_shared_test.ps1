# ratelimit_shared_test.ps1 — 多实例限流共享验证
# ============================================================
# 验证：多个 gateway 实例共享同一个 Redis 令牌桶，合计限流到 rate，
#       而不是"各实例独立限流导致总量翻倍"。
#
# 原理：两个 gateway 连同一个 Redis，同一 client+path → 同一桶 key
#       ratelimit:anonymous:/health。
#       同时并发打两个实例，实测两个压测进程的"重叠时长 overlap"，
#       按 overlap 计算共享桶理论放行 = Capacity + Rate×overlap，
#       若实际合计放行接近该值（而非 2×(Capacity+Rate×T)），则证明共享。
#
# 前置：docker compose 已起中间件；gateway(:8080) 和 gateway2(:8081) 都已在跑。
#       起 gateway2:  go run ./cmd/gateway -config configs/gateway2.yaml
# 用法：.\hack\ratelimit_shared_test.ps1 [-QpsPerGw 500] [-DurationSec 15] [-Rate 100] [-Capacity 200]
# ============================================================
param(
    [int]$QpsPerGw = 500,
    [int]$DurationSec = 15,
    [int]$Rate = 100,
    [int]$Capacity = 200,
    [int]$Gw1Port = 8080,
    [int]$Gw2Port = 8081,
    [double]$Tolerance = 0.30
)

$ErrorActionPreference = "Stop"
$PROJECT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$PROJECT_DIR = Split-Path -Parent $PROJECT_DIR   # hack -> 项目根
Set-Location $PROJECT_DIR

$LOADGEN = Join-Path $PROJECT_DIR "bin\loadgen.exe"
$REPORT  = Join-Path $PROJECT_DIR "results\ratelimit_shared_report.txt"
$RESULTS_DIR = Join-Path $PROJECT_DIR "results"
New-Item -ItemType Directory -Force -Path $RESULTS_DIR | Out-Null

$GW1 = "http://localhost:$Gw1Port"
$GW2 = "http://localhost:$Gw2Port"

# ── 0. 环境检查 ──
if (-not (Test-Path $LOADGEN)) {
    Write-Host "未找到 bin\loadgen.exe，先构建:" -ForegroundColor Yellow
    Write-Host "  go build -o bin\loadgen.exe ./cmd/loadgen" -ForegroundColor Gray
    exit 1
}
foreach ($gw in @($GW1, $GW2)) {
    $probe = curl.exe -s --max-time 3 -o NUL -w "%{http_code}" "$gw/health"
    if ($probe -ne "200") {
        if ($probe -eq "429") {
            Write-Host "ℹ️  $gw 存活但桶已空（/health=429）。建议重启 gateway（等桶过期 60s）保证空桶起点。" -ForegroundColor Yellow
        } else {
            Write-Host "❌ $gw 未就绪（/health=$probe）。gateway2 需单独起:" -ForegroundColor Red
            Write-Host "  go run ./cmd/gateway -config configs/gateway2.yaml" -ForegroundColor Gray
            exit 1
        }
    }
}

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
Write-Host "  多实例限流共享验证  Gw1=$GW1 Gw2=$GW2"
Write-Host "  Qps/实例=$QpsPerGw  Duration=${DurationSec}s  Rate=$Rate  Cap=$Capacity"
Write-Host "════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "⚠️  需空桶：测试前请重启两个 gateway（等待 Redis 桶 key 过期 60s）或重启 Redis，" -ForegroundColor Yellow
Write-Host "   保证桶从满 Capacity 开始，否则理论放行会偏小。" -ForegroundColor Yellow

# ── R0（两个实例各自）──
$R0_1 = Get-RejectedTotal -Base $GW1 -PathLabel "/health"
$R0_2 = Get-RejectedTotal -Base $GW2 -PathLabel "/health"
Write-Host "R0: gw1=$R0_1  gw2=$R0_2"

# ── 并发打两个 gateway 的同一 path，实测重叠时长 ──
$totalReq = 2 * $QpsPerGw * $DurationSec
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$out1 = Join-Path $RESULTS_DIR "shared_lg1.out"
$err1 = Join-Path $RESULTS_DIR "shared_lg1.err"
$out2 = Join-Path $RESULTS_DIR "shared_lg2.out"
$err2 = Join-Path $RESULTS_DIR "shared_lg2.err"
$arg1 = @("-url", "$GW1/health", "-qps", "$QpsPerGw", "-duration", "${DurationSec}s", "-concurrency", "50")
$arg2 = @("-url", "$GW2/health", "-qps", "$QpsPerGw", "-duration", "${DurationSec}s", "-concurrency", "50")

$p1 = Start-Process -FilePath $LOADGEN -ArgumentList $arg1 -RedirectStandardOutput $out1 -RedirectStandardError $err1 -NoNewWindow -PassThru
$t1start = $sw.Elapsed.TotalSeconds
$p2 = Start-Process -FilePath $LOADGEN -ArgumentList $arg2 -RedirectStandardOutput $out2 -RedirectStandardError $err2 -NoNewWindow -PassThru
$t2start = $sw.Elapsed.TotalSeconds
Write-Host "▶ 已并发启动两个 loadgen（gw1 @ ${t1start}s, gw2 @ ${t2start}s），等待结束..."

$p1.WaitForExit(); $t1end = $sw.Elapsed.TotalSeconds
$p2.WaitForExit(); $t2end = $sw.Elapsed.TotalSeconds
$sw.Stop()

# 用 loadgen 实际发送数校准（QPS 未必达标称）
$totalReq = 0
foreach ($f in @($out1, $out2)) {
    if (Test-Path $f) {
        $m = [regex]::Match([System.IO.File]::ReadAllText($f), '总请求数:\s*(\d+)')
        if ($m.Success) { $totalReq += [int]$m.Groups[1].Value }
    }
}
if ($totalReq -le 0) { $totalReq = 2 * $QpsPerGw * $DurationSec } # 兜底：解析失败用标称值

# 重叠时长 = 两个进程同时运行的秒数（信息用）
$overlap = [math]::Max(0, [math]::Min($t1end, $t2end) - [math]::Max($t1start, $t2start))
$dur1 = $t1end - $t1start
$dur2 = $t2end - $t2start
# 共享桶实际被消费的窗口 = 从最早启动到最晚结束的并集
$activeWindow = [math]::Max($t1end, $t2end) - [math]::Min($t1start, $t2start)
Write-Host ("两个压测进程已结束。gw1 运行 {0:N1}s，gw2 运行 {1:N1}s，重叠 {2:N1}s，消费窗口 {3:N1}s" -f $dur1, $dur2, $overlap, $activeWindow)

# ── R1 ──
$R1_1 = Get-RejectedTotal -Base $GW1 -PathLabel "/health"
$R1_2 = Get-RejectedTotal -Base $GW2 -PathLabel "/health"
Write-Host "R1: gw1=$R1_1  gw2=$R1_2"

$delta1 = $R1_1 - $R0_1
$delta2 = $R1_2 - $R0_2
$deltaTotal = $delta1 + $delta2
$actualTotalPass = $totalReq - $deltaTotal

# ── 压测后健康探针：区分"网关被压垮"与"限流不共享" ──
$post1 = curl.exe -s --max-time 3 -o NUL -w "%{http_code}" "$GW1/health"
$post2 = curl.exe -s --max-time 3 -o NUL -w "%{http_code}" "$GW2/health"
$gatewaysAlive = ($post1 -ne "000") -and ($post1 -ne "") -and ($post2 -ne "000") -and ($post2 -ne "")
if (-not $gatewaysAlive) {
    Write-Host "⚠️  压测后 gw1=$post1 gw2=$post2——网关疑似被压垮/失联，本组数据无效。" -ForegroundColor Yellow
    Write-Host "   请重启两个 gateway 后重跑。" -ForegroundColor Yellow
}

# 理论值
$sharedPass = $Capacity + $Rate * $activeWindow          # 共享桶：初始容量 + 整段消费窗口按 rate 补
$indepPass  = 2 * $Capacity + $Rate * ($dur1 + $dur2)    # 独立桶：各实例各自 容量 + rate×运行时长

# ── 判定：实际放行更接近共享理论还是独立理论 ──
$shareErr = [math]::Abs($actualTotalPass - $sharedPass) / [math]::Max($sharedPass, 1)
$indepErr = [math]::Abs($actualTotalPass - $indepPass) / [math]::Max($indepPass, 1)
$isShared = $gatewaysAlive -and ($shareErr -le $Tolerance) -and ($shareErr -lt $indepErr)

Write-Host ""
Write-Host "── 结果 ──" -ForegroundColor Cyan
Write-Host ("总请求:        {0}  (2 × {1} × {2}s)" -f $totalReq, $QpsPerGw, $DurationSec)
Write-Host ("重叠时长:      {0:N1}s" -f $overlap)
Write-Host ("合计放行:      {0}" -f $actualTotalPass)
Write-Host ("  共享桶理论:   {0}  (Cap {1} + Rate×窗口 {2}×{3:N1})" -f [math]::Round($sharedPass), $Capacity, $Rate, $activeWindow)
Write-Host ("  独立桶理论:   {0}  (2×Cap {1} + Rate×({2:N1}+{3:N1}))" -f [math]::Round($indepPass), $Capacity, $dur1, $dur2)
Write-Host ("合计拒绝ΔR:    {0}  (gw1={1} gw2={2})" -f $deltaTotal, $delta1, $delta2)
Write-Host ("  贴近度: 共享误差 {0:P1} / 独立误差 {1:P1}" -f $shareErr, $indepErr)
Write-Host ("网关状态:      gw1={0} gw2={1}" -f $post1, $post2)
if (-not $gatewaysAlive) {
    Write-Host "❌ 结果无效：压测后网关无响应。请重启两个 gateway 后重跑。" -ForegroundColor Red
} elseif ($isShared) {
    Write-Host "✅ PASS：合计放行贴近共享桶理论——限流状态跨实例共享，避免总量翻倍" -ForegroundColor Green
} else {
    Write-Host "❌ FAIL：合计放行更接近独立桶（或两者都偏离）。检查两个 gateway 是否连到同一 Redis、同一 db、同一 path。" -ForegroundColor Red
}

# ── 报告存档 ──
$reportText = @"
Dolphin 多实例限流共享测试报告
时间: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
配置: Gw1=$GW1 Gw2=$GW2 Qps/实例=$QpsPerGw Duration=${DurationSec}s Rate=$Rate Capacity=$Capacity
网关状态: $(if ($gatewaysAlive) { "压测后存活" } else { "压测后无响应，结果无效" })
总请求: $totalReq
重叠时长: $overlap s (消费窗口: $activeWindow s)
合计放行: $actualTotalPass
  共享桶理论: $sharedPass
  独立桶理论: $indepPass
合计拒绝ΔR: $deltaTotal (gw1=$delta1 gw2=$delta2)
贴近度: 共享误差 $shareErr / 独立误差 $indepErr
结果: $(if ($isShared) { "PASS - 共享限流" } else { "FAIL" })
"@
[System.IO.File]::WriteAllText($REPORT, $reportText)
Write-Host "报告已保存: $REPORT" -ForegroundColor Gray

exit $(if ($isShared) { 0 } else { 1 })
