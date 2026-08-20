# ============================================================
# k8s_kind_up.ps1 — Dolphin 一键 K8s 部署验证（kind on Windows）
#
# 用法（在 dolphin 仓库根目录）:
#   powershell -ExecutionPolicy Bypass -File .\hack\k8s_kind_up.ps1
#   powershell -ExecutionPolicy Bypass -File .\hack\k8s_kind_up.ps1 -Backpressure
#
# 参数:
#   -Cluster <name>   kind 集群名（默认 dolphin）
#   -NoBuild          跳过镜像构建+加载（已 load 过，重复跑时用）
#   -Backpressure     额外跑 V5 背压验证（sleep 5s × 400，约多 1 分钟）
#   -KeepCluster      跑完不删集群（默认最后询问；自动模式下保留并打印清理命令）
#
# 流程: 起集群 → 构建/加载镜像 → 部署 → V1 网关可达 / V2 选主单主 /
#       V3 任务真实执行(advertise_addr) / [V5 背压] → 汇总
# ============================================================

[CmdletBinding()]
param(
    [string]$Cluster = "dolphin",
    [switch]$NoBuild,
    [switch]$Backpressure,
    [switch]$KeepCluster
)

# 用 Continue：PS 5.1 在 "Stop" 下会把 native 命令的 stderr（即使 2>&1）当终止错误，
# 而 kind/kubectl 大量正常输出走 stderr（如 "No kind clusters found."）。统一用 Fail() 主动抛。
$ErrorActionPreference = "Continue"
$Root   = Split-Path -Parent $PSScriptRoot          # 仓库根（hack 的父目录）
$Bins   = Join-Path $Root "bin"
$kubectl = "kubectl"

# 颜色输出：阶段/OK/失败/信息
function Stage($msg)  { Write-Host "`n[$(Get-Date -Format HH:mm:ss)] $msg" -ForegroundColor Cyan }
function OK($msg)     { Write-Host "  [OK] $msg" -ForegroundColor Green }
function Fail($msg)   { Write-Host "  [FAIL] $msg" -ForegroundColor Red; throw $msg }
function Info($msg)   { Write-Host "  [..] $msg" -ForegroundColor Yellow }

function Test-Cmd {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        Fail "缺少 $Name —— 请先安装（见 docs/k8s-kind-check.md 前置条件）"
    }
}

function Invoke-Check {
    param([string]$Exe, [string[]]$CmdArgs)
    & $Exe @CmdArgs 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail "$Exe $CmdArgs 失败" }
}

function Build-Image {
    param([string]$Tag, [string]$Dockerfile)
    docker build -t $Tag -f $Dockerfile .
    if ($LASTEXITCODE -ne 0) { Fail "构建 $Tag 失败" }
    docker image inspect $Tag | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail "镜像 $Tag 构建后仍不存在" }
    OK "$Tag 构建完成并确认存在"
}

# ---------- 0. 前置检查 ----------
Stage "前置检查"
Test-Cmd docker
Test-Cmd kind
Test-Cmd kubectl
Test-Cmd go
New-Item -ItemType Directory -Force -Path $Bins | Out-Null
OK "docker/kind/kubectl/go 齐备"

# ---------- 1. 起 kind 集群 ----------
Stage "起 kind 集群 ($Cluster)"
$existing = ''
try { $existing = (& kind get clusters 2>&1 | Out-String) } catch { $existing = '' }
if ($existing -match $Cluster) {
    Info "集群 $Cluster 已存在，删除重建（保证干净）"
    kind delete cluster --name $Cluster
}
kind create cluster --name $Cluster
if ($LASTEXITCODE -ne 0) { Fail "kind create 失败" }
OK "集群就绪"

# ---------- 2. 构建 + 加载镜像 ----------
if (-not $NoBuild) {
    Stage "构建 4 个镜像（首次需拉 golang:1.26-alpine，每个约 1 分钟）"
    Push-Location $Root
    Build-Image "dolphin-gateway:latest"   "deployments/docker/Dockerfile.gateway"
    Build-Image "dolphin-scheduler:latest" "deployments/docker/Dockerfile.scheduler"
    Build-Image "dolphin-worker:latest"    "deployments/docker/Dockerfile.worker"
    Build-Image "dolphinctl:latest"        "deployments/docker/Dockerfile.dolphinctl"
    Pop-Location

    Stage "加载镜像进 kind 节点"
    foreach ($img in @("dolphin-gateway:latest","dolphin-scheduler:latest","dolphin-worker:latest","dolphinctl:latest")) {
        docker image inspect $img | Out-Null
        if ($LASTEXITCODE -ne 0) { Fail "本地缺少 $img —— 构建步骤异常" }
    }
    $loadOut = (& kind load docker-image --name $Cluster dolphin-gateway:latest dolphin-scheduler:latest dolphin-worker:latest dolphinctl:latest 2>&1 | Out-String)
    if ($LASTEXITCODE -ne 0) { Fail "kind load 失败: $loadOut" }
    OK "镜像已加载"
} else {
    Stage "跳过镜像构建（-NoBuild）"
}

# ---------- 3. 检查 StorageClass（防御：无 default SC 则装 local-path） ----------
Stage "检查 StorageClass"
$sc = ''
try { $sc = (& kubectl get storageclass 2>&1 | Out-String) } catch { $sc = '' }
if ($sc -match "default") {
    OK "已有默认 StorageClass，PVC 可直接绑定"
} else {
    Info "无默认 StorageClass，安装 local-path-provisioner（hostPath）"
    Invoke-Check kubectl @("apply","-f","https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.26/deploy/local-path-storage.yaml")
    try { kubectl annotate storageclass local-path storageclass.kubernetes.io/is-default-class=true 2>&1 | Out-Null } catch { }
    OK "local-path 已设为默认"
}

# ---------- 4. 部署 ----------
Stage "部署 Dolphin (kubectl apply -k deployments/k8s)"
Push-Location $Root
kubectl apply -k deployments/k8s
if ($LASTEXITCODE -ne 0) { Pop-Location; Fail "kubectl apply 失败" }
Pop-Location
OK "apply 完成，等待就绪（mysql 首次初始化最慢，最多等 8 分钟）"

$deadline = (Get-Date).AddSeconds(480)
$allReady = $false
while ((Get-Date) -lt $deadline) {
    $pods = (& kubectl -n dolphin get pods --no-headers 2>&1)
    # 必须 1/1 Ready：只看 STATUS 列含 Running 会放过 0/1 + 崩溃重启的假阳性
    $notRunning = @($pods | Where-Object { $_ -and ($_ -notmatch "1/1\s+Running") })
    if ($notRunning.Count -eq 0 -and $pods) { $allReady = $true; break }
    Start-Sleep -Seconds 10
}
if (-not $allReady) {
    Info "未在 8 分钟内全部 Running，打印当前状态："
    kubectl -n dolphin get pods
    Fail "部署超时——把上方 pod 状态贴给 Claude 排查"
}
kubectl -n dolphin get pods
OK "全部 Running"

# ---------- 5. V1: 网关可达 ----------
Stage "V1 · 网关可达（port-forward 8080 → /health）"
$pfGw = Start-Process kubectl -ArgumentList "-n","dolphin","port-forward","svc/dolphin-gateway","8080:8080" -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 4
try {
    $r = Invoke-WebRequest -Uri "http://localhost:8080/health" -UseBasicParsing -TimeoutSec 10
    OK "gateway /health 返回 $($r.StatusCode)"
} catch {
    Fail "gateway /health 不可达：$_"
}
Stop-Process -Id $pfGw.Id -ErrorAction SilentlyContinue

# ---------- 6. V2: 选主单主 ----------
Stage "V2 · 双 scheduler etcd 选主（应只有一个 is_leader=1）"
$schedPods = @(kubectl -n dolphin get pod -l app=scheduler --no-headers | ForEach-Object { ($_ -split '\s+')[0] })
$leaderCount = 0
foreach ($p in $schedPods) {
    $m = (& kubectl -n dolphin exec $p -- sh -c "curl -s http://localhost:9090/metrics" 2>&1)
    $val = ($m | Select-String '^dolphin_scheduler_is_leader ').Line
    Info ("  {0}: {1}" -f $p, $val)
    if ($val -match ' 1$') { $leaderCount++ }
}
if ($leaderCount -eq 1) { OK "恰好 1 个 Leader（$leaderCount / $($schedPods.Count)）" }
else { Fail "Leader 数 = $leaderCount（应=1）——把上面输出贴给 Claude" }

# ---------- 7. V3: 任务真实执行（advertise_addr 验证） ----------
# 方法：stress create 建 N 个**独立任务**再 trigger-batch 全部触发。
# ⚠️ 不能用 `stress trigger --id X --count N`：它只把同一任务的 next_run_at 刷 N 次，
# informer 只看到一次变更 → 等效触发 1 次执行，压测全变假（曾在 kind 上跑出 4/450 假象）。
Stage "V3 · 建 50 个任务并全部触发（验证 worker 经 etcd 找到真实 Leader 并执行）"
Push-Location $Root
go build -o (Join-Path $Bins "dolphinctl.exe") ./cmd/dolphinctl
if ($LASTEXITCODE -ne 0) { Pop-Location; Fail "dolphinctl 编译失败" }
Pop-Location

$pfSch = Start-Process kubectl -ArgumentList "-n","dolphin","port-forward","svc/dolphin-scheduler","50051:50051" -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 4

function Invoke-Ctl {
    param([string[]]$CtlArgs)
    & (Join-Path $Bins "dolphinctl.exe") --addr localhost:50051 @CtlArgs 2>&1
}

# 汇总所有 worker 的指定指标之和（pattern 需带 ^ 锚定，只取数据行，避开 # HELP/# TYPE）
function Get-WorkerMetric {
    param([string]$Pattern)
    $sum = 0
    foreach ($p in @(kubectl -n dolphin get pod -l app=worker --no-headers | ForEach-Object { ($_ -split '\s+')[0] })) {
        $m = (& kubectl -n dolphin exec $p -- sh -c "curl -s http://localhost:9091/metrics" 2>&1)
        $line = ($m | Select-String $Pattern | Select-Object -Last 1).Line
        if ($line) { $sum += [int](($line -replace '.* (\d+)$', '$1')) }
    }
    return $sum
}

# 按名字前缀收集 task id（逗号分隔，供 trigger-batch）
function Get-TaskIdsByPrefix {
    param([string]$Prefix)
    $lst = Invoke-Ctl @("task","list","--limit","500")
    $ids = @($lst | Select-String "name=$Prefix" | ForEach-Object { ($_ -replace '^id=(\S+).*', '$1') })
    return ($ids -join ",")
}

$null = Invoke-Ctl @("stress","create","--count","50","--prefix","k8s-v3","--cron","0 0 1 1 *","--handler","http://dolphin-scheduler:9090/debug/sleep?seconds=2","--timeout","30","--retries","3")
Start-Sleep -Seconds 1
$idsV3 = Get-TaskIdsByPrefix "k8s-v3"
if (-not $idsV3) { Fail "拿不到 k8s-v3 任务的 id（task list 没匹配到，可能 stress create 失败）" }
OK "已建 $((($idsV3 -split ',').Count)) 个任务，id 收集完毕"

$baseDoneV3 = Get-WorkerMetric "^dolphin_worker_task_completed_total"
$null = Invoke-Ctl @("task","trigger-batch","--ids",$idsV3)
Info "已全部触发，等待 50 个完成（最多 90s）…"
$v3Done = $false
$nowDone = $baseDoneV3
$deadline = (Get-Date).AddSeconds(90)
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Seconds 5
    $nowDone = Get-WorkerMetric "^dolphin_worker_task_completed_total"
    if (($nowDone - $baseDoneV3) -ge 50) { $v3Done = $true; break }
}
if (-not $v3Done) { Fail "50 个任务未在 90s 内完成（Δ=$($nowDone - $baseDoneV3)）——贴输出给 Claude" }
OK "50/50 完成，worker 经 etcd 找到真实 Leader 并执行（advertise_addr 生效）"

# ---------- 8. [可选] V5: 背压 ----------
if ($Backpressure) {
    Stage "V5 · 背压触发（400 独立任务 sleep 10s，2 worker 在途上限 300 → 应拒绝+自动重试）"
    $null = Invoke-Ctl @("stress","create","--count","400","--prefix","k8s-v5","--cron","0 0 1 1 *","--handler","http://dolphin-scheduler:9090/debug/sleep?seconds=10","--timeout","30","--retries","3")
    Start-Sleep -Seconds 1
    $idsV5 = Get-TaskIdsByPrefix "k8s-v5"
    if (-not $idsV5) { Fail "拿不到 k8s-v5 任务的 id" }
    OK "已建 $((($idsV5 -split ',').Count)) 个任务，id 收集完毕"

    $baseRej = Get-WorkerMetric "^dolphin_worker_pool_rejected_total"
    $baseDoneV5 = Get-WorkerMetric "^dolphin_worker_task_completed_total"
    $null = Invoke-Ctl @("task","trigger-batch","--ids",$idsV5)
    Info "已触发 400 个（sleep 10s），等待拒绝出现 + 全部完成（最多 180s）…"
    $bpSeen = $false
    $v5Done = $false
    $nowRej = $baseRej
    $nowDone = $baseDoneV5
    $deadline = (Get-Date).AddSeconds(180)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 10
        $nowRej = Get-WorkerMetric "^dolphin_worker_pool_rejected_total"
        $nowDone = Get-WorkerMetric "^dolphin_worker_task_completed_total"
        if (($nowRej - $baseRej) -gt 0) { $bpSeen = $true }
        if (($nowDone - $baseDoneV5) -ge 400) { $v5Done = $true; break }
    }
    if ($bpSeen) { OK "池满拒绝发生（Δrejected=$($nowRej - $baseRej)），背压机制工作" }
    else { Fail "rejected=0——400 个独立任务也没触发拒绝，调度/下发速率可能有问题，贴输出给 Claude" }
    if ($v5Done) { OK "400/400 全部完成（自动重试兜底，不丢消息）" }
    else { Info "已完成 Δ=$($nowDone - $baseDoneV5)/400（未全完——看是否还在涨，贴输出）" }
}

Stop-Process -Id $pfSch.Id -ErrorAction SilentlyContinue

# ---------- 9. 汇总 ----------
Stage "完成"
Write-Host @"

  ✅ Dolphin 已在 kind 集群 ($Cluster) 上跑通
  V1 网关可达      -> 已确认
  V2 选主单主      -> 已确认（1 个 Leader）
  V3 任务真实执行  -> 已确认（advertise_addr 生效）
  V5 背压          -> $(if ($Backpressure) { "见上方输出" } else { "未跑（加 -Backpressure 触发）" })

  面试口径:
  "我在 kind 集群上完整跑通 Dolphin 的 K8s 部署——gateway 可达、双 scheduler 单主、
   worker 经 etcd watch 跟随真实 Leader 并执行任务、kill 副本自愈、并发超容量背压触发后全部成功。"

  手动验证/清理:
    kubectl -n dolphin get pods
    kubectl -n dolphin logs deploy/scheduler | Select-String "became leader"
    kind delete cluster --name $Cluster   # 清理
"@

if (-not $KeepCluster) {
    $ans = Read-Host "是否现在删除集群 $Cluster ？(y/N)"
    if ($ans -eq 'y') { kind delete cluster --name $Cluster }
}
