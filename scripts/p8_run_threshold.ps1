param(
    [Parameter(Mandatory = $true)]
    [string]$ThresholdLabel,

    [Parameter(Mandatory = $true)]
    [UInt64]$ThresholdBytes,

    [string]$Datasets = 'mixed,bigFiles,manySmallFiles',
    [UInt32]$Repeats = 100,
    [string]$OutputRoot = 'target\bench-threshold-confirm-100',
    [UInt32]$Threads = 1,
    [UInt64]$FrameTargetMiB = 16,
    [UInt64]$ReadBufferKiB = 64
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$outputDir = Join-Path $repoRoot $OutputRoot
$thresholdDir = Join-Path $outputDir $ThresholdLabel
New-Item -ItemType Directory -Force -Path $thresholdDir | Out-Null

Remove-Item -Recurse -Force (Join-Path $repoRoot 'target\bench-matrix') -ErrorAction SilentlyContinue
$env:FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES = [string]$ThresholdBytes

Write-Host "=== threshold $ThresholdLabel ($ThresholdBytes bytes) repeats=$Repeats datasets=$Datasets ==="
cargo bench --bench loopback_send_dump -- --dataset $Datasets --threads $Threads --frames $FrameTargetMiB --buffers $ReadBufferKiB --repeats $Repeats

Copy-Item (Join-Path $repoRoot 'target\bench-matrix\results_raw.csv') (Join-Path $thresholdDir 'results_raw.csv') -Force
Copy-Item (Join-Path $repoRoot 'target\bench-matrix\results_summary.csv') (Join-Path $thresholdDir 'results_summary.csv') -Force

Remove-Item Env:FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES -ErrorAction SilentlyContinue
