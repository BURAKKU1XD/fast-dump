$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$dest = Join-Path $repoRoot 'target\bench-threshold-sweep\8MiB'
New-Item -ItemType Directory -Force -Path $dest | Out-Null

$env:FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES = '8388608'
Remove-Item -Recurse -Force (Join-Path $repoRoot 'target\bench-matrix') -ErrorAction SilentlyContinue

cargo bench --bench loopback_send_dump -- --dataset bigFiles,manySmallFiles --threads 1 --frames 16 --buffers 64 --repeats 2

Copy-Item (Join-Path $repoRoot 'target\bench-matrix\results_raw.csv') (Join-Path $dest 'followup_results_raw.csv') -Force
Copy-Item (Join-Path $repoRoot 'target\bench-matrix\results_summary.csv') (Join-Path $dest 'followup_results_summary.csv') -Force

$rows = Import-Csv (Join-Path $dest 'followup_results_summary.csv') |
    Where-Object { $_.dataset -in @('bigFiles', 'manySmallFiles') }
$rows |
    Select-Object dataset, threads, frame_target_mb, read_buffer_kb, sent_frames,
        total_duration_ms, ack_wait_ms, compression_ratio_pct, max_rss_mib, total_mib_s |
    Format-Table -AutoSize |
    Out-Host

Remove-Item Env:FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES -ErrorAction SilentlyContinue
