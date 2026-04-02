$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$outputRoot = Join-Path $repoRoot 'target\bench-threshold-confirm'
New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

$thresholds = [ordered]@{
    '8MiB' = 8388608
    '16MiB' = 16777216
}

$datasets = 'mixed,bigFiles,manySmallFiles'
$repeats = 10

foreach ($pair in $thresholds.GetEnumerator()) {
    Write-Host "=== confirm sweep: $($pair.Key) ($($pair.Value) bytes) ==="
    Remove-Item -Recurse -Force (Join-Path $repoRoot 'target\bench-matrix') -ErrorAction SilentlyContinue
    $env:FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES = [string]$pair.Value

    cargo bench --bench loopback_send_dump -- --dataset $datasets --threads 1 --frames 16 --buffers 64 --repeats $repeats

    $dest = Join-Path $outputRoot $pair.Key
    New-Item -ItemType Directory -Force -Path $dest | Out-Null
    Copy-Item (Join-Path $repoRoot 'target\bench-matrix\results_raw.csv') (Join-Path $dest 'results_raw.csv') -Force
    Copy-Item (Join-Path $repoRoot 'target\bench-matrix\results_summary.csv') (Join-Path $dest 'results_summary.csv') -Force
}

$rows = foreach ($pair in $thresholds.GetEnumerator()) {
    $summaryPath = Join-Path (Join-Path $outputRoot $pair.Key) 'results_summary.csv'
    foreach ($row in Import-Csv $summaryPath) {
        [pscustomobject]@{
            threshold_label = $pair.Key
            threshold_bytes = $pair.Value
            dataset = $row.dataset
            threads = $row.threads
            frame_target_mb = $row.frame_target_mb
            read_buffer_kb = $row.read_buffer_kb
            completed_repeats = $row.completed_repeats
            sent_frames = $row.sent_frames
            total_duration_ms = $row.total_duration_ms
            ack_wait_ms = $row.ack_wait_ms
            compression_ratio_pct = $row.compression_ratio_pct
            max_rss_mib = $row.max_rss_mib
            total_mib_s = $row.total_mib_s
            frame_aborts = $row.frame_aborts
            frame_rebuilds = $row.frame_rebuilds
        }
    }
}

$rows |
    Sort-Object dataset, threshold_bytes |
    Export-Csv (Join-Path $outputRoot 'confirm_summary.csv') -NoTypeInformation -Encoding UTF8

$rows |
    Sort-Object dataset, total_duration_ms |
    Format-Table dataset, threshold_label, completed_repeats, sent_frames, total_duration_ms, ack_wait_ms, compression_ratio_pct, max_rss_mib, total_mib_s -AutoSize |
    Out-Host

Remove-Item Env:FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES -ErrorAction SilentlyContinue
