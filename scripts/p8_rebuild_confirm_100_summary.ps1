$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$outputRoot = Join-Path $repoRoot 'target\bench-threshold-confirm-100'
$thresholds = [ordered]@{
    '4MiB' = 4194304
    '8MiB' = 8388608
    '16MiB' = 16777216
    '32MiB' = 33554432
}

$rows = foreach ($pair in $thresholds.GetEnumerator()) {
    $summaryPath = Join-Path (Join-Path $outputRoot $pair.Key) 'results_summary.csv'
    if (-not (Test-Path $summaryPath)) {
        continue
    }

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

$summaryPath = Join-Path $outputRoot 'confirm_summary.csv'
$rows | Sort-Object dataset, threshold_bytes | Export-Csv $summaryPath -NoTypeInformation -Encoding UTF8

$rows |
    Sort-Object dataset, total_duration_ms |
    Format-Table dataset, threshold_label, completed_repeats, sent_frames, total_duration_ms, ack_wait_ms, compression_ratio_pct, max_rss_mib, total_mib_s -AutoSize |
    Out-Host
