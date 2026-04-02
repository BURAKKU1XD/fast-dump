$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$sweepRoot = Join-Path $repoRoot 'target\bench-threshold-sweep'
$thresholds = [ordered]@{
    'baseline-64KiB' = 65537
    '1MiB' = 1048576
    '8MiB' = 8388608
    '16MiB' = 16777216
    '32MiB' = 33554432
}

$rows = foreach ($pair in $thresholds.GetEnumerator()) {
    $dir = Join-Path $sweepRoot $pair.Key
    foreach ($summaryName in @('mixed_results_summary.csv', 'followup_results_summary.csv')) {
        $summaryPath = Join-Path $dir $summaryName
        if (-not (Test-Path $summaryPath)) {
            continue
        }

        foreach ($row in Import-Csv $summaryPath) {
            [pscustomobject]@{
                threshold_label = $pair.Key
                threshold_bytes = $pair.Value
                source_file = $summaryName
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
}

$rows |
    Sort-Object dataset, threshold_bytes |
    Export-Csv (Join-Path $sweepRoot 'overall_threshold_summary.csv') -NoTypeInformation -Encoding UTF8
