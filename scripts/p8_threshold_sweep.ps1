$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$sweepRoot = Join-Path $repoRoot 'target\bench-threshold-sweep'
New-Item -ItemType Directory -Force -Path $sweepRoot | Out-Null

$thresholds = [ordered]@{
    'baseline-64KiB' = 65537
    '1MiB' = 1048576
    '8MiB' = 8388608
    '16MiB' = 16777216
    '32MiB' = 33554432
}

function Invoke-MatrixRun {
    param(
        [string]$DatasetList,
        [int]$Repeats,
        [string]$OutputPrefix
    )

    foreach ($pair in $thresholds.GetEnumerator()) {
        Write-Host "=== $OutputPrefix sweep: $($pair.Key) ($($pair.Value) bytes) ==="
        Remove-Item -Recurse -Force (Join-Path $repoRoot 'target\bench-matrix') -ErrorAction SilentlyContinue
        $env:FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES = [string]$pair.Value

        cargo bench --bench loopback_send_dump -- --dataset $DatasetList --threads 1 --frames 16 --buffers 64 --repeats $Repeats

        $dest = Join-Path $sweepRoot $pair.Key
        New-Item -ItemType Directory -Force -Path $dest | Out-Null
        Copy-Item (Join-Path $repoRoot 'target\bench-matrix\results_raw.csv') (Join-Path $dest "${OutputPrefix}_results_raw.csv") -Force
        Copy-Item (Join-Path $repoRoot 'target\bench-matrix\results_summary.csv') (Join-Path $dest "${OutputPrefix}_results_summary.csv") -Force
    }
}

function Load-ThresholdSummary {
    param(
        [string]$SummaryFileName,
        [string[]]$Datasets
    )

    foreach ($pair in $thresholds.GetEnumerator()) {
        $summaryPath = Join-Path (Join-Path $sweepRoot $pair.Key) $SummaryFileName
        $rows = Import-Csv $summaryPath
        foreach ($dataset in $Datasets) {
            $row = $rows | Where-Object { $_.dataset -eq $dataset }
            if ($null -eq $row) {
                continue
            }
            [pscustomobject]@{
                threshold_label = $pair.Key
                threshold_bytes = $pair.Value
                dataset = $dataset
                sent_frames = [double]$row.sent_frames
                total_duration_ms = [double]$row.total_duration_ms
                ack_wait_ms = [double]$row.ack_wait_ms
                compression_ratio_pct = [double]$row.compression_ratio_pct
                max_rss_mib = [double]$row.max_rss_mib
                total_mib_s = [double]$row.total_mib_s
                frame_aborts = [double]$row.frame_aborts
                frame_rebuilds = [double]$row.frame_rebuilds
            }
        }
    }
}

Invoke-MatrixRun -DatasetList 'mixed' -Repeats 3 -OutputPrefix 'mixed'
$mixedRows = Load-ThresholdSummary -SummaryFileName 'mixed_results_summary.csv' -Datasets @('mixed')
$mixedRows | Sort-Object total_duration_ms | Format-Table -AutoSize | Out-Host
$mixedRows | Export-Csv (Join-Path $sweepRoot 'mixed_threshold_summary.csv') -NoTypeInformation -Encoding UTF8

$bestMixed = $mixedRows | Sort-Object total_duration_ms, ack_wait_ms | Select-Object -First 1
$followupLabels = @('baseline-64KiB', '16MiB')
if ($bestMixed.threshold_label -notin $followupLabels) {
    $followupLabels += $bestMixed.threshold_label
}
$followupThresholds = [ordered]@{}
foreach ($label in $followupLabels | Select-Object -Unique) {
    $followupThresholds[$label] = $thresholds[$label]
}
$thresholds = $followupThresholds

Invoke-MatrixRun -DatasetList 'bigFiles,manySmallFiles' -Repeats 2 -OutputPrefix 'followup'
$followupRows = Load-ThresholdSummary -SummaryFileName 'followup_results_summary.csv' -Datasets @('bigFiles', 'manySmallFiles')
$followupRows | Sort-Object dataset, total_duration_ms | Format-Table -AutoSize | Out-Host
$followupRows | Export-Csv (Join-Path $sweepRoot 'followup_threshold_summary.csv') -NoTypeInformation -Encoding UTF8

Remove-Item Env:FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES -ErrorAction SilentlyContinue
