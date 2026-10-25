param(
    [Parameter(Mandatory = $true)][string]$Plan,
    [switch]$Execute
)

$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot)).TrimEnd('\', '/')
$repoPrefix = $repoRoot + [IO.Path]::DirectorySeparatorChar
$allowedBuilds = @('debug', 'release', 'linux-tui', 'linux-tui-msrv', 'msrv-tui',
    'perf-tui', 'pipeline-status-validation', 'x86_64-pc-windows-msvc',
    'x86_64-unknown-linux-musl', 'zig-cache', 'zig-global-cache', 'zig-local-cache', '__pycache__')

function Assert-RepositoryPath([string]$Path) {
    $absolute = [IO.Path]::GetFullPath($Path)
    if (-not $absolute.StartsWith($repoPrefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Path escapes repository: $Path"
    }
    $relative = $absolute.Substring($repoPrefix.Length).Replace('\', '/')
    if ($relative.StartsWith('target/cleanup', [StringComparison]::OrdinalIgnoreCase)) {
        throw "Active cleanup artifacts are protected: $Path"
    }
    # Проверка каждого родителя исключает переход через junction или symlink.
    $current = $absolute
    while ($current -ne $repoRoot) {
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "Reparse point is not allowed: $current"
            }
        }
        $current = Split-Path -Parent $current
    }
    return $absolute
}

function Assert-Hash([string]$Path, [string]$Expected) {
    $absolute = Assert-RepositoryPath $Path
    if (-not (Test-Path -LiteralPath $absolute -PathType Leaf)) {
        throw "Required file is missing: $absolute"
    }
    if ((Get-FileHash -LiteralPath $absolute -Algorithm SHA256).Hash -ne $Expected) {
        throw "File changed since preparation: $absolute"
    }
}

$planPath = Assert-RepositoryPath $Plan
$data = Get-Content -LiteralPath $planPath -Raw -Encoding UTF8 | ConvertFrom-Json
if ($data.schema_version -ne 1 -or $data.repository -ne $repoRoot) {
    throw 'The plan belongs to another repository or schema.'
}
$historyRoot = Assert-RepositoryPath $data.history_root
$historyRelative = $historyRoot.Substring($repoPrefix.Length).Replace('\', '/')
if ($historyRelative -notmatch '^results/history/\d{4}-\d{2}-\d{2}$') {
    throw 'Unexpected historical archive root.'
}
$archiveRoot = Assert-RepositoryPath $data.archive_root
$archiveRelative = $archiveRoot.Substring($repoPrefix.Length).Replace('\', '/')
if ($archiveRelative -notmatch '^results/history/\d{4}-\d{2}-\d{2}$' -or $archiveRoot -eq $historyRoot) {
    throw 'Unexpected evidence destination.'
}
Assert-Hash $data.preservation_manifest $data.preservation_manifest_sha256
$preservation = Get-Content -LiteralPath $data.preservation_manifest -Raw -Encoding UTF8 | ConvertFrom-Json
$preservedBySource = @{}
foreach ($entry in $preservation.files) {
    Assert-Hash $entry.preserved_path $entry.sha256
    if ($entry.path) { $preservedBySource[$entry.path] = $entry.preserved_path }
}
foreach ($bundle in $preservation.zip_evidence) {
    foreach ($entry in $bundle.evidence) {
        Assert-Hash $entry.preserved_path $entry.sha256
    }
}

$paths = [Collections.Generic.List[string]]::new()
$seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
foreach ($entry in $data.entries) {
    $absolute = Assert-RepositoryPath $entry.path
    $relative = $absolute.Substring($repoPrefix.Length).Replace('\', '/')
    if ($relative -ne $entry.relative_path -or -not $seen.Add($absolute)) {
        throw "Inconsistent or duplicate plan entry: $absolute"
    }
    $allowed = $false
    if ($relative.StartsWith('target/')) {
        $parts = $relative.Split('/')
        $allowed = ($parts.Count -gt 2 -and $allowedBuilds -contains $parts[1] -and $entry.category -eq 'build-cache')
        $allowed = $allowed -or ($relative -eq 'target/.rustc_info.json' -and $entry.category -eq 'build-cache')
        $allowed = $allowed -or ($parts[1] -eq 'stdlib-vendor-repair-20261025-001029429Z' -and $entry.category -eq 'downloaded-build-dependencies')
        $allowed = $allowed -or (@('local-release-smoke', 'linux-windows-archive-verification') -contains $parts[1] -and
            @('verified-generated-dump', 'duplicate-verified-generated-dump', 'generated-smoke-fixture', 'generated-smoke-lock') -contains $entry.category)
        $allowed = $allowed -or ($entry.category -eq 'archived-current-evidence' -and $preservedBySource.ContainsKey($absolute))
    }
    elseif ($relative.StartsWith('releases/')) {
        $allowed = @('compiled-binary', 'release-package') -contains $entry.category
        $allowed = $allowed -or ($entry.category -eq 'archived-release-evidence' -and $preservedBySource.ContainsKey($absolute))
    }
    elseif ($relative.StartsWith($historyRelative + '/')) {
        $allowed = @('compiled-binary', 'debug-symbols', 'release-package', 'verified-generated-dump',
            'duplicate-verified-generated-dump', 'generated-compatibility-fixture') -contains $entry.category
        $allowed = $allowed -or ($entry.category -eq 'mixed-evidence-bundle' -and
            @('full-dump.zip', 'measurements.zip', 'core-readers-change.zip') -contains (Split-Path -Leaf $absolute))
    }
    if (-not $allowed) { throw "Path/category is outside the cleanup allowlist: $absolute" }
    Assert-Hash $absolute $entry.sha256
    if ((Get-Item -LiteralPath $absolute).Length -ne $entry.bytes) {
        throw "File size changed since preparation: $absolute"
    }
    $paths.Add($absolute)
}

# До этой точки никаких удалений: сначала проверяем весь план и сохранённые копии.
Write-Output "Verified $($paths.Count) files; planned bytes: $($data.summary.deletion_bytes)."
if (-not $Execute) {
    Write-Output 'Validation only. Pass -Execute to remove exactly these verified files.'
    exit 0
}
for ($index = 0; $index -lt $paths.Count; $index += 256) {
    $last = [Math]::Min($index + 255, $paths.Count - 1)
    Remove-Item -LiteralPath $paths[$index..$last] -Force
}
Write-Output 'Removed the verified files. Empty directories were retained.'
