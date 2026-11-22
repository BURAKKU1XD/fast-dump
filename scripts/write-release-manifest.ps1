param(
    [Parameter(Mandatory = $true)]
    [string]$Binary,
    [Parameter(Mandatory = $true)]
    [string]$Target,
    [Parameter(Mandatory = $true)]
    [string]$OutputPath
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'release-common.ps1')

function Invoke-Checked {
    param([string]$Program, [string[]]$Arguments)
    $output = & $Program @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Program failed with exit code $LASTEXITCODE"
    }
    return $output
}

$binaryPath = (Resolve-Path -LiteralPath $Binary).Path
$manifestPath = [System.IO.Path]::GetFullPath($OutputPath)
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    # Манифест привязан к чистому коммиту; target/ и releases/ исключены из Git.
    $status = Invoke-Checked 'git' @('status', '--porcelain', '--untracked-files=all')
    if ($status) {
        throw 'Release manifest requires a clean working tree.'
    }
    $buildContext = Get-ReleaseBuildContext -RepositoryRoot $repoRoot
    Assert-ReleaseFilePrivacy -Path @($binaryPath, (Join-Path $repoRoot 'Cargo.lock')) -PathMap $buildContext.PathMap
    $commit = Invoke-Checked 'git' @('rev-parse', 'HEAD')
    $tree = Invoke-Checked 'git' @('rev-parse', 'HEAD^{tree}')
    $metadataJson = Invoke-Checked 'cargo' @('metadata', '--locked', '--offline', '--no-deps', '--format-version', '1')
    $metadata = ($metadataJson -join "`n") | ConvertFrom-Json
    $package = $metadata.packages | Where-Object { $_.name -eq 'fast-dump' }
    if (@($package).Count -ne 1) {
        throw 'Expected exactly one fast-dump package.'
    }
    $compiler = Invoke-Checked 'rustc' @('--version', '--verbose')
    $cargoVersion = Invoke-Checked 'cargo' @('--version')
    $manifest = [ordered]@{
        package = $package.name
        version = $package.version
        commit = $commit
        source_tree = $tree
        target = $Target
        profile = 'release'
        features = @()
        rustc = $compiler -join "`n"
        cargo = $cargoVersion -join "`n"
        cargo_lock_sha256 = (Get-FileHash -LiteralPath Cargo.lock -Algorithm SHA256).Hash.ToLowerInvariant()
        binary = [System.IO.Path]::GetFileName($binaryPath)
        binary_bytes = (Get-Item -LiteralPath $binaryPath).Length
        binary_sha256 = (Get-FileHash -LiteralPath $binaryPath -Algorithm SHA256).Hash.ToLowerInvariant()
        created_utc = [DateTime]::UtcNow.ToString('o')
    }
    $manifestJson = $manifest | ConvertTo-Json -Depth 4
    Assert-ReleaseBytesPrivacy -Bytes ([System.Text.Encoding]::UTF8.GetBytes($manifestJson)) `
        -Description 'release manifest' -PathMap $buildContext.PathMap
    $manifestDir = Split-Path -Parent $manifestPath
    New-Item -ItemType Directory -Force -Path $manifestDir | Out-Null
    $manifestJson | Set-Content -LiteralPath $manifestPath -Encoding utf8
}
finally {
    Pop-Location
}
