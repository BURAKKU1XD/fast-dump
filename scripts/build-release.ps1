#Requires -Version 7.0
param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9_][A-Za-z0-9_.-]+$')]
    [string]$Target,
    [ValidateSet('cargo', 'zigbuild')]
    [string]$BuildTool = 'cargo'
)

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'release-common.ps1')
$repoRoot = Split-Path -Parent $PSScriptRoot
$previousEncodedFlags = [Environment]::GetEnvironmentVariable('CARGO_ENCODED_RUSTFLAGS')
Push-Location $repoRoot
try {
    $context = Get-ReleaseBuildContext -RepositoryRoot $repoRoot
    $flags = @(Get-ReleaseRustFlags -PathMap $context.PathMap -Target $Target)
    $env:CARGO_ENCODED_RUSTFLAGS = $flags -join [char]0x1f
    $subcommand = if ($BuildTool -eq 'zigbuild') { 'zigbuild' } else { 'build' }
    # Настройки профиля ограничены этим вызовом; обычные команды Cargo не меняются.
    & cargo $subcommand --locked --release --target $Target --bin fast-dump `
        --config 'profile.release.debug=0' --config 'profile.release.strip="debuginfo"' | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "Release build failed for $Target" }

    $binaryName = if ($Target -like '*-windows-*') { 'fast-dump.exe' } else { 'fast-dump' }
    $binaryPath = Join-Path $context.TargetDirectory "$Target/release/$binaryName"
    Assert-ReleaseFilePrivacy -Path $binaryPath -PathMap $context.PathMap
    # Только проверенный путь попадает к вызывающему скрипту для копирования и упаковки.
    (Resolve-Path -LiteralPath $binaryPath).ProviderPath
}
finally {
    [Environment]::SetEnvironmentVariable('CARGO_ENCODED_RUSTFLAGS', $previousEncodedFlags)
    Pop-Location
}
