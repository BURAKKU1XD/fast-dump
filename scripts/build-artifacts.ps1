param(
    [switch]$RequireAll
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$distDir = Join-Path $repoRoot 'dist'

New-Item -ItemType Directory -Force -Path $distDir | Out-Null

function Get-InstalledTargets {
    $targets = @{}
    foreach ($target in (& rustup target list --installed)) {
        $trimmed = $target.Trim()
        if ($trimmed) {
            $targets[$trimmed] = $true
        }
    }
    return $targets
}

function Get-HostTriple {
    $hostLine = & rustc -vV | Select-String '^host:\s+'
    if (-not $hostLine) {
        throw 'failed to determine rust host triple'
    }

    return $hostLine.ToString().Split(':', 2)[1].Trim()
}

function Test-MuslToolchainAvailable {
    foreach ($name in @(
        'CC_x86_64-unknown-linux-musl',
        'CC_x86_64_unknown_linux_musl',
        'TARGET_CC',
        'CC'
    )) {
        $value = [System.Environment]::GetEnvironmentVariable($name)
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            return $true
        }
    }

    return $null -ne (Get-Command 'x86_64-linux-musl-gcc' -ErrorAction SilentlyContinue)
}

function Get-ZigExe {
    $localZig = Join-Path $repoRoot '.tools\zig\zig-x86_64-windows-0.15.2\zig.exe'
    if (Test-Path $localZig) {
        return $localZig
    }

    $command = Get-Command 'zig' -ErrorAction SilentlyContinue
    if ($null -ne $command) {
        return $command.Source
    }

    return $null
}

function Test-CargoZigbuildAvailable {
    return $null -ne (Get-Command 'cargo-zigbuild.exe' -ErrorAction SilentlyContinue)
}

function Get-TargetAvailability {
    param(
        [string]$Triple,
        [hashtable]$InstalledTargets,
        [string]$HostTriple,
        [bool]$HasMuslToolchain,
        [string]$ZigExe,
        [bool]$HasCargoZigbuild
    )

    switch ($Triple) {
        'x86_64-pc-windows-msvc' {
            if ($HostTriple -notlike '*windows*') {
                return @{
                    available = $false
                    reason = "current host $HostTriple cannot produce the Windows MSVC artifact"
                }
            }
        }
        'x86_64-unknown-linux-musl' {
            if (-not $InstalledTargets.ContainsKey($Triple)) {
                return @{
                    available = $false
                    reason = "rustup target $Triple is not installed"
                    method = ''
                }
            }

            if ($HasMuslToolchain) {
                return @{
                    available = $true
                    reason = ''
                    method = 'cargo'
                }
            }

            if ($ZigExe -and $HasCargoZigbuild) {
                return @{
                    available = $true
                    reason = ''
                    method = 'zigbuild'
                }
            }

            return @{
                available = $false
                reason = 'missing musl C toolchain and no zig/cargo-zigbuild fallback'
                method = ''
            }
        }
    }

    if (-not $InstalledTargets.ContainsKey($Triple)) {
        return @{
            available = $false
            reason = "rustup target $Triple is not installed"
            method = ''
        }
    }

    return @{
        available = $true
        reason = ''
        method = 'cargo'
    }
}

Push-Location $repoRoot

try {
    $installedTargets = Get-InstalledTargets
    $hostTriple = Get-HostTriple
    $hasMuslToolchain = Test-MuslToolchainAvailable
    $zigExe = Get-ZigExe
    $hasCargoZigbuild = Test-CargoZigbuildAvailable
    $specs = @(
        @{
            platform = 'windows-x86_64'
            triple = 'x86_64-pc-windows-msvc'
            artifact = 'fast-dump-windows-x86_64.exe'
            builtFile = 'fast-dump.exe'
        },
        @{
            platform = 'linux-x86_64-musl'
            triple = 'x86_64-unknown-linux-musl'
            artifact = 'fast-dump-linux-x86_64-musl'
            builtFile = 'fast-dump'
        }
    )

    $results = foreach ($spec in $specs) {
        $availability = Get-TargetAvailability `
            -Triple $spec.triple `
            -InstalledTargets $installedTargets `
            -HostTriple $hostTriple `
            -HasMuslToolchain $hasMuslToolchain `
            -ZigExe $zigExe `
            -HasCargoZigbuild $hasCargoZigbuild
        if (-not $availability.available) {
            [pscustomobject]@{
                platform = $spec.platform
                target = $spec.triple
                status = 'skipped'
                artifact = $spec.artifact
                note = $availability.reason
            }
            continue
        }

        Write-Host "==> Building $($spec.platform) ($($spec.triple))"
        if ($availability.method -eq 'zigbuild') {
            $zigDir = Split-Path -Parent $zigExe
            $env:Path = "$zigDir;$env:Path"
            & cargo zigbuild --release --target $spec.triple --bin fast-dump
        }
        else {
            & cargo build --release --target $spec.triple --bin fast-dump
        }
        if ($LASTEXITCODE -ne 0) {
            throw "build failed for $($spec.triple)"
        }

        $builtPath = Join-Path $repoRoot "target\$($spec.triple)\release\$($spec.builtFile)"
        if (-not (Test-Path $builtPath)) {
            throw "expected build artifact is missing: $builtPath"
        }

        $artifactPath = Join-Path $distDir $spec.artifact
        Copy-Item $builtPath $artifactPath -Force

        [pscustomobject]@{
            platform = $spec.platform
            target = $spec.triple
            status = 'built'
            artifact = $spec.artifact
            note = "$($availability.method): $artifactPath"
        }
    }

    Write-Host ''
    $results | Format-Table platform, target, status, artifact, note -AutoSize

    $failed = @($results | Where-Object status -eq 'failed')
    $skipped = @($results | Where-Object status -eq 'skipped')
    if ($failed.Count -gt 0) {
        throw 'one or more target builds failed'
    }
    if ($RequireAll -and $skipped.Count -gt 0) {
        throw 'not all supported targets are available in the current environment'
    }
}
finally {
    Pop-Location
}
