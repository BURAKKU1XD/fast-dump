param(
    [switch]$RequireAll
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$artifactDir = Join-Path $repoRoot 'releases/local'

New-Item -ItemType Directory -Force -Path $artifactDir | Out-Null

function Get-InstalledTargets {
    $sysroot = & rustc --print sysroot
    if ($LASTEXITCODE -ne 0) { throw 'rustc --print sysroot failed' }
    if ([string]::IsNullOrWhiteSpace($sysroot)) {
        throw 'rustc returned an empty sysroot'
    }
    $sysroot = $sysroot.Trim()
    if (-not (Test-Path -LiteralPath $sysroot -PathType Container)) {
        throw "Rust sysroot directory does not exist: $sysroot"
    }

    $targets = @{}
    $rustlibDir = Join-Path $sysroot 'lib/rustlib'
    if (-not (Test-Path -LiteralPath $rustlibDir -PathType Container)) {
        return $targets
    }

    # Проверяем библиотеки активного компилятора: пустой каталог ещё не означает установленный target.
    foreach ($target in (Get-ChildItem -LiteralPath $rustlibDir -Directory)) {
        $libDir = Join-Path $target.FullName 'lib'
        if (-not (Test-Path -LiteralPath $libDir -PathType Container)) {
            continue
        }
        $standardLibrary = Get-ChildItem -LiteralPath $libDir -Filter 'libstd-*.rlib' -File |
            Select-Object -First 1
        if ($null -ne $standardLibrary) {
            $targets[$target.Name] = $true
        }
    }
    return $targets
}

function Get-HostTriple {
    $version = & rustc -vV
    if ($LASTEXITCODE -ne 0) { throw 'rustc -vV failed' }
    $hostLine = $version | Select-String '^host:\s+'
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
    return $null -ne (Get-Command 'cargo-zigbuild' -ErrorAction SilentlyContinue)
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
                    reason = "standard library for $Triple is not installed in the active Rust toolchain"
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
            reason = "standard library for $Triple is not installed in the active Rust toolchain"
            method = ''
        }
    }

    return @{
        available = $true
        reason = ''
        method = 'cargo'
    }
}

$originalPath = $env:Path
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
        },
        @{
            platform = 'linux-x86_64-musl'
            triple = 'x86_64-unknown-linux-musl'
            artifact = 'fast-dump-linux-x86_64-musl'
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
            $env:Path = $zigDir + [IO.Path]::PathSeparator + $originalPath
        }
        else {
            $env:Path = $originalPath
        }
        $builtPath = & (Join-Path $PSScriptRoot 'build-release.ps1') `
            -Target $spec.triple -BuildTool $availability.method
        $artifactPath = Join-Path $artifactDir $spec.artifact
        Copy-Item -LiteralPath $builtPath -Destination $artifactPath -Force

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

    $skipped = @($results | Where-Object status -eq 'skipped')
    if ($RequireAll -and $skipped.Count -gt 0) {
        throw 'not all supported targets are available in the current environment'
    }
}
finally {
    $env:Path = $originalPath
    Pop-Location
}
