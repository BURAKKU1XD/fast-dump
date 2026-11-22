# Общие правила сборки и проверки распространяемых файлов; окружение машины в них не сохраняем.

function Get-ReleasePathMap {
    param(
        [string]$RepositoryRoot,
        [string]$CargoHome,
        [string]$Sysroot,
        [string]$TargetDirectory,
        [string]$UserDirectory,
        [string]$TemporaryDirectory
    )

    $prefixes = [System.Collections.Generic.Dictionary[string, string]]::new([StringComparer]::Ordinal)
    foreach ($entry in @(
        @{ From = $UserDirectory; To = '/build/home' },
        @{ From = $TemporaryDirectory; To = '/build/tmp' },
        @{ From = $CargoHome; To = '/build/cargo' },
        @{ From = $Sysroot; To = '/build/rust' },
        @{ From = $RepositoryRoot; To = '/build/src' },
        @{ From = $TargetDirectory; To = '/build/target' }
    )) {
        if ([string]::IsNullOrWhiteSpace($entry.From)) { continue }
        $prefix = $entry.From.TrimEnd([char[]]'\/')
        # Корень тома не содержит личных данных и совпал бы почти с любым путём.
        if (-not $prefix -or $prefix -match '^[A-Za-z]:$') { continue }
        foreach ($variant in @($prefix, $prefix.Replace('\', '/'), $prefix.Replace('/', '\'))) {
            $prefixes[$variant] = $entry.To
        }
    }

    # rustc выбирает последнее совпадение, поэтому вложенные каталоги идут после родителей.
    $prefixes.GetEnumerator() | Sort-Object { $_.Key.Length }, Key | ForEach-Object {
        [pscustomobject]@{ From = $_.Key; To = $_.Value }
    }
}

function Get-ReleaseBuildContext {
    param([Parameter(Mandatory = $true)][string]$RepositoryRoot)

    $RepositoryRoot = [IO.Path]::GetFullPath($RepositoryRoot, $PWD.ProviderPath)
    $metadataJson = & cargo metadata --manifest-path (Join-Path $RepositoryRoot 'Cargo.toml') --locked --offline --no-deps --format-version 1
    if ($LASTEXITCODE -ne 0) { throw 'cargo metadata failed' }
    $metadata = ($metadataJson -join "`n") | ConvertFrom-Json
    if ([string]::IsNullOrWhiteSpace($metadata.target_directory)) { throw 'Cargo target directory is missing' }
    $sysrootOutput = & rustc --print sysroot
    if ($LASTEXITCODE -ne 0) { throw 'rustc --print sysroot failed' }
    $sysroot = ($sysrootOutput -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($sysroot)) { throw 'Rust sysroot is missing' }

    $userDirectory = if ($IsWindows) { $env:USERPROFILE } else { $env:HOME }
    if ([string]::IsNullOrWhiteSpace($userDirectory)) {
        $userDirectory = [Environment]::GetFolderPath('UserProfile')
    }
    $cargoDirectory = $env:CARGO_HOME
    if ([string]::IsNullOrWhiteSpace($cargoDirectory)) {
        $cargoDirectory = Join-Path $userDirectory '.cargo'
    }
    $pathMap = @(Get-ReleasePathMap `
        -RepositoryRoot ([IO.Path]::GetFullPath($RepositoryRoot)) `
        -CargoHome ([IO.Path]::GetFullPath($cargoDirectory, $PWD.ProviderPath)) `
        -Sysroot ([IO.Path]::GetFullPath($sysroot)) `
        -TargetDirectory ([IO.Path]::GetFullPath($metadata.target_directory)) `
        -UserDirectory $userDirectory `
        -TemporaryDirectory ([IO.Path]::GetTempPath()))
    [pscustomobject]@{
        TargetDirectory = $metadata.target_directory
        PathMap = $pathMap
    }
}

function Get-ReleaseRustFlags {
    param(
        [Parameter(Mandatory = $true)][object[]]$PathMap,
        [Parameter(Mandatory = $true)][string]$Target
    )

    # Повторяем приоритет Cargo; encoded-форма сохраняет пробелы внутри одного аргумента.
    $encoded = [Environment]::GetEnvironmentVariable('CARGO_ENCODED_RUSTFLAGS')
    if ($null -ne $encoded) {
        if ($encoded.Length -gt 0) { $encoded.Split([char]0x1f) }
    }
    elseif (-not [string]::IsNullOrWhiteSpace($env:RUSTFLAGS)) {
        $env:RUSTFLAGS -split '\s+' | Where-Object { $_.Length -gt 0 }
    }
    foreach ($mapping in $PathMap) {
        '--remap-path-prefix=' + $mapping.From + '=' + $mapping.To
    }
    # Явные rustflags идут после профиля Cargo и не должны возвращать отладочные секции.
    '-Cdebuginfo=0'
    '-Cstrip=debuginfo'
    if ($Target -like '*-windows-msvc') {
        '-Clink-arg=/PDBALTPATH:fast_dump.pdb'
    }
}

function Assert-ReleaseBytesPrivacy {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][byte[]]$Bytes,
        [Parameter(Mandatory = $true)][string]$Description,
        [Parameter(Mandatory = $true)][object[]]$PathMap
    )

    $roots = @($PathMap | ForEach-Object {
        [regex]::Replace($_.From.Replace('\', '/'), '/+', '/')
    } | Sort-Object -Unique)
    $texts = [System.Collections.Generic.List[string]]::new()
    $texts.Add([Text.Encoding]::UTF8.GetString($Bytes))
    # Широкая строка может начинаться на любом смещении внутри PE/ELF.
    foreach ($offset in @(0, 1)) {
        $length = $Bytes.Length - $offset
        $length -= $length % 2
        if ($length -gt 0) {
            $texts.Add([Text.Encoding]::Unicode.GetString($Bytes, $offset, $length))
            $texts.Add([Text.Encoding]::BigEndianUnicode.GetString($Bytes, $offset, $length))
        }
    }
    foreach ($text in $texts) {
        # Нормализуем и смешанные разделители, и удвоенные обратные слеши из JSON.
        $normalized = [regex]::Replace($text.Replace('\', '/'), '/+', '/')
        $neutralPattern = '/build/(?:home|cargo|rust|src|target|tmp)(?=/|[^\w.-]|$)'
        $neutralPrefixes = [regex]::Matches($normalized, $neutralPattern)
        foreach ($root in $roots) {
            $pattern = [regex]::Escape($root) + '(?=/|[^\w.-]|$)'
            foreach ($match in [regex]::Matches($normalized, $pattern, [Text.RegularExpressions.RegexOptions]::IgnoreCase)) {
                # /tmp внутри /build/tmp безопасен; исходный /build/tmp/person — уже нет.
                $insideNeutralPrefix = $false
                foreach ($prefix in $neutralPrefixes) {
                    if (($match.Index -gt $prefix.Index -and $match.Index -lt ($prefix.Index + $prefix.Length)) -or
                        ($match.Index -eq $prefix.Index -and $match.Length -le $prefix.Length)) {
                        $insideNeutralPrefix = $true
                        break
                    }
                }
                if (-not $insideNeutralPrefix) {
                    throw "Release privacy check failed for ${Description}: local build path remains."
                }
            }
        }
        # Обнаруживаем также пути другого пользователя, пришедшие из готовых зависимостей.
        $genericText = [regex]::Replace($normalized, $neutralPattern, '@remapped@')
        $userPath = '(?i)(?:[a-z]:/users/[^/\x00-\x1f"]+(?=/|[\x00-\x1f"]|$)|/(?:home|Users)/[^/\x00-\x1f"]+(?=/|[\x00-\x1f"]|$)|/root(?=/|[\x00-\x1f"]|$))'
        if ([regex]::IsMatch($genericText, $userPath)) {
            throw "Release privacy check failed for ${Description}: user directory remains."
        }
    }
}

function Assert-ReleaseFilePrivacy {
    param(
        [Parameter(Mandatory = $true)][string[]]$Path,
        [Parameter(Mandatory = $true)][object[]]$PathMap
    )
    foreach ($file in $Path) {
        $resolved = (Resolve-Path -LiteralPath $file -ErrorAction Stop).ProviderPath
        Assert-ReleaseBytesPrivacy -Bytes ([IO.File]::ReadAllBytes($resolved)) `
            -Description ([IO.Path]::GetFileName($resolved)) -PathMap $PathMap
    }
}
