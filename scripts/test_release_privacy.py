"""Регрессии обезличивания релизов без компиляции проекта и зависимостей."""

import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
PWSH = shutil.which("pwsh")
COMMON = ROOT / "scripts/release-common.ps1"
BUILD = ROOT / "scripts/build-release.ps1"


def ps_string(value):
    return "'" + str(value).replace("'", "''") + "'"


@unittest.skipUnless(PWSH, "PowerShell 7 is required for release helper tests")
class ReleasePrivacyTests(unittest.TestCase):
    def run_ps(self, body):
        environment = os.environ.copy()
        for name in (
            "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_RUSTFLAGS",
            "CARGO_PROFILE_RELEASE_DEBUG", "CARGO_PROFILE_RELEASE_STRIP",
        ):
            environment.pop(name, None)
        script = (
            "$ErrorActionPreference = 'Stop'\n"
            f". {ps_string(COMMON)}\n" + body
        )
        result = subprocess.run(
            [PWSH, "-NoProfile", "-NonInteractive", "-Command", script],
            cwd=ROOT, env=environment, capture_output=True, text=True,
            encoding="utf-8", timeout=45,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return json.loads(result.stdout.strip().splitlines()[-1])

    def test_path_map_prefers_specific_roots_and_preserves_spaces(self):
        mappings = self.run_ps(r"""
            $map = @(Get-ReleasePathMap `
                -RepositoryRoot 'C:\Users\Build Person\project' `
                -CargoHome 'C:\Users\Build Person\.cargo' `
                -Sysroot 'C:\Users\Build Person\.rustup\toolchains\stable' `
                -TargetDirectory 'C:\Users\Build Person\project\target' `
                -UserDirectory 'C:\Users\Build Person' `
                -TemporaryDirectory 'C:\Users\Build Person\AppData\Local\Temp')
            ConvertTo-Json -InputObject $map -Compress
        """)
        cases = {
            r"C:\Users\Build Person\profile.txt": "/build/home",
            r"C:\Users\Build Person\.cargo\registry\src\crate.rs": "/build/cargo",
            r"C:\Users\Build Person\.rustup\toolchains\stable\library\std.rs": "/build/rust",
            r"C:\Users\Build Person\project\src\main.rs": "/build/src",
            r"C:\Users\Build Person\project\target\generated.rs": "/build/target",
            r"C:\Users\Build Person\AppData\Local\Temp\generated.rs": "/build/tmp",
        }
        for path, expected in cases.items():
            for variant in (path, path.replace("\\", "/")):
                with self.subTest(path=variant):
                    matches = [m for m in mappings if variant.startswith(m["From"])]
                    self.assertTrue(matches, variant)
                    self.assertEqual(matches[-1]["To"], expected)

    def test_binary_scan_rejects_private_paths_in_every_encoding(self):
        paths = [
            r"X:\Private Build\project\src\main.rs",
            "x:/private build/project/src/main.rs",
            "X:\\Private Build/project\\src/main.rs",
            r"C:\Users\Another Person\.cargo\src\lib.rs",
            "c:/uSeRs/Another Person/.rustup/library/std.rs",
            "/home/another/.cargo/src/lib.rs",
            "/Users/another/project/main.rs",
            "/root/project/main.rs",
            "panicfailed/home/OtherUser/private/src/lib.rs",
            "panicfailed/root/private/src/lib.rs",
            "C:\\Users\\OtherUser\0next",
            "/home/OtherUser\0next",
            "C:/Users/Сборщик/project/main.rs",
            json.dumps({"build": r"X:\Private Build\project\src\main.rs"}),
            json.dumps({"build": r"C:\Users\Another Person\.cargo\source.rs"}),
        ]
        cases = []
        for path in paths:
            for encoding in ("utf-8", "utf-16le", "utf-16be"):
                for alignment in (0, 1):
                    payload = b"\xff" * alignment + path.encode(encoding) + b"\0\0"
                    cases.append({
                        "label": f"{path}:{encoding}:{alignment}",
                        "payload": base64.b64encode(payload).decode("ascii"),
                    })
        results = self.run_ps(
            "$map = @([pscustomobject]@{ From = 'X:\\Private Build\\project'; To = '/build/src' })\n"
            f"$cases = {ps_string(json.dumps(cases))} | ConvertFrom-Json\n"
            r"""
            $results = foreach ($case in $cases) {
                $rejected = $false
                try {
                    Assert-ReleaseBytesPrivacy -Bytes ([Convert]::FromBase64String($case.payload)) `
                        -Description $case.label -PathMap $map
                }
                catch {
                    if ($_.Exception.Message -notlike 'Release privacy check failed*') { throw }
                    $rejected = $true
                }
                [pscustomobject]@{ label = $case.label; rejected = $rejected }
            }
            ConvertTo-Json -InputObject @($results) -Compress
            """
        )
        for result in results:
            self.assertTrue(result["rejected"], result["label"])

    def test_binary_scan_checks_reserved_prefix_and_network_roots(self):
        roots = [
            "/build/src/PrivateUser/project",
            r"E:\build\src\PrivateUser\project",
            r"\\server\PersonalShare\private-project",
            r"\\?\E:\private-project",
        ]
        cases = [{"root": root, "path": variant + "/src/main.rs"}
                 for root in roots for variant in (root, root.replace("\\", "/"))]
        results = self.run_ps(
            f"$cases = {ps_string(json.dumps(cases))} | ConvertFrom-Json\n"
            r"""
            $results = foreach ($case in $cases) {
                $map = @([pscustomobject]@{ From = $case.root; To = '/build/src' })
                $rejected = $false
                try {
                    Assert-ReleaseBytesPrivacy -Bytes ([Text.Encoding]::UTF8.GetBytes($case.path)) `
                        -Description 'original root' -PathMap $map
                }
                catch {
                    if ($_.Exception.Message -notlike 'Release privacy check failed*') { throw }
                    $rejected = $true
                }
                [pscustomobject]@{ path = $case.path; rejected = $rejected }
            }
            ConvertTo-Json -InputObject @($results) -Compress
            """
        )
        for result in results:
            self.assertTrue(result["rejected"], result["path"])

    def test_binary_scan_accepts_neutral_paths_and_ordinary_words(self):
        payload = (
            "user root Users home build profile "
            "/build/home/profile /build/cargo/registry/src/crate.rs "
            "/build/rust/library/std.rs /build/src/src/main.rs "
            "/build/target/generated.rs /build/tmp/generated.rs "
            "src/main.rs X:/Private Build/project-other/src/main.rs"
        )
        cases = [base64.b64encode(payload.encode(enc)).decode("ascii")
                 for enc in ("utf-8", "utf-16le", "utf-16be")]
        result = self.run_ps(
            "$map = @(\n"
            "[pscustomobject]@{ From = 'X:\\Private Build\\project'; To = '/build/src' }\n"
            "[pscustomobject]@{ From = '/tmp'; To = '/build/tmp' }\n"
            "[pscustomobject]@{ From = '/home/user'; To = '/build/home' }\n"
            "[pscustomobject]@{ From = '/build'; To = '/build/target' }\n"
            ")\n"
            f"$cases = {ps_string(json.dumps(cases))} | ConvertFrom-Json\n"
            r"""
            foreach ($payload in $cases) {
                Assert-ReleaseBytesPrivacy -Bytes ([Convert]::FromBase64String($payload)) `
                    -Description 'clean binary' -PathMap $map
            }
            ConvertTo-Json -InputObject $true
            """
        )
        self.assertTrue(result)

    def test_file_scan_checks_every_payload_and_rejects_missing_files(self):
        with tempfile.TemporaryDirectory(prefix="release privacy ") as directory:
            clean = Path(directory) / "binary"
            leaked = Path(directory) / "manifest.json"
            missing = Path(directory) / "missing"
            clean.write_bytes(b"clean /build/src/main.rs")
            leaked.write_text('{"build": "C:/Users/Private Person/project"}', encoding="utf-8")
            result = self.run_ps(
                "$map = @([pscustomobject]@{ From = 'X:/project'; To = '/build/src' })\n"
                f"$clean = {ps_string(clean)}\n"
                f"$leaked = {ps_string(leaked)}\n"
                f"$missing = {ps_string(missing)}\n"
                r"""
                Assert-ReleaseFilePrivacy -Path @($clean) -PathMap $map
                $leakRejected = $false
                $missingRejected = $false
                try { Assert-ReleaseFilePrivacy -Path @($clean, $leaked) -PathMap $map }
                catch {
                    if ($_.Exception.Message -notlike 'Release privacy check failed*') { throw }
                    $leakRejected = $true
                }
                try { Assert-ReleaseFilePrivacy -Path @($missing) -PathMap $map }
                catch {
                    if ($_.CategoryInfo.Category -ne 'ObjectNotFound') { throw }
                    $missingRejected = $true
                }
                @{ leak = $leakRejected; missing = $missingRejected } | ConvertTo-Json -Compress
                """
            )
            self.assertTrue(result["leak"])
            self.assertTrue(result["missing"])

    def test_rustflags_priority_and_space_preservation(self):
        result = self.run_ps(r"""
            $map = @([pscustomobject]@{ From = 'X:\Private Build\project'; To = '/build/src' })
            $env:RUSTFLAGS = '--cfg from_plain -C opt-level=2 -Cdebuginfo=2 -Cstrip=none'
            $plain = @(Get-ReleaseRustFlags -PathMap $map -Target 'x86_64-unknown-linux-gnu')
            $env:CARGO_ENCODED_RUSTFLAGS = @('--cfg', 'from_encoded', '-Clink-arg=path with spaces', '-Cdebuginfo=2', '-Cstrip=none') -join [char]31
            $encoded = @(Get-ReleaseRustFlags -PathMap $map -Target 'x86_64-pc-windows-msvc')
            @{ plain = $plain; encoded = $encoded } | ConvertTo-Json -Compress
        """)
        self.assertIn("from_plain", result["plain"])
        self.assertIn("opt-level=2", result["plain"])
        self.assertIn("from_encoded", result["encoded"])
        self.assertNotIn("from_plain", result["encoded"])
        self.assertIn("-Clink-arg=path with spaces", result["encoded"])
        for flags in result.values():
            self.assertTrue(any("X:\\Private Build\\project=/build/src" in flag for flag in flags))
            self.assertEqual([flag for flag in flags if flag.startswith("-Cdebuginfo=")][-1], "-Cdebuginfo=0")
            self.assertEqual([flag for flag in flags if flag.startswith("-Cstrip=")][-1], "-Cstrip=debuginfo")

    def test_relative_cargo_home_uses_powershell_working_directory(self):
        with tempfile.TemporaryDirectory(prefix="release working directory ") as directory:
            result = self.run_ps(
                f"$fixture = {ps_string(directory)}\n"
                f"$repository = {ps_string(ROOT)}\n"
                r"""
                function cargo {
                    $global:LASTEXITCODE = 0
                    @{ target_directory = (Join-Path $fixture 'target') } | ConvertTo-Json -Compress
                }
                function rustc {
                    $global:LASTEXITCODE = 0
                    Join-Path $fixture 'toolchain'
                }
                $env:CARGO_HOME = 'relative cargo'
                Push-Location $fixture
                try {
                    $context = Get-ReleaseBuildContext -RepositoryRoot $repository
                    @{ actual = @($context.PathMap | Where-Object To -eq '/build/cargo' | ForEach-Object From)
                       expected = Join-Path $fixture 'relative cargo' } | ConvertTo-Json -Compress
                }
                finally { Pop-Location }
                """
            )
        self.assertIn(result["expected"], result["actual"])

    def test_build_restores_environment_and_only_returns_verified_binary(self):
        with tempfile.TemporaryDirectory(prefix="release helper ") as directory:
            result = self.run_ps(
                f"$fixture = {ps_string(directory)}\n"
                f"$buildScript = {ps_string(BUILD)}\n"
                r"""
                $target = 'x86_64-pc-windows-msvc'
                $global:FixtureTarget = Join-Path $fixture 'target'
                $global:FixtureBinary = Join-Path $global:FixtureTarget "$target/release/fast-dump.exe"
                $global:FixtureSysroot = Join-Path $fixture 'toolchain'
                New-Item -ItemType Directory -Force -Path (Split-Path $global:FixtureBinary), $global:FixtureSysroot | Out-Null
                function cargo {
                    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
                    $global:LASTEXITCODE = 0
                    if ($Arguments -contains 'metadata') {
                        if ($global:BuildMode -eq 'metadata-fail') { $global:LASTEXITCODE = 29; return }
                        @{ target_directory = $global:FixtureTarget; packages = @(@{ name = 'fast-dump'; version = '0.1.0' }) } | ConvertTo-Json -Compress
                    }
                    elseif (($Arguments -contains 'build') -or ($Arguments -contains 'zigbuild')) {
                        $global:ObservedFlags = $env:CARGO_ENCODED_RUSTFLAGS
                        $global:ObservedArguments = @($Arguments)
                        $global:ObservedBuildTool = if ($Arguments -contains 'zigbuild') { 'zigbuild' } else { 'cargo' }
                        if ($global:BuildMode -eq 'fail') { $global:LASTEXITCODE = 17; return }
                        $payload = if ($global:BuildMode -eq 'leak') { 'C:/Users/Private Builder/.cargo/source.rs' } else { 'clean /build/src/src/main.rs' }
                        [IO.File]::WriteAllText($global:FixtureBinary, $payload)
                    }
                    else { throw "Unexpected mock cargo arguments: $Arguments" }
                }
                function rustc {
                    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
                    $global:LASTEXITCODE = 0
                    if ($Arguments -contains 'sysroot') { return $global:FixtureSysroot }
                    throw "Unexpected mock rustc arguments: $Arguments"
                }
                $names = @('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CARGO_PROFILE_RELEASE_DEBUG', 'CARGO_PROFILE_RELEASE_STRIP')
                $results = foreach ($mode in @('clean', 'fail', 'leak', 'unset', 'metadata-fail')) {
                    $env:RUSTFLAGS = '--cfg original_plain'
                    $env:CARGO_ENCODED_RUSTFLAGS = '--cfg' + [char]31 + 'original_encoded'
                    $env:CARGO_PROFILE_RELEASE_DEBUG = '2'
                    $env:CARGO_PROFILE_RELEASE_STRIP = 'none'
                    if ($mode -eq 'unset') {
                        foreach ($name in $names) { [Environment]::SetEnvironmentVariable($name, $null, 'Process') }
                    }
                    $before = @{}
                    foreach ($name in $names) { $before[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
                    $global:BuildMode = $mode
                    $global:ObservedFlags = ''
                    $global:ObservedArguments = @()
                    $global:ObservedBuildTool = ''
                    $copy = Join-Path $fixture "copy-$mode.exe"
                    $rejected = $false
                    $failure = ''
                    $returned = @()
                    $tool = if ($mode -eq 'unset') { 'zigbuild' } else { 'cargo' }
                    try {
                        $returned = @(& $buildScript -Target $target -BuildTool $tool)
                        Copy-Item -LiteralPath $returned[0] -Destination $copy
                    }
                    catch { $rejected = $true; $failure = $_.Exception.Message }
                    $restored = $true
                    foreach ($name in $names) {
                        if ($before[$name] -cne [Environment]::GetEnvironmentVariable($name, 'Process')) { $restored = $false }
                    }
                    [pscustomobject]@{
                        mode = $mode; rejected = $rejected; restored = $restored
                        copied = Test-Path -LiteralPath $copy
                        returned = $returned
                        binary = $global:FixtureBinary
                        flags = $global:ObservedFlags
                        arguments = $global:ObservedArguments
                        tool = $global:ObservedBuildTool
                        error = $failure
                    }
                }
                ConvertTo-Json -InputObject @($results) -Compress -Depth 5
                """
            )
        for case in result:
            with self.subTest(mode=case["mode"]):
                self.assertTrue(case["restored"])
                rejected = case["mode"] in ("fail", "leak", "metadata-fail")
                self.assertEqual(case["rejected"], rejected)
                self.assertEqual(case["copied"], not rejected)
                if case["mode"] == "metadata-fail":
                    self.assertEqual(case["error"], "cargo metadata failed")
                    self.assertEqual(case["tool"], "")
                    self.assertEqual(case["arguments"], [])
                    continue
                if case["mode"] == "fail":
                    self.assertIn("Release build failed", case["error"])
                elif case["mode"] == "leak":
                    self.assertIn("Release privacy check failed", case["error"])
                else:
                    self.assertEqual(case["error"], "")
                self.assertIn("profile.release.debug=0", case["arguments"])
                self.assertIn('profile.release.strip="debuginfo"', case["arguments"])
                self.assertIn("--remap-path-prefix", case["flags"])
                if case["mode"] != "unset":
                    self.assertIn("original_encoded", case["flags"])
                    self.assertNotIn("original_plain", case["flags"])
                if not rejected:
                    self.assertEqual(case["returned"], [case["binary"]])
                self.assertEqual(case["tool"], "zigbuild" if case["mode"] == "unset" else "cargo")


if __name__ == "__main__":
    unittest.main()
