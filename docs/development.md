# Development


Rust `1.89` or newer is required.

Build:

```powershell
cargo build --locked
```

Run tests:

```powershell
cargo test --locked --quiet
```

Push/PR CI checks Windows and Linux with Rust `1.89.0` and the current stable
toolchain. It uses the committed `Cargo.lock`, runs default/all-feature tests,
Clippy with warnings denied, bench compilation, and formatting checks.
Storage fault tests exercise real partial writes and injected disk-full, archive
sync, WAL append/sync, rename, and cleanup errors. They verify acknowledgement
barriers and recovery; these are not hardware power-loss simulations.

The manual **Release artifacts** workflow builds the selected branch/tag only
after CI and release stress checks pass. It produces Windows MSVC and Linux GNU
archives with a build manifest and SHA-256 checksums. It uploads them as
workflow artifacts, along with standalone binaries and their `.sha256`
files. Attach `fast-dump-x86_64-pc-windows-msvc.exe`,
`fast-dump-x86_64-unknown-linux-gnu` to the public release marked Latest to
enable the relay's download commands. Publish the matching `.sha256` files
alongside them for manual verification.
It builds through `scripts/build-release.ps1`, checks the resulting binary for
local build paths, and runs its `--version` and `--help` commands. Before packaging,
it also checks the staged binary, manifest, and lockfile for path leaks.
The manifest records the clean source commit/tree, compiler, target, lockfile,
and binary hash. Packages use
`releases/<version>-<short-commit>/<Rust target triple>/` for the binary,
`manifest.json`, and `Cargo.lock`; `.tar.gz` archives and their `.sha256` files
sit in the version directory. Package tar headers use `ustar`, UID/GID `0`, and
neutral owner/group names (`root` on Windows, omitted on Linux). No PAX metadata
is included, so headers do not expose the builder's account or local paths.
Release packages are disposable build output; keep
verification manifests and test results in the results archive when removing them.

`releases/` is ignored by Git and survives `cargo clean`. The local
`scripts/build-artifacts.ps1` helper copies replaceable binaries into
`releases/local/`; these builds do not run the release workflow's stress gates.
Cargo compilation output remains under `target/release/` (or the target-specific
`target/<Rust target triple>/release/`).

On Windows, build both Windows MSVC and Linux musl x86-64 artifacts with:

```powershell
pwsh -NoProfile -File ./scripts/build-artifacts.ps1 -RequireAll
```

The helper uses `cargo` and `rustc` from the environment. It detects installed
targets by checking for standard libraries in the active compiler's sysroot;
the `rustup` command is not required. Linux also requires a musl C toolchain or
Zig with `cargo-zigbuild`. `-RequireAll` fails if either target is unavailable.
The shared RustRover configuration `Build Windows + Linux` runs this command.

For a checked default-feature build of one target, build from a clean commit
and record it:

```powershell
$binary = & ./scripts/build-release.ps1 -Target x86_64-pc-windows-msvc
./scripts/write-release-manifest.ps1 -Binary $binary -Target x86_64-pc-windows-msvc -OutputPath releases/local/windows-manifest.json
```

Use `x86_64-unknown-linux-gnu` for native Linux builds, or
`-Target x86_64-unknown-linux-musl -BuildTool zigbuild` with Zig and
`cargo-zigbuild` available on `PATH`. The helper returns the validated absolute
binary path on stdout and respects Cargo's configured target directory. Both
the local dual-platform helper and release CI use it; IDE builds inherit the
same behavior through `scripts/build-artifacts.ps1`.

The shared release helpers support Rust `1.89` or newer. They remap discovered
repository, Cargo, toolchain, user-profile, temporary, and build-directory
prefixes to neutral paths, set release debug information to `0`, and strip
debug information. Windows PDB references use a filename without a local path.
The final file check rejects remaining local build paths before copying or
packaging. The manifest writer checks its input binary before hashing and its
JSON before writing. These settings affect build metadata; runtime behavior,
wire protocol, and optimization settings stay the same.

When both are set, explicit `CARGO_ENCODED_RUSTFLAGS` takes precedence over
`RUSTFLAGS`. The helper preserves the selected explicit flags and appends its
required release settings and path mappings. Its encoded flags supersede
`build.rustflags` and target `rustflags` from Cargo configuration; pass needed
custom flags explicitly through the environment for these builds. Environment
changes made by the helper are restored even when a build or validation fails.

Ordinary `cargo build --release` remains available for development and does not
provide this path-removal guarantee. Only validated output from the release
helpers should be distributed. The check covers builder paths in the supplied
files; it is not a scanner for arbitrary secrets embedded by source code or
external dependencies. Debug symbols, build caches, and local logs remain local.

The Windows/Linux scheduled stress workflow runs three explicit ignored tests:
the binary roundtrip with `10,116` files and per-file hashes; process-kill and
resume in both push and pull modes with a durable prefix and an uncommitted tail;
and `128` complete sessions with byte-for-byte archive checks and no retained
receiver reader handles before the peer closes. These checks exercise process
failure, not power loss or a physical 1/10 GbE link. The workflow checks that each
named test exists before running it; a removed test cannot silently pass CI.

```powershell
cargo test --locked --release --test integration connectivity::roundtrip_fast::sender_and_receiver_roundtrip_random_files_and_verify_hashes -- --exact --ignored --nocapture
cargo test --locked --release --test integration connectivity::recovery_stress::receiver_process_kill_and_resume_preserves_archive_push_and_pull -- --exact --ignored --nocapture
cargo test --locked --release --lib receiver::network::lifecycle_tests::repeated_full_transfers_release_all_reader_handles -- --exact --ignored --nocapture
```

Run lints:

```powershell
cargo clippy --locked --all-targets --all-features -- -D warnings
```

## Complete local verification

```text
cargo fmt --all --check
cargo test --locked --quiet
cargo test --locked --all-features --quiet
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo check --locked --benches --all-features
python -B -m unittest discover -s scripts -p "test_*.py"
```

Record verification with `python scripts/results.py record --kind test --label default-tests -- cargo test --locked --quiet`.
A recorder run preserves the command exit code. Run the commands from the repository root.
Rust checks require the Rust toolchain and a C compiler for zstd. Script checks and the
results viewer require Python 3.10 or newer; PowerShell helpers require PowerShell 7.
The Windows/Linux and Rust 1.89/stable CI matrix remains the compatibility gate.

## Code map

- `src/app.rs`, `launch.rs`, `cli/`: launch, validation and command dispatch.
- `src/sender/`, `receiver/`: thin public facades over local implementation modules.
- `src/network/`: transport and wire format; protocol reference stays there.
- `src/ui/`, `progress.rs`, `runtime.rs`: terminal presentation, observations and cancellation.
- `tests/connectivity/`: transport, recovery, retries and lifecycle integration tests.
- `benches/`, `scripts/`: measurement harnesses and result management.

Keep explanatory code comments in Russian. Use private or `pub(crate)` interfaces
unless an existing public API requires otherwise. Preserve protocol and runtime
contracts during structural changes; see [runtime](runtime.md) and [AGENTS](../AGENTS.md).

## Result retention

See [benchmark history](benchmarks.md#persistent-results-and-history). `target/`
is disposable after collecting results. Compiled binaries, symbols, generated
fixtures and dump payloads are not permanent history. Preserve raw measurements,
verification reports, parameters, hashes and source provenance instead.
Local tooling under `.tools/` and personal IDE settings are not build artifacts.
