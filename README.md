# fast-dump

Resumable directory transfer over TCP, producing a valid `.tar.zst` archive.
One Rust binary runs as a sender, receiver, or relay on Windows and Linux. The
v4 dump protocol supports direct push, direct pull, and transfers through a relay.

## Build

Rust 1.89 or newer and a C compiler are required:

```text
cargo build --locked --release
```

The binary is in `target/release/` (`fast-dump.exe` on Windows).
This direct Cargo build can retain local source paths, including the builder's
user-profile name. For binaries to distribute, use PowerShell 7 and the checked
release helper, selecting your installed target:

```text
pwsh -NoProfile -File ./scripts/build-release.ps1 -Target x86_64-pc-windows-msvc
```

Use `x86_64-unknown-linux-gnu` on Linux. The helper removes local build-path
prefixes and debug information, then rejects remaining build-path leaks before
returning the binary path. The local dual-platform helper, shared IDE build and
release CI use this same check; see [release tooling](docs/development.md).

Run it without arguments in a terminal for the English setup wizard, or use
explicit commands. The wizard starts with Relay, Listen, or Connect and enables
the relevant settings after you confirm the mode. Relay starts the server.
For P2P, one peer uses Listen and the other uses Connect → Direct. Both relay
clients use Connect → Via relay. Connected peers see hostnames and can offer
transfers in either direction. Complete transfer commands show a dashboard;
add `--no-gui` for plain output. Redirected output automatically selects plain mode.

## Push

On the receiving host:

```text
fast-dump receiver --listen 0.0.0.0:7000 --data-dir ./received
```

On the sending host, replace the address with the receiver's network IP:

```text
fast-dump sender --connect 192.168.1.20:7000 --source ./data
```

## Pull

On the sending host:

```text
fast-dump sender --listen 0.0.0.0:7000 --source ./data
```

On the receiving host, use the sender's network IP:

```text
fast-dump receiver --connect 192.168.1.10:7000 --data-dir ./received
```

Roles describe data flow. Sender and receiver select exactly one transport with
`--listen`, `--connect`, or `--relay`. `127.0.0.1` connects only to the same host.

## Relay

Use a reachable relay when both peers can make outbound connections but have no
reachable listener. Start Relay in the wizard and share the invitation it
prints with the clients. The relay generates its TLS key pair in memory.
Up to 15 clients can use Connect → Via relay with the invitation, which carries
the public-key fingerprint and the 12-character access token. Connected
clients choose a recipient separately for each transfer. The relay prints
one-paste Windows and Linux commands that download and verify the latest
GitHub release binary, then open the lobby with the invitation. The download
commands become usable when the release assets are published.

For scripted transfers, run the role-key relay on your server and start the
sender and receiver in either order. Each uses its own key for the same room.

```text
fast-dump relay --listen 0.0.0.0:9876 --config relay.toml
fast-dump sender --relay relay.example:9876 --room backup --relay-token-file sender.key --source ./data
fast-dump receiver --relay relay.example:9876 --room backup --relay-token-file receiver.key --data-dir ./received
```

Both peers must be online during the transfer. Each connection uses TLS to the
relay, whose operator can read the forwarded bytes. The receiver keeps the durable
resume state. See [Relay setup](docs/cli.md#relay) for certificates, room keys,
timeouts, and retries.

## Resume and output

The dump ID defaults to the source directory name. The receiver writes
`<dump_id>.tar.zst.part` and durable resume state; successful completion produces
`<dump_id>.tar.zst`. Repeat the same commands to resume an interrupted dump.
Keep the receiver's state and coordination files together. Existing completed
archives are preserved; use a different dump ID for another dump.

Ctrl+C requests cooperative shutdown and returns 130 after cleanup. Regular
files and readable symlinks are included; empty directories and directory
metadata are omitted. Linux system filesystems are excluded by filesystem type;
see the [selection rules](docs/runtime.md#linux-system-filesystem-exclusions).

## Performance history

Payload XXH3 checksums automatically use AVX2 on supported x86-64 CPUs, with
SSE2/scalar fallbacks and NEON on supported AArch64 CPUs. Tar-header checksums
also use AVX2/SSE2 on x86-64. No AVX2-only build or matching peer CPU is required;
the v4 checksums and archive format are unchanged.

Record checks and benchmarks, then open the generated `results/index.html`:

```text
python scripts/results.py record --kind test --label tests -- cargo test --locked --quiet
python scripts/results.py record --kind benchmark --label scan -- cargo bench --bench scan_source
python scripts/results.py index
```

Raw measurements and compact summaries are tracked in Git. Full logs stay local;
all retained results live outside Cargo's disposable `target/` directory.
The viewer filters runs and compares compatible measurements without a server.

## Guides

- [CLI and terminal guide](docs/cli.md): options, defaults, dashboard, retries and cancellation.
- [Runtime contract](docs/runtime.md): pipeline, source policy, bounded memory and recovery.
- [Protocol v4](src/network/protocol.md): wire format and durable message flow.
- [Development](docs/development.md): checks, code map, CI and release tooling.
- [Benchmarks](docs/benchmarks.md): harnesses, metric definitions and comparison methodology.
- [Contributor contract](AGENTS.md): requirements for changes.
- [Historical designs](docs/history/README.md): earlier plans and completed migrations.
