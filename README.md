# fast-dump

Resumable directory transfer over TCP, producing a valid `.tar.zst` archive.
One Rust binary runs as a sender or receiver on Windows and Linux. The active
wire protocol is v4; both push and pull transfers are supported.

## Build

Rust 1.89 or newer and a C compiler are required:

```text
cargo build --locked --release
```

The binary is in `target/release/` (`fast-dump.exe` on Windows).
Run it without arguments in a terminal for the English setup wizard, or use
explicit commands. Complete commands show a dashboard; add `--no-gui` for plain
output. Redirected output automatically selects plain mode.

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

Roles describe data flow. Each command requires exactly one of `--listen` and
`--connect`. `127.0.0.1` connects only to the same host.

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
