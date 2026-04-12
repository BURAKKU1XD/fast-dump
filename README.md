# fast-dump

`fast-dump` is a Rust implementation of a resumable directory dump over a custom
TCP protocol.

The sender walks a source tree in deterministic order, captures best-effort file
snapshots, builds tar entries, compresses them with zstd, and sends committed
zstd frames to a receiver. The receiver writes those frames into
`<dump_id>.tar.zst.part`, persists resume state next to the partial archive, and
finalizes the result into a valid `.tar.zst` file on successful completion.

## Current Status

- Working `sender` and `receiver` subcommands.
- Both transport pairings are implemented:
  - `push`: `sender --connect` with `receiver --listen`
  - `pull`: `sender --listen` with `receiver --connect`
- Active custom binary wire protocol is packet version `3`.
- Session flow is manifest-first: handshake, resume bootstrap, durable manifest
  replication, per-frame commit, typed issue tail, and finish acknowledgement.
- Receiver journal and rollback-to-last-ack semantics are implemented.
- Sender emits aggregate session statistics to stdout.
- Sender internals are already split into `source`, `readers`, `frame`, and
  `session`.
- Integration coverage exists for transport, protocol flow, resume, roundtrip,
  observability, and stress scenarios.

## CLI

Top-level help:

```text
fast-dump [OPTIONS] <COMMAND>
```

Commands:

- `fast-dump sender`
- `fast-dump receiver`

Global options:

- `--debug`

### Sender

```text
fast-dump sender [OPTIONS] --source <SOURCE> <--listen <LISTEN>|--connect <CONNECT>>
```

Required:

- `--source <DIR>`
- exactly one of `--listen <ADDR>` or `--connect <ADDR>`

Optional:

- `--dump-id <ID>`
- `--token <TOKEN>`
- `-l, --level <N>` default `3`
- `-t, --threads <N|auto>` default `adaptive` when omitted
- `--min-readers <N>` default `2`
- `--max-readers <N>` default `8`
- `--read-buffer-kb <N>` default `adaptive` when omitted
- `--frame-target-mb <N>` default `adaptive` when omitted
- `--manifest-chunk-kb <N>` default `512`
- `--manifest-ack-records <N>` default `4096`
- `--manifest-ack-kb <N>` default `8192`

Notes:

- If `--dump-id` is omitted, the sender currently falls back to the last path
  component of `--source`.
- `--threads auto` resolves to `available_parallelism() - 1`, with a minimum of
  `1`.
- If `--threads`, `--read-buffer-kb`, or `--frame-target-mb` are omitted, the
  sender samples a bounded deterministic prefix of the source tree and picks one
  of these runtime profiles:
  - `tiny-heavy`: `threads=2`, `frame_target_mb=64`, `read_buffer_kb=1024`
  - `mixed`: `threads=2`, `frame_target_mb=64`, `read_buffer_kb=64`
  - `large-heavy`: `threads=4`, `frame_target_mb=64`, `read_buffer_kb=1024`
- Explicit CLI values stay stronger than adaptive selection for the same knob.
  This includes `--threads auto`, which is treated as an explicit thread
  override.
- Manifest tuning flags are validated against the protocol hard caps:
  `ManifestChunk <= 1 MiB`, total unacked manifest window `<= 8 MiB` and
  `<= 65536` records.
- The default manifest ack record budget is intentionally smaller than the
  protocol cap so a fresh `v3` session can commit an initial manifest prefix
  and start archive frames earlier on file-heavy trees.
- Sender-side manifest spool is bounded internally to `512 MiB`. If that quota
  is exceeded while sealing the durable spool, the sender fails loudly and
  removes the partial spool plus stale checkpoint instead of leaving unbounded
  disk growth behind.

### Receiver

```text
fast-dump receiver [OPTIONS] --data-dir <DATA_DIR> <--listen <LISTEN>|--connect <CONNECT>>
```

Required:

- `--data-dir <DIR>`
- exactly one of `--listen <ADDR>` or `--connect <ADDR>`

Optional:

- `--token <TOKEN>`
- `--max-sessions <N>` default `1`

Notes:

- `--max-sessions` is meaningful in `receiver --listen` mode. In
  `receiver --connect` mode the process handles a single session.
- Receiver-side temp stores are bounded internally: `<dump_id>.manifest.part`
  is capped at `512 MiB`, `<dump_id>.tar.zst.issues.tsv.part` at `64 MiB`.
  When quota or disk exhaustion is hit, the receiver keeps the last durable
  manifest/archive boundary and removes stale partial issue tails on restart.

## Usage Examples

Push mode:

```powershell
cargo run -- receiver --listen 127.0.0.1:7001 --data-dir D:\dump-recv --token secret
cargo run -- sender --connect 127.0.0.1:7001 --source D:\data --dump-id dump-001 --token secret
```

Pull mode:

```powershell
cargo run -- sender --listen 127.0.0.1:7001 --source D:\data --dump-id dump-001
cargo run -- receiver --connect 127.0.0.1:7001 --data-dir D:\dump-recv
```

The sender prints aggregate session statistics such as:

- `status`
- `files_total_discovered`
- `files_sent_clean`
- `files_sent_best_effort`
- `files_skipped_open_failed`
- `files_skipped_unstable_after_retries`
- `retries_total`
- `bytes_sent_raw`
- `bytes_sent_best_effort_raw`
- `frames_sent`
- `final_archive_bytes`

The stable sender summary field `file` is retained as an alias for the
committed `work_ordinal` in the current `v3` runtime.

The receiver prints the committed resume boundary and final archive path for
completed sessions.

## Runtime Model

Actual runtime message flow:

1. Sender opens the transport session.
2. Sender sends `Hello { role, protocol_version, dump_id, token }`.
3. Receiver replies with `HelloAck { role }` or `Error`.
4. Sender sends `ResumeQuery { dump_id }`.
5. Receiver opens or creates local state, truncates `.part` back to the
   committed `archive_len`, and returns `ResumeState`.
6. Sender sends `ManifestStart { base_manifest }`.
7. Sender streams zero or more durable `ManifestChunk` batches and receives
   `ManifestAck` boundaries from the receiver.
8. Sender closes manifest replication with `ManifestSeal`; receiver replies with
   the final sealed `ManifestAck`.
9. For each committed sender-side zstd frame:
   - sender sends `FrameStart`
   - sender streams one or more `FrameChunk`
   - sender sends `FrameEnd`
   - receiver verifies checksum and length, fsyncs, persists journal, then sends
     `Ack`
10. If terminal skips happened, sender uploads zero or more typed `IssueChunk`
    control messages.
11. Sender sends `Finish`.
12. Receiver appends tar end blocks as a local final zstd frame, fsyncs, renames
   `.part` into the final `.tar.zst`, removes the journal, and replies with
   `FinishAck`. When terminal skips were uploaded, receiver also persists
   `<dump_id>.tar.zst.issues.tsv` next to the final archive.

At most one unacknowledged frame is lost on disconnect.

## Current Data-Path Behavior

- The archive contains regular files that were successfully captured and symlink entries whose targets are preserved without dereferencing.
- Empty directories and directory metadata are ignored.
- Source traversal is deterministic:
  - depth-first
  - entries within each directory are sorted by normalized relative path
- Tar paths are stored relative to the selected root and normalized to `/`.
- Live files use best-effort snapshot semantics:
  - sender opens the file immediately before an attempt
  - sender reads `size_at_open` from the opened handle
  - sender reads exactly `size_at_open` bytes for that attempt
  - appended tail after `open` is ignored for the current dump
  - short read or open failure restarts the file from scratch
  - at most `3` immediate attempts are made
  - exhausted files are skipped and counted in aggregate stats
- Symlinks are stored as tar symlink entries and keep their original link target when the sender can read that target.
- Symlinks whose link targets become unreadable during source traversal are skipped with a warning.
- On Linux, entries on virtual filesystems such as `procfs`, `sysfs`, `cgroup2`, `debugfs`, and `tracefs` are skipped with a warning based on a cached `/proc/self/mountinfo` filesystem-type lookup.
- Unsupported special entries such as sockets, devices, and FIFOs are skipped with a kind-specific warning.
- Frame boundaries stay on completed files, but `frame_target` is treated as a
  soft target rather than a mandatory per-file flush boundary.
- Large direct-stream files and previously retried files may be isolated into
  dedicated frames to reduce rollback cost after a retryable failure.
- Terminal skips still stay aggregate-only in sender CLI output, but the sender
  now uploads a compact issue journal tail and the receiver persists it as
  `<dump_id>.tar.zst.issues.tsv` on successful finish.

Implementation detail:

- The sender keeps accepted small-file snapshots in memory and streams accepted
  large files directly from an authoritative source handle into tar, zstd, and
  the transport. This keeps large files out of RAM while removing temp-file
  overhead from the common send path.
- The production send path streams compressed bytes directly to the transport via
  `FrameChunk` messages.
- Mainline no longer carries a separate buffered dump-builder path.

## Current Benchmark Notes

The current `v3` default keeps the smaller bootstrap manifest horizon for
file-heavy starts while keeping a larger horizon when the first queued file
already looks large, and now uses `manifest_ack_records=4096` after a focused
localhost retune. When `--threads`, `--frame-target-mb`, or
`--read-buffer-kb` are omitted, the sender also applies a bounded workload
probe and selects one of the built-in sender profiles. The current localhost-
confirmed adaptive mapping is:

- `manySmallFiles` / `tiny-heavy`: `threads=2`, `frame_target_mb=64`, `read_buffer_kb=1024`
- `mixed`: `threads=2`, `frame_target_mb=64`, `read_buffer_kb=64`
- `bigFiles` / `large-heavy`: `threads=4`, `frame_target_mb=64`, `read_buffer_kb=1024`

Shaped-network confirmation for the full adaptive step is still pending. The
focused static baseline that is still useful as a reference lands roughly at:

- `manySmallFiles`: `4185.19 ms`
- `mixed`: `2168.71 ms`
- `bigFiles`: `839.64 ms`

These numbers come from the localhost confirmation sweep in
`target/bench-blazing-manifest-records-confirm-20260408/` on `2026-04-08`
with `repeats=3`.

## Repository Layout

- `src/app.rs`: CLI dispatch and runtime entrypoints.
- `src/cli/mod.rs`: clap-based CLI shape and runtime validation helpers.
- `src/network/transport.rs`: TCP session setup and handshake bootstrap.
- `src/network/protocol.rs`: wire codec and message helpers.
- `src/network/protocol.md`: protocol reference for the current wire format.
- `src/sender/source.rs`: deterministic source discovery and resume validation.
- `src/sender/readers.rs`: snapshot attempts, retries, small-file buffering,
  large-file direct handles, and adaptive reader scaling.
- `src/sender/frame.rs`: tar and zstd frame assembly helpers.
- `src/sender/session.rs`: streaming send loop and aggregate reporting.
- `src/receiver/journal.rs`: persisted receiver state and recovery.
- `src/receiver/frame.rs`: frame commit path.
- `src/receiver/finish.rs`: final archive finalization.
- `src/receiver/listener.rs`: listen-mode receiver loop with session limits.
- `tests/connectivity/`: end-to-end and protocol-flow coverage.
- `benches/`: criterion microbenchmarks plus the dedicated loopback matrix runner.

## Development

Build:

```powershell
cargo build
```

Build release artifacts for every supported target that is available in the
current environment:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build-artifacts.ps1
```

The script writes artifacts to `dist/`, builds `x86_64-pc-windows-msvc` and
`x86_64-unknown-linux-musl`, prefers a native musl toolchain for Linux, and can
fall back to local `zig` plus `cargo-zigbuild` when available. Unsupported
targets are marked as `skipped` with an explicit reason. Use `-RequireAll` to
turn skipped targets into a hard failure.

Run tests:

```powershell
cargo test --quiet
```

Run lints:

```powershell
cargo clippy --all-targets --all-features
```

Run benches:

```powershell
cargo bench
```

Run only the loopback matrix runner:

```powershell
cargo bench --bench loopback_send_dump -- --dataset manySmallFiles,bigFiles,mixed --network-profile loopback-localhost,wan-high-rtt,bandwidth-limited
```

The matrix runner now writes `benchmark_plan.md`, `results_raw.csv`,
`results_summary.csv`, and `benchmark_report.md` under
`target/bench-matrix` (or `FAST_DUMP_BENCH_MATRIX_DIR`). Synthetic datasets
default to a short temp-root-backed cache so Windows path length does not skew
benchmark behavior; override it with `FAST_DUMP_BENCH_DATASET_DIR` when needed.

Related docs:

- [`AGENTS.md`](./AGENTS.md)
- [`src/network/protocol.md`](./src/network/protocol.md)
- [`BENCH.md`](./BENCH.md)
- [`REFACTORING.md`](./REFACTORING.md)
- [`REFACTORING2.MD`](./REFACTORING2.MD)
- [`REFACTORING3.md`](./REFACTORING3.md)
