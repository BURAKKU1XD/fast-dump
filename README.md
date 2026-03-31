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
- Custom binary wire protocol with handshake, resume bootstrap, per-frame commit,
  and finish acknowledgement.
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
- `-t, --threads <N|auto>` default `auto`
- `--min-readers <N>` default `2`
- `--max-readers <N>` default `8`
- `--read-buffer-kb <N>` default `1024`
- `--frame-target-mb <N>` default `64`

Notes:

- If `--dump-id` is omitted, the sender currently falls back to the last path
  component of `--source`.
- `--threads auto` resolves to `available_parallelism() - 1`, with a minimum of
  `1`.

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
6. For each committed sender-side zstd frame:
   - sender sends `FrameStart`
   - sender streams one or more `FrameChunk`
   - sender sends `FrameEnd`
   - receiver verifies checksum and length, fsyncs, persists journal, then sends
     `Ack`
7. Sender sends `Finish`.
8. Receiver appends tar end blocks as a local final zstd frame, fsyncs, renames
   `.part` into the final `.tar.zst`, removes the journal, and replies with
   `FinishAck`.

At most one unacknowledged frame is lost on disconnect.

## Current Data-Path Behavior

- The archive contains only regular files that were successfully captured.
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
- Non-regular entries are skipped with a warning.

Implementation detail:

- The current sender stages each accepted snapshot into a temporary file on disk
  before compression. This keeps large files out of RAM while allowing the
  compressor and network writer to stream from a stable snapshot.
- The production send path streams compressed bytes directly to the transport via
  `FrameChunk` messages.
- The buffered `prepare_dump` path still exists for tests and benchmarks.

## Repository Layout

- `src/app.rs`: CLI dispatch and runtime entrypoints.
- `src/cli/mod.rs`: clap-based CLI shape and runtime validation helpers.
- `src/network/transport.rs`: TCP session setup and handshake bootstrap.
- `src/network/protocol.rs`: wire codec and message helpers.
- `src/network/protocol.md`: protocol reference for the current wire format.
- `src/sender/source.rs`: deterministic source discovery and resume validation.
- `src/sender/readers.rs`: snapshot attempts, retries, staging, and adaptive
  reader scaling.
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
cargo bench --bench loopback_send_dump -- --dataset manySmallFiles,bigFiles,mixed
```

Related docs:

- [`AGENTS.md`](./AGENTS.md)
- [`src/network/protocol.md`](./src/network/protocol.md)
- [`BENCH.md`](./BENCH.md)
- [`REFACTORING.md`](./REFACTORING.md)
- [`REFACTORING2.MD`](./REFACTORING2.MD)
- [`REFACTORING3.md`](./REFACTORING3.md)
