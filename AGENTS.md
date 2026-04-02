# fast-dump

This file describes the current project rules and the state that is already
implemented in the repository. Treat it as a working engineering map, not as a
wishlist.

## Goal

- Maintain a sender/receiver pair that streams a resumable dump to a remote
  server.
- Final artifact on the receiver is a valid `.tar.zst` file.
- Terminology is based on data flow:
  - `sender` reads files, builds tar, compresses zstd, and sends frames
  - `receiver` accepts frames, persists the partial archive, and maintains
    resume state
- Connection establishment is a separate concern from role naming.

## Current Status

- The repository already contains a working implementation, not just a stub.
- `fast-dump sender` and `fast-dump receiver` both exist and are exercised by
  integration tests.
- Push and pull transport pairings both work.
- The custom binary protocol, transport handshake, receiver journal, resume
  model, finish path, and aggregate sender reporting are implemented.
- Sender internals are already split into:
  - `src/sender/source.rs`
  - `src/sender/readers.rs`
  - `src/sender/frame.rs`
  - `src/sender/session.rs`
- Receiver internals are split into:
  - `src/receiver/journal.rs`
  - `src/receiver/resume.rs`
  - `src/receiver/frame.rs`
  - `src/receiver/finish.rs`
  - `src/receiver/session.rs`
  - `src/receiver/listener.rs`
- `src/network/protocol.md` documents the current wire format and should stay in
  sync with `src/network/protocol.rs`.

## Comment Policy

- Leave explanatory comments in code in Russian.
- Prefer comments that explain purpose, data flow, and non-obvious Rust details.
- Keep comments close to the code they explain.
- Avoid comments that merely restate syntax without adding meaning.

## Documentation Policy

- Keep `README.md`, `AGENTS.md`, and `src/network/protocol.md` aligned with
  actual runtime behavior.
- When CLI flags, protocol fields, session flow, or resume semantics change,
  update docs in the same task.
- Do not describe aspirational behavior as if it were already implemented.

## CLI Contract

- Use one binary with two subcommands:
  - `fast-dump sender`
  - `fast-dump receiver`
- Each subcommand supports exactly one transport selector:
  - `--listen <ADDR>`
  - `--connect <ADDR>`
- Valid transport pairings for v2:
  - `push`: `sender --connect <ADDR>` with `receiver --listen <ADDR>`
  - `pull`: `sender --listen <ADDR>` with `receiver --connect <ADDR>`
- Local CLI parsing enforces "exactly one of `--listen` or `--connect`".
- Cross-process pairing errors are rejected during transport establishment and
  handshake, not by local CLI parsing alone.

## Sender CLI

- Required:
  - `--source <DIR>`
  - exactly one of `--listen <ADDR>` or `--connect <ADDR>`
- Optional:
  - `--dump-id <ID>`
  - `--token <TOKEN>`
  - `-l, --level <N>` default `3`
  - `-t, --threads <N|auto>` default `auto`
  - `--min-readers <N>` default `2`
  - `--max-readers <N>` default `8`
  - `--read-buffer-kb <N>` default `1024`
  - `--frame-target-mb <N>` default `64`
- Current behavior:
  - if `--dump-id` is omitted, sender falls back to the last path component of
    `--source`
  - `--threads auto` resolves to `available_parallelism() - 1`, minimum `1`
  - runtime validation rejects zero threads, zero reader counts, and
    `min_readers > max_readers`

## Receiver CLI

- Required:
  - `--data-dir <DIR>`
  - exactly one of `--listen <ADDR>` or `--connect <ADDR>`
- Optional:
  - `--token <TOKEN>`
  - `--max-sessions <N>` default `1`
- Current behavior:
  - `max_sessions` is validated to be at least `1`
  - `receiver --listen` uses `max_sessions`
  - `receiver --connect` still validates the flag, but handles one session

## Code Map

- `src/main.rs`: binary entrypoint.
- `src/lib.rs`: public module exports.
- `src/app.rs`: CLI dispatch and runtime orchestration.
- `src/cli/mod.rs`: clap model and argument validation helpers.
- `src/network/transport.rs`: TCP connection setup and handshake bootstrap.
- `src/network/protocol.rs`: packet codec, message definitions, protocol
  helpers, and protocol-level validation.
- `src/sender/mod.rs`: thin facade with re-exports only.
- `src/receiver/mod.rs`: receiver facade with re-exports.
- `src/telemetry.rs`: logging initialization.
- `tests/connectivity/`: end-to-end scenarios.
- `benches/`: benchmark entrypoints.

## Archive Rules

- Archive contains regular files that were successfully sent and symlink entries whose targets are preserved without dereferencing.
- Empty directories and directory metadata are ignored.
- Paths stored in tar are relative to the selected root and normalized to `/`.
- File traversal order is deterministic:
  - depth-first
  - entries within each directory sorted by normalized relative path
- The dump is best-effort for live files and is not a strict point-in-time
  snapshot.
- Files that exhaust the live-file retry policy are omitted from the archive and
  counted as skip.
- Large files must never be loaded fully into RAM.
- Current implementation detail:
  - accepted small-file snapshots are kept in memory before compression
  - accepted large files are reopened from an authoritative handle and streamed
    directly into tar, zstd, and `FrameChunk` packets
  - the production send path no longer requires temp-file snapshots in the
    common case

## Source Metadata Policy

- Source discovery is used only for deterministic ordering, path normalization,
  filtering, and stable `file_index` assignment.
- Scan-time metadata such as file size must not be used as the source of truth
  for tar header sizing or live-file validation.
- The sender opens each file immediately before a snapshot attempt and captures
  authoritative metadata from the opened handle.

## Live File Policy

- Apply best-effort snapshot semantics to regular files.
- After a successful `open`, capture `size_at_open` from the opened handle.
- Tar headers use `size_at_open`.
- Sender reads exactly `size_at_open` bytes for the current attempt.
- If the file grows after `open`, the appended tail is ignored for the current
  dump and is not an error.
- If `open` fails or reading reaches EOF before `size_at_open`, restart the file
  from scratch.
- Retry policy:
  - at most `3` immediate attempts per file
  - no backoff between attempts
- If all attempts fail, skip the file and continue the dump.
- Exclusive-share failures count as `open` failures.
- Same-size in-place rewrites during read remain an accepted v2 risk.

## Compression Rules

- Compression runs on the sender.
- zstd level is CLI-configurable through `-l, --level`.
- zstd worker threads are CLI-configurable through `-t, --threads`.
- Sender keeps frame boundaries on completed files, but does not rely on a
  mandatory zstd flush after every file.
- Receiver appends bytes to `<dump_id>.tar.zst.part` and renames on successful
  completion.
- Receiver writes tar end blocks locally as a final zstd frame during
  finalization.

## Resume Model

- Do not use one endless zstd stream for the whole dump.
- The output `.tar.zst` is a concatenation of multiple zstd frames.
- Each sender-side zstd frame ends only on a completed file boundary.
- A frame may contain many files.
- `frame_target` is a soft target, not a hard compressed-byte cutoff.
- If the soft target is exceeded in the middle of processing a file, that file
  stays in the current frame and the frame closes after the file completes.
- Large direct-stream files and previously retried files may be isolated into
  dedicated frames to reduce rollback scope.
- Receiver commits only whole frames.
- On disconnect, at most the current uncommitted frame is lost.
- Resume progresses by deterministic `file_index`, not by scan-time size
  metadata.
- Skipped files are terminal for the current dump and must not trigger endless
  retry loops across reconnects.
- Committed progress must be able to advance across skipped indices even when
  skipped files produce no tar payload.

## Sender Pipeline

- Runtime send path:
  - `send_dump` in `src/sender/session.rs` is the production session loop
  - it uses `PendingFileWalker` and `PendingFrameFiles` for incremental
    deterministic discovery
  - it uses `ReaderPool` for bounded prefetch and adaptive scaling
  - it streams zstd output directly into protocol `FrameChunk` packets through
    `ProtocolChunkWriter`
- Buffered path:
  - `prepare_dump` still exists for tests and benchmarks
  - it materializes chunks in memory and should not be confused with the
    production streaming path
- Reader behavior:
  - retry and skip decisions happen at whole-file granularity
  - snapshot buffering is file-based, not whole-dump-based
  - small files use bounded in-memory snapshots, while large files use
    direct-stream handles
  - adaptive scaling uses bounded crossbeam queues
- Keep `src/sender/mod.rs` as a thin facade with re-exports only.
- Prefer `pub(crate)` over `pub` where possible.
- Keep tests close to the sender module they exercise.

## Sender Reporting

- The sender emits aggregate session statistics only.
- Required counters already exist in `SendReport` and CLI output:
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
- `SendReport` keeps a local issue journal for terminal skips.
- On successful finish, sender also uploads that issue journal to the receiver
  as a compact protocol tail.
- Stable CLI output remains aggregate-only unless intentionally changed.
- Classification:
  - `clean`: file sent without retry-driven degradation
  - `best_effort`: file sent after one or more failed attempts but before the
    retry budget was exhausted
  - `skip`: file could not be opened or fully read within the retry budget
- Session status:
  - `clean` when `best_effort == 0` and `skip == 0`
  - `completed_with_warnings` when any `best_effort` or `skip` file exists

## Receiver State

- Persist journal next to the partial archive.
- Journal fields currently stored:
  - `protocol_version`
  - `dump_id`
  - `last_committed_frame`
  - `last_committed_file_index`
  - `last_committed_file_path`
  - `archive_len`
- Before resuming, truncate any trailing bytes beyond `archive_len`.
- If trailing bytes cannot be reconciled with the journal, fail loudly.
- Finalization writes tar end blocks, closes the last local zstd frame, fsyncs,
  renames the file, and removes the journal.
- If sender uploaded terminal skip details, receiver also persists
  `<dump_id>.tar.zst.issues.tsv` next to the final archive.

## Wire Protocol

- Transport is a long-lived TCP stream between sender and receiver.
- Do not use HTTP, JSON, gRPC, or WebSocket for v2.
- Use one custom binary protocol over one ordered stream.
- Support two transport modes:
  - `push`: sender connects to receiver
  - `pull`: receiver connects to sender
- Protocol semantics stay the same in both modes because roles are defined by
  data direction, not by who initiated TCP.
- All messages are length-prefixed binary packets.
- Packet header layout:
  - `magic: u32` = `0x504D4446` (`FDMP`)
  - `version: u16` = `2`
  - `kind: u16`
  - `body_len: u32`
- Integer encoding is little-endian.
- Path fields are encoded as length-prefixed UTF-8 relative paths normalized to
  `/`.
- `FrameChunk` body is raw compressed bytes of the current zstd frame.
- Default max `FrameChunk` payload size is `1 MiB`.
- The receiver streams `FrameChunk` bytes directly to disk and must not buffer
  the whole frame in RAM.

Current runtime message sequence:

- `Hello { role, protocol_version, dump_id, token }`
- `HelloAck { role }`
- `ResumeQuery { dump_id }`
- `ResumeState { last_committed_frame, last_committed_file_index, last_committed_file_path, archive_len }`
- repeated for each frame:
  - `FrameStart { frame_no, first_file_index, first_file_path, level, threads }`
  - zero or more `FrameChunk { bytes }`
  - `FrameEnd { frame_no, last_file_index, last_file_path, compressed_len, compressed_xxh3_64 }`
  - `Ack { frame_no, last_file_index, last_file_path, archive_len }`
- optional tail before finish:
  - zero or more `IssueJournalChunk { entry_count, entries... }`
- `Finish { final_frame_no, final_file_index, final_file_path }`
- `FinishAck`

Protocol rules:

- `last_committed_file_index` is the monotonic index in deterministic traversal
  order.
- `last_committed_file_path` is stored for diagnostics and extra resume
  validation.
- Sender waits for `Ack` after every `FrameEnd`.
- Receiver computes `compressed_len` and `compressed_xxh3_64` incrementally while
  writing `FrameChunk`s.
- On `FrameEnd`, receiver verifies length and checksum, fsyncs, persists the
  journal, and only then sends `Ack`.
- `IssueJournalChunk` is allowed only after the frame loop and before `Finish`.
- Uploaded issue journal data must not affect resume state; it is persisted only
  after successful finalization.
- If verification fails, receiver sends `Error` and truncates back to the
  previous committed `archive_len`.
- If the connection drops before `Ack`, receiver discards the partial frame by
  truncating back to the last committed `archive_len`.

## File Type Policy

- Support regular files and symlinks.
- Preserve symlinks as symlink entries in tar without dereferencing targets.
- Skip sockets, devices, FIFOs, and other unsupported special entries with a kind-specific warning.

## Testing and Verification

- Main regression coverage lives in:
  - `tests/connectivity/transport.rs`
  - `tests/connectivity/protocol_flow.rs`
  - `tests/connectivity/resume.rs`
  - `tests/connectivity/roundtrip_fast.rs`
  - `tests/connectivity/observability.rs`
  - `tests/connectivity/stress.rs`
- Benchmarks live under `benches/`.
- Before landing behavior changes, run:
  - `cargo test --quiet`
  - `cargo clippy --all-targets --all-features`

## Change Guidelines

- Preserve wire compatibility unless the task explicitly changes protocol
  versioning.
- Do not silently change sender or receiver stdout fields; tests and operators
  rely on them.
- Keep `src/network/protocol.rs` and `src/network/protocol.md` synchronized.
- Keep `README.md` synchronized with real CLI and runtime behavior.
- When refactoring, prefer moving logic between existing modules over creating
  parallel duplicate flows.
