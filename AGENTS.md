# fast-dump

Operational contract for contributors and agents. Keep this file about current behavior, hard constraints, and navigation.

## Goal

- Maintain a sender/receiver pair that streams a resumable dump to a remote host.
- Receiver output must be a valid `.tar.zst`.
- `sender` and `receiver` are named by data flow, not by who opened TCP.

## Priorities

- `BLAZING FAST`: prefer the fastest correct design.
- In hot paths, avoid unnecessary copies, allocations, buffering, syscalls, temp files, and abstraction unless clearly justified.
- Do not trade away external contract, correctness, or resumability for speed unless the task explicitly changes that contract.

## Current Status

- The implementation already works and is covered by integration tests.
- Supported transport pairings:
  - `push`: `sender --connect` with `receiver --listen`
  - `pull`: `sender --listen` with `receiver --connect`
- The active CLI/runtime contract is packet version `3`.
- Mainline runtime is single-stack: sender, receiver, CLI, and benches all use the same `v3` protocol path.

## Hard Contract

- Preserve wire compatibility unless the task explicitly changes protocol versioning.
- Preserve Linux/Windows runtime compatibility unless the task is explicitly platform-scoped.
- Ship one binary with two subcommands: `fast-dump sender`, `fast-dump receiver`.
- Each subcommand accepts exactly one transport selector: `--listen <ADDR>` or `--connect <ADDR>`.
- Reject cross-process role/pairing mismatches during transport establishment and handshake, not only in local CLI parsing.
- Required args: sender `--source <DIR>`, receiver `--data-dir <DIR>`.
- Default `--dump-id` to the last path component of `--source`.
- `--threads auto` = `available_parallelism() - 1`, minimum `1`.
- Reject invalid runtime settings: zero threads, zero readers, `min_readers > max_readers`, `max_sessions < 1`.
- Archive rules:
  - include successfully sent regular files
  - include readable symlinks without dereferencing
  - ignore empty directories and directory metadata
  - use relative tar paths normalized to `/`
- Keep traversal deterministic: depth-first, with each directory sorted by normalized relative path.
- Source discovery is only for ordering, filtering, normalization, and stable `file_index`; scan-time metadata is not authoritative for tar sizing or live-file validation.
- Live-file policy for regular files:
  - open immediately before each attempt
  - use `size_at_open` from the opened handle
  - read exactly `size_at_open` bytes
  - ignore appended tail after `open`
  - on `open` failure or short read, restart the file from scratch
  - allow at most `3` immediate attempts, then skip the file
- Output `.tar.zst` is a concatenation of zstd frames, not one endless stream.
- Sender closes zstd frames only at completed file boundaries.
- Receiver commits only whole frames; after disconnect, at most the current unacked frame may be lost.
- Active resume state is split into durable manifest and archive boundaries; skipped files remain terminal for the current dump and are reported through the typed issue tail.
- Sender CLI stdout stays aggregate-only; do not silently change stable fields or counters.
- Keep `src/network/protocol.rs` and `src/network/protocol.md` synchronized.

## Read First

- `README.md`: user overview, CLI examples, runtime summary.
- `AGENTS.md`: contributor contract and navigation.
- `src/network/protocol.md`: wire format and current message flow.

## More Context

- `docs/SAFETY.md`: reliability and test strategy; planning doc, not runtime contract.
- `docs/BLAZING_PERF_PLAN.md`: active perf backlog, attribution baseline, and current ROI order.
- `docs/V2_V3_TEST_TABLE.md`: historical `v2 vs v3` comparison snapshot plus current `v3` reference rows.
- Runtime truth lives in code, `README.md`, and `src/network/protocol.md`. Planning docs explain pending work; do not present frozen or planned behavior as active.

## Code Map

- Top-level flow: `src/main.rs`, `src/app.rs`, `src/cli/mod.rs`, `src/network/transport.rs`, `src/network/protocol.rs`.
- Sender: `src/sender/source.rs`, `src/sender/readers.rs`, `src/sender/frame.rs`, `src/sender/session.rs`, `src/sender/profile.rs`.
- Receiver: `src/receiver/journal.rs`, `src/receiver/resume.rs`, `src/receiver/frame.rs`, `src/receiver/finish.rs`, `src/receiver/session.rs`, `src/receiver/listener.rs`, `src/receiver/shared.rs`.
- Validation: `tests/connectivity/`, `benches/`.

## Working Rules

- Follow `YAGNI`, `KISS`, and `SOLID` when designing and changing code.
- Write explanatory code comments in Russian; prefer comments about purpose, data flow, and non-obvious Rust details.
- Keep fast paths compatible with Linux and Windows, or provide a correct fallback with the same external behavior.
- Keep `src/sender/mod.rs` and `src/receiver/mod.rs` as thin re-export facades.
- Prefer `pub(crate)` over `pub`.
- Keep tests close to the sender module they exercise when practical.
- Keep `README.md`, `AGENTS.md`, and `src/network/protocol.md` aligned with actual runtime behavior.
- If CLI flags, protocol fields, session flow, or resume semantics change, update docs in the same task.

## Verification

- `cargo test --quiet`
- `cargo clippy --all-targets --all-features`
