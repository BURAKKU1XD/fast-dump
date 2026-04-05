# fast-dump

This file is the short operational contract for contributors and agents.
Keep it focused on current behavior, hard constraints, and navigation.

## Goal

- Maintain a sender/receiver pair that streams a resumable dump to a remote host.
- The final receiver artifact must be a valid `.tar.zst` file.
- `sender` and `receiver` are named by data flow, not by who opened TCP.

## Performance Priority

- The project bias is maximum performance: `BLAZING FAST`.
- Prefer the fastest correct design, even if it is harder to implement or less pretty internally.
- In hot paths, avoid unnecessary copies, allocations, buffering, syscalls, temp files, and abstraction layers unless they are clearly justified.
- When choosing between multiple correct approaches, prefer higher throughput, lower latency, and lower RAM/disk overhead.
- Do not break the external contract, correctness, or resumability for speed unless the task explicitly changes that contract.

## Current Runtime Status

- The repository already contains a working implementation.
- `fast-dump sender` and `fast-dump receiver` are both exercised by integration tests.
- Both transport pairings work:
  - `push`: `sender --connect` with `receiver --listen`
  - `pull`: `sender --listen` with `receiver --connect`
- The active CLI/runtime contract is still packet version `2`.
- Frozen `v3` protocol and library-level session helpers exist in code, tests, and benches, but CLI transport has not switched to `v3` by default.

## Non-Negotiable Contract

- Preserve wire compatibility unless the task explicitly changes protocol versioning.
- Preserve runtime compatibility between Linux and Windows unless the task explicitly scopes behavior to one platform.
- Use one binary with two subcommands:
  - `fast-dump sender`
  - `fast-dump receiver`
- Each subcommand accepts exactly one transport selector:
  - `--listen <ADDR>`
  - `--connect <ADDR>`
- Cross-process role/pairing errors are rejected during transport establishment and handshake, not by local CLI parsing alone.
- Sender requires `--source <DIR>`; receiver requires `--data-dir <DIR>`.
- If `--dump-id` is omitted, sender falls back to the last path component of `--source`.
- `--threads auto` resolves to `available_parallelism() - 1`, minimum `1`.
- Runtime validation rejects zero threads, zero reader counts, `min_readers > max_readers`, and `max_sessions < 1`.
- Archive contents:
  - regular files that were successfully sent
  - symlink entries whose targets are preserved without dereferencing when readable
  - empty directories and directory metadata are ignored
- Tar paths are relative to the selected root and normalized to `/`.
- Traversal order is deterministic: depth-first with entries inside each directory sorted by normalized relative path.
- Source discovery is only for deterministic ordering, filtering, normalization, and stable `file_index` assignment. Scan-time metadata is not the source of truth for tar sizing or live-file validation.
- Live-file policy for regular files:
  - open the file immediately before an attempt
  - take `size_at_open` from the opened handle
  - read exactly `size_at_open` bytes for that attempt
  - if the file grows after `open`, ignore the appended tail for the current dump
  - if `open` fails or a short read happens, restart the file from scratch
  - allow at most `3` immediate attempts
  - if all attempts fail, skip the file and continue
- The output `.tar.zst` is a concatenation of zstd frames, not one endless zstd stream.
- Sender-side zstd frames close only on completed file boundaries.
- Receiver commits whole frames only. On disconnect, at most the current unacked frame may be lost.
- Active `v2` resume progress is based on deterministic `file_index` plus diagnostic path; skipped files are terminal for the current dump.
- Sender CLI output is aggregate-only. Do not silently change stable stdout fields or counters.
- Keep `src/network/protocol.rs` and `src/network/protocol.md` synchronized.

## Route Through Docs

Read these in order when you need orientation:

- `README.md`: user-facing overview, CLI examples, runtime summary.
- `AGENTS.md`: short contributor contract and navigation.
- `src/network/protocol.md`: wire format, message flow, and frozen `v3` notes.

Use these for deeper context:

- `docs/SAFETY.md`: reliability and test strategy. This is a plan document, not the active runtime contract.
- `docs/PERFOMANCE_V3.md`: `v3` performance plan, benchmark gates, and current cutover status.
- `docs/V3_BOTTLENECK.md`: current `v3` bottleneck analysis.
- `docs/TASKS.json`: machine-readable tracker derived from planning docs.

Rule of thumb:

- Runtime truth lives in code, `README.md`, and `src/network/protocol.md`.
- Planning docs in `docs/` explain why work exists and what is still pending.
- Do not describe frozen or planned behavior as if it were already active.

## Route Through The Project

Start here for top-level flow:

- `src/main.rs`: binary entrypoint.
- `src/app.rs`: CLI dispatch and runtime orchestration.
- `src/cli/mod.rs`: clap model and argument validation.
- `src/network/transport.rs`: TCP setup and handshake bootstrap.
- `src/network/protocol.rs`: packet codec, message definitions, and protocol validation.

Go here for sender work:

- `src/sender/source.rs`: deterministic source discovery and resume validation helpers.
- `src/sender/readers.rs`: snapshot attempts, retries, buffering, and large-file direct handles.
- `src/sender/frame.rs`: tar and zstd frame assembly helpers.
- `src/sender/session.rs`: production streaming send loop.
- `src/sender/profile.rs`: sender-side pipeline observability.

Go here for receiver work:

- `src/receiver/journal.rs`: persisted receiver state and recovery.
- `src/receiver/resume.rs`: resume bootstrap and reconciliation.
- `src/receiver/frame.rs`: frame commit path.
- `src/receiver/finish.rs`: finalization into the completed archive.
- `src/receiver/session.rs`: receiver session loop.
- `src/receiver/listener.rs`: listen-mode accept loop and session limits.
- `src/receiver/shared.rs`: shared receiver helpers and types.

Go here for validation:

- `tests/connectivity/`: end-to-end transport, protocol flow, resume, roundtrip, and observability regressions.
- `benches/`: benchmark entrypoints, including the loopback matrix runner.

## Working Rules

- Leave explanatory comments in code in Russian.
- Prefer comments that explain purpose, data flow, and non-obvious Rust details.
- Keep fast paths and optimizations compatible with both Linux and Windows, or provide a correct fallback with the same external behavior.
- Keep `src/sender/mod.rs` and `src/receiver/mod.rs` as thin facades with re-exports only.
- Prefer `pub(crate)` over `pub` where possible.
- Keep tests close to the sender module they exercise when practical.
- Keep `README.md`, `AGENTS.md`, and `src/network/protocol.md` aligned with actual runtime behavior.
- When CLI flags, protocol fields, session flow, or resume semantics change, update docs in the same task.

## Verification

Before landing behavior changes, run:

- `cargo test --quiet`
- `cargo clippy --all-targets --all-features`
