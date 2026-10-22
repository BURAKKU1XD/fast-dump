# fast-dump

Operational contract for contributors and agents. Keep this file about current
behavior, hard constraints, and navigation.

## Goal

- Maintain a sender/receiver pair that streams a resumable dump to a remote
  host.
- Receiver output must be a valid `.tar.zst`.
- `sender` and `receiver` are named by data flow, not by who opened TCP.

## Priorities

- `BLAZING FAST`: prefer the fastest correct design.
- In hot paths, avoid unnecessary copies, allocations, buffering, syscalls, temp
  files, and abstraction unless clearly justified.
- Do not trade away external contract, correctness, or resumability for speed
  unless the task explicitly changes that contract.

## Current Status

- The implementation already works and is covered by integration tests.
- Supported transport pairings:
  - `push`: `sender --connect` with `receiver --listen`
  - `pull`: `sender --listen` with `receiver --connect`
- The active CLI/runtime contract is packet version `4`.
- Mainline execution path uses the bundle-first runtime in canonical module
  locations.
- Source scanning uses up to `8` subtree workers and merges results in the same
  deterministic depth-first order.
- File reads use ordered, bounded read-ahead: `4` entries per reader and at most
  `256 KiB` per prefetched regular-file payload. The default reader maximum of
  `32` bounds this window to `128` entries and `32 MiB` of file payload.
  Workers process up to four adjacent entries per job; small-file payloads share
  one immutable buffer sized from opened handles. A batch crossing a bundle
  boundary can retain up to `3 * 256 KiB` of already-consumed payload in addition
  to the window until its last entry is released.
  Larger files retain bounded open handles until the planner materializes or
  streams them.
- `--min-readers` (default `2`) and `--max-readers` (default `32`) bound the
  requested available-parallelism reader count; actual workers are capped at
  `ceil(pending entries / 32)` to amortize startup. A count of `1`, including
  pending sets of at most `32` entries, uses the serial path.
  `--threads` controls zstd compression workers (`1` is synchronous);
  `--read-buffer-kb` is still validated but does not tune the data path.
- Bundle `1` uses aggregation target `min(configured target, 4 MiB)`; later
  bundles use the configured target (`64 MiB` by CLI default). Whole files may
  exceed the aggregation target. The materialization/streaming threshold always
  remains the configured target.
- Buffered and streamed sender paths reuse zstd contexts across bundles and
  retries while producing independent frames.
- Streamed reading/compression overlaps network writes through a two-chunk
  queue. Four reused `4 MiB` buffers bound this stage to `16 MiB` of compressed
  payload, excluding zstd workspace. Cancel and join the producer before abort,
  retry, or returning a network error.
- Current active protocol reference lives in `src/network/protocol.md`.

## Hard Contract

- Preserve wire compatibility unless the task explicitly changes protocol
  versioning.
- Preserve Linux/Windows runtime compatibility unless the task is explicitly
  platform-scoped.
- Ship one binary with two subcommands: `fast-dump sender`, `fast-dump receiver`.
- Each subcommand accepts exactly one transport selector:
  `--listen <ADDR>` or `--connect <ADDR>`.
- Reject cross-process role/pairing mismatches during transport establishment
  and handshake, not only in local CLI parsing.
- Required args: sender `--source <DIR>`, receiver `--data-dir <DIR>`.
- Default `--dump-id` to the last path component of `--source`.
- Validate dump IDs as portable single filename components at the CLI, handshake,
  and receiver storage boundaries; reject path separators, control characters,
  Windows metacharacters/device names, and trailing dots/spaces.
- `--threads auto` resolves to `available_parallelism() - 1`, minimum `1`.
- Reject invalid runtime settings:
  - `--frame-target-mb 0`
  - `--threads 0`
  - `--min-readers 0`
  - `--max-readers 0`
  - `--min-readers > --max-readers`
  - `--read-buffer-kb 0`
  - `--max-sessions 0`
- Archive rules:
  - include successfully sent regular files
  - include readable symlinks without dereferencing
  - ignore empty directories and directory metadata
  - use relative tar paths normalized to `/`
- Keep traversal deterministic: depth-first, with each directory sorted by
  normalized relative path.
- Keep each encoded `BundlePlan` body within `4 MiB`. Split metadata-heavy
  bundles by carrying the next whole entry forward; never skip an entry to meet
  this limit. Reject a single descriptor that cannot fit.
- Live-file policy for regular files:
  - open immediately before each attempt
  - use `size_at_open` from the opened handle
  - read exactly `size_at_open` bytes
  - ignore appended tail after `open`
  - on `open` failure or short read, restart the file from scratch
  - allow at most `3` immediate attempts across all reader/planner/streaming
    stages, then skip the file
- Output `.tar.zst` is a concatenation of zstd frames, not one endless stream.
- Receiver resumes only from durable committed bundle boundaries.
- Each WAL record stores that bundle's exact archive end offset. Recovery removes
  only an incomplete final record without a newline. Reject malformed complete
  records, inconsistent commit sequences, and archives shorter than the durable
  boundary without resetting acknowledged progress.
- Hold an exclusive OS lock on `<dump_id>.lock` from before recovery/migration
  until archive/WAL handles close. Keep the lock file after releasing the lock.
- Serialize receiver initialization with `.fast-dump-open-lock` to distinguish
  canonical `name.v4` files (marked by `name.v4.lock`) from legacy `name` state.
  Reject existing empty or malformed terminal-skip catalogs without replacing them.
- Preserve completed archives even when stale `.part` or WAL files remain;
  reject reopening the dump rather than overwriting it.
- On Unix, sync state parent directories after creation/rename and before
  acknowledging durability or removing resume state.
- After disconnect, at most the current uncommitted receiver commit group may be
  lost.
- Terminal skips remain durable for the current dump and are reported through
  the issue tail file.
- Sender CLI stdout stays aggregate-only; do not silently change stable fields
  or counters.
- Keep `README.md`, `AGENTS.md`, and `src/network/protocol.md` synchronized with the
  active runtime behavior.

## Read First

- `README.md`: user overview, CLI examples, runtime summary.
- `AGENTS.md`: contributor contract and navigation.
- `src/network/protocol.md`: active wire format and message flow.

## More Context

- `docs/history/V4_CUTOVER_PLAN.md`: completed cutover record and checklist; planning
  doc, not runtime contract.
- `docs/history/REWRITE_PERF_PLAN.md`: bundle-first perf backlog and rewrite rationale.
- `docs/history/`: historical `v3` planning docs and reference material.
- Runtime truth lives in code, `README.md`, and `src/network/protocol.md`. Historical
  docs explain old behavior only.

## Code Map

- Top-level flow: `src/main.rs`, `src/app.rs`, `src/cli/mod.rs`.
- Active runtime:
  - `src/network/`
  - `src/sender/`
  - `src/receiver/`
  - `src/network/protocol.md`
- Validation: `tests/connectivity/`, `tests/bench_matrix.rs`, `benches/`.

## Working Rules

- Follow `YAGNI`, `KISS`, and `SOLID` when designing and changing code.
- Write explanatory code comments in Russian; prefer comments about purpose,
  data flow, and non-obvious Rust details.
- Keep fast paths compatible with Linux and Windows, or provide a correct
  fallback with the same external behavior.
- Keep `src/sender/mod.rs` and `src/receiver/mod.rs` as thin public facades over
  their local implementation modules.
- Prefer `pub(crate)` over `pub`.
- Keep tests close to the module they exercise when practical.
- If CLI flags, protocol fields, session flow, archive naming, or resume
  semantics change, update docs in the same task.

## Verification

- Minimum supported Rust version: `1.89`.
- `cargo test --quiet`
- `cargo clippy --all-targets --all-features`
- `cargo check --benches`
