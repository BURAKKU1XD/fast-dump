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

## Current runtime

The working sender/receiver pair uses protocol v4 and supports push and pull.
Read [docs/runtime.md](docs/runtime.md) for all implementation invariants and
[docs/cli.md](docs/cli.md) for CLI, wizard, dashboard and retry behavior.
These documents are part of the contributor contract. Preserve their requirements.

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

- `README.md`: overview, quickstart and navigation.
- `docs/cli.md`: CLI and presentation contract.
- `docs/runtime.md`: data path, memory, lifecycle and implementation invariants.
- `src/network/protocol.md`: active wire format, durability and session flow.
- `docs/development.md`: code map and verification.
- `docs/benchmarks.md`: measurement semantics and persistent results.

## Historical material and results

`docs/history/` describes earlier designs; it is not the active runtime contract.
Compact benchmark measurements and provenance under `results/benchmarks/` and
verification summaries under `results/tests/` are tracked. Detailed logs, imports
and historical evidence remain local under `results/local/` and `results/history/`.
Generate `results/index.html` with `python scripts/results.py index`.
Keep historical verification manifests unchanged; record cleanup in a separate
migration manifest. Builds, symbols, generated fixtures and dump payloads are
disposable after their results have been collected and verified.

## Code Map

- Top-level flow: `src/main.rs`, `src/app.rs`, `src/cli/mod.rs`.
- Terminal presentation: `src/launch.rs`, `src/ui/`, `src/progress.rs`.
- Cooperative cancellation and socket ownership: `src/runtime.rs`.
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
- `cargo fmt --all --check`
- `cargo test --locked --quiet`
- `cargo test --locked --all-features --quiet`
- `cargo clippy --locked --all-targets --all-features -- -D warnings`
- `cargo check --locked --benches --all-features`
- `python -B -m unittest discover -s scripts -p "test_*.py"`
- Push/PR CI covers Windows/Linux and Rust `1.89.0`/stable with locked
  dependencies. Manual release packaging additionally gates on release tests
  and correctness stress, then records source/compiler/binary hashes.
- Keep storage fault hooks under `cfg(test)`; release I/O must not pay for
  injection checks. Lifecycle regressions must leave the other peer channel
  open until the receiver returns and verify that its reader handles are gone.
