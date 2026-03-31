# REFACTORING3

## Goal

Close the short-term audit backlog without changing the v1 product contract:

- keep the receiver artifact a valid `.tar.zst`
- keep frame-based resume semantics unchanged
- keep `cargo test` green
- restore engineering hygiene so the repo is `fmt`-clean and `clippy -D warnings` clean
- remove panic-based failure paths from the production sender state machine
- make the protocol markdown a reliable source of truth again

## Current Verified State

Signals rechecked after the audits:

- `cargo test -q` passes
- `cargo fmt --check` fails
- `cargo clippy --all-targets --all-features -- -D warnings` fails

Current known red items:

- `src/sender/frame.rs`: `clippy::too_many_arguments` on `prepare_frame_build`
- `src/receiver/shared.rs`: `clippy::items_after_test_module`
- `src/sender/session.rs`: `expect` on streaming sender invariants
- `src/sender/source.rs`: `expect` on source-walker invariant
- `src/network/protocol.md`: documents an outdated handshake-only runtime model
- `Cargo.toml`: unused `jwalk` dependency unless there is an immediate plan to reintroduce it

## Non-Goals

- do not change wire behavior
- do not change CLI semantics
- do not redesign sender/receiver architecture again
- do not mix this pass with new product features
- do not expand telemetry or perf infrastructure before the repo is stable again

## Priority Order

1. Restore `fmt` and `clippy` gates.
2. Remove panic-based invariants from the production sender path.
3. Sync `src/network/protocol.md` with the real runtime contract.
4. Remove small hygiene debt such as `jwalk`.
5. Move heavy stress/perf coverage into a regular pipeline as a follow-up.

## Workstreams

### 1. Restore Quality Gates

Objective:

- make the repository green under the existing local quality commands

Files primarily affected:

- `src/receiver/shared.rs`
- `src/sender/frame.rs`
- formatting-only touches in files already modified by the recent sender/receiver work

Tasks:

- run `cargo fmt` and commit only mechanical formatting changes
- fix `clippy::items_after_test_module` in `src/receiver/shared.rs`
- fix `clippy::too_many_arguments` in `src/sender/frame.rs`
- prefer a small `pub(crate)` parameter object such as `FrameBuildOptions` or `FrameBuildContext`
- avoid `#[allow(clippy::too_many_arguments)]` unless refactoring the signature would materially worsen readability

Constraints:

- no wire change
- no behavioral change
- no broad cleanup outside the touched files

Exit criteria:

- `cargo fmt --check` passes
- `cargo clippy --all-targets --all-features -- -D warnings` passes
- `cargo test -q` still passes

### 2. Remove Panic-Based Sender Invariants

Objective:

- turn internal sender invariant violations into controlled `Result` failures instead of process aborts

Files primarily affected:

- `src/sender/session.rs`
- `src/sender/source.rs`

Known locations:

- `src/sender/session.rs`: scheduled file lookup in the streaming frame loop
- `src/sender/session.rs`: consumed file lookup in the streaming frame loop
- `src/sender/source.rs`: non-empty DFS frame pop

Tasks:

- replace `expect(...)` with `anyhow::bail!(...)` or an equivalent typed error path
- preserve the invariant check itself; only change the failure mode
- make each message explicit about the broken invariant
- keep the messages operationally useful for logs and postmortems
- add focused tests where the invariant can be exercised through existing hooks or local helpers

Constraints:

- do not silently swallow invariant failures
- do not convert invariant failures into ambiguous generic I/O errors
- do not change the happy-path control flow

Exit criteria:

- no `expect(` remains in the production sender path for these known invariant cases
- affected tests remain green
- failure logs become diagnosable without a Rust panic backtrace

### 3. Re-Sync Protocol Documentation

Objective:

- make `src/network/protocol.md` describe the real v1 runtime behavior instead of an obsolete transitional state

Files primarily affected:

- `src/network/protocol.md`

Tasks:

- rewrite the runtime overview so it describes the actual live sequence:
  - `Hello`
  - `ResumeQuery`
  - `ResumeState`
  - repeated `FrameStart` / `FrameChunk` / `FrameEnd` / `Ack`
  - `Finish`
  - `FinishAck`
- describe `FrameChunk` limits and disk-streaming semantics
- document receiver commit ordering and rollback behavior for unacked frames
- document that the final archive is a concatenation of zstd frames
- separate protocol contract from implementation notes where helpful
- remove statements claiming that live runtime uses only handshake messages
- remove statements claiming that `dump_id` is sent as an empty string in the current runtime

Constraints:

- documentation must match the current code and tests, not an earlier milestone
- if behavior is inferred from code rather than explicitly stated elsewhere, keep the wording precise and conservative

Exit criteria:

- the markdown can be used as a current source of truth for integration work
- no obvious contradiction remains between the document and runtime behavior

### 4. Clean Small Repo Hygiene Debt

Objective:

- remove minor debt that adds noise but should not block the earlier hardening steps

Files primarily affected:

- `Cargo.toml`
- `Cargo.lock`

Tasks:

- verify that `jwalk` is not used in `src`, `tests`, or `benches`
- remove `jwalk` from normal dependencies if it is truly unused
- keep this cleanup isolated from larger code refactors
- make a brief note in the PR description if the dependency is intentionally kept after review

Exit criteria:

- dependency graph no longer contains obviously unused normal dependencies
- no test or build target regresses after the cleanup

### 5. Promote Stress Coverage Into Regular Automation

Objective:

- stop relying on manual execution for the heaviest scenario

Files primarily affected:

- `tests/connectivity/stress.rs`
- CI or local automation files once the project chooses a concrete pipeline shape

Tasks:

- decide whether the heavy scenario belongs in nightly CI, scheduled soak CI, or an explicit long-running verification job
- keep it separate from the fast default gate
- retain useful logs or artifacts for diagnosing throughput or flakiness issues
- document how developers are expected to run it locally

Constraints:

- this is a follow-up after the repo is green again
- do not make normal iteration loops slower just to say the test is automated

Exit criteria:

- the heavy scenario runs automatically somewhere on a regular basis
- the team no longer depends on memory or manual discipline to run it

## Recommended PR Sequence

### PR1: Hygiene Gate Recovery

Scope:

- `cargo fmt`
- fix `src/receiver/shared.rs`
- fix `src/sender/frame.rs`

Acceptance:

- green `fmt`
- green `clippy`
- green tests

### PR2: Sender Invariant Hardening

Scope:

- replace sender `expect` paths with controlled errors
- add targeted tests if feasible

Acceptance:

- same runtime behavior on success
- improved failure behavior on invariant break

### PR3: Protocol Markdown And Dependency Cleanup

Scope:

- rewrite `src/network/protocol.md`
- remove `jwalk` if confirmed unused

Acceptance:

- docs match the runtime contract
- dependency graph is cleaner

### PR4: Stress Automation Follow-Up

Scope:

- introduce scheduled or long-running stress coverage

Acceptance:

- heavy scenario executes regularly outside the fast gate

## Suggested Execution Notes

- Keep PR1 and PR2 separate even if both are small. The first restores trust in the hygiene signals, the second changes failure behavior.
- Avoid bundling `protocol.md` edits into the gate-recovery PR. Documentation drift is important, but it is easier to review after code-health signals are green.
- If `prepare_frame_build` needs only a tiny wrapper struct to satisfy `clippy`, prefer that over a larger refactor.
- If invariant tests are hard to trigger through public APIs, add narrow local tests near the affected modules rather than broad integration scaffolding.

## Definition Of Done

This refactoring plan is complete when all of the following are true:

- `cargo fmt --check` passes
- `cargo clippy --all-targets --all-features -- -D warnings` passes
- `cargo test -q` passes
- known sender invariant `expect` calls in the production path are removed
- `src/network/protocol.md` accurately describes the real runtime protocol
- unused dependency debt is removed or explicitly justified
- a concrete path exists for regular stress/perf execution beyond manual runs
