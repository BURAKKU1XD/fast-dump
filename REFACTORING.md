# REFACTORING

## Goal

Implement the new best-effort live-file model without breaking the core dump contract:

- keep the receiver artifact a valid `.tar.zst`
- keep frame-based resume semantics
- stop using scan-time file size as the source of truth
- avoid endless retry loops on hot files
- emit aggregate sender reporting instead of per-file reporting

## Target Behavior

- Discovery defines deterministic order, normalized relative paths, filtering, and stable `file_index` values only.
- The sender opens each file immediately before archiving it.
- Tar headers use `size_at_open` from the opened handle.
- Reading exactly `size_at_open` bytes is the success condition for a file snapshot attempt.
- File growth after `open` is accepted; the appended tail is ignored for the current dump.
- `open` failures and short reads are retried immediately up to `3` times.
- After `3` failed attempts, the file is skipped.
- The dump completes with aggregate warnings instead of failing the whole session on a single hot file.

## Constraints

- No per-file report output.
- No infinite retry behavior on hot files.
- Large files must still be streamed and must not be loaded fully into RAM.
- Frame boundaries must still close only on completed file boundaries.
- Prefer no wire-format change; if skipped files require boundary handling, solve it inside the current frame/ack model first.

## Main Design Risk

Skipped files create a progress gap: the receiver currently expects contiguous `file_index` progress, but skipped files do not produce tar payload.

This must be settled before broad code changes. Preferred direction:

- keep the current message schema
- keep skipped files terminal for the current dump
- advance committed progress across skipped indices without introducing per-file wire messages
- if a skipped prefix would otherwise block contiguous progress, use a sender-side synthetic commit strategy inside the existing frame protocol rather than adding a new message type

## Workstreams

### 1. Rework Discovery Semantics

Files:

- `src/sender/source.rs`

Tasks:

- remove scan-time size from the correctness path
- keep only ordering, path normalization, filtering, and stable file indexing as authoritative discovery outputs
- review resume validation so it depends on deterministic file order and paths, not on stale file sizes

### 2. Rework Live File Snapshot Attempts

Files:

- `src/sender/readers.rs`

Tasks:

- open the file per snapshot attempt instead of trusting scan metadata
- capture `size_at_open` from the opened handle
- read exactly `size_at_open` bytes
- treat file growth after `open` as success, not as an error
- retry `open` failures and short reads immediately up to `3` times
- classify each file as `clean`, `best_effort`, or `skip`
- preserve bounded buffering and existing reader-pool backpressure guarantees

### 3. Rework Tar/Zstd Frame Assembly

Files:

- `src/sender/frame.rs`

Tasks:

- build tar headers from per-attempt snapshot metadata instead of discovery-time size
- pass snapshot results from the reader layer into frame assembly
- preserve the current frame-target rule: a frame may exceed the target only to finish the current file
- ensure skipped files do not accidentally produce tar headers or tar payload

### 4. Rework Session Progress and Resume Semantics

Files:

- `src/sender/session.rs`
- `src/receiver/frame.rs`
- `src/receiver/finish.rs`
- `src/receiver/journal.rs`

Tasks:

- define how committed progress advances across skipped file indices
- keep reconnect/resume from retrying the same permanently skipped hot file forever
- ensure `Finish` remains consistent with the last committed boundary after best-effort sends and skips
- verify whether receiver-side validation must change or whether sender-side synthetic commits are sufficient

### 5. Add Aggregate Sender Reporting

Files:

- `src/sender/session.rs`
- `src/app.rs`

Tasks:

- extend sender-side result accounting with aggregate counters
- track:
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
- expose a session status of either `clean` or `completed_with_warnings`
- print or otherwise emit only aggregate summary information

### 6. Test the New Semantics

Files:

- sender unit tests near `src/sender/source.rs`
- sender unit tests near `src/sender/readers.rs`
- sender unit tests near `src/sender/frame.rs`
- sender unit tests near `src/sender/session.rs`
- focused receiver tests if resume validation changes

Required coverage:

- file append during read no longer fails when growth happens after `open`
- short read triggers retry and eventually skip after the retry budget is exhausted
- exclusive-share `open` failure becomes skip after `3` attempts
- retry success is counted as `best_effort`
- skipped files do not create endless retry loops across reconnects
- aggregate counters and final session status are correct
- frame and finish boundaries remain valid after best-effort and skip scenarios

## Suggested Implementation Order

1. Settle the skipped-index advancement strategy.
2. Update `src/sender/source.rs` so discovery no longer owns authoritative file size.
3. Rework `src/sender/readers.rs` around `size_at_open`, retries, and file outcome classification.
4. Rework `src/sender/frame.rs` to consume live snapshot metadata.
5. Rework `src/sender/session.rs` to track progress, reporting, and skipped-file behavior.
6. Adjust receiver-side validation only if sender-side boundary handling is insufficient.
7. Add and stabilize tests for hot-file, append, skip, and resume scenarios.

## Definition of Done

The refactor is complete when all of the following are true:

- scan-time file size is no longer used as the tar correctness source of truth
- file append after `open` no longer fails the dump by itself
- a hot or exclusively locked file is retried at most `3` times and then skipped
- the dump no longer loops forever on the same unstable file across reconnects
- the sender emits aggregate reporting only
- the receiver still produces a valid `.tar.zst`
- frame commit and finish boundaries remain coherent under the new best-effort model
