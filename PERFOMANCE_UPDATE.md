# Performance Update

Status: redesign and follow-up issue journal upload implemented; coarse matrix rerun completed.

Date: 2026-03-31

## Post-Redesign Outcome

The agreed redesign is no longer only a plan. The following pieces are now in
the runtime path:

- explicit `FrameAbort` / `AbortAck`
- whole-frame rollback and rebuild without reconnect
- bounded sender-side issue journal
- receiver-side issue journal upload and persistence
- in-memory small-file snapshots
- direct streaming for large files
- bounded ordered parallel walk
- soft frame policy
- reduced per-file flush pressure
- extended bench/profile metrics for traversal wait, ack wait, frame aborts,
  frame rebuilds, flush count, skip counts, and files per frame

The full coarse matrix (`216` scenarios, `repeats=1`) was rerun after the
redesign.

Best measured scenarios:

- `manySmallFiles`: `threads=1`, `frame_target_mb=16`, `read_buffer_kb=64`
  - `38.18 MiB/s`
  - `2301.80 ms`
  - `10` frames, `4000` files per frame
- `bigFiles`: `threads=4`, `frame_target_mb=64`, `read_buffer_kb=4096`
  - `1543.16 MiB/s`
  - `829.47 ms`
  - `3` frames, `1.00` files per frame
- `mixed`: `threads=2`, `frame_target_mb=16`, `read_buffer_kb=64`
  - `400.09 MiB/s`
  - `2184.43 ms`
  - `137` frames, `73.97` files per frame

Practical conclusions:

- `manySmallFiles` improved by roughly two orders of magnitude versus the old
  pre-redesign sub-`1 MiB/s` behavior and no longer rewards higher `threads`.
- `bigFiles` prefers moderate parallelism and large read buffers, but gains
  flatten out before extreme thread counts.
- `mixed` now lands on moderate `threads` and medium frame size, which is more
  operationally reasonable than simply taking the highest core count.
- `frame_aborts` stayed at `0` in healthy coarse-matrix runs, which means the
  rollback path is available but does not cost steady-state throughput.
- `ack_wait_ms` and `traversal_wait_ms` are now measurable and should be used
  for future receiver-side and traversal tuning instead of guessing.

## Why This Exists

The current benchmark results show that `manySmallFiles` does not scale with higher
`threads` the way we expected. The agreed interpretation is:

- `zstd threads` are not the main bottleneck for file-heavy workloads.
- Full prescan is still rejected because it is too expensive for large and highly
  branched source trees.
- Disk-backed per-file snapshots are too expensive for hot-path performance and
  are a bad fit for very large files.

The remaining sections keep the original rationale and phase breakdown, but the
runtime behavior described above is already implemented in the repository.

## Main Constraints

- Preserve deterministic traversal order and stable `file_index`.
- Preserve resume semantics based on committed frames.
- Do not require full pending-file prescan.
- Do not require local temp disk proportional to the size of the largest file.
- Keep file retry/skip semantics explicit and bounded.

## Agreed Direction

### 1. Ordered Parallel Walk

We will keep a single traversal coordinator that remains the source of truth for
deterministic DFS order and `file_index` assignment.

We will add a bounded directory-listing worker pool:

- When the coordinator discovers a directory, it may submit a listing task.
- A listing task performs `read_dir`, entry-type inspection, path normalization,
  and local sorting for that directory.
- The coordinator consumes prepared directory listings only in deterministic DFS
  order.
- The number of in-flight directory tasks stays bounded, so this is not a full
  prescan.

Expected effect:

- Better overlap of directory listing and metadata work on wide, branched trees.
- Lower traversal latency without losing deterministic ordering.

Important limitation:

- This does not remove per-file hot-path costs such as `open`, metadata
  capture, retry bookkeeping, tar header/padding work, and frame bookkeeping.

### 2. Per-File Path Still Matters

For `manySmallFiles`, throughput is dominated by fixed work per file, not only by
bulk byte throughput.

The important fixed costs are:

- `open` and metadata capture
- retry bookkeeping
- tar header and padding generation
- frame bookkeeping
- sender-side coordination around file boundaries

This is why parallel walk alone is not expected to unlock large scaling.

### 3. Direct Streaming With Whole-Frame Rollback

We will remove disk-backed per-file snapshots from the main send path.

New core model:

- Sender reads file data and streams it directly into tar, zstd, and network.
- If a file read fails during an uncommitted frame, the sender aborts the whole
  current frame.
- The receiver rolls back to the last committed `archive_len`.
- The sender rebuilds the frame and retries the failed file.

Reasoning:

- This removes the temp-disk bottleneck from the hot path.
- This allows sending files that would not fit into local temp staging.
- This matches the already accepted model that only committed frames are durable.

### 4. Retry And Skip Policy

The retry policy stays file-based and bounded:

- Retry a file up to `3` times.
- A failure during the file causes whole-frame rollback.
- After the third failed attempt, the file becomes `skip`.
- Skipped files are terminal for the current dump and must not cause endless
  frame rebuilds.

Sender records a local issue journal entry for every terminal skip.

Minimum journal fields:

- `file_index`
- `path`
- `attempts`
- `reason`
- `last_error`

The journal is included in sender reporting and is uploaded to the receiver as a
compact protocol tail before `Finish`. Receiver persists it next to the final
archive as `<dump_id>.tar.zst.issues.tsv`.

### 5. Protocol Update For Explicit Frame Abort

To support direct streaming without forced reconnect-based rollback, the protocol
will be extended.

Planned messages:

- `FrameAbort { frame_no }`
- `AbortAck { frame_no, archive_len }`

Planned behavior:

- Sender sends `FrameAbort` when the current uncommitted frame must be discarded.
- Receiver truncates the partial archive back to the last committed `archive_len`.
- Receiver clears transient state for the aborted frame and replies with
  `AbortAck`.
- Sender then rebuilds the frame from the last committed boundary.

This keeps rollback explicit and avoids using connection drops as control flow.

### 6. Two Data Paths Instead Of Temp-File Staging

We will split the hot path by file size and risk profile.

Small files:

- Reader pool may prefetch file contents into memory.
- Goal: keep file-level parallelism for file-heavy workloads without temp files.

Large files:

- Stream directly from source file to tar -> zstd -> network.
- Goal: avoid local temp-disk requirements and unnecessary double I/O.

This replaces disk staging as the default strategy.

### 7. Dynamic Frame Policy

We will replace the current strict fixed-size frame handling with a soft policy.

Planned frame policy inputs:

- `soft_target_bytes`
- `max_files_per_frame`
- `large_file_single_frame_threshold`
- `retried_file_isolated`

Planned behavior:

- `soft_target_bytes` is a target, not a hard boundary.
- Very large files should go into their own frame.
- A file that already failed at least once should be isolated into its own frame
  when practical.
- For many tiny files, `max_files_per_frame` limits rollback blast radius.

### 8. Per-File Flush Reduction

The current design flushes too aggressively at file boundaries. The updated
design will reduce that.

Planned change:

- Stop treating every file boundary as a mandatory zstd flush point.
- Close or flush a frame only when the frame policy requires it.

Tradeoff:

- Frame size becomes less exact.
- Compression and write batching should improve.

## Implementation Order

### Phase 1. Protocol And Safety

- Add `FrameAbort` and `AbortAck`.
- Implement receiver rollback for explicit frame abort.
- Keep current commit semantics unchanged for committed frames.

### Phase 2. Sender Retry/Skip Rebuild Logic

- Refactor sender frame builder around whole-frame rollback.
- Preserve `3` attempts per file.
- Add sender-side issue journal for terminal skips.

### Phase 3. Remove Disk Staging From Hot Path

- Introduce direct streaming for large files.
- Introduce in-memory prefetch for small files.
- Remove temp-file snapshots from the normal fast path.

### Phase 4. Ordered Parallel Walk

- Add bounded async directory listing.
- Keep DFS ordering and stable `file_index`.
- Avoid full pending-file prescan.

### Phase 5. Dynamic Frames

- Add soft frame policy.
- Isolate large files.
- Isolate retried files where practical.
- Reduce per-file flush pressure.

### Phase 6. Bench And Observability

Extend benchmarks and profiling so we can validate the redesign with stage-level
evidence.

Wanted measurements:

- directory listing wait time
- frame abort count
- frame rebuild count
- skipped file count and reasons
- per-frame file count
- explicit ack wait time
- flush count

## Non-Goals

- Reintroducing full prescan for the main send path
- Preserving disk-backed temp snapshots as the primary correctness mechanism
- Using connection drops as the normal control path for frame rollback

## Current Decision Summary

The agreed update direction is:

1. Keep deterministic order, but parallelize directory listing in a bounded way.
2. Improve the protocol with explicit frame abort and rollback.
3. Replace temp-disk staging with direct streaming plus bounded retry/skip logic.
4. Add dynamic frame sizing policy instead of relying on strict fixed frame
   targets and aggressive per-file flushing.
