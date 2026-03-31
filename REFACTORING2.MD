# REFACTORING2

## Goal

Refactor the sender pipeline in two global phases so it scales both by:

- number of files
- total dump size

while preserving the v1 contract:

- the receiver artifact remains a valid `.tar.zst`
- resume stays frame-based
- frame boundaries still close only on completed file boundaries
- large files are still streamed and never loaded fully into RAM
- wire semantics stay stable unless phase 2 explicitly requires an internal sender-side reshaping only

## Problem Statement

The current sender has two separate scalability limits:

- it fully materializes the pending file map before the first frame starts
- it fully materializes each compressed frame in memory before the first `FrameChunk` is sent

This means the sender scales poorly in two different dimensions:

- many files cause long startup latency and high metadata RAM usage
- large frame targets or poor compression ratios cause high per-frame RAM usage

The refactor should remove these limits in two controlled steps instead of trying to rewrite the whole sender pipeline at once.

## Global Strategy

### Phase 1: Streaming Discovery And Lazy Frame Assembly

Replace full pending-file materialization with an incremental DFS walker that feeds the sender pipeline on demand.

Target outcome:

- no full `Vec<InputFile>` for the production send path
- resume validation happens while discovery progresses
- frame assembly pulls files lazily from discovery instead of from a prebuilt slice
- sender can start building the first resumable frame shortly after it passes the resume boundary

### Phase 2: Streaming Frame Send

Replace "build full frame in memory, then send it" with direct frame streaming from tar/zstd assembly into protocol `FrameChunk` writes.

Target outcome:

- no full compressed frame buffering in RAM for the production send path
- sender sends `FrameChunk` packets as compressed bytes are produced
- sender still waits for `Ack` only after `FrameEnd`
- sender still guarantees that the receiver commits only whole frames

## Non-Goals

- do not change sender/receiver roles or CLI semantics
- do not introduce HTTP, JSON, gRPC, or WebSocket transport
- do not change frame commit semantics on the receiver
- do not remove the existing test-oriented helpers unless phase work makes them redundant
- do not break the current public sender API during intermediate refactor steps

## Phase 1

### Objective

Eliminate full pending-file materialization from the production send path while preserving deterministic traversal, stable `file_index`, and current resume semantics.

### Scope

Files primarily affected:

- `src/sender/source.rs`
- `src/sender/readers.rs`
- `src/sender/frame.rs`
- `src/sender/session.rs`

### Design Direction

- Introduce a stateful DFS walker for sender discovery.
- Keep directory-local sorting so global traversal order remains deterministic.
- Validate the committed resume point while walking instead of after building the whole map.
- Yield pending files one by one after the committed prefix is validated and skipped.
- Rework reader prefetch so it schedules concrete files, not slice positions inside a full pending vector.
- Rework frame assembly so it consumes a lazy file source and returns frame metadata for exactly one completed frame.

### Required Architectural Changes

#### 1. Source Layer

Replace the current "discover everything first" contract with two separate modes:

- production mode: streaming walker for `send_dump`
- diagnostics/test mode: materialized scan helper can remain for `scan_source` and similar tooling

Tasks:

- introduce a walker type such as `PendingFileWalker`
- use an explicit directory stack instead of recursive full-tree materialization
- keep normalized relative paths and deterministic DFS order
- assign `file_index` monotonically during walking
- validate `last_committed_file_index` and `last_committed_file_path` on the fly
- detect resume mismatch as soon as it becomes provable
- expose the final discovered file boundary only after walking completes

#### 2. Reader Layer

Remove the assumption that readers prefetch from a stable slice plus cursor.

Tasks:

- replace `schedule_prefetch(pending, cursor)` with task-oriented scheduling
- let the pool accept concrete files from the caller
- keep a bounded window of scheduled-but-not-consumed files
- preserve adaptive scaling semantics
- preserve bounded event buffering
- preserve chunked file reads

#### 3. Frame Layer

Make frame assembly consume files lazily.

Tasks:

- replace the current `prepare_frame(&[InputFile], &mut cursor, ...)` shape with a source-driven contract
- keep frame closure only on completed file boundaries
- keep the current frame target rule unchanged
- return enough metadata for the session layer to send `FrameStart`, `FrameEnd`, and `Finish`
- keep test-oriented helpers if they still add value, but isolate them from the production path if needed

#### 4. Session Layer

Convert `send_dump` from "collect pending, then loop" to "walk, assemble, send".

Tasks:

- bootstrap resume as today
- create the streaming walker after resume bootstrap
- build and send frames until the walker is exhausted
- track the latest discovered file boundary during iteration
- compute `final_frame_no`, `final_file_index`, and `final_file_path` without needing a prebuilt pending vector

### Risks

- resume validation becomes stateful and easier to get subtly wrong
- the end-of-walk behavior for empty sources and fully resumed dumps must remain exact
- current tests may assume a materialized pending list and will need careful rewriting
- diagnostics paths such as `scan_source` may still need a materialized helper for reporting

### Phase 1 Exit Criteria

- production send path no longer materializes the full pending file list
- deterministic traversal order is unchanged
- resume validation remains correct for exact match, missing committed file, and changed committed path
- sender can start frame work without a full source scan completing first
- no wire-format change is required

## Phase 2

### Objective

Eliminate full compressed-frame buffering from the production send path by streaming protocol chunks directly from the tar/zstd encoder.

### Scope

Files primarily affected:

- `src/sender/frame.rs`
- `src/sender/session.rs`
- `src/network/protocol.rs`
- receiver-side tests that assert exact frame transfer behavior

### Design Direction

- Introduce a sender-side frame sink that writes protocol `FrameChunk` packets as compressed bytes are produced.
- Separate frame metadata lifecycle into:
  - open frame
  - stream chunk bytes
  - finalize frame checksum and length
  - send `FrameEnd`
  - wait for `Ack`
- Keep the receiver contract unchanged: commit only after verified `FrameEnd` and durable write.

### Required Architectural Changes

#### 1. Streaming Chunk Sink

Replace `ChunkCollector` in the production send path with a writer that emits protocol chunks.

Tasks:

- create a writer that buffers only up to `MAX_FRAME_CHUNK_BYTES`
- when the buffer is full, emit one `FrameChunk`
- maintain incremental `compressed_len`
- maintain incremental `compressed_xxh3_64`
- flush the final partial chunk before `FrameEnd`

#### 2. Session/Frame Integration

Merge frame preparation and frame sending into one streaming operation for the production path.

Tasks:

- send `FrameStart` before the first compressed bytes of that frame
- pipe tar/zstd output directly into the chunk sink
- send `FrameEnd` only after the encoder cleanly finishes the frame
- wait for `Ack` as today
- keep `Finish` unchanged after the last acknowledged frame

#### 3. Failure Semantics

Preserve the current resumability guarantees during mid-frame failure.

Tasks:

- ensure disconnect before `Ack` still means the receiver discards the partial frame
- ensure sender does not mark a frame committed before `Ack`
- keep receiver-side verification based on streamed `FrameChunk` bytes plus `FrameEnd`
- verify that sender-side chunk flushing cannot emit bytes belonging to a future frame

#### 4. Compatibility Layer

Keep existing helper APIs where useful, but make the production path streaming-first.

Tasks:

- keep a buffered path only for tests/benchmarks if it still helps
- make sure public sender API remains stable even if internals split into streaming and buffered code paths
- isolate the streaming writer so it can be exercised in focused tests

### Risks

- sender-side state machine becomes more coupled to protocol sequencing
- errors in chunk flushing can corrupt checksum, length accounting, or frame boundaries
- tests that currently inspect completed in-memory frames will need new coverage around message ordering
- backpressure now includes the network stream directly, not just the in-memory collector

### Phase 2 Exit Criteria

- production send path no longer buffers the whole compressed frame before sending
- `FrameChunk` packets are emitted while compression is still in progress
- frame checksum and length remain correct
- ack/commit semantics are unchanged
- receiver still produces a valid `.tar.zst`

## Recommended Execution Order

1. Complete phase 1 fully before changing frame send semantics.
2. Preserve a buffered compatibility path while phase 2 is being introduced.
3. Switch the production send path to streaming chunks only after focused tests cover:
   - chunk ordering
   - frame checksum accounting
   - disconnect before `Ack`
   - resume after partial frame discard
4. Remove dead buffered-only production code only after both phases are stable.

## Test Plan

### Phase 1 Tests

- deterministic DFS order remains unchanged
- resume mismatch is detected without full pending-list materialization
- fully resumed dump produces no frames and correct `Finish`
- empty source still behaves correctly
- reader prefetch still stays bounded under adaptive scaling

### Phase 2 Tests

- `FrameStart` is sent before any `FrameChunk`
- `FrameChunk` packets are emitted incrementally during frame production
- `FrameEnd` length and checksum match the exact compressed byte stream
- disconnect before `Ack` leaves the receiver able to truncate back to the previous committed boundary
- resume after a lost in-flight frame restarts at the first file after the last committed file

## Definition Of Done

The refactor is complete when all of the following are true:

- the production sender path no longer requires a full file map in RAM
- the production sender path no longer requires a full compressed frame in RAM
- deterministic traversal and frame-based resume semantics remain intact
- large files are still streamed chunk-by-chunk
- the receiver still commits only whole verified frames
- the final receiver artifact remains a valid `.tar.zst`
