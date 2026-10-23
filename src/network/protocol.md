# v4 Protocol

Mainline wire protocol for the current bundle-first runtime.

## Scope

- bundle-first flow with no separate manifest-first phase;
- split transport:
  - control TCP channel for handshake, resume, plans, commits, acks, skips, finish;
  - data TCP channel for `BundleData` payload only;
- bounded sender-side bundle window with cumulative ordered `BundleAck`;
- receiver-side speculative archive append plus bounded group commit;
- resume only from durable bundle boundaries;
- terminal skips are durable before `SkipAck`;
- large regular files may use direct streaming without materializing the whole compressed payload.

## Transport

`v4` opens two TCP channels per dump session against the same address.

Current mainline transport preserves both pairings:

- `push`: `sender --connect` to `receiver --listen`
- `pull`: `sender --listen` with `receiver --connect`

Each channel uses the same `Hello -> HelloAck` handshake. `Hello` carries:

- `role`
- `protocol_version = 4`
- `dump_id`
- optional `token`
- `channel_kind = control | data`
- `session_nonce`

`session_nonce` pairs the control and data channels into one dump session. New
senders derive this opaque value from a process-specific random seed and a local
sequence; listeners keep pending channels separate by peer IP as well. The wire
field is unchanged and older nonce values remain accepted.

The sender's dump ID is validated before a successful handshake: it must be a
single portable filename component, with no path separators, control characters,
Windows filename metacharacters/device names, or trailing dot/space. Unicode and
internal spaces are allowed. Receiver storage validates it again before creating
state files.

## Versioning And Migration

- Both peers speak protocol version `4`.
- There is no wire compatibility between `v3` and `v4`.
- There is no runtime negotiation with `v3`.
- Receiver state uses canonical output names without `.v4.` suffix.
- Legacy experimental `*.v4.*` receiver state is migrated in place on open.

## Control Flow

Control channel flow:

1. `Hello`
2. `HelloAck`
3. `ResumeQuery`
4. `ResumeState`
5. zero or more:
   - `SkipEntries`
   - `SkipAck`
6. zero or more bundle attempts:
   - `BundlePlan`
   - later either:
     - `BundleCommit`
     - or `BundleAbort`
7. zero or more cumulative `BundleAck`
8. `Finish`
9. `FinishAck`

Data channel flow:

- zero or more `BundleData`

The sender may keep up to `4` submitted bundles awaiting durable acknowledgement.
Acks stay ordered and cumulative by `bundle_no`.

## Bundle Attempt Model

Each live bundle transfer has:

- `bundle_no`
- `attempt_no`

`attempt_no` matters for direct-stream retries. A failed large-file attempt may be
aborted and retried from scratch with the same `bundle_no` but a larger
`attempt_no`. This lets the receiver ignore late data from the old attempt.

The sender overlaps streamed reading/compression with network writes through a
bounded two-chunk queue. Its producer is stopped and joined before `BundleAbort`
or a retry. Completed chunks from a failed read are drained before abort; data
already buffered in TCP can still arrive late and is ignored by attempt number.
This scheduling does not change the message format or durable commit boundary.

`BundlePlan` carries:

- `attempt_no`
- `descriptor` with:
  - `bundle_no`
  - `first_work_ordinal`
  - `last_work_ordinal`
  - `last_relative_path`
  - `entries`
  - `raw_payload_bytes`

`BundleCommit` carries:

- `attempt_no`
- `descriptor` with:
  - `bundle_no`
  - `tar_payload_bytes`
  - `compressed_payload_bytes`
  - `compressed_xxh3_64`

`BundleAbort` carries:

- `bundle_no`
- `attempt_no`

`BundleAbortAck` carries:

- `bundle_no`
- `attempt_no`
- `archive_len`

## Data Messages

`BundleData` is sent on the data channel only and carries:

- `bundle_no`
- `attempt_no`
- `is_last`
- payload bytes

`is_last` marks the final chunk of one bundle attempt. This gives the receiver a
stable data boundary even if control and data packets are reordered across the
two TCP channels.

Each successful bundle contributes a complete zstd frame containing tar entries
without tar end markers. Entries may include GNU long-path and long-link
extensions. `Finish` appends the tar end markers in one final local zstd frame,
so normal tar readers see all entries across bundle boundaries.

## Sender Preparation

Up to `2` workers on Windows and `8` on other platforms scan independent
subtrees. Entries and work ordinals retain deterministic depth-first order with
each directory sorted by normalized relative path. Each directory is fully
enumerated and sorted, then scanner channels carry batches of at most `1024`
entries on Windows and `64` on other platforms, with at most two queued batches
per channel. These limits count entries, not metadata bytes; variable-length
paths, producer/consumer batches, and sorted directories retained by active DFS
walks require additional memory. Scanner concurrency is independent of the
physical-core file-reader count.
Ordinary trees start feeding readers before the complete scan finishes. A native
basename containing a backslash or requiring lossy Unicode conversion triggers
full collection of its containing subtree before any of that subtree is
published. This preserves suppression of regular/symlink paths colliding with
unsupported entries after normalization. Public scan entries and all work
ordinals remain unchanged, including entries suppressed from pending work.
The committed resume prefix is validated before pending files are opened.

File read-ahead is bounded to four entries per reader and consumed in that
same order. The requested count is the detected physical-core count, clamped
to the CLI's reader bounds (default minimum `1`, maximum the detected core count).
Library defaults use the same count; there is no fixed `32`-reader ceiling.
Detection is cached and includes all Windows processor groups. Linux counts
cores allowed by CPU affinity and caps the result by available logical
parallelism for CPU quotas. Unavailable topology falls back to available
logical parallelism, minimum `1`. Equal explicit CLI bounds select a fixed
count. The actual worker count is capped at `ceil(pending entries / 32)` to
amortize startup. Initial lookahead reads at most `32 * requested readers`
pending entries to resolve that cap without collecting the entire tree.
A resulting count of `1` uses serial reading. The parallel
window holds four entries per actual reader (`64` for `16`, `704` for `176`).
A separate shared `8 MiB` budget limits prefetched payload
pending delivery, independently of the reader count. Workers process up to four
adjacent entries per job, reserving their opened-handle sizes and sharing one
immutable buffer. Files up to the configured materialization threshold may be
prefetched if the whole payload fits; no single payload or batch buffer exceeds
`8 MiB`. A reservation is released after the batch's last successful prefetched
entry is delivered to the planner, or when cancellation drops the buffer.
Already-delivered slices remain charged until that last delivery. The planner's
materialized bundle is separate; a buffer spanning bundle boundaries can retain
up to `8 MiB` of already-consumed payload until its remaining entries are released.
Workers never wait for byte-budget capacity: they return bounded open handles
for the planner to materialize or stream. Sharing does not change entry order,
per-file attempt limits, or the bytes described by `size_at_open`.

Bundle `1` uses an aggregation target of `min(configured target, 4 MiB)`;
subsequent bundles use the configured target (`64 MiB` by CLI default). Files
remain whole and can individually exceed the aggregation target. The configured
target still determines whether a regular file is materialized or streamed.
Resume after a committed bundle uses the normal configured target.

After each materialized entry is successfully appended to tar/zstd, its source
payload is released while its descriptor remains available for the bundle plan.
A shared slab is freed after its last slice is released, including slices carried
to the next bundle.

Compression may use synchronous zstd or multiple workers within a frame.
Materialized bundles and streamed files share one sender context across
successfully completed frames, but each attempt starts an independent frame.
After unfinished, failed, or aborted compression, the sender recreates the
context with the same settings before the next attempt, avoiding stale worker
state from an abandoned frame. The chunk pool remains reusable. This failure
recovery changes no CLI fields or wire messages. `BundlePlan` is sent
before compression; zstd writes directly into the final chunks, which can be
transmitted before compression finishes. No complete compressed bundle is held
for later sending; `BundleCommit` carries the final compressed length and hash.

Planner handoff uses a zero-capacity rendezvous. At most the current materialized
bundle, the next materialized bundle, and one whole file carried forward retain
source payload, each at most the configured bundle target. Read-ahead, retained
shared slabs, metadata, and zstd workspace are additional costs. The unchanged
four-bundle acknowledgement window does not retain sent payloads.

Both input paths overlap compression and network writes through a two-chunk
queue. Four `4 MiB` compressed buffers are allocated on first use and reused
across bundles and attempts. One separate `1 MiB` source-read buffer is allocated
only on first streamed-file use and reused thereafter. Each attempt starts a
scoped producer, which is cancelled and joined before abort/retry or returning
a network error. These choices preserve packet fields, entry order, and durable
resume boundaries.

Scanner profiling measures elapsed scan duration excluding batch-send
backpressure, not CPU time. Time to first bundle measures task/plan availability
before compression completes, payload delivery, or acknowledgement. Scan, read,
compression, and send durations overlap and cannot be added as sequential stages.

## Receiver Rules

- `SkipEntries` are persisted to the terminal-skip catalog before `SkipAck`.
- Payload is appended to archive `.part` speculatively as `BundleData` arrives.
- `BundleCommit` becomes durable only after:
  - archive sync;
  - WAL append for every bundle in the commit group;
  - WAL sync.
- After the durable group finishes, receiver sends one cumulative `BundleAck`
  for the latest durable `bundle_no`.
- `BundleAbort` rolls the archive back to the bundle boundary captured at
  its first data append. An attempt aborted before any append does not truncate
  preceding bundles, even when control messages arrived ahead of their data.
- Late `BundleData` for an aborted attempt is ignored by `(bundle_no, attempt_no)`.

The data reader allocates reusable buffers lazily, with at most ten buffers of
up to `4 MiB` each (`40 MiB` payload storage). This covers its bounded queue,
the current/orphan chunk, and its next read. Each packet carries its actual
payload length separately from retained storage length; only those payload
bytes are written and hashed. Buffers return after consumption or discarded
late/orphan data, without blocking cancellation. This is a local implementation
choice and does not alter packet framing or limits.

Receiver still commits bundles in bundle order. Windowing exists on sender side:
the sender may continue sending later bundles while earlier durable acks are still
unread.

The receiver owns both network readers through session exit. It shuts down the
channels, cancels blocked I/O and buffer-pool waits, disconnects queues, and joins
both readers on success or error. Windows additionally uses `CancelIoEx` for
already-pending Winsock reads. A separate bounded failure notification wakes
orphan-plan waiting without draining additional payload. After `Finish`, control
errors and unexpected further controls interrupt waiting for pending data.
These are local lifecycle rules; packet formats, durable boundaries, and CLI
fields are unchanged. There is no general deadline for a silent connected peer.

## Group Commit

Current `v4` receiver commits a ready group when it reaches either threshold:

- `4` ready bundles
- `1 GiB` compressed payload in one group

A single bundle can exceed the byte threshold; it is not split for group commit.

Before the next ready-group check, the receiver handles up to eight already-ready
control messages so queued plans and commits can join a group. It does not wait
for new control messages to fill that group. After the bounded control burst,
ready data receives priority unless an orphan packet needs its plan first.
This scheduling changes neither group thresholds nor archive/WAL/ACK ordering.

Receiver keeps batching while a later in-flight bundle is still pending. A ready
prefix is flushed when one of these happens:

- the ready prefix reaches a commit threshold;
- no later bundle is still pending in the in-flight window;
- `Finish` forces the final durable flush.

This changes the durability contract compared to `v3`:

- after disconnect or crash, receiver may lose up to one uncommitted group;
- recovery remains bounded and deterministic because only WAL-confirmed bundle
  boundaries are resumable.

Every WAL record contains its bundle's exact archive end offset, including when
several bundles share a commit group. The archive is also synced whenever
appended bytes since its last sync reach `64 MiB`, spreading writeback across a
large transfer. These periodic syncs do not advance resumable state. Group commit
still requires archive sync, per-bundle WAL records, and WAL sync before
acknowledgement. A successful periodic or prior commit sync satisfies the archive
barrier only while no subsequent write, creation, or truncation remains dirty;
in that case the duplicate archive sync call is skipped. New writes and all
truncations restore the dirty state before the filesystem operation. Only a
successful sync clears it; WAL records are still synced before `BundleAck`.

Receiver profiling retains the existing aggregate write/sync fields and adds
`bundle_write_io`, `bundle_periodic_sync`, and `bundle_sync_skipped` to distinguish
write I/O, periodic sync work, and avoided clean commit-sync calls. These local
measurements do not add protocol fields or change sender CLI stdout.

## Resume State

`ResumeState` reflects durable receiver state only:

- `last_committed_bundle_no`
- `last_committed_work_ordinal`
- `last_committed_relative_path`
- `archive_len`
- `terminal_skips`

Before migration or recovery, the receiver acquires an exclusive OS lock on
`<dump_id>.lock` in the data directory. Another session or process targeting that
dump fails instead of sharing its archive/WAL. The lock stays held through
finalization and handle closure; its empty file remains after the lock is released.
Initialization additionally holds `.fast-dump-open-lock` for the data directory.
While that lock is held, an existing `name.v4.lock` marks files as canonical state
of dump `name.v4`, so they are not migrated as legacy state of dump `name`.
Unmarked legacy files still follow the historical naming convention.

On restart, receiver truncates archive `.part` back to `archive_len` from the
last valid WAL record and reuses `terminal_skips` as terminal for the current dump.
Only an incomplete final WAL record without a newline is removed before new
records are appended. Malformed complete records or inconsistent commit sequences
cause recovery to fail without truncating acknowledged progress. Bundle numbers
and work ordinals must advance; bundle numbers may have gaps after skipped streamed
attempts. Archive end offsets must advance by each record's compressed byte count.
If `.part` is shorter than the durable WAL boundary, recovery fails instead of
extending it with zeroes.
An existing terminal-skip catalog must have its complete header and newline-ended
records; empty, incomplete, or malformed catalogs cause an error instead of
silently dropping durable skips.

Finalization preserves WAL state until the complete archive has been renamed
into place. If the final archive exists, reopening that dump is rejected even
when stale `.part` or WAL files remain, preserving the completed archive. On Unix,
state creation and rename operations sync their parent directory before durable
acknowledgement or cleanup of resume state.

## Current Limits

- packet magic: `0x34564446`
- protocol version: `4`
- max `BundleData` payload bytes per message: `4 MiB`
- max control-packet body: `4 MiB`

Embedded string lengths and list counts are checked against the remaining body
before allocation, so a short malformed packet cannot request unbounded memory.

`ResumeState` carries the entire terminal-skip catalog in one control packet.
The encoded resume state must therefore fit within `4 MiB`; very large skip
catalogs cannot currently be resumed. The catalog remains on disk. Paging that
catalog requires an explicit protocol extension.

These limits belong to the current production runtime for protocol version `4`.

The exact encoded `BundlePlan` body size, excluding the packet header, is:

```text
44 + last_relative_path.len()
   + sum(22 + entry.relative_path.len() + symlink_extra)
```

Lengths are UTF-8 byte lengths. `symlink_extra` is `0` for a regular file and
`4 + target.len()` for a symlink. The sender checks this independently of the
payload target. If another entry would exceed `4 MiB`, it starts the next bundle
with that whole entry, preserving every entry and its order. An individual
descriptor that cannot fit in one control message produces an explicit error.
