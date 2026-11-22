# Runtime and implementation contract

## Session flow and durability

The mainline uses the bundle-first runtime in the canonical sender, receiver,
and network modules. Command-line options and terminal behavior are specified
in [the CLI guide](cli.md).

The current `v4` runtime uses two TCP channels per dump session:

- control channel for handshake, resume, plans, commits, acks, skips, and finish
- data channel for bundle payload bytes only

Actual high-level flow:

1. Sender and receiver establish paired control/data channels.
2. Both channels complete `Hello -> HelloAck`.
3. Sender sends `ResumeQuery`.
4. Receiver opens local state, migrates legacy `*.v4.*` files when needed,
   truncates `.part` back to the last durable archive length, and replies with
   `ResumeState`.
5. Sender uploads zero or more terminal skip batches.
6. Sender streams one or more bundle attempts:
   - `BundlePlan`
   - `BundleData*`
   - `BundleCommit` or `BundleAbort`
7. Receiver durably commits ready bundles in order and replies with cumulative
   `BundleAck`.
8. Sender sends `Finish`.
9. Receiver appends tar termination blocks as a final local zstd frame, finalizes
   `.issues.tsv` when needed, fsyncs and renames `.part` into the final archive,
   removes partial resume state, and replies with `FinishAck`.

After disconnect or crash, at most the current uncommitted receiver commit group
may be lost.

Each committed bundle records its own archive end offset in the WAL. Recovery
truncates only an incomplete final WAL record without a newline. A malformed
complete record, inconsistent commit sequence, or partial archive shorter than
the durable boundary causes an error without resetting acknowledged progress.
A completed archive is preserved and cannot be resumed or overwritten, even
when stale `.part` or WAL files remain. On Unix, state-file creation and renames
also sync the parent directory before acknowledgement or removal of resume state.
An existing empty or malformed terminal-skip catalog is rejected rather than
interpreted as an empty skip list.

The detailed wire reference for the active runtime lives in
[`src/network/protocol.md`](../src/network/protocol.md).

## Relay transport

Both peers connect outbound using TLS 1.3. The relay pairs their control channels
by configured room and opposite authenticated roles, then accepts their data
channels for the same random attempt ID. Only after all four sockets are ready
do the peers start the v4 handshake. The relay forwards opaque bytes in both
directions and keeps no archive or durable transfer state.

There are at most 64 configured rooms, one active attempt per room, four queued
registrations per room, and 64 concurrent registration tasks. Forwarding uses
bounded buffers and applies socket backpressure. Transport errors end the whole
attempt. After a clean EOF, the relay gives the remaining directions up to two
seconds to drain so data-channel closure can race with the final control ACK.
Retried clients establish a new attempt and ask the receiver for the durable v4
boundary. The same receiver storage and ACK ordering apply to all transports.

The access-token relay adds a persistent lobby over TLS with at most 15
members. Its peers join before picking a transfer role. An accepted offer
reserves one transfer per peer and creates separate random attachment
credentials for their two v4 channels. Unattached channel reservations expire;
finished transfers release both peers for further offers. Each persistent
connection uses bounded frame queues, and a client that stops reading and
fills its queue is disconnected. The relay still forwards v4 bytes unchanged.

By default the access-token relay generates an in-memory Ed25519 TLS identity.
Clients use the invitation to pin its public key before sending the access
token. Configured certificate chains remain available for CLI deployments.

In the interactive P2P flow, peers keep a separate control connection while
idle and between transfers. The listener and connecting peer can each send an
offer. When the recipient accepts, the two v4 channels use the original P2P
listener address and a fresh transfer credential. The archive data travels
directly on those sockets.

### Resume state size

Protocol `v4` sends the terminal-skip catalog inside one `ResumeState` packet.
If that encoded state exceeds `4 MiB`, resume fails while preserving local
state. Large skip catalogs currently require a future protocol extension for
paging; the successful-file count does not impose this particular limit.

## Source preparation and bounded data flow

- The archive contains regular files that were successfully captured and
  readable symlink entries without dereferencing.
- Linux senders automatically exclude the kernel/control filesystems listed
  below, wherever mounted. Other platforms keep their existing selection rules.
- Empty directories and directory metadata are ignored.
- Source traversal is deterministic:
  - depth-first
  - entries within each directory are sorted by normalized relative path
- Independent directory subtrees are scanned by up to `2` workers on Windows
  and `8` on other platforms, then merged in the same deterministic traversal
  order. Scanner batches contain up to `1024` entries on Windows and `64` on
  other platforms, with at most two queued batches per channel. These are
  entry-count limits, not a metadata byte budget: paths vary in length, and
  batches being produced or consumed and sorted directories retained by active
  DFS walks use additional memory. Each directory is fully enumerated and
  sorted; ordinary trees do not require a complete in-memory file list.
  Larger Windows batches reduce handoffs but can delay the first entries while
  a batch fills. The scanner limit is independent of the physical-core file
  reader count. The reader initially collects
  up to `32 * requested readers` pending entries to preserve its worker cap.
  The committed resume prefix is checked before pending files are opened.
  A native filename containing a backslash or requiring lossy Unicode conversion
  can alias another normalized path. Its containing subtree is collected before
  publication so unsupported entries suppress all colliding pending paths while
  public scan entries, traversal order, and resume ordinals remain unchanged.
- File reading uses an ordered, bounded read-ahead window of `4` entries per
  actual reader (`64` entries for `16` readers, `704` for `176`). A shared
  `8 MiB` byte budget bounds prefetched payload pending delivery, independently
  of the reader count. Workers open up to four adjacent entries per job and
  reserve space using their opened-handle sizes. Regular files up to the
  configured materialization threshold can be prefetched when their entire
  payload fits the remaining budget; a single payload or shared batch buffer
  cannot exceed `8 MiB`. Entries retain immutable slices without copying bytes.
  A batch's reservation is released after its last successful prefetched entry
  is delivered to the planner, or when cancellation drops its buffer. Until
  delivery completes, already-delivered slices remain charged to that budget.
  The planner's current materialized bundle is separate from the pending budget;
  one shared buffer crossing a bundle boundary can additionally retain up to
  `8 MiB` of already-consumed payload until its remaining entries are released.
  When the budget is unavailable, workers return bounded open handles without
  waiting. The planner reads those same handles up to the configured bundle-size
  target and streams larger files. The smaller first-bundle aggregation target
  does not change this materialization threshold.
- Bundle `1` aggregates up to `min(configured target, 4 MiB)`; later bundles
  use the configured target (`64 MiB` by CLI default). Whole files can exceed
  the aggregation target. The materialization/streaming threshold always stays
  at the configured target. Resume after a committed bundle uses the normal
  configured aggregation target.
- Bundle plans also obey the `4 MiB` control-body limit. If metadata would
  exceed it, the next complete entry moves to the next bundle, including for
  empty files or long paths. A single descriptor that cannot fit causes an
  explicit error.
- Materialized bundles aggregate small tar writes in a `128 KiB` input buffer before
  zstd; large file slices bypass that buffer. Streamed file bodies feed zstd
  directly. After each successful entry append, the sender releases its source
  payload while keeping its descriptor for the bundle plan. A shared slab is
  freed after its last slice is released, which may be in the next bundle.
  Numeric tar header fields use allocation-free encoding equivalent
  to the tar library, with its GNU extended-size fallback.
  Header byte sums use runtime-selected AVX2 or SSE2 on x86-64 and a portable
  scalar expression elsewhere, preserving the tar library's exact checksum.
  Bundle frames contain no tar end markers; the receiver appends those only once
  after the final durable commit.
- Materialized bundles and streamed files share one sender compression context
  across successfully completed frames; each attempt starts an independent zstd
  frame. After unfinished, failed, or aborted compression, the sender recreates
  the context with the same settings before the next attempt. This avoids stale
  worker state after an abandoned frame; the chunk pool remains reusable, and
  failure recovery changes no CLI fields or wire messages.
  Zstd writes directly into the final compressed chunk allocation. The sender
  sends `BundlePlan` before compression and begins transmitting completed chunks
  while the remaining payload is still being compressed; it does not retain a
  complete compressed bundle.
- The planner hands off materialized payload through a zero-capacity rendezvous
  channel. The sender can own the current bundle while the planner prepares the
  next and retains one whole file carried forward. Each of those three source
  payloads is at most the configured bundle target; read-ahead, shared-slab
  retention, metadata, and zstd workspace are additional memory costs. The
  four-bundle acknowledgement window does not retain sent payloads.
- Both input paths use a producer while the sending thread writes completed
  chunks to the network. The queue holds at most two `4 MiB` chunks; four reusable
  buffers, including producer and consumer, bound this stage's compressed payload
  memory to `16 MiB`, excluding zstd's workspace. They are allocated on first use
  and reused across bundles and attempts. A separate `1 MiB` source-read buffer
  is allocated only when a streamed file first needs it and reused thereafter.
  A scoped producer is
  started for each attempt.
  Cancel and join the producer before abort, retry, or returning a network
  error; no producer is detached.
- The receiver reuses up to ten `4 MiB` data buffers (`40 MiB` total payload
  storage), allocated and grown only as needed. Only the current packet's payload
  is written and hashed; retained buffer tails are never part of the archive.
  Buffers also return to the pool when late or orphaned data is discarded.
- Sender and receiver hash compressed payload with standard unseeded XXH3-64.
  The allocation-free streaming state uses `twox-hash` with a shared static
  default secret. CPU/OS detection selects AVX2, SSE2, NEON, or scalar code at
  runtime; global AVX2 compiler flags are not required. Checksum values, chunk
  boundaries, v4 interoperability, and resume semantics are unchanged.
- Archive writes are periodically synced after `64 MiB`. A commit still makes
  archive data durable before syncing its WAL records and acknowledging progress.
  If a successful sync already covered every write and no truncation followed,
  the receiver skips a redundant archive sync. Creation, writes, and truncation
  mark the archive dirty; periodic sync alone never advances resumable state.
  Receiver profiles retain aggregate fields and expose `bundle_write_io`,
  `bundle_periodic_sync`, and `bundle_sync_skipped` separately; CLI stdout stays
  unchanged.
- Before its next commit check, the receiver processes up to eight already-ready
  control messages so queued plans and commits can join the same group. It never
  waits for new control messages to enlarge a group. After that bounded burst,
  ready data gets priority unless an orphan packet still needs its plan. The
  archive-sync, WAL-sync, and acknowledgement ordering is unchanged.
- Tar paths are stored relative to the selected root and normalized to `/`.
- Live files use best-effort snapshot semantics:
  - sender opens the file immediately before an attempt
  - sender reads `size_at_open` from the opened handle
  - sender reads exactly `size_at_open` bytes for that attempt
  - appended tail after `open` is ignored for that attempt
  - short read or open failure restarts the file from scratch
  - at most `3` immediate attempts are made in total across read-ahead,
    planner-side materialization, and streamed retries
  - exhausted files are skipped and persisted through the terminal skip catalog
- Bundle boundaries are resumable only after durable receiver commit.
- Terminal skips remain durable in the catalog during transfer; finalization
  publishes `<dump_id>.tar.zst.issues.tsv` before acknowledging successful finish.

Library callers can set `BundlePlanOptions.readers` and
`BundlePlanOptions.compression_threads` independently. The library defaults to
the same detected physical-core count as the CLI, with a minimum of `1`, and
`1` compression thread. The actual reader count still obeys the pending-entry
cap.

### Linux system-filesystem exclusions

The sender identifies filesystems by their actual type, not by directory names.
It checks directories before listing them and prunes excluded trees, including
any mounts below them. It also checks every newly opened regular-file handle,
including retries, before reading payload. This catches a single procfs file
bind-mounted inside an otherwise ordinary directory. An excluded source root is
an error rather than an empty successful dump.

| Filesystem types | Reason for exclusion |
| --- | --- |
| `procfs` | Live process/kernel state, including the enormous virtual `/proc/kcore` image. |
| `sysfs` | Device and kernel-object attributes exposed through file operations. |
| `debugfs`, `tracefs` | Debug interfaces and event streams that can block or consume events. |
| `cgroup` v1/v2, `resctrl` | Runtime process and resource controls. |
| `configfs`, `binfmt_misc` | Kernel-object and executable-format configuration interfaces. |
| `securityfs`, `selinuxfs`, `smackfs` | Active kernel security interfaces; ordinary configuration files in `/etc` remain included. |
| `bpffs`, `nsfs` | Kernel BPF objects and namespace handles, not portable file payloads. |
| `efivarfs` | Firmware variables requiring a dedicated backup/restore procedure. |
| `mqueue`, `devpts` | Live message queues and pseudoterminals. |
| `fusectl`, `rpc_pipefs`, `nfsd` | Kernel/service control and communication interfaces. |

Ordinary FUSE, NFS and other network filesystems, OverlayFS, tmpfs, ramfs,
hugetlbfs, and unknown filesystem types remain eligible. Directories named
`proc` or `sys` on an ordinary filesystem are unaffected. There is no blanket
exclusion for `/dev`, `/run`, `/tmp`, or `/mnt`, and large/sparse files are not
excluded merely because of their size. Device nodes, FIFOs, and sockets remain
unsupported; readable symlinks outside pruned trees remain symlinks.

Each pruned directory produces one `unsupported_entry` record with ordinal `0`,
not a record for every unvisited descendant. Local diagnostics name the path,
filesystem, and reason; a skip is durable only after the receiver acknowledges
it, and appears in the final issues report. The record names that exact path;
it is not a permanent prefix exclusion if the mount layout later changes.
Filesystem types are reevaluated on every scan. Never cache permission to read
solely by `st_dev`. An excluded open file handle immediately becomes
`UnsupportedEntry`; classification I/O errors consume the existing open-attempt
budget. Directory classification errors fail the scan; retry `EINTR`. Unknown
classification errors are never treated as permission to read payload.

Pruning changes later work ordinals compared with older sender versions. Resume
still validates the committed ordinal/path and rejects a mismatch without
resetting acknowledged data. Use a new `--dump-id` when repeating an older `/`
test with the new exclusions. Existing archives and partial state are preserved;
wire version `4` and receiver formats do not change.

## Session ownership, cancellation, and retries

Receiver library calls require `CloneableStream` for both channels. Custom
implementations must provide `shutdown_stream` that prevents further I/O and
interrupts pending reads across clones. Receiver sessions own both network reader
threads. Every return, including success, shuts down channels, cancels buffer-pool
waits, disconnects result queues, and joins the readers before releasing the
listener session slot. Windows also calls `CancelIoEx` to interrupt pending
Winsock reads. Detected data-reader failure wakes orphan-plan waiting. After
`Finish`, waiting for pending data observes control errors and rejects further
controls. Silent connected peers have no general idle-session deadline.

Binary Ctrl+C cooperatively cancels both presentation modes and returns `130`
after cleanup. Library APIs never install process signal handlers. Register
sockets throughout connection, handshake, and session lifetime. Cancellation
must wake accept, connect, initialization-lock, and queue waits, then join
listener-session workers and network readers. It creates no terminal skips and
never sends `Finish`. Do not interrupt an in-progress durable commit or
finalization transaction. Current filesystem I/O or a compression call may delay
shutdown; the existing five-second handshake limits remain.

Whole-session retry policy uses typed socket I/O provenance, never error text or
unqualified `io::Error` kinds, to distinguish transient network failures from
local file EOF or malformed packet-body decoding. Known protocol, authentication,
and storage rejections and cancellation are not retried. A lost peer error reply
can appear as a network disconnect. Finish cleanup and join all workers before
the retry wait; Ctrl+C must wake that wait. Exhaustion returns the last error,
and success ends the invocation's budget. Receiver Listen already isolates failed
sessions and stays outside the whole-run retry loop. Retrying preserves v4
resume, archive finalization, aggregate stdout, and the separate immediate
per-file attempt limit. User-facing defaults are in [the CLI guide](cli.md).

## Instrumentation without hot-path overhead

The CLI guide defines displayed counters and stage meanings. Their implementation
must keep payload counters atomic without per-block locks, allocations, or clocks.
Observed materialized writes use slices of at most `1 MiB` for live progress;
unobserved writes keep the direct path.

Record stage state and transition time atomically without locks or allocations.
Repeated states must not reset wait timers. Unobserved runs collect no stage
timings. Preserve detected failure states and freeze timers at session end.

Scan duration excludes batch-send backpressure but measures elapsed time, not CPU
time. Time to first bundle means task/plan availability before compression,
payload delivery, or acknowledgement. Overlapping stage durations are not
additive.
