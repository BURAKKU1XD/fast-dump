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
- Interactive binary launches without a subcommand open an English setup wizard.
  Complete sender/receiver commands start immediately with a terminal dashboard;
  explicitly incomplete commands still fail validation. Global `--no-gui`,
  non-terminal stdin/stdout/stderr, or `TERM=dumb` selects the plain path.
  Help/version bypass terminal setup. The dashboard renders to stderr at most
  five times per second, honors `NO_COLOR`, and preserves aggregate stdout.
  Wizard Listen defaults to `0.0.0.0:7000`; Connect has a separate address
  defaulting to `127.0.0.1:7000` for local use. Preserve edits independently
  when switching transport modes; remote Connect needs the peer's network IP.
  Sender setup offers `Zstd level`, default `3`, validated against the linked
  zstd library's supported range. Preserve the edited level across role and
  transport switches; receiver setup omits it. Review and dashboard show the
  selected value. CLI selection remains `-l`/`--level` (default `3`).
  Counters are observational: transferred compressed payload includes retries,
  durable size follows validated ACKs (sender) or archive/WAL sync (receiver),
  and resume ordinals are never substituted for successful entry counts.
  Do not pre-scan the source to display an overall percentage or ETA; omit these
  estimates while the total is unknown. Receiver Written counts archive appends
  including later rollbacks, excluding discarded late/orphan payload.
  Keep logs bounded to 200 messages and completed session history to 20.
- Sender details show the current tar/zstd entry, its `size_at_open`, processed
  payload bytes, and five-second source-processing speed separately from network
  speed. Count regular-file payload accepted by tar/zstd, including retries;
  exclude read-ahead, tar headers/padding, and symlink metadata. This is neither
  physical disk throughput nor durable progress. Reset current-file progress on
  each attempt, clear it at entry end, and preserve cumulative session counters.
  Update payload counters atomically without per-block locks, allocations, or
  clocks. Observed materialized writes use at most `1 MiB` slices for live
  progress; unobserved writes retain the direct path.
- The dashboard's Pipeline panel shows local stage operations, explicit queue/
  peer/storage wait reasons, and elapsed time in the current state. Sender
  stages are Scan, Read, Plan, Compress, Data, and Control; receiver stages are
  Data, Control, and Storage. Read represents ordered file preparation; streamed
  source reads appear under Compress. Socket I/O states include blocking time
  and never imply throughput or diagnose packet loss. Silent waits are not
  errors. Preserve detected failure states and freeze timers at session end.
  Record stage state and transition time atomically without locks/allocations
  on each update; repeated states must not reset wait timers. Unobserved runs
  do not collect stage timings. Narrow layouts prioritize pipeline visibility.
- Global `--retries <N>` counts additional whole-session attempts after transient
  network failures; default `0` in plain mode, `10` in GUI. Global
  `--retry-delay-secs <SECONDS>` defaults to `10` and requires at least `1`.
  Explicit `--retries 0` disables GUI retries; wizard flags must survive setup.
  Resolve presentation defaults at launch and preserve them on UI fallback.
  Exhaustion returns the last error; success ends the budget. Use typed socket
  I/O provenance, never error text or unqualified io::Error kinds, to distinguish
  network failure from local file EOF or malformed packet-body decoding.
  Known protocol/auth/storage rejections and cancellation are not retried.
  A lost peer error response may still appear as a network disconnect.
  Finish cleanup/join all workers before waiting; Ctrl+C wakes retry waiting.
  Receiver Listen already isolates failed sessions and stays outside the
  whole-run retry loop. Preserve v4 resume, archive finalization, aggregate
  stdout, and the separate immediate per-file attempt limit.
- Binary Ctrl+C cooperatively cancels both presentation modes and returns 130
  after cleanup. Library APIs do not install process signal handlers. Register
  sockets throughout connection/handshake/session lifetime; cancellation must
  wake accept/connect/initialization-lock and queue waits, join listener session
  workers and network readers, and never create terminal skips or send Finish.
  Do not interrupt an in-progress durable commit or finalization transaction.
  Current filesystem I/O or a compression call may delay shutdown. Existing
  five-second handshake limits and lack of a general idle deadline remain.
- Mainline execution path uses the bundle-first runtime in canonical module
  locations.
- Linux senders exclude procfs, sysfs, debugfs, tracefs, cgroup v1/v2, resctrl,
  configfs, binfmt_misc, securityfs, selinuxfs, smackfs, bpffs, nsfs, efivarfs,
  mqueue, devpts, fusectl, rpc_pipefs, and nfsd by filesystem type. Check before
  directory enumeration and prune the whole tree, including nested mounts.
  Reject an excluded source root. Check every newly opened regular-file handle
  before payload reads, including retries and individual bind-mounted files;
  do not cache permission to read solely by `st_dev`. An excluded handle is an
  immediate `UnsupportedEntry`; classification I/O errors use the existing open
  attempt budget. Directory classification errors fail the scan; retry `EINTR`.
  Unknown FS types, ordinary FUSE/network FS, OverlayFS, tmpfs/ramfs/hugetlbfs,
  large/sparse files, and symlinks outside excluded trees remain eligible.
  Windows selection is unchanged. The reason list is documented in README.
  Emit one `UnsupportedEntry` with ordinal `0` per pruned directory and log its
  filesystem/reason locally. Durability follows `SkipAck`; unvisited descendants
  are not individual terminal skips. Records retain exact-path semantics, not
  permanent prefix filtering. Reevaluate filesystem types on every scan.
  Pruning changes ordinals relative to older senders; preserve committed-prefix
  validation and never reset acknowledged state on mismatch. Use a new dump ID
  for old root-dump tests. Protocol v4 and receiver formats stay unchanged.
- Source scanning uses up to `2` subtree workers on Windows and `8` on other
  platforms, merging results in the same deterministic depth-first order.
  Batches contain up to `1024` entries on Windows and `64` on other platforms;
  each scanner channel holds at most two batches. These are entry-count limits,
  not a metadata byte cap. Variable-length paths, producer/consumer batches,
  and sorted directories retained by active DFS walks use additional memory.
  The scanner limit does not change the physical-core file-reader bounds.
  Enumerate and sort each directory fully, but feed ordinary trees to reading
  before the complete scan finishes. Read
  at most `32 * requested readers` pending entries initially to preserve the
  worker cap. Validate the committed resume prefix before opening pending files.
  At a directory containing any native basename with backslashes or lossy
  Unicode, collect that entire subtree before publication. Preserve suppression
  of all normalized paths colliding with unsupported entries, without changing
  public scan entries or any resume ordinal.
- File reads use ordered, bounded read-ahead: `4` entries per actual reader
  (`64` entries for `16` readers, `704` for `176`). A shared `8 MiB`
  budget limits prefetched payload pending delivery, independently of reader
  count. Up to four adjacent entries share one immutable buffer sized from
  opened handles. Whole files up to the configured materialization threshold
  are eligible if they fit the remaining budget; a single file or batch buffer
  cannot exceed `8 MiB`. Release a batch's reservation only after its last
  successful prefetched entry is delivered to the planner, or on cancellation.
  Already-delivered slices stay charged until then. The planner's materialized
  bundle is separate; one buffer crossing a bundle boundary can retain up to
  `8 MiB` of already-consumed payload until its remaining entries are released.
  When no budget is available, return bounded open handles without waiting;
  the planner materializes or streams those same handles.
- `--min-readers` (default `1`) and `--max-readers` (default: detected physical
  cores) bound the requested physical-core count; there is no fixed `32`-reader
  ceiling. Actual workers are capped at `ceil(pending entries / 32)` to amortize
  startup. A count of `1`, including pending sets of at most `32` entries, uses
  the serial path. Library defaults use the same detected core count. Explicit
  equal min/max selects a fixed count while retaining the pending-entry cap.
  Cache core detection: Windows includes all processor groups; Linux counts
  cores allowed by CPU affinity and caps the result by available logical
  parallelism for CPU quotas. Fall back to available logical parallelism,
  minimum `1`, when physical topology is unavailable.
  `--threads` controls zstd compression workers (`1` is synchronous);
  `--read-buffer-kb` is still validated but does not tune the data path.
- Bundle `1` uses aggregation target `min(configured target, 4 MiB)`; later
  bundles use the configured target (`64 MiB` by CLI default). Whole files may
  exceed the aggregation target. The materialization/streaming threshold always
  remains the configured target.
- Materialized and streamed sender paths share one zstd context across
  successfully completed frames. After unfinished, failed, or aborted compression,
  recreate the context with the same settings before the next attempt to avoid
  stale worker state. Keep reusing the chunk pool; failure recovery changes no
  CLI fields or wire messages. Each attempt starts an independent frame.
  Zstd writes directly into the final compressed chunks. Send the plan before
  compression and stream chunks
  without retaining a complete compressed bundle.
- Release each materialized entry's source payload after its successful tar/zstd
  append, preserving the descriptor for the bundle plan. A shared slab is freed
  after its last slice is released, including a slice carried to the next bundle.
- Planner handoff uses a zero-capacity rendezvous. Materialized source payload
  may include the current bundle, the next bundle, and one carried whole file,
  each bounded by the configured target. Read-ahead, shared-slab retention,
  metadata, and zstd workspace are additional. The four-bundle ACK window
  retains no sent payload.
- All bundle compression overlaps network writes through a two-chunk queue.
  Four reused `4 MiB` buffers bound this stage to `16 MiB` of compressed payload,
  excluding zstd workspace. Allocate them on first use and reuse across bundles
  and attempts. Allocate the separate `1 MiB` source-read buffer only on first
  streamed-file use and reuse it thereafter. Each
  attempt still owns a scoped producer. Cancel and join it before abort, retry,
  or returning a network error.
- Receiver data reads reuse a lazy pool of at most ten `4 MiB` buffers, bounded
  to `40 MiB` of payload storage. Write/hash only the current packet length;
  return buffers after consumption and discarded late/orphan data.
- Receiver sessions own both network reader threads. Every exit shuts down
  channels, cancels buffer-pool waits, disconnects result queues, and joins the
  readers before releasing the listener session slot. Windows shutdown also
  calls `CancelIoEx` to interrupt pending Winsock reads. Both library channel
  types implement `CloneableStream`, including interruptible `shutdown_stream`.
  Detected data-reader failure also wakes orphan-plan waiting. After `Finish`,
  waiting for pending data observes control errors and rejects further controls.
  Silent connected peers have no general idle-session deadline.
- Receiver periodic archive sync remains at `64 MiB`. Skip a commit's archive
  sync only when a successful previous sync covered all writes and no creation
  or truncation remains dirty; WAL sync before acknowledgement is unchanged.
  Additive receiver profile fields separate write I/O, periodic sync, and clean
  commit-sync skips while preserving existing aggregate fields and CLI stdout.
- Before checking ready commits, consume at most eight already-queued control
  messages without waiting for additional controls. Afterwards, give ready data
  a turn unless orphan data requires a plan first. Group thresholds and durable
  archive/WAL/ACK ordering remain unchanged.
- Scan duration excludes batch-send backpressure but is elapsed time, not CPU
  time. Time to first bundle means task/plan availability before compression,
  payload delivery, or acknowledgement. Overlapping stage durations are not
  additive.
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
- `results/history/2026-10-24/index.html`: local historical result index from
  cleanup. Copies retain original relative paths (including `target/bench-*`);
  `manifest.csv` records SHA-256 checksums and original modification times.
  The archive also includes `inventory-before.json` and a cleanup report.
  `results/history/` is ignored by Git; keep future result copies outside
  `target/` so `cargo clean` preserves them.
- `releases/index.html`: local saved release index. New packages use
  `releases/<version>-<short-commit>/<Rust target triple>/` for the binary,
  `manifest.json`, and `Cargo.lock`, with `.tar.gz` and `.sha256` files in the
  version directory. Preserve historical verification files for older copies.
  `releases/local/` holds replaceable local builds without release stress gates.
  All of `releases/` is ignored by Git and stays outside Cargo's `target/`.
- Runtime truth lives in code, `README.md`, and `src/network/protocol.md`. Historical
  docs explain old behavior only.

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
- `cargo test --quiet`
- `cargo clippy --all-targets --all-features`
- `cargo check --benches`
- Push/PR CI covers Windows/Linux and Rust `1.89.0`/stable with locked
  dependencies. Manual release packaging additionally gates on release tests
  and correctness stress, then records source/compiler/binary hashes.
- Keep storage fault hooks under `cfg(test)`; release I/O must not pay for
  injection checks. Lifecycle regressions must leave the other peer channel
  open until the receiver returns and verify that its reader handles are gone.
