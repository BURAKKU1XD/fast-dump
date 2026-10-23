# fast-dump

`fast-dump` is a Rust implementation of a resumable directory dump over a
custom TCP protocol.

The sender walks a source tree in deterministic order, captures best-effort
file snapshots, builds tar entries, compresses them with zstd, and streams
committed bundle payloads to a receiver. The receiver writes those bundles into
`<dump_id>.tar.zst.part`, persists durable resume state next to the partial
archive, and finalizes the result into a valid `.tar.zst` file on successful
completion.

## Current Status

- Working `sender` and `receiver` subcommands.
- Both transport pairings are implemented:
  - `push`: `sender --connect` with `receiver --listen`
  - `pull`: `sender --listen` with `receiver --connect`
- Mainline wire/runtime contract is protocol version `4`.
- Mainline session flow is bundle-first:
  - dual-channel handshake
  - resume bootstrap
  - terminal skip catalog sync
  - bundle plan/data/commit flow
  - cumulative bundle acks
  - finish acknowledgement
- Receiver resume state is durable at committed bundle boundaries.
- Receiver output naming is canonical:
  - `<dump_id>.tar.zst.part`
  - `<dump_id>.tar.zst`
  - `<dump_id>.wal.part`
  - `<dump_id>.catalog.part`
  - `<dump_id>.tar.zst.issues.tsv.part`
  - `<dump_id>.tar.zst.issues.tsv`
- Legacy experimental `*.v4.*` receiver state is migrated in place on open.
- One receiver session at a time may write a given dump in one data directory.
  An OS lock on `<dump_id>.lock` covers recovery through finalization; the empty
  lock file remains after exit and is not an indication of an active session.
- Receiver startup briefly holds `.fast-dump-open-lock` in the data directory
  to coordinate state initialization and legacy migration. A retained lock marker
  for dump `name.v4` prevents its canonical files being migrated as legacy `name`.
  Keep these coordination files when copying or restoring receiver state.
- Integration and module-level coverage exists for transport, roundtrip, resume,
  skip persistence, large-file retries, and listener session limits.
- Receiver sessions own and join both network readers on every exit. Cleanup
  shuts down both channels and cancels blocked reads, queue sends, and buffer-pool
  waits; Windows additionally cancels pending socket I/O. A detected data-reader
  failure interrupts waiting for its plan. While finishing pending payload,
  control-channel failure or an unexpected control message ends the session.
  No general idle-session deadline is imposed on a connected but silent peer.

## CLI

Top-level help:

```text
fast-dump [OPTIONS] <COMMAND>
```

Commands:

- `fast-dump sender`
- `fast-dump receiver`

Global options:

- `--debug`

### Sender

```text
fast-dump sender [OPTIONS] --source <SOURCE> <--listen <LISTEN>|--connect <CONNECT>>
```

Required:

- `--source <DIR>`
- exactly one of `--listen <ADDR>` or `--connect <ADDR>`

Optional:

- `--dump-id <ID>`
- `--token <TOKEN>`
- `-l, --level <N>` default `3`
- `--threads <N|auto>` default `auto`
- `--min-readers <N>` default `1`
- `--max-readers <N>` defaults to the detected physical-core count
- `--read-buffer-kb <N>` default `1024`
- `--frame-target-mb <N>` default `64`

Notes:

- If `--dump-id` is omitted, the sender falls back to the last path component of
  `--source`.
- The dump ID must be one portable filename component. Path separators, control
  characters, Windows filename metacharacters/device names, and a trailing dot
  or space are rejected locally and during handshake. Unicode and spaces inside
  the component are supported.
- `--threads auto` resolves to `available_parallelism() - 1`, with a minimum of
  `1`. `--threads 1` compresses synchronously; larger values enable that many
  zstd compression workers within each frame.
- File-reader concurrency uses the detected physical-core count, clamped
  between `--min-readers` and `--max-readers` (defaults `1` and the detected core
  count), then capped at `ceil(pending entries / 32)` to amortize worker startup.
  A resulting count of `1`, including all sets of at most `32` pending entries,
  uses the serial path. There is no fixed `32`-reader ceiling: `176` detected
  cores request `176` readers before the entry-count cap. Equal explicit reader
  bounds select a fixed count before that cap.
- Physical-core detection is cached. Windows includes all processor groups;
  Linux counts cores available through CPU affinity and caps the result by
  available logical parallelism to respect CPU quotas. If topology is
  unavailable, use available logical parallelism, with a minimum of `1`.
- `--read-buffer-kb` remains accepted and validated, but does not currently tune
  the `v4` data path.
- `--frame-target-mb` is the public bundle-size target for the current `v4`
  runtime and the threshold for streaming an individual regular file. Bundle
  `1` uses an aggregation target of `min(configured target, 4 MiB)`; subsequent
  bundles use the configured target, `64 MiB` by default. An individual file
  can exceed the aggregation target without being split.

### Receiver

```text
fast-dump receiver [OPTIONS] --data-dir <DATA_DIR> <--listen <LISTEN>|--connect <CONNECT>>
```

Required:

- `--data-dir <DIR>`
- exactly one of `--listen <ADDR>` or `--connect <ADDR>`

Optional:

- `--token <TOKEN>`
- `--max-sessions <N>` default `1`

Notes:

- `--max-sessions` matters in `receiver --listen` mode.
- In `receiver --connect` mode one process handles one dump session.

## Usage Examples

Push mode:

```powershell
cargo run -- receiver --listen 127.0.0.1:7001 --data-dir D:\dump-recv --token secret
cargo run -- sender --connect 127.0.0.1:7001 --source D:\data --dump-id dump-001 --token secret
```

Pull mode:

```powershell
cargo run -- sender --listen 127.0.0.1:7001 --source D:\data --dump-id dump-001
cargo run -- receiver --connect 127.0.0.1:7001 --data-dir D:\dump-recv
```

## Sender And Receiver Output

Sender stdout stays aggregate-only. The current mainline summary includes:

- `status`
- `files_total_discovered`
- `files_sent`
- `files_skipped`
- `bundles_sent`
- `final_bundle`
- `final_work_ordinal`

Receiver completion output includes:

- `dump_id`
- `final_bundle`
- `final_work_ordinal`
- `terminal_skips`
- final archive path

## Runtime Model

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
[`src/network/protocol.md`](./src/network/protocol.md).

Protocol `v4` sends the terminal-skip catalog inside one `ResumeState` packet.
If that encoded state exceeds `4 MiB`, resume fails while preserving local
state. Large skip catalogs currently require a future protocol extension for
paging; the successful-file count does not impose this particular limit.

## Current Data-Path Behavior

- The archive contains regular files that were successfully captured and
  readable symlink entries without dereferencing.
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
  is allocated only when a streamed file first needs it. A scoped producer is
  started for each attempt.
  A failed attempt is fully stopped before abort/retry; no producer is detached.
- The receiver reuses up to ten `4 MiB` data buffers (`40 MiB` total payload
  storage), allocated and grown only as needed. Only the current packet's payload
  is written and hashed; retained buffer tails are never part of the archive.
  Buffers also return to the pool when late or orphaned data is discarded.
- Archive writes are periodically synced after `64 MiB`. A commit still makes
  archive data durable before syncing its WAL records and acknowledging progress.
  If a successful sync already covered every write and no truncation followed,
  the receiver skips a redundant archive sync. Creation, writes, and truncation
  mark the archive dirty; periodic sync alone never advances resumable state.
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

Receiver library calls now require `CloneableStream` for both channels.
Custom implementations must provide `shutdown_stream` that prevents further I/O
and interrupts pending reads across clones. This is a Rust source API change;
the CLI and protocol version `4` remain compatible. Receiver-owned channels are
closed when the call returns, including successful completion.

## Repository Layout

- `src/app.rs`: CLI dispatch and runtime entrypoints.
- `src/cli/mod.rs`: clap-based CLI parsing and validation.
- `src/network/`: active transport and wire codec implementation.
- `src/network/protocol.md`: active protocol reference.
- `src/sender/`: active sender implementation.
- `src/receiver/`: active receiver implementation.
- `tests/connectivity/`: end-to-end and protocol-flow coverage.
- `benches/`: criterion microbenchmarks plus the loopback matrix runner.

## Development

Rust `1.89` or newer is required.

Build:

```powershell
cargo build
```

Run tests:

```powershell
cargo test --quiet
```

Push/PR CI checks Windows and Linux with Rust `1.89.0` and the current stable
toolchain. It uses the committed `Cargo.lock`, runs default/all-feature tests,
Clippy with warnings denied, bench compilation, and formatting checks.
Storage fault tests exercise real partial writes and injected disk-full, archive
sync, WAL append/sync, rename, and cleanup errors. They verify acknowledgement
barriers and recovery; these are not hardware power-loss simulations.

The manual **Release artifacts** workflow builds the selected branch/tag only
after CI and release stress checks pass. It produces Windows MSVC and Linux GNU
archives with a build manifest and SHA-256 checksums; it does not publish a release.
The manifest records the clean source commit/tree, compiler, target, lockfile,
and binary hash. The existing local MUSL build script remains separate.

For a local default-feature release, build from a clean commit and record it:

```powershell
cargo build --locked --release --bin fast-dump
./scripts/write-release-manifest.ps1 -Binary target/release/fast-dump.exe -Target x86_64-pc-windows-msvc -OutputPath target/release-artifacts/manifest.json
```

The Windows/Linux scheduled stress workflow runs three explicit ignored tests:
the binary roundtrip with `10,116` files and per-file hashes; process-kill and
resume in both push and pull modes with a durable prefix and an uncommitted tail;
and `128` complete sessions with byte-for-byte archive checks and no retained
receiver reader handles before the peer closes. These checks exercise process
failure, not power loss or a physical 1/10 GbE link. The workflow checks that each
named test exists before running it; a removed test cannot silently pass CI.

```powershell
cargo test --release --test integration connectivity::roundtrip_fast::sender_and_receiver_roundtrip_random_files_and_verify_hashes -- --exact --ignored --nocapture
cargo test --release --test integration connectivity::recovery_stress::receiver_process_kill_and_resume_preserves_archive_push_and_pull -- --exact --ignored --nocapture
cargo test --release --lib receiver::network::lifecycle_tests::repeated_full_transfers_release_all_reader_handles -- --exact --ignored --nocapture
```

Run lints:

```powershell
cargo clippy --all-targets --all-features
```

Run benches:

```powershell
cargo bench
```

For traversal alone, run `cargo bench --bench scan_source`. Its
`scan_source_walk` Criterion group measures the sender's deterministic directory
walk and result cleanup, with throughput in files/s. It does not perform the
additional per-file metadata pass used by the public `scan_source` API to report
`pending_bytes`; historical `scan_source` benchmark timings measured that extra
work and are not directly comparable.

Measure scanning or reading a fixed source with the opt-in file benchmark:

```powershell
cargo bench --bench file_pipeline --features bench-internals -- --source path/to/fixed-source --stage scan --repeats 7
cargo bench --bench file_pipeline --features bench-internals -- --source path/to/fixed-source --stage read --readers 8 --repeats 7
```

`scan` measures the runtime walk. `read` prepares its scan and fixture-size
checks before timing, then measures reader construction, ordered reads, worker
joins, and result cleanup. It retains payloads for verification and accepts at
most `64 MiB` per file and `512 MiB` total. Content/order verification and fixture
fingerprinting run outside measured intervals; cleanup is measured separately.
`[file-perf]` reports `elapsed_ns = work_ns + drop_ns`, user/kernel CPU, and
available cycle, I/O, context-switch, and page-fault counters. These resource
counts cover all process threads during work and cleanup; preparation and
verification are excluded. RSS is the process lifetime peak, not a peak isolated
to that stage. First samples are retained with no implicit warmup; verification
can warm the source cache for subsequent samples.

For allocation diagnostics, build a separate binary with `bench-allocations`
and pass `--allocations`. Timing comparisons should use `bench-internals`
without `bench-allocations`, so allocator counters cannot distort their hot
paths. Compare saved baseline and candidate binaries with the same fixture,
instrumentation, and reader settings; these stage measurements supplement the
verified end-to-end dump benchmark.

The allocation diagnostics retain `allocs`/`alloc_bytes` as request counts and
requested bytes, including reallocations and failed requests. Schema `2` also
reports successful allocations separately from successful reallocations, their
requested sizes, failures, freed bytes, and successfully requested zeroed bytes.
Reallocation bytes are the full new size, not growth alone. Freed bytes can
include objects allocated before the interval; these counters are not live-memory
or RSS measurements.

Compare two saved binaries with matching instrumentation:

```powershell
python scripts/bench-files-paired.py --before path/to/baseline --after path/to/candidate --source path/to/fixed-source --output target/file-comparison --repeats 7 --stages scan,read --readers 1,4,8,32
```

Build source snapshots in separate Cargo target directories and retain each
binary before measuring. Both paired drivers reject identical binary SHA-256
hashes by default. Intentional A/A calibration requires
`--allow-identical-binaries`; the manifest records the comparison type.
The end-to-end paired driver also accepts `--readers N`, separate
`--before-readers N` / `--after-readers N`, and `--source DIR` for a prepared
fixture. Explicit readers cannot be combined with matrix `--resume`, because
the historical CSV scenario key does not include that override. Keep output
directories outside the source fixture.

The driver alternates AB/BA pairs and keeps every sample. `samples.csv` retains
raw counts and adds counts per entry and per MiB (`1048576` fixture bytes).
`comparison.csv` preserves the existing summary names; `resources.csv` provides
one row per resource and denominator, with sample statistics, after/before ratio,
and paired wins (a lower count wins). Entries include regular files and symlinks;
the MiB denominator uses regular-file payload bytes. Unsupported counters and
zero denominators are `null` in JSON and blank in CSV, never fabricated zeroes.
Sample counts and `paired_n` show coverage; ratios require complete measurements
on both sides and a nonzero baseline. The manifest records binary hashes,
counter scopes, and sources. Different `counters_schema`, `cycle_source`, or
`io_source` values cannot be mixed; older binaries without these fields are
treated as schema `1`.
The output directory must be outside the source tree, so measurement logs cannot
change the fixture being scanned.

Counter meanings depend on the platform:

| Counters | Meaning |
| --- | --- |
| `cpu_cycles` | Windows process cycle accounting, summed across user/kernel execution of all threads; unavailable on the current Linux path. Cycles are not derived from CPU time or converted using clock frequency. |
| `io_*_ops`, `io_*_bytes` | Windows process I/O accounting includes files, devices, and network activity. Linux uses read/write-family syscall counts and logical byte counts from `/proc/self/io`; these do not count every open, stat, directory enumeration, or close. |
| `storage_*_bytes` | Linux storage-layer accounting; cached reads can be zero, and accounted writes do not imply durable writes. |
| `page_faults`, `minor_faults`, `major_faults` | OS page-fault accounting. Windows exposes a combined count; Linux also distinguishes minor and major faults. |

Snapshot collection has overhead. Schema `2` Linux read counters subtract the
instrumentation's own `/proc/self/io` read from each interval: one read operation
and its actual returned bytes. `io_read_ops_raw`/`io_read_bytes_raw` preserve
the uncorrected counts; `io_observer_read_ops`/`io_observer_read_bytes` expose
the subtraction. Windows reports its original I/O counts as raw and zero
observer reads when available. `io_source` identifies these paths as
`linux-proc-io-corrected` or `windows-process-io`.

Use `--stage noop` for a single calibration, or `--stages noop` in the paired
driver, to measure empty intervals with the same instrumentation. CPU and wall
overhead are reported without automatic subtraction. Noop has zero entries and
bytes, so its normalized values stay unmeasured. Retired instructions, cache
events, and branch events are not measured by this harness.

Run only the loopback matrix runner:

```powershell
cargo bench --bench loopback_send_dump -- --dataset manySmallFiles,bigFiles,mixed --network-profile loopback-localhost,link-1gbps,link-10gbps --frames 64 --buffers 1024 --executor default
```

The matrix runner writes `benchmark_plan.md`, `results_raw.csv`,
`results_summary.csv`, and `benchmark_report.md` under `target/bench-matrix`
(or `FAST_DUMP_BENCH_MATRIX_DIR`).

Use `--executor default` for new runs. Without it, the legacy Unix default runs
both `default` and `abs_path` labels; both use the same v4 implementation and do
not represent different workloads.

The `link-1gbps` and `link-10gbps` profiles limit application protocol bytes to
`125000000` and `1250000000` bytes/s respectively, with a shared receive budget
for control and data channels in each direction. Cloned streams share that
budget; reverse traffic has an independent budget. Each byte is charged once
before delivery to the protocol parser. Up to `8 MiB` of burst credit absorbs
timer oversleep without accumulating unlimited idle credit. A single read is
capped at `4 MiB`, leaving room for oversleep credit even when it fills its
buffer. These profiles add no RTT and do not model TCP congestion or Ethernet
overhead.

Calibrate both rates with real loopback TCP before comparing matrix results:

```powershell
cargo test --release --test bench_matrix shared_link_rate_calibration_over_tcp -- --ignored --nocapture --test-threads=1
```

This manual test transfers `1 GiB` per direction over two concurrent TCP
connections for each rate and each `1 MiB`/`4 MiB` read buffer. It reports actual
read sizes, checks the combined byte bound including the `8 MiB` burst, and
requires at least `90%` of the configured rate in each direction. A calibration
failure means the host or
pacing implementation cannot sustain that model; do not interpret its matrix
results as the requested link speed. Calibration excludes compression and disk.

The older `lan-low-rtt`, `wan-moderate-rtt`, `wan-high-rtt`, and
`bandwidth-limited` profiles retain their approximate per-I/O sleep model for
historical comparisons. They do not emulate physical TCP RTT or one shared
network link. In particular, their `125 MiB/s` setting is not exactly `1 Gbit/s`.

For Linux kernel TCP/netem measurements, build the benchmark and use the Linux
executable path printed by Cargo:

```bash
cargo bench --bench loopback_send_dump --no-run
sudo bash scripts/bench-netem.sh 1gbit before-1g /path/to/linux-loopback-bench \
  --dataset manySmallFiles,bigFiles,mixed --executor default --threads 4 \
  --frames 64 --buffers 1024 --repeats 3
```

The script needs Linux root, `unshare`, `ip`, `tc`, and `python3`. It creates a
fresh network namespace and configures only its loopback interface. Use
`10gbit` for the other rate, or `none` for an unshaped reference. Omitting the
benchmark executable runs calibration alone. Results go to
`target/bench-netem/<label>`; `FAST_DUMP_NETEM_DIR` overrides that root. Existing
labels are rejected so an earlier run is not overwritten.

Each run first measures TCP echo RTT and forward, reverse, and simultaneous
bidirectional goodput with `scripts/bench-calibrate-tcp.py`. Defaults are
`NETEM_DELAY=500us` per direction, `NETEM_MTU=1500`, and
`CALIBRATION_MIB=256` per direction. The nominal added RTT is twice the delay;
use the measured RTT and goodput when interpreting results. One loopback qdisc
shares its rate across data and returning TCP ACKs in both directions. This
exercises real TCP and kernel scheduling, but does not model two independent
full-duplex rate limits or a physical NIC. The benchmark uses
`loopback-localhost` internally to avoid application shaping a second time.

Compare completed runs with an explicit network-experiment label, since their CSV
network profile remains `loopback-localhost`:

```bash
python3 scripts/bench-compare.py --before target/bench-netem/before-1g \
  --after target/bench-netem/after-1g --label linux-netem-1g \
  --output target/bench-netem/comparison-1g
```

The comparison writes CSV and JSON with all observations, elapsed-time
statistics, ratios of means, and separate aggregate protocol TX/RX throughput.
It checks fixture counts/bytes, zero skips/aborts/rebuilds, and exact log-to-CSV
matching; unmatched scenarios remain visible. No outliers are discarded.

For a paired confirmation, alternate the two saved binaries within one isolated
network configuration:

```bash
sudo bash scripts/bench-netem.sh 10gbit paired-10g scripts/bench-paired.py \
  --before /path/to/baseline --after /path/to/candidate --threads 3 --repeats 5
```

The driver runs five samples per binary for every requested scenario, switching
AB/BA order between pairs. It retains the first samples, performs no automatic
warmup or trimming, and forces archive verification on. Each invocation keeps
its original CSV/log; merged inputs for `bench-compare.py` are under
`target/bench-netem/paired-10g/bench/before` and `after`. The manifest records
binary hashes, commands, and pair order. Original child `repeat_index=1` values
are preserved, so the comparator reports retained duplicate repeat identities.
Run the driver directly with `--output DIR`, or let it use
`FAST_DUMP_BENCH_MATRIX_DIR` supplied by the netem wrapper. Python drivers are
invoked through `python3` and need no executable bit on the mounted filesystem.

Each run prints `[bench-link]` with sender protocol `tx_bytes`, `rx_bytes`,
`elapsed_ms`, decimal `tx_gbps`, successful `logical_bytes`, and `raw_read_bytes`,
after timing finishes. Wire counters exclude transport establishment and TCP/IP
headers. The CSV `wire_bytes` still combines both directions; `total_mib_s` uses
successfully transferred logical payload bytes, without counting retries twice.
It can exceed link throughput when compression reduces the transferred data.
`raw_read_bytes` can include repeated or failed attempt reads; the legacy ratio
columns retain that read-byte denominator. `frame_aborts` and `frame_rebuilds`
map to actual bundle attempt counters. With retries, `wire_overhead_bytes` also
includes discarded attempt payload, not just protocol headers.

Every completed matrix archive is verified by default against its fixed source
fixture: all file bytes, the complete unique path set,
symlink targets, and archive termination. Verification runs after the measured
transfer and resource sampler finish, so its work is excluded from reported
durations, CPU, and memory metrics. Each successful check prints a `[bench-verify]`
entry with the verified file count and byte count. `FAST_DUMP_BENCH_VERIFY_ARCHIVE=0`
explicitly disables verification; `1` keeps it enabled, and other values are
rejected. Legacy non-matrix profile sampling does not verify its archives.
CPU/RSS sampling covers the combined benchmark process on Windows; zero values
on other targets mean unmeasured resources, not zero resource use.
Markdown reports display those RSS values as `unmeasured` and exclude them from
the memory-budget view.

The CSV schema retains historical manifest columns for compatibility. In `v4`,
`manifest_build_duration_ms` represents source scanning and `manifest_build_bytes`
is the discovered entry count; manifest transfer, spool, and replay columns are
zero because those stages no longer exist. Scanning overlaps reading and
compression. Its elapsed-time counter excludes scanner batch-send backpressure;
it is not a measurement of scanner CPU time. `time_to_first_frame_ms` maps to
`time_to_first_bundle`: the first materialized or streamed task is available to
the sender and its `BundlePlan` can be sent. It does not measure completion of
compression, first payload delivery, or acknowledgement. With parallel read-ahead,
`read_duration_ms` measures the planner's time obtaining ordered results,
including planner-side materialization, rather than summed worker time.
All bundle compression runs in a producer while the main thread sends compressed
chunks. Compression duration excludes producer queue waits and, for streamed
files, source reads, while retaining tar work. Overlapping stage durations
are not an additive breakdown of total elapsed time.
Receiver frame-write duration includes periodic archive synchronization every
`64 MiB`; the separate frame-sync duration measures commit-group synchronization.
These elapsed-time counters are not isolated CPU or disk-throughput measurements.
Additive receiver profile fields separate the work for committed bundles:

| Profile field | Meaning |
| --- | --- |
| `bundle_write_io` | Compressed bytes appended and write duration excluding periodic sync |
| `bundle_periodic_sync` | Periodic sync duration; its `bytes` member counts sync calls |
| `bundle_sync_skipped` | Commit-group archive sync calls skipped because the archive was already clean |

Existing aggregate fields retain their meanings. `bundle_sync.bytes` counts
commit-group sync checks, including clean skips. The matrix runner emits the new
details in `[bench-receiver-work]` log lines without changing sender CLI stdout.

Related docs:

- [`AGENTS.md`](./AGENTS.md)
- [`docs/history/V4_CUTOVER_PLAN.md`](./docs/history/V4_CUTOVER_PLAN.md)
- [`docs/history/REWRITE_PERF_PLAN.md`](./docs/history/REWRITE_PERF_PLAN.md)
- [`src/network/protocol.md`](./src/network/protocol.md)
