# Benchmarks and performance history

## Persistent results and history

Use the recorder from the repository root:

```text
python scripts/results.py record --kind benchmark --label scan -- cargo bench --bench scan_source
python scripts/results.py record --kind test --label tests -- cargo test --locked --quiet
python scripts/results.py import results/history/2026-10-24
python scripts/results.py index
```

Open `results/index.html` locally after generating it. It works without a server.
Compact raw measurements, run parameters and summaries live in Git under
`results/benchmarks/<id>/`; test summaries live under `results/tests/<id>/`.
Full logs/reports live in ignored `results/local/`; historical evidence in
`results/history/` is also local. The generated HTML is ignored and can be rebuilt
from a fresh clone even when local logs are unavailable.

Each run has a unique ID. Existing observations are never silently overwritten.
Matrix `--resume` requires an explicit `FAST_DUMP_BENCH_MATRIX_DIR` with existing
raw results. The recorder assigns private output directories through
`FAST_DUMP_RESULTS_RUN_DIR`, `FAST_DUMP_BENCH_MATRIX_DIR`, `CRITERION_HOME`, and
`FAST_DUMP_NETEM_DIR`. Explicit output options retain their precedence; use
`import` for results deliberately written outside the recorder directory.
Without the recorder, benchmarks also default to persistent local output outside
`target/`; import those outputs to add their compact measurements to Git.

Keep one copy of each sample set: paired aggregate raw rows rather than their
child copies; Criterion `new` samples rather than identical `base` copies.
Missing counters or provenance remain unknown. Summary-only legacy reports are
labelled explicitly. Failed, incomplete and invalid runs remain visible and
cannot establish a speedup. Unknown formats retain their original evidence.

Compare runs only with compatible workload, machine, measurement scope and
instrumentation. Binary hashes identify executables; the current checkout does
not identify the source of arbitrary saved binaries. Source digests exclude
`results/`, so saving a measurement does not change the measured source identity.
Detailed counter meanings and paired-run methodology follow below.

## Harnesses and measurement method

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
python scripts/bench-files-paired.py --before path/to/baseline --after path/to/candidate --source path/to/fixed-source --output results/local/benchmarks/file-comparison --repeats 7 --stages scan,read --readers 1,4,8,32
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
`results_summary.csv`, and `benchmark_report.md` in a unique directory under `results/local/benchmarks/`
(or `FAST_DUMP_BENCH_MATRIX_DIR`).

Local historical results from the 2026-10-24 cleanup are in
`results/history/2026-10-24/`; open its optional local `index.html`
to browse reports. Copies retain their original relative paths, including
`target/bench-*`. The archive includes `manifest.csv` with SHA-256 checksums and
original modification times, `inventory-before.json`, and a cleanup report.
`results/history/` is ignored by Git. Keep future result copies outside `target/`
so `cargo clean` does not erase the history.

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
`results/local/benchmarks/netem/<label>`; `FAST_DUMP_NETEM_DIR` overrides that root. Existing
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
python3 scripts/bench-compare.py --before results/local/benchmarks/netem/before-1g \
  --after results/local/benchmarks/netem/after-1g --label linux-netem-1g \
  --output results/local/benchmarks/netem/comparison-1g
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
`results/local/benchmarks/netem/paired-10g/bench/before` and `after`. The manifest records
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

- [`AGENTS.md`](../AGENTS.md)
- [`docs/history/V4_CUTOVER_PLAN.md`](history/V4_CUTOVER_PLAN.md)
- [`docs/history/REWRITE_PERF_PLAN.md`](history/REWRITE_PERF_PLAN.md)
- [`src/network/protocol.md`](../src/network/protocol.md)
