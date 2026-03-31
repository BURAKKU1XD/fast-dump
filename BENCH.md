# Benchmark Plan

## Current Status

- The loopback matrix runner already exists.
- Raw and summary CSV output already exist.
- Terminal markdown tables already exist.
- Fixed-point `Criterion` benchmarks for `prepare_dump`, `scan_source`, and
  `receive_frame_commit` remain useful and stay separate from the matrix runner.
- Only `manySmallFiles` is implemented in the matrix runner today.
- This document captures the agreed target state and the implementation order
  for the ongoing benchmark work. Runtime documentation in `README.md` should
  continue to describe only what is already implemented.

## Goal

- Keep `Criterion` only for small, fixed-point microbenchmarks.
- Use the dedicated loopback matrix runner as the main tool for choosing sender
  runtime knobs.
- Keep the result format simple: raw CSV, summary CSV, and terminal markdown
  tables.
- Make the matrix small enough for a practical coarse sweep and rich enough to
  support a focused rerun.

## Coarse Matrix Defaults

- `threads = dedup(filter<=cores_limit([1, 2, 4, 8, 16, cores_limit]))`
- `frame_target_mb = [4, 16, 64]`
- `read_buffer_kb = [64, 256, 1024, 4096]`
- `cores_limit = available_parallelism() - 1`, minimum `1`

Rationale:

- `threads`: keep only scaling checkpoints instead of almost every even number.
- `frame_target_mb`: avoid tiny frames that mostly measure protocol overhead and
  avoid oversized frames that give little additional signal for the coarse pass.
- `read_buffer_kb`: avoid both syscall-noisy tiny buffers and obviously
  excessive per-reader allocations.

Example:

- On a machine with `cores_limit = 31`, the default thread set becomes
  `[1, 2, 4, 8, 16, 31]`.
- The coarse matrix size becomes `6 * 3 * 4 = 72` scenarios per dataset.
- With 3 datasets, the full coarse pass becomes `216` scenario runs.

## Workflow

- Phase 1: run the full coarse matrix with `repeats = 1`.
- Phase 2: rerun only the top configurations with `repeats = 3..5`.
- Persist results after every completed run.
- Support interrupted runs through `--resume`.

## Dataset Cases

The matrix runner should support exactly 3 dataset cases:

- `manySmallFiles`
- `bigFiles`
- `mixed`

### manySmallFiles

- Keep the current pathological small-file case.
- About `40_000` regular files.
- Deterministic nesting depth up to `7`.
- Intended to stress traversal order, file-open overhead, queueing, and file
  boundary behavior.

### bigFiles

- Replace the old `bigFiles_1g/2g/4g` family with one practical coarse-sweep
  case.
- Use several large files instead of one giant file.
- Initial target shape:
  - `256 MiB`
  - `512 MiB`
  - `512 MiB`
- Total target size: about `1.25 GiB`.
- Use a deterministic block-based generator with a mix of repeated and noisy
  blocks so compression behavior is not reduced to pure random or pure zero
  data.

### mixed

- Make this the closest-to-production dataset.
- Initial target shape:
  - about `10_000` nested small files
  - about `128` medium files in the `256 KiB .. 4 MiB` range
  - about `6` large files in the `32 MiB .. 256 MiB` range
- Do not build `mixed` as `manySmallFiles + 3 random bin files`.
- The purpose is to combine path overhead, queueing effects, steady-state
  streaming, and a more realistic size distribution.

## Metrics Per Scenario

Each raw scenario row and each aggregated summary row should include:

- `dataset`
- `threads`
- `frame_target_mb`
- `read_buffer_kb`
- `files_count`
- `sent_frames`
- `raw_read_bytes`
- `tar_input_bytes`
- `compressed_payload_bytes`
- `wire_bytes`
- `wire_overhead_bytes`
- `read_duration_ms`
- `compress_duration_ms`
- `send_duration_ms`
- `total_duration_ms`
- `read_mib_s`
- `compress_mib_s`
- `send_mib_s`
- `total_mib_s`
- `avg_cpu_pct`
- `max_cpu_pct`
- `avg_rss_mib`
- `max_rss_mib`
- `avg_private_mib`
- `max_private_mib`
- `compression_ratio_pct`
- `wire_pct_of_read`
- `wire_overhead_pct`

Notes:

- `network_pct_of_read` is intentionally removed because it duplicates the
  information already carried by `compression_ratio_pct`.
- `avg_private_mib` and `max_private_mib` are secondary and may be dropped from
  operator-facing markdown summaries before they are removed from CSV entirely.

## Metric Definitions

- `compression_ratio_pct = 100 * (1 - compressed_payload_bytes / raw_read_bytes)`
- `wire_pct_of_read = 100 * wire_bytes / raw_read_bytes`
- `wire_overhead_bytes = wire_bytes - compressed_payload_bytes`
- `wire_overhead_pct = 100 * wire_overhead_bytes / raw_read_bytes`
- `raw_read_bytes` means bytes read from source files before tar framing and
  zstd compression.
- `compressed_payload_bytes` means bytes sent inside protocol `FrameChunk`
  payloads.
- `wire_bytes` means total bytes written to the transport, including protocol
  overhead.
- `*_duration_ms` means absolute wall-clock stage duration exported from the
  sender pipeline profile for one loopback run.

## Output Format

- No HTML output anywhere in the matrix flow.
- Append one raw results file:
  - `target/bench-matrix/results_raw.csv`
- Rewrite one summary results file after each completed run:
  - `target/bench-matrix/results_summary.csv`
- Print markdown tables to stdout after each dataset.

Markdown summary should prioritize operator-readable comparisons:

- fastest total time
- best compression
- fastest total time under an RSS budget

Operator-facing markdown should show a compact subset of metrics:

- `threads`
- `frame_target_mb`
- `read_buffer_kb`
- `completed_repeats`
- `total_duration_ms`
- `total_mib_s`
- `sent_frames`
- `compression_ratio_pct`
- `max_rss_mib`
- `avg_cpu_pct`
- `wire_overhead_pct`

## Execution Model

- Run matrix scenarios serially.
- Persist raw results after every completed scenario.
- Rebuild the summary file after every completed scenario.
- Keep filters for:
  - `--dataset`
  - `--threads`
  - `--frames`
  - `--buffers`
  - `--resume`
  - `--repeats`

## Implementation Order

1. Update the matrix runner result schema to include duration and new wire/frame
   metrics.
2. Thread `SendReport` through the loopback scenario path so the matrix runner
   records actual `sent_frames`.
3. Narrow the coarse matrix defaults to the agreed knob values.
4. Add `bigFiles` and rebuild `mixed`.
5. Update terminal markdown ranking to show total time directly.
6. Sync `README.md` once runtime behavior matches the new benchmark state.
7. Add point checks for CSV parsing, aggregation, and dataset selection.
