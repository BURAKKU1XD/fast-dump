# CLI

`fast-dump` provides two subcommands:

- `fast-dump sender`
- `fast-dump receiver`

Names are defined by data flow. Active wire/runtime contract: protocol `v4`.

## Common

```text
fast-dump [--debug] [--no-gui] [--retries <N>] [--retry-delay-secs <SECONDS>] [sender|receiver] ...
```

Each subcommand requires exactly one transport selector:

- `--listen <ADDR>`
- `--connect <ADDR>`

With no subcommand, interactive terminals open an English setup wizard.
Complete commands open a live dashboard; missing required flags on an explicit
subcommand remain errors. Global `--no-gui`, redirected stdin/stdout/stderr,
and `TERM=dumb` use plain output. Omitting the command in plain mode is a usage error.
`--help`, `help sender`, `help receiver`, and `--version` bypass terminal setup.
The wizard defaults Listen to `0.0.0.0:7000`; Connect keeps a separate address
(`127.0.0.1:7000` for local use). For remote transfers, Connect must use the
listening host's network IP. Each mode retains its edited address.
Sender setup includes a `Zstd level` field, default `3`, and validates the linked
zstd library's supported range before starting. Negative fast levels and `0`
(zstd default) are accepted. The chosen level survives role/transport switches
and appears on Review and in the dashboard; receiver setup omits this field.
CLI launches use `--level <N>`, with `--level=-1` syntax for a negative level.

The dashboard shows compressed payload throughput, current-session entry
counts, durable archive size, a separate resume baseline, and bounded logs.
It refreshes at most 5 Hz and honors `NO_COLOR`. Whole-transfer percent and
ETA are intentionally absent because the source scan streams concurrently.
Use F1 for help, Tab/Shift+Tab for focus, arrows/Enter for selection, and
PgUp/PgDn/End for log scrolling/following. Tokens stay masked in the wizard.

Ctrl+C safely stops either presentation mode and returns exit code 130 after
worker cleanup. Repeat the same command to resume an unfinished dump. A current
filesystem operation may delay stopping; an already-started finalization can
finish, and a completed archive is preserved. Only the executable installs the
signal handler. Aggregate stdout remains unchanged.
Receiver Listen keeps accepting connections after an individual session fails.
Sender and receiver Connect support global `--retries <N>` (additional attempts
after the initial attempt) and `--retry-delay-secs <SECONDS>` (default `10`,
minimum `1`). Plain mode defaults to `0` retries; GUI mode defaults to `10`.
Explicit values override these defaults, including `--retries 0` in the wizard.
The GUI default is resolved at launch when the terminal is selected; a later
terminal rendering failure does not change the chosen policy.

Only transient errors originating from network I/O qualify. Validation, local
file/storage failures, known protocol/authentication rejections, and completed
archives stop the run. A remote failure whose error response is lost can still
look like a disconnect. Each attempt cleans up before the delay; Ctrl+C cancels
the countdown without another attempt. Resume uses the same dump ID, source,
and data directory. The retry budget covers the entire invocation. Receiver
Listen keeps serving sessions independently and ignores the restart budget.
A listening sender starts listening again after a retry delay; its receiver
must reconnect too. Existing per-file immediate attempts are unchanged.

Supported pairings:

- `push`: `sender --connect` + `receiver --listen`
- `pull`: `sender --listen` + `receiver --connect`

## Sender

```text
fast-dump sender --source <DIR> <--listen <ADDR>|--connect <ADDR>> [OPTIONS]
```

Options:

- `--dump-id <ID>`: default is the last path component of `--source`
- `--token <TOKEN>`
- `-l, --level <N>`: default `3`
- `--threads <N|auto>`: default `auto`
- `--min-readers <N>`: default `1`
- `--max-readers <N>`: defaults to the detected physical-core count
- `--read-buffer-kb <N>`: default `1024`
- `--frame-target-mb <N>`: default `64`

Validation:

- reject `--frame-target-mb 0`, `--threads 0`, `--min-readers 0`,
  `--max-readers 0`, `--read-buffer-kb 0`
- reject `--min-readers > --max-readers`
- `--threads auto` = `available_parallelism() - 1`, minimum `1`
- `--dump-id` is required if `--source` has no normal final path component
- Explicit and default dump IDs must be portable single filename components:
  no path separators, control characters, Windows metacharacters/device names,
  or trailing dots/spaces. Unicode and internal spaces are supported.

The current `v4` data path uses `--level`, `--frame-target-mb`, `--threads`,
`--min-readers`, and `--max-readers`:

- `--threads 1` uses synchronous zstd compression; a larger value enables that
  many compression workers within each frame.
- Reader concurrency uses the detected physical-core count, clamped to the
  configured minimum and maximum (defaults `1` and the detected core count),
  then capped at `ceil(pending entries / 32)` to amortize worker startup. There
  is no fixed `32`-reader ceiling. A resulting count of `1`, including sets of
  at most `32` pending entries, uses serial reading. Parallel reading preserves
  traversal order with at most four entries per actual reader: `64` entries for
  `16` readers, `704` for `176`. A shared `8 MiB` budget separately bounds pending
  prefetched payload. Whole files up to the materialization threshold can be
  prefetched when they fit that budget; otherwise bounded open-handle slots
  retain them for the planner. A shared batch reservation is released after
  its last prefetched entry is delivered, or on cancellation. The planner's
  materialized bundle is separate; a batch crossing a bundle boundary can
  additionally retain up to `8 MiB` of already-consumed payload.
  Equal explicit `--min-readers` and `--max-readers` select a fixed count before
  the entry-count cap. Library `BundlePlanOptions` defaults use the same detected
  physical-core count, with a minimum of `1`.
- Core detection is cached. Windows includes all processor groups; Linux
  counts cores allowed by CPU affinity and caps the result by available logical
  parallelism for CPU quotas. If physical topology is unavailable, use available
  logical parallelism, with a minimum of `1`.
- `--frame-target-mb` controls the bundle-size target and the threshold above
  which a regular file is streamed directly. Bundle `1` uses aggregation target
  `min(configured target, 4 MiB)`; later bundles use the configured target
  (`64 MiB` by default). A whole file can exceed the aggregation target; the
  threshold for materializing or streaming it remains the configured target.
- Metadata can close a bundle earlier to keep its `BundlePlan` body within
  `4 MiB`. The next whole entry is carried forward, without being skipped.

`--read-buffer-kb` remains accepted and validated but does not currently tune the
data path. Directory scanning independently uses up to `2` subtree workers on
Windows and `8` on other platforms, preserving deterministic depth-first
traversal. Compression contexts are reused across successfully completed frames,
with a separate zstd frame for each attempt. After unfinished, failed, or aborted
compression, the context is recreated with the same settings before the next attempt.
Streamed attempts reuse four `4 MiB` compressed buffers and one `1 MiB`
source-read buffer across files in the session; these buffers are allocated on
first use. The producer is still scoped to each attempt.

## Receiver

```text
fast-dump receiver --data-dir <DIR> <--listen <ADDR>|--connect <ADDR>> [OPTIONS]
```

Options:

- `--token <TOKEN>`
- `--max-sessions <N>`: default `1`

Validation:

- reject `--max-sessions 0`

`--max-sessions` matters only in `receiver --listen` mode.
Sessions targeting the same dump in one data directory are rejected while its
OS lock is held. The persistent `<dump_id>.lock` file is only a coordination file;
its existence does not mean the dump is active.

## Examples

```powershell
fast-dump receiver --listen 127.0.0.1:7001 --data-dir D:\dump-recv
fast-dump sender --connect 127.0.0.1:7001 --source D:\data
```

```powershell
fast-dump sender --listen 127.0.0.1:7001 --source D:\data
fast-dump receiver --connect 127.0.0.1:7001 --data-dir D:\dump-recv
```

Details: [`README.md`](../../README.md), [`src/network/protocol.md`](../network/protocol.md)
