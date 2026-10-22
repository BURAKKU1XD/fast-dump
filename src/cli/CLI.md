# CLI

`fast-dump` provides two subcommands:

- `fast-dump sender`
- `fast-dump receiver`

Names are defined by data flow. Active wire/runtime contract: protocol `v4`.

## Common

```text
fast-dump [--debug] <sender|receiver> ...
```

Each subcommand requires exactly one transport selector:

- `--listen <ADDR>`
- `--connect <ADDR>`

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
- `--min-readers <N>`: default `2`
- `--max-readers <N>`: default `32`
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
- Reader concurrency is `available_parallelism()` clamped to the configured
  minimum and maximum, then capped at `ceil(pending entries / 32)` to amortize
  worker startup. A resulting count of `1`, including sets of at most `32`
  pending entries, uses serial reading. Parallel reading preserves traversal
  order with at most four entries per reader: up to `128` entries and `32 MiB`
  of prefetched file payload at the default maximum of `32` readers. Each
  prefetched regular-file payload is at most `256 KiB`; larger files occupy
  bounded open-handle slots.
- `--frame-target-mb` controls the bundle-size target and the threshold above
  which a regular file is streamed directly. Bundle `1` uses aggregation target
  `min(configured target, 4 MiB)`; later bundles use the configured target
  (`64 MiB` by default). A whole file can exceed the aggregation target; the
  threshold for materializing or streaming it remains the configured target.
- Metadata can close a bundle earlier to keep its `BundlePlan` body within
  `4 MiB`. The next whole entry is carried forward, without being skipped.

`--read-buffer-kb` remains accepted and validated but does not currently tune the
data path. Directory scanning independently uses up to `8` subtree workers while
preserving deterministic depth-first traversal. Compression contexts are reused
between bundles and retries, with a separate zstd frame for each attempt.

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
