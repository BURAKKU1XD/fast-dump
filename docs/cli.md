# Command-line and terminal guide

## CLI

Run `fast-dump` without arguments in a terminal to open the English setup wizard. It
guides you through the role, transport, address, paths, optional dump ID,
sender zstd level, and masked token. A complete `sender` or `receiver` command
starts the transfer directly with a live terminal dashboard. Explicit incomplete
commands remain argument errors; the wizard does not silently fill missing
command flags.
In the wizard, Listen defaults to `0.0.0.0:7000` (all IPv4 interfaces).
Connect uses a separate address field: `127.0.0.1:7000` is only for peers on
the same host; enter the listening host's network IP for a remote transfer.
Changing transport modes preserves the address entered for each mode.
The sender's `Zstd level` field defaults to `3`; the review screen and dashboard
show the selected level. The wizard accepts levels supported by the linked zstd
library, including negative fast levels and `0` for zstd's default. Switching
roles or transport modes preserves the edited sender level. Receiver setup has
no compression-level field. For command-line launches use `--level 3` (or, for
example, `--level 1`); write negative levels as `--level=-1`.

The English dashboard uses a dark cyan/violet theme, speed and durability
cards, a one-minute throughput graph, session details, and a log bounded to 200
messages.
Speed is smoothed over the latest five seconds; the session average is shown
separately. Sender details distinguish an ongoing scan from a completed scan.
Sender details also show `Current file`, its `File size` captured at open,
`File processed` as processed/size, and `Source processing` speed over the latest
five seconds. Symlink entries have zero payload size and processed bytes.
Source processing counts regular-file payload accepted by the tar/zstd input,
including repeated work on retries. It excludes tar headers, padding, symlink
metadata, and read-ahead; it is not a disk-throughput or durability measurement.
Materialized input can still be in the tar input buffer when counted. The
current-file position resets on each attempt and disappears when the entry ends;
the session total remains cumulative. Network speed continues to count compressed
payload separately. A highly compressible file can therefore have high source
processing speed and low network speed without being stalled.
The `Pipeline` panel shows each local stage's current operation and time in that
state. Sender stages are Scan, Read, Plan, Compress, Data, and Control; receiver
stages are Data, Control, and Storage. Wait reasons distinguish scanner/readers,
bundle preparation, compressed output, downstream queues, free buffers, peer
ACKs, and storage locks. Storage also shows archive writes and archive/WAL sync.
`Read` describes ordered file preparation; a large streamed file's source reads
appear under `Compress`. Socket read/write states include time blocked inside
that operation: they do not prove data is moving or identify packet loss.
An idle connection or a long wait is not reported as a network error. Detected
errors remain in session details and the activity log. Timers measure elapsed
wall time, not CPU usage, and freeze when the session ends.
On narrow terminals the pipeline takes priority over the throughput graph;
`Tab` opens details and their scrollable configuration/error text.
It adapts to terminal size, updates at most five times per second, and does
not require special fonts. `NO_COLOR` selects a monochrome presentation.
`F1` opens help, `Tab` changes focus, arrows select items, and `PgUp`/`PgDn`
scroll the log; `End` resumes following new messages.

Use global `--no-gui` before or after the subcommand for plain output:

```text
fast-dump --no-gui sender --connect 192.168.1.20:7001 --source ./data
```

The dashboard is automatically disabled if stdin, stdout, or stderr is not
a terminal, or if `TERM=dumb`. Help and version never initialize the terminal.
Without a command in plain mode, the program reports usage and exits with an
error. Sender stdout keeps its existing aggregate fields; terminal rendering
uses stderr, and success summaries are printed after restoring the terminal.

Live sender `Sent` bytes count compressed payload in the current session,
including retried attempts, not TCP headers. Receiver `Written` counts payload
successfully appended locally, including subsequently rolled-back attempts;
discarded late/orphan packets are excluded. Completed attempts later aborted are
removed from the receiver's received-entry count. The durable archive size includes
previously resumed data, whose starting boundary is displayed separately.
Sender durability advances only on validated receiver ACKs and describes the
committed bundle boundary (the receiver's final tar-termination frame is not
included in those ACKs); receiver durability advances after archive and WAL
sync. Entry counts include regular files and
symlinks, and resumed ordinals are not reported as successful file counts.
The receiver cannot know the sender's complete scan count. No overall percentage
or ETA is displayed because scanning overlaps transfer and the total is unknown.
Do not pre-scan the source to obtain a total for these estimates.
Listener mode shows active sessions and the latest twenty completed sessions.

`Ctrl+C` requests a safe stop in both dashboard and plain modes. It interrupts
network waits, stops new work, and waits for worker cleanup; the exit code is
`130`. A current filesystem operation or compression call may delay cleanup.
Repeated interrupts do not force termination. Repeat the same commands to
resume an unfinished dump from durable boundaries. A finalization already in
progress may complete before stopping, even if its final ACK is lost; existing
completed archives are always preserved. A full-command launch exits when
finished; a wizard launch keeps the result visible until dismissed. Receiver
listen mode stays active until stopped.

A failed incoming channel or transfer does not stop `receiver --listen`: it
releases that session and waits for another connection. A `Failed` session in
the dashboard does not mean that the listener stopped. Sender runs and
`receiver --connect` can restart after transient network failures using
`--retries <N>` and `--retry-delay-secs <SECONDS>`. Plain mode defaults to no
retries; the GUI defaults to **10 retries with a 10-second delay**. An explicit
`--retries 0` disables retries in either mode. Global retry flags survive wizard
setup. Resolve presentation defaults at launch and preserve them if terminal
initialization falls back to plain output. The count means additional
attempts after the first one, and the delay must be at least one second.

```text
fast-dump --no-gui --retries 5 --retry-delay-secs 10 sender --connect 192.168.1.20:7001 --source ./data
fast-dump --retries 0
```

Each failed attempt finishes worker/socket cleanup before the delay starts.
The dashboard shows the next retry and a countdown; `Ctrl+C` interrupts the
wait. Retries reuse the same parameters and resume from durable boundaries.
The budget applies to the entire invocation and does not reset after partial
progress. Validation, local storage errors, protocol/authentication rejection,
and an already-completed archive are not retried when their cause is known.
If the peer's error response is lost, its failure can appear as a network
disconnect and use the retry budget. No idle-session timeout is added.
`receiver --listen` remains a persistent listener and does not restart or
disconnect other sessions according to these flags. With a listening sender,
the connecting receiver must also reconnect (manually or with retries).

Top-level help:

```text
fast-dump [OPTIONS] [COMMAND]
```

Commands:

- `fast-dump sender`
- `fast-dump receiver`

Global options:

- `--debug`
- `--no-gui`
- `--retries <N>`: extra network attempts; default `0` in plain mode, `10` in GUI
- `--retry-delay-secs <SECONDS>`: default `10`, minimum `1`

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

See [runtime](runtime.md) for data-path details and [protocol](../src/network/protocol.md) for interoperability.
