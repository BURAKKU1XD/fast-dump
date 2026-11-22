# Command-line and terminal guide

## CLI

Run `fast-dump` without arguments in a terminal to open the English setup wizard.
Choose Relay, Listen, or Connect with the arrow keys and confirm with Enter.
Other fields are dimmed until you confirm. Relay starts the server with
separate listen address and port fields, prefilled with `0.0.0.0` and `9876`.
It generates its TLS key pair and prints a relay invitation after startup.
Use Connect → Via relay when peers can reach the relay but cannot accept
incoming connections from each other. Enter the relay host, port, and the
invitation. A short access token for a relay with a configured certificate
opens the optional CA file field. For P2P, use Listen on one peer and
Connect → Direct on the other.
Provide the same optional shared P2P token on both ends.

Once a peer is available, the connected screen shows its system hostname.
The P2P recipient is preselected. In Relay, use the arrow keys to select a
recipient. Duplicate hostnames are shown with their connection numbers.
Press Enter on the selected recipient and enter the source directory. On
this form, F2 cycles between the source and optional dump ID and zstd level.
Enter submits a transfer offer with the source's short name. The recipient
sees the sender's hostname and the offer. Enter lets them choose a destination
and accept. Esc declines. After the transfer, close the result screen to
return to the same connection; either peer can then offer another folder.
A complete `sender` or `receiver` command starts the transfer directly with a
live terminal dashboard. Explicit incomplete commands remain argument errors.
Use `--join-relay <HOST:PORT> --invitation <STRING>` to open the relay lobby
directly from a terminal. These options are used together at the top level.
In the wizard, Listen defaults to `0.0.0.0:7000` (all IPv4 interfaces).
Connect uses a separate address field: `127.0.0.1:7000` is only for peers on
the same host; enter the listening host's network IP for a remote transfer.
Changing transport modes preserves the address entered for each mode.
The offer's `Zstd level` field defaults to `3`. It accepts the levels supported
by the linked zstd library, including negative fast levels and `0` for zstd's
default. The dashboard displays the chosen level. For command-line launches
use `--level 3` (or, for example, `--level 1`); write negative levels as
`--level=-1`.

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
The selected completed session shows its skip count on the main screen. A
completed receiver session also shows the archive path. An idle receiver
listener shows that it is waiting for another transfer.

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
connecting receivers can restart after transient network failures using
`--retries <N>` and `--retry-delay-secs <SECONDS>`. Plain mode defaults to no
retries; the GUI defaults to **10 retries with a 10-second delay**. An explicit
`--retries 0` disables retries in either mode. Global retry flags survive wizard
setup. For the connected interface they govern initial connection attempts;
submit a new offer to retry an interrupted transfer on a surviving connection.
Resolve presentation defaults at launch and preserve them if terminal
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
- `fast-dump relay`

Global options:

- `--debug`
- `--no-gui`
- `--retries <N>`: extra network attempts; default `0` in plain mode, `10` in GUI
- `--retry-delay-secs <SECONDS>`: default `10`, minimum `1`

### Sender

```text
fast-dump sender [OPTIONS] --source <SOURCE> <--listen <LISTEN>|--connect <CONNECT>|--relay <HOST:PORT>>
```

Required:

- `--source <DIR>`
- exactly one of `--listen <ADDR>`, `--connect <ADDR>`, or `--relay <HOST:PORT>`

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
fast-dump receiver [OPTIONS] --data-dir <DATA_DIR> <--listen <LISTEN>|--connect <CONNECT>|--relay <HOST:PORT>>
```

Required:

- `--data-dir <DIR>`
- exactly one of `--listen <ADDR>`, `--connect <ADDR>`, or `--relay <HOST:PORT>`

Optional:

- `--token <TOKEN>`
- `--max-sessions <N>` default `1`

Notes:

- `--max-sessions` matters in `receiver --listen` mode.
- In `receiver --connect` and `receiver --relay` modes one process handles one dump session.

### Relay

On a reachable host, choose Relay in the wizard or run
`fast-dump relay --access --listen 0.0.0.0:9876`. The relay generates a fresh
TLS key pair and prints an invitation after binding the listener. Share the
whole invitation through a trusted channel. A client pins the public-key
fingerprint from the invitation before sending the 12-character access token.
The relay prints a PowerShell command for Windows x86-64 and a Bash command
for Linux x86-64. Each downloads the latest GitHub release binary and its
matching `.sha256` file into a temporary directory, verifies the binary, and
starts it in the same terminal. When the client exits, the command removes
its temporary files. Share the commands only with intended participants
because they contain the invitation.

If the relay listens on all interfaces, replace `relay.example` with a host
or IP that the clients can reach.

Restarting the relay creates a new key pair and invitation. Up to 15 clients
can join at once. A valid token receives an immediate admission or capacity response.
An invalid token receives a rejection one second after its request is read.
Clients stay connected while idle and can choose a different recipient or
switch roles after each transfer. Each client handles one transfer at a time.

Run `fast-dump relay --listen <IP:PORT> --config <FILE>` for scripted transfers
with role keys and use explicit sender and receiver commands on the clients.
An access-token relay started from the CLI can also use `--config <FILE>` or
`--certificate <CHAIN.pem> --private-key <KEY.pem>`. In this mode it prints
the short access token. Clients trust the chain and verify the relay hostname
or IP as usual.
Paths in the TOML file are relative to the file's directory.

```toml
certificate = "fullchain.pem"
private_key = "privkey.pem"

[rooms.backup]
sender_key_file = "sender.key"
receiver_key_file = "receiver.key"
```

Generate a separate random 32-byte key for each role in each room, encoded as
64 hexadecimal digits. Keep the key files private and give each client the file
for its role. The service loads the configured keys and certificates at startup.
Room names use 1–64 ASCII letters, digits, hyphens, or underscores.

- Both clients pass `--relay <HOST:PORT> --room <NAME> --relay-token-file <FILE>`.
- Clients use the bundled public trust anchors by default. For a private CA, give
  both clients `--relay-ca-file <PEM>` to use that trust store. The certificate
  must match the hostname or IP in `--relay`. Bracket IPv6 addresses.
  The hostname must route both peers to the same relay instance.
- Relay role keys authorize joining a room. The optional existing `--token`
  still applies to the v4 handshake between the two peers.
- Each room handles one transfer attempt at a time. The first peer can wait up to
  60 seconds for the second. After matching, both data channels have 10 seconds
  to attach. Direct push and pull use their existing connection behavior.
- When a connection drops, both peers reconnect to resume. To enable automatic
  retries in plain mode, pass `--retries <N>` to both clients. A pairing timeout
  or busy room uses the network retry budget. A rejected room key does not.
- TLS terminates at the relay. Use a relay operator trusted with dump contents.

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
