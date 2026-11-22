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

Local [presentation](../../docs/cli.md) and
[cancellation/retry policy](../../docs/runtime.md#session-ownership-cancellation-and-retries)
add no wire messages. Cancellation closes both channels without sending `Finish`
or recording a terminal skip; an already-started durable commit or finalization
follows the ordering below. Retries establish a fresh v4 session and resume only
from durable boundaries. After a lost final ACK, an already-completed archive is
rejected rather than overwritten or inferred successful. Known initialization
errors use the existing `Error` message best-effort; a lost reply can appear as a
disconnect. Local diagnostics do not alter packet limits, idle deadlines, retry
decisions, or commit ordering.

`v4` opens two TCP channels per dump session against the same address.

Current mainline transport preserves both pairings:

- `push`: `sender --connect` to `receiver --listen`
- `pull`: `sender --listen` with `receiver --connect`

The interactive P2P connection starts with a separate `FDP1` handshake.
The connecting peer sends the four-byte magic, its big-endian 16-bit
optional token length (up to 4096), the token bytes, and a 60-byte UTF-8
hostname padded with zeros. The listener checks the token and responds with
the magic and its own hostname. The connection then exchanges fixed 65-byte
offer, acceptance, and decline messages. An accepted offer carries a fresh
16-byte credential, encoded as hex for the two v4 channel handshakes.
Those channels use the listener's original address and port. The offer
channel stays open while v4 transfers run independently of it.

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

### Relay registration

The access-token lobby has a separate `FDA1` registration. The header is
the 4-byte magic, a one-byte selector (0 join, 1 control attachment, 2 data
attachment), and 16 bytes of credentials. Join uses a 12-byte ASCII token
followed by a feature byte and three reserved bytes. Bit 0 requests hostname
metadata. Bit 1 requests peer TLS transfers. Bit 2, together with bit 1,
requests receiver keys supplied by the relay. The relay echoes the features
in the first byte of the join reply payload. Current clients require bits 1
and 2. Clients must use the same transfer mode to pair. When two legacy
clients pair, each relay TLS attachment uses the full 16-byte credential.
The default access-token TLS setup uses a server raw public key (RFC 7250).
Temporary-relay clients learn the relay public key during the lobby TLS
handshake and check the signature before sending the access token.
Existing `FD1` and `FD2` invitations still authenticate the relay with the
supplied fingerprint or public key. Configured X.509 access relays use the
short token with the configured TLS trust model.
Once registered, lobby control messages have a one-byte kind, a big-endian
32-bit client ID or peer count, and 60 bytes of payload. A peer list uses
one big-endian ID per entry. The token is generated from a 32-symbol alphabet.
The server responds to a wrong token after a one-second delay and applies no
such delay to valid tokens. At most 15 joined clients hold persistent lobby
connections. A peer-name update carries a zero-padded UTF-8 hostname.
A peer TLS offer identifies the recipient and leaves the payload empty.
Legacy offers may carry the source short name. Acceptance assigns the sender
and receiver individual 16-byte attachment credentials over the TLS lobby.
When bit 2 is active, the receiver sends its 44-byte DER Ed25519
SubjectPublicKeyInfo at the start of the `ACCEPT` payload. The relay includes
the key in `START_SEND` immediately after the sender's attachment credential.

For peer TLS attachments, each of the four fresh TCP sockets sends the
five-byte `FDA1` prefix and channel selector (1 control, 2 data). The relay
sends a fresh random 16-byte challenge. The client sends HMAC-SHA256 of the
five-byte header followed by the challenge, keyed by the attachment credential.
Once all four attachments are authenticated, the relay sends the existing
`ATTACHED` frame on each socket and begins forwarding both duplex streams.
The sender is the TLS 1.3 client on both channels. The receiver presents its
lobby client's Ed25519 public key on each channel. With bit 2 active, the
sender verifies it against the key in `START_SEND` on each TLS handshake,
then starts v4.

Older clients using bit 1 alone learn the key on the first peer TLS handshake
and require the same key on both. They derive 32 bytes from each peer TLS
channel using the TLS exporter
label `EXPORTER-fast-dump-transfer-verification` and contexts `control` and
`data`. The displayed code is the first 16 bytes of SHA-256 over the exporter
values in control-then-data order. Each peer verifies the full displayed code
with the other peer over a separate trusted channel. After local confirmation,
each sends byte `1` inside the peer control TLS channel and waits for the
other confirmation before sending any v4 messages. Both v4 channels then
carry their existing handshake and transfer inside peer TLS.

The lobby stays connected between transfers. Reply 23 releases a transfer
after its channels were ready.
The v4 exchange determines archive success. There is at most one outstanding
offer or transfer per member.

### Role-key relay registration

Relay transport preserves both v4 channels. Before the v4 handshakes, each
client opens a TLS 1.3 connection and sends a relay registration. Its fields, in
order, are ASCII `FDR1`, role (`1` sender, `2` receiver), channel (`0` control,
`1` data), one byte of room length, the 32-byte binary role key, a 16-byte attempt
ID for data registrations, and the ASCII room name of length 1–64.

Once both control registrations are accepted, the relay replies on both with
`1` (`Matched`) followed by the same 16-byte attempt ID. Both clients then open
their data connections and send that ID. Once all four sockets are paired, the
relay sends `2` (`Ready`) on each, then forwards the bytes of each channel.
`3` means room or key rejected, `4` means room busy, and `5` means the pairing
expired or the data attempt ID is stale. The relay header and replies never enter
the v4 stream. `Hello` and `HelloAck` still travel between sender and receiver.

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

The [runtime contract](../../docs/runtime.md#source-preparation-and-bounded-data-flow)
defines source selection, ordered scanning/read-ahead, memory limits, compression
context reuse, and worker ownership. These local choices preserve the following
interoperability requirements:

- Work ordinals follow deterministic depth-first traversal, sorting each
  directory by normalized relative path. Validate the committed ordinal/path
  prefix before opening pending files. Unsupported native-path collisions do
  not change public scan entries or work ordinals.
- Each pruned Linux system-filesystem directory produces one `SkippedEntry`
  with reason `UnsupportedEntry` and ordinal `0`. Its path identifies only that
  directory; unvisited descendants receive no ordinals or records. Filesystem
  names and explanations are local diagnostics. `SkipEntries`/`SkipAck` durability
  and exact-path semantics remain unchanged. Reevaluate mounts on later scans;
  incompatible committed-prefix validation fails without resetting progress.
- Entries remain whole. Encoded plans obey the control-body limit; a descriptor
  that cannot fit is rejected rather than skipped or split.
- Each attempt starts an independent zstd frame. Send `BundlePlan` before
  compression; transmit completed chunks as compression proceeds.
  `BundleCommit` carries the final compressed length and hash.
- The compressed hash remains standard XXH3-64 with the default seed and secret.
  Local SIMD selection changes no checksum values or wire fields; peers do not
  need matching CPU features or implementations.
- Compression level and worker count are sender-local settings. They add no wire
  fields and require no matching receiver options.
- Local pipeline timings and counters add no messages and do not represent
  durable progress; only validated ACKs establish the sender's durable boundary.

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

Each data packet carries its actual payload length; only those bytes are written
and hashed. Buffer reuse and lifecycle are specified by the
[runtime contract](../../docs/runtime.md#source-preparation-and-bounded-data-flow).

Receiver still commits bundles in bundle order. Windowing exists on sender side:
the sender may continue sending later bundles while earlier durable acks are still
unread.

Session exit closes both channels and joins their readers. After `Finish`,
control errors and unexpected further controls interrupt waiting for pending
data. There is no general deadline for a silent connected peer; detailed shutdown
requirements are in the [runtime contract](../../docs/runtime.md#session-ownership-cancellation-and-retries).

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
