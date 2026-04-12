# fast-dump protocol

Reference for the active wire protocol implemented in [`protocol.rs`](./protocol.rs),
transport bootstrap in [`transport.rs`](./transport.rs), and the current sender
and receiver session loops.

## Scope

- The transport layer establishes a TCP session and validates sender/receiver
  roles during handshake.
- The protocol layer owns packet framing, message encoding, decoding, and the
  dump session state machine.
- `sender` and `receiver` describe data flow, not which side opened the TCP
  socket.

The active packet version is `3`.

## Packet Header

Every message is encoded as:

| Field | Type | Size | Value |
|---|---:|---:|---|
| `magic` | `u32` | 4 | `0x504D4446` (`FDMP`) |
| `version` | `u16` | 2 | `3` |
| `kind` | `u16` | 2 | message kind |
| `body_len` | `u32` | 4 | body size in bytes |

All integers are little-endian.

## Field Encoding

- `u8/u16/u32/u64/i32`: little-endian
- `string`: `u32 len` followed by `len` UTF-8 bytes
- optional `u64`: encoded as `u64`, where `0` means `None`
- `FrameChunk.bytes`: raw compressed frame bytes, length comes from packet header
- `ManifestChunk.bytes`: raw manifest payload bytes, length is carried in the
  message body

`dump_id` is a regular UTF-8 string at codec level. Receiver storage additionally
validates it as a safe single path component.

## Size Limits

- `ManifestChunk` packet body: maximum `1 MiB`
- `ManifestChunk.bytes` payload: maximum `1048556` bytes
- `FrameChunk`: maximum `1 MiB`
- every other control body: maximum `64 KiB`
- total unacked manifest window: maximum `8 MiB` and `65536` records
- `FinishAck`: empty body

These limits are enforced on both write and read paths.

## Roles

| Role | Value |
|---|---:|
| `sender` | `1` |
| `receiver` | `2` |

## Messages

| Kind | No. | Body |
|---|---:|---|
| `Hello` | 1 | `role: u8`, `protocol_version: u16`, `dump_id: string`, `token: string` |
| `HelloAck` | 2 | `role: u8` |
| `ResumeQuery` | 3 | `dump_id: string` |
| `ResumeState` | 4 | `manifest_bytes_persisted: u64`, `last_record_seq: u64?`, `last_work_ordinal: u64?`, `sealed: bool`, `last_committed_frame: u64`, `last_committed_work_ordinal: u64?`, `archive_len: u64` |
| `ManifestStart` | 5 | `base_manifest_bytes_persisted: u64`, `base_last_record_seq: u64?`, `base_last_work_ordinal: u64?`, `base_sealed: bool` |
| `ManifestChunk` | 6 | `last_record_seq: u64?`, `last_work_ordinal: u64?`, `payload_len: u32`, `payload_bytes...` |
| `ManifestAck` | 7 | `manifest_bytes_persisted: u64`, `last_record_seq: u64?`, `last_work_ordinal: u64?`, `sealed: bool` |
| `ManifestSeal` | 8 | `final_record_seq: u64?`, `final_work_ordinal: u64?` |
| `FrameStart` | 9 | `frame_no: u64`, `first_work_ordinal: u64`, `level: i32`, `threads: u32` |
| `FrameChunk` | 10 | raw compressed bytes |
| `FrameEnd` | 11 | `frame_no: u64`, `last_work_ordinal: u64`, `compressed_len: u64`, `compressed_xxh3_64: u64` |
| `Ack` | 12 | `frame_no: u64`, `last_work_ordinal: u64`, `archive_len: u64` |
| `FrameAbort` | 13 | `frame_no: u64` |
| `AbortAck` | 14 | `frame_no: u64`, `archive_len: u64` |
| `IssueChunk` | 15 | `entry_count: u32`, then repeated `scope: u8`, `record_seq: u64?`, `work_ordinal: u64?`, `path: string`, `attempts: u32`, `code: u8`, `detail: string`, `context: string` |
| `Finish` | 16 | `final_frame_no: u64`, `final_work_ordinal: u64?` |
| `FinishAck` | 17 | empty body |
| `Error` | 18 | `message: string` |

## Durable Boundaries

`ResumeState` carries two independent durable boundaries:

- `manifest`
  - `manifest_bytes_persisted`
  - `last_record_seq`
  - `last_work_ordinal`
  - `sealed`
- `archive`
  - `last_committed_frame`
  - `last_committed_work_ordinal`
  - `archive_len`

Rules:

- `work_ordinal` is the primary archive-progress coordinate.
- `record_seq` tracks manifest progress and may advance ahead of the committed
  archive boundary.
- a manifest boundary must not carry `last_work_ordinal` without
  `last_record_seq`
- a zero-byte manifest boundary must not claim progress or `sealed = true`

## Handshake

After TCP is established, the sender always starts the protocol session:

1. sender writes `Hello`
2. receiver validates protocol version, opposite role, token, and `dump_id`
3. receiver replies with `HelloAck` or `Error`

If handshake succeeds, transport bootstrap timeouts are removed and the session
enters the long-lived dump loop.

## Session Order

The active session model is:

1. `Hello`
2. `HelloAck`
3. `ResumeQuery`
4. `ResumeState`
5. `ManifestStart`
6. zero or more `ManifestChunk`
7. zero or more `ManifestAck`
8. optional overlap of additional manifest transfer with archive frame traffic,
   but frames may only start at or below the latest durably acked manifest
   boundary
9. `ManifestSeal`
10. final sealed `ManifestAck`
11. repeated frame groups:
    `FrameStart -> FrameChunk* -> (FrameEnd -> Ack | FrameAbort -> AbortAck)`
12. zero or more `IssueChunk`
13. `Finish`
14. `FinishAck`

## Resume Semantics

Sender:

- sends `ResumeQuery { dump_id }`
- accepts either `ResumeState` or `Error`

Receiver:

- validates that `ResumeQuery.dump_id` matches the `dump_id` announced in `Hello`
- opens or creates receiver state
- truncates `<dump_id>.tar.zst.part` back to committed `archive_len`
- returns the current durable manifest and archive boundaries in `ResumeState`

## Manifest Transfer Semantics

- `ManifestStart.base_manifest` must match the receiver's durable manifest
  boundary from `ResumeState`
- `ManifestChunk` may advance discovery ahead of archive transfer
- every durably committed manifest chunk must end on a full manifest record
  boundary so reconnect can restart from `last_record_seq`
- receiver emits `ManifestAck` after a configured byte/record window and must
  emit a final sealed `ManifestAck` after `ManifestSeal`
- after a sealed `ManifestAck`, no further `ManifestChunk` is valid

## Frame Transfer Semantics

- each sender-side zstd frame is transferred as:
  `FrameStart -> FrameChunk* -> (FrameEnd -> Ack | FrameAbort -> AbortAck)`
- `FrameStart.first_work_ordinal` must not exceed the latest durably acked
  manifest boundary
- `FrameEnd.last_work_ordinal` must not move backwards and must stay within the
  same durable manifest boundary
- receiver writes `FrameChunk` directly into `<dump_id>.tar.zst.part`
- receiver computes compressed length and `xxh3_64` incrementally
- `Ack` is sent only after checksum/length validation, archive `fsync`, and
  journal persistence
- `FrameAbort` rolls back only the current uncommitted archive frame

At most one unacknowledged frame may be lost after disconnect.

## Issue Stream

`IssueChunk` carries a typed tail instead of a path-based journal.

`scope` values:

- `1 = File`
- `2 = DiscoveryEntry`
- `3 = DiscoverySubtree`
- `4 = SenderState`

`code` values:

- `1 = OpenFailed`
- `2 = UnstableAfterRetries`
- `3 = UnsupportedEntry`
- `4 = UnreadableSymlinkTarget`
- `5 = VirtualFilesystem`
- `6 = Io`
- `7 = SenderStateEvent`
- `8 = ReadDirFailed`
- `9 = SourceInvalidated`

Validation rules:

- file-scope issues must carry `work_ordinal`, must not carry `record_seq`, and
  must carry non-zero `attempts`
- discovery and sender-state issues must not carry `work_ordinal`
- issue chunks are valid only after manifest transfer is sealed and archive
  frame transfer is complete
- issue chunks never change manifest or archive resume state

Receiver persists accepted issue chunks into
`<dump_id>.tar.zst.issues.tsv.part` and renames that file to the final
`.issues.tsv` only after successful `Finish`.

## Finish Semantics

- `Finish` is valid only after the manifest stream is sealed
- `Finish.final_work_ordinal` must match the current committed archive boundary
- before `FinishAck`, receiver:
  - validates final frame/work-ordinal boundary against journal state
  - appends two tar end blocks as a local final zstd frame
  - `fsync`s the archive
  - renames `.tar.zst.part` into the final `.tar.zst`
  - removes the journal

If issue chunks were accepted, receiver also renames `.issues.tsv.part` into the
final `.issues.tsv`.

## Error and Rollback Rules

- receiver may send `Error` during any phase when validation fails
- manifest failures must not advance archive progress
- if manifest temp-store quota or disk-full is hit, receiver keeps the previous
  durable manifest boundary
- checksum or length mismatch during frame transfer causes `Error` and rollback
  to the previous committed `archive_len`
- if `Ack` was not delivered, sender must treat the frame as uncommitted
- stale partial issue tails are removed on restart

## Contract Notes

Stable external protocol contract:

- packet header layout
- endianness
- message numbers and body layouts
- role model
- session ordering and validation rules
- manifest, frame, finish, and rollback semantics
- packet size limits

Implementation details that may change without changing wire format:

- exact logging text
- transport timeout values
- sender reader-pool internals
- receiver journal file layout
