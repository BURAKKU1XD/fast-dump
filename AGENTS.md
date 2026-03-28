# fast-dump

## Goal
- Build a sender/receiver pair that streams a resumable dump to a remote server.
- Final artifact on the receiver is a valid `.tar.zst` file.
- Terminology is based on data flow:
  - `sender` reads files, builds tar, compresses zstd, and sends frames
  - `receiver` accepts frames, persists the partial archive, and maintains resume state
- Connection establishment is a separate concern from role naming.

## CLI Contract
- Use one binary with two subcommands:
  - `fast-dump sender`
  - `fast-dump receiver`
- Each subcommand supports exactly one transport selector:
  - `--listen <ADDR>`
  - `--connect <ADDR>`
- Valid mode pairs for v1:
  - `push`: `sender --connect <ADDR>` with `receiver --listen <ADDR>`
  - `pull`: `sender --listen <ADDR>` with `receiver --connect <ADDR>`
- Reject invalid combinations at argument-validation level.

## Sender CLI
- Required:
  - `--source <DIR>`
  - exactly one of `--listen <ADDR>` or `--connect <ADDR>`
- Optional:
  - `--dump-id <ID>`
  - `--token <TOKEN>`
  - `-l, --level <N>` default `3`
  - `-t, --threads <N|auto>` default `auto`
  - `--min-readers <N>` default `2`
  - `--max-readers <N>` default `8`
  - `--read-buffer-kb <N>` default `1024`
  - `--frame-target-mb <N>` default `64`

## Receiver CLI
- Required:
  - `--data-dir <DIR>`
  - exactly one of `--listen <ADDR>` or `--connect <ADDR>`
- Optional:
  - `--token <TOKEN>`
  - `--max-sessions <N>` default `1`

## Archive Rules
- Archive contains only regular files.
- Empty directories and directory metadata are ignored.
- Paths stored in tar are relative to the selected root and normalized to `/`.
- File traversal order must be deterministic:
  - depth-first
  - entries within each directory sorted by normalized relative path
- Large files are streamed chunk-by-chunk and must never be loaded fully into RAM.

## Compression Rules
- Compression runs on the sender.
- Use zstd with CLI-configurable level:
  - `-l, --level <N>`
  - default: `3`
- Use CLI-configurable zstd worker threads:
  - `-t, --threads <N>`
  - default: `available_parallelism() - 1`
  - minimum: `1`
- The receiver only appends bytes to `<dump_id>.tar.zst.part` and renames on successful completion.

## Resume Model
- Do not use one endless zstd stream for the whole dump.
- The output `.tar.zst` is a concatenation of multiple zstd frames.
- Each zstd frame must end only on a completed file boundary.
- A frame may contain many files.
- If the current frame target size is exceeded in the middle of a file, that file stays in the current frame and the frame closes after the file completes.
- The receiver commits only whole frames.
- On disconnect, at most the current uncommitted frame is lost.
- Resume always starts from the first file after the last committed file.

## Sender Pipeline
- Walk files in deterministic order.
- Write tar headers and file data as a stream.
- Feed tar bytes into a framed zstd writer.
- Use adaptive reader workers with CLI bounds:
  - `--min-readers <N>` default `2`
  - `--max-readers <N>` default `8`
  - `--read-buffer-kb <N>` default `1024`
  - `--frame-target-mb <N>` default `64`
- Use a bounded in-memory queue so RAM usage stays capped.
- Auto-scaling policy:
  - start at `min_readers`
  - add a reader when the queue is frequently empty and the compressor/network waits for input
  - stop scaling when the queue stays near full or throughput stops increasing
  - never exceed `max_readers`

## Receiver State
- Persist `journal` next to the partial archive.
- Required journal fields:
  - `protocol_version`
  - `dump_id`
  - `last_committed_frame`
  - `last_committed_file`
  - `archive_len`
- Before resuming, truncate any trailing bytes beyond `archive_len`.
- Finalize by writing tar end blocks, closing the last zstd frame, fsyncing, and renaming the file.

## Wire Protocol
- Transport is a long-lived TCP stream between sender and receiver.
- Optional TLS may wrap the TCP stream later without changing the protocol.
- Do not use HTTP, JSON, gRPC, or WebSocket for v1.
- Use one custom binary protocol over one ordered stream.
- Support two transport modes:
  - `push`: sender connects to receiver
  - `pull`: receiver connects to sender
- Protocol semantics stay the same in both modes because roles are defined by data direction, not by which side initiated TCP.
- All messages are length-prefixed binary packets.
- Packet header layout:
  - `magic: u32` = `0x504D4446` (`FDMP` in little-endian byte order on the wire)
  - `version: u16` = `1`
  - `kind: u16`
  - `body_len: u32`
- Integer encoding is little-endian.
- Path fields are encoded as length-prefixed UTF-8 relative paths normalized to `/`.
- Compressed frame payload is transferred as explicit `FrameChunk` packets between frame control messages.
- `FrameChunk` body is raw compressed bytes of the current zstd frame.
- Default max `FrameChunk` payload size: `1 MiB`.
- The receiver must stream `FrameChunk` bytes directly to disk and must not buffer the whole frame in RAM.
- Required message sequence:
  - `Hello { protocol_version, dump_id }`
  - `ResumeQuery { dump_id }`
  - `ResumeState { last_committed_frame, last_committed_file_index, last_committed_file_path, archive_len }`
  - repeated for each frame:
    - `FrameStart { frame_no, first_file_index, first_file_path, level, threads }`
    - zero or more `FrameChunk { bytes }`
    - `FrameEnd { frame_no, last_file_index, last_file_path, compressed_len, compressed_xxh3_64 }`
    - `Ack { frame_no, last_file_index, last_file_path, archive_len }`
  - `Finish { final_frame_no, final_file_index, final_file_path }`
  - `FinishAck`
- `last_committed_file_index` is the monotonic index of the file in deterministic traversal order.
- `last_committed_file_path` is stored for diagnostics and extra resume validation.
- The sender waits for `Ack` after every `FrameEnd`.
- The receiver computes `compressed_len` and `compressed_xxh3_64` incrementally while writing `FrameChunk`s.
- On `FrameEnd`, the receiver verifies length and checksum, fsyncs, persists the journal, and only then sends `Ack`.
- If verification fails, the receiver sends `Error` and truncates back to the previous committed `archive_len`.
- If the connection drops before `Ack`, the receiver discards the partial frame by truncating back to the last committed `archive_len`.

## File Type Policy
- Support regular files first.
- Skip symlinks, sockets, devices, and other special entries with a warning.
