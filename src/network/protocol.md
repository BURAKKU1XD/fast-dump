# fast-dump protocol

Краткая спецификация текущего wire protocol из [`protocol.rs`](./protocol.rs),
[`transport.rs`](./transport.rs), sender session flow и receiver session flow.

## Назначение

Протокол работает поверх уже установленного двустороннего байтового потока.
В v2 `transport` поднимает TCP-сессию и делает bootstrap-handshake, а `protocol`
кодирует и декодирует бинарные сообщения для полного dump/resume session loop.

Имена `sender` и `receiver` описывают направление данных, а не того, кто открыл
TCP-сокет. В push и pull transport-парах sender всё равно первым начинает
wire session после установки TCP.

## Runtime Model

Фактическая последовательность сообщений в live runtime такая:

1. sender отправляет `Hello`
2. receiver отвечает `HelloAck` или `Error`
3. sender отправляет `ResumeQuery`
4. receiver открывает локальное состояние dump-а, восстанавливает `.part` до
   `archive_len` из journal и отвечает `ResumeState`
5. для каждого отправляемого zstd-frame:
   - sender отправляет `FrameStart`
   - sender отправляет ноль или больше `FrameChunk`
   - sender либо отправляет `FrameEnd`, либо явно отменяет текущий незакоммиченный frame через
     `FrameAbort`
   - при `FrameEnd` receiver проверяет длину и checksum, `fsync`-ает `.part`, persist-ит journal
     и только потом отвечает `Ack`
   - при `FrameAbort` receiver откатывает `.part` обратно к последнему committed `archive_len`
     и отвечает `AbortAck`
6. если в сессии были terminal skip, sender отправляет `IssueJournalChunk` zero or more times
7. sender отправляет `Finish`
8. receiver локально дописывает tar end blocks как отдельный финальный zstd-frame,
   `fsync`-ает архив, переименовывает `.part` в финальный `.tar.zst`,
   при наличии terminal skip пишет рядом `<dump_id>.tar.zst.issues.tsv`,
   удаляет journal и отвечает `FinishAck`

Если frame не дошёл до `Ack`, receiver не продвигает committed state. При ошибке
или разрыве текущий незакоммиченный frame откатывается обратно к предыдущему
`archive_len`.

## Packet Header

Каждое сообщение передаётся как:

| Поле | Тип | Размер | Значение |
|---|---:|---:|---|
| `magic` | `u32` | 4 | `0x504D4446` (`FDMP`) |
| `version` | `u16` | 2 | `2` |
| `kind` | `u16` | 2 | тип сообщения |
| `body_len` | `u32` | 4 | длина body в байтах |

Все целые числа кодируются в `little-endian`.

## Field Encoding

- `u8/u16/u32/u64/i32`: `little-endian`
- `string`: `u32 len` + `len` байт UTF-8
- `path`: тот же `string`, но путь должен быть:
  - относительным
  - с разделителями `/`
  - не начинаться с `/`
  - не содержать пустых сегментов, `.` и `..`
- пустая строка как sentinel разрешена только для:
  - `ResumeState.last_committed_file_path`
  - `Finish.final_file_path`
- `FrameChunk.bytes`: raw compressed bytes без внутреннего префикса длины;
  длина берётся из packet `body_len`

Замечание по `dump_id`:

- на уровне codec это обычный UTF-8 string
- на receiver storage layer он дополнительно валидируется как безопасный
  одиночный компонент имени файла

## Size Limits

- `FrameChunk`: максимум `1 MiB`
- все остальные message body: максимум `64 KiB`
- `FinishAck`: body должен быть пустым

Эти ограничения применяются и при записи, и при чтении пакетов.

## Role Encoding

| Role | Значение |
|---|---:|
| `sender` | `1` |
| `receiver` | `2` |

## Messages

| Kind | № | Body |
|---|---:|---|
| `Hello` | 1 | `role: u8`, `protocol_version: u16`, `dump_id: string`, `token: string` |
| `HelloAck` | 2 | `role: u8` |
| `ResumeQuery` | 3 | `dump_id: string` |
| `ResumeState` | 4 | `last_committed_frame: u64`, `last_committed_file_index: u64`, `last_committed_file_path: path`, `archive_len: u64` |
| `FrameStart` | 5 | `frame_no: u64`, `first_file_index: u64`, `first_file_path: path`, `level: i32`, `threads: u32` |
| `FrameChunk` | 6 | raw compressed bytes |
| `FrameEnd` | 7 | `frame_no: u64`, `last_file_index: u64`, `last_file_path: path`, `compressed_len: u64`, `compressed_xxh3_64: u64` |
| `Ack` | 8 | `frame_no: u64`, `last_file_index: u64`, `last_file_path: path`, `archive_len: u64` |
| `Finish` | 9 | `final_frame_no: u64`, `final_file_index: u64`, `final_file_path: path` |
| `FinishAck` | 10 | пустое body |
| `Error` | 11 | `message: string` |
| `FrameAbort` | 12 | `frame_no: u64` |
| `AbortAck` | 13 | `frame_no: u64`, `archive_len: u64` |
| `IssueJournalChunk` | 14 | `entry_count: u32`, затем повторяющиеся `file_index: u64`, `file_path: path`, `attempts: u32`, `reason: u8`, `last_error: string` |

## Handshake Semantics

После установки TCP sender всегда первым пишет `Hello`, даже если TCP-сокет
открыл receiver в pull-режиме.

### `Hello`

- sender обязан передать непустой `dump_id`
- `token` передаётся как строка; если аутентификация отключена, используется
  пустая строка

### `HelloAck`

- receiver подтверждает противоположную роль
- после `HelloAck` transport-timeout снимается, и поток переходит в
  долгоживущую dump-сессию

### `Error`

- может прийти вместо ожидаемого handshake reply
- используется и в последующих фазах session loop для reject/abort сигналов

## Resume Semantics

`ResumeQuery` и `ResumeState` работают поверх уже завершённого handshake.

Sender:

- отправляет `ResumeQuery { dump_id }`
- ждёт либо `ResumeState`, либо `Error`

Receiver:

- сверяет `ResumeQuery.dump_id` с `dump_id`, объявленным в `Hello`
- открывает или создаёт receiver state
- восстанавливает `.tar.zst.part` до `archive_len`, записанного в journal
- отвечает `ResumeState`

`ResumeState` описывает последнюю committed boundary:

- `last_committed_frame`
- `last_committed_file_index`
- `last_committed_file_path`
- `archive_len`

## Frame Transfer Semantics

Каждый sender-side zstd-frame переносится отдельной группой сообщений:

1. `FrameStart`
2. `FrameChunk`*
3. либо `FrameEnd`, либо `FrameAbort`
4. либо `Ack`, либо `AbortAck`

Правила:

- итоговый `.tar.zst` на receiver side является конкатенацией нескольких
  sender-side zstd-frame-ов плюс локального финального frame-а с tar end blocks
- каждый sender-side zstd-frame закрывается только на границе завершённого файла
- `FrameChunk` передаются как уже сжатые байты текущего zstd-frame
- receiver пишет `FrameChunk` напрямую в `<dump_id>.tar.zst.part` и считает
  `compressed_len` и `compressed_xxh3_64` инкрементально
- `Ack` отправляется только после успешной проверки `FrameEnd`, `fsync` partial
  archive и persist journal
- `FrameAbort` разрешён только для текущего незакоммиченного frame и не меняет committed journal
- `AbortAck.archive_len` всегда равен последнему committed `archive_len` после rollback
- при checksum/length mismatch receiver отправляет `Error` и откатывает
  partial archive обратно к предыдущему committed `archive_len`
- при разрыве соединения до `Ack` sender считает frame незакоммиченным,
  а receiver не продвигает committed state

## Finish Semantics

`Finish` закрывает dump по последней уже committed boundary, а не по "частично
отправленному" состоянию.

Перед `Finish` sender может передать zero or more `IssueJournalChunk`:

- chunk-ы идут только после всех committed frame-ов этой сессии
- каждый chunk содержит часть sender-side issue journal для terminal skip
- receiver держит эти записи в памяти до `Finish` и не влияет ими на resume state

Receiver перед `FinishAck`:

- валидирует `final_frame_no`, `final_file_index` и `final_file_path`
  против journal
- при наличии `IssueJournalChunk` пишет их рядом с финальным архивом в
  `<dump_id>.tar.zst.issues.tsv`
- дописывает два tar end blocks (`1024` нулевых байта) как отдельный локальный
  zstd-frame
- `fsync`-ает архив
- переименовывает `.tar.zst.part` в финальный `.tar.zst`
- удаляет journal

## Frozen v3 Model

Status note:

- Current CLI/runtime still speaks packet version `2` by default.
- [`protocol.rs`](./protocol.rs) now also contains a frozen packet version `3`
  message model, codec tests, and library-level session helpers used by tests
  and benches.
- The `v3` model below is implemented for opt-in library paths, but it is not
  yet the default CLI/runtime session loop.
- Current automated benchmark campaign keeps the cutover decision at `no-go`:
  the active runtime stays on packet version `2`, while the current measured
  `v3` tuning candidate is `manifest_chunk_kb=512`,
  `manifest_ack_records=65536`, `manifest_ack_kb=8192` for further library-side
  work and comparison.

Packet version `3` keeps the same fixed header layout:

| Field | Type | Size | Value |
|---|---:|---:|---|
| `magic` | `u32` | 4 | `0x504D4446` (`FDMP`) |
| `version` | `u16` | 2 | `3` |
| `kind` | `u16` | 2 | v3 message kind |
| `body_len` | `u32` | 4 | body size in bytes |

Field encoding for `v3`:

- `u8/u16/u32/u64/i32`: little-endian
- `string`: `u32 len` + UTF-8 bytes
- optional `u64`: encoded as `u64`, where `0` means `none`
- `FrameChunk.bytes`: raw compressed bytes, length comes from packet header
- `ManifestChunk.bytes`: raw manifest payload bytes, length is part of the
  message body

V3 size limits:

- `ManifestChunk`: maximum `1 MiB`
- `FrameChunk`: maximum `1 MiB`
- every other v3 control body: maximum `64 KiB`
- total unacked manifest window: maximum `8 MiB` and `65536` records
- `FinishAck`: empty body

### V3 boundaries

`ResumeStateV3` carries two independent durable boundaries:

- `manifest`
  - `manifest_bytes_persisted: u64`
  - `last_record_seq: u64?`
  - `last_work_ordinal: u64?`
  - `sealed: bool`
- `archive`
  - `last_committed_frame: u64`
  - `last_committed_work_ordinal: u64?`
  - `archive_len: u64`

Rules:

- `work_ordinal` is the primary progress coordinate for frame, ack, and finish
  validation.
- `record_seq` tracks manifest discovery progress and may advance beyond the
  committed archive boundary.
- a manifest boundary must not carry `last_work_ordinal` without
  `last_record_seq`.
- a zero-byte manifest boundary must not claim progress or `sealed = true`.

### V3 messages

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

### V3 typed issue stream

`IssueChunk` uses a single typed tail instead of separate file/discovery
journals.

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
- `path` is diagnostic text in `v3`; it is not a resume boundary field
- receiver persists accepted `IssueChunk` entries incrementally into
  `<dump_id>.tar.zst.issues.tsv.part` instead of buffering the whole typed tail
  in RAM

### V3 ordering rules

The frozen `v3` session model is:

1. `Hello`
2. `HelloAck`
3. `ResumeQuery`
4. `ResumeState`
5. `ManifestStart`
6. zero or more `ManifestChunk`
7. zero or more `ManifestAck`
8. optional overlap of additional `ManifestChunk`/`ManifestAck` traffic with
   committed archive frame traffic, but only for `work_ordinal` values already
   covered by the latest durable `ManifestAck`
9. optional `ManifestSeal`
10. final sealed `ManifestAck`
11. repeated frame groups:
    `FrameStart -> FrameChunk* -> (FrameEnd -> Ack | FrameAbort -> AbortAck)`
12. zero or more `IssueChunk`
13. `Finish`
14. `FinishAck`

Explicit rules:

- `FrameStart.first_work_ordinal` must not point beyond the receiver's latest
  durably acked manifest boundary.
- `ManifestChunk` may advance manifest discovery ahead of archive transfer, but
  must not retroactively shrink or unseal the committed manifest boundary.
- each durably committed `ManifestChunk` must end on a full manifest record
  boundary, so reconnect can restart from `last_record_seq` without preserving a
  partial record tail.
- `ManifestSeal` closes the manifest stream for the session; after a sealed
  `ManifestAck`, no further `ManifestChunk` is valid.
- receiver may emit `ManifestAck` after a batch reaches the negotiated runtime
  `manifest_ack_records` or `manifest_ack_kb` window, and must emit a final
  sealed `ManifestAck` after `ManifestSeal`.
- `IssueChunk` is allowed only after both manifest transfer and frame transfer
  are complete.
- `Finish` is valid only after the manifest stream is sealed and the final
  committed archive boundary matches `final_work_ordinal`.

### V3 error and rollback rules

- if receiver rejects a `ManifestChunk` or `ManifestSeal`, it sends `Error` and
  keeps the previous durable manifest boundary
- manifest failures must not advance `archive_len` or the committed archive
  boundary
- receiver additionally enforces local temp-store caps:
  - `<dump_id>.manifest.part`: `512 MiB`
  - `<dump_id>.tar.zst.issues.tsv.part`: `64 MiB`
- quota exhaustion or OS-level disk-full errors on those temp stores cause
  `Error`, and receiver rolls the affected temp file back to its pre-append
  length when possible
- `ManifestAck` is the only signal that allows sender to treat a manifest prefix
  as durably replicated on the receiver
- `FrameAbort` rolls back only the current uncommitted archive frame and does
  not change the committed manifest boundary
- typed issue chunks never change manifest or archive resume state and may be
  discarded safely on abort before `FinishAck`
- receiver renames `.issues.tsv.part` into the final `.issues.tsv` only after a
  successful `Finish`; stale partial issue tails are removed on restart, and
  abandoned `.manifest.part` files with no journal are discarded on fresh open

## Contract Versus Implementation Notes

Стабильной частью v2 contract считаются:

- packet header layout
- integer endianness
- message kinds and body layout
- role model
- порядок `Hello -> HelloAck -> ResumeQuery -> ResumeState -> Frame* -> (Ack | AbortAck) -> IssueJournalChunk* -> Finish -> FinishAck`
- frame commit semantics через `Ack`
- лимиты размера `FrameChunk` и control messages

Implementation notes, которые важны сейчас, но могут быть переоформлены без
смены wire format:

- точные logging details
- конкретные timeout values в transport bootstrap
- внутренняя организация sender pipeline
- конкретный layout receiver-side journal file
