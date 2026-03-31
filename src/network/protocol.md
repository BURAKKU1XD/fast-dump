# fast-dump protocol

Краткая спецификация текущего wire protocol из [`protocol.rs`](./protocol.rs),
[`transport.rs`](./transport.rs), sender session flow и receiver session flow.

## Назначение

Протокол работает поверх уже установленного двустороннего байтового потока.
В v1 `transport` поднимает TCP-сессию и делает bootstrap-handshake, а `protocol`
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
   - sender отправляет `FrameEnd`
   - receiver проверяет длину и checksum, `fsync`-ает `.part`, persist-ит journal
     и только потом отвечает `Ack`
6. sender отправляет `Finish`
7. receiver локально дописывает tar end blocks как отдельный финальный zstd-frame,
   `fsync`-ает архив, переименовывает `.part` в финальный `.tar.zst`,
   удаляет journal и отвечает `FinishAck`

Если frame не дошёл до `Ack`, receiver не продвигает committed state. При ошибке
или разрыве текущий незакоммиченный frame откатывается обратно к предыдущему
`archive_len`.

## Packet Header

Каждое сообщение передаётся как:

| Поле | Тип | Размер | Значение |
|---|---:|---:|---|
| `magic` | `u32` | 4 | `0x504D4446` (`FDMP`) |
| `version` | `u16` | 2 | `1` |
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
3. `FrameEnd`
4. `Ack`

Правила:

- итоговый `.tar.zst` на receiver side является конкатенацией нескольких
  sender-side zstd-frame-ов плюс локального финального frame-а с tar end blocks
- каждый sender-side zstd-frame закрывается только на границе завершённого файла
- `FrameChunk` передаются как уже сжатые байты текущего zstd-frame
- receiver пишет `FrameChunk` напрямую в `<dump_id>.tar.zst.part` и считает
  `compressed_len` и `compressed_xxh3_64` инкрементально
- `Ack` отправляется только после успешной проверки `FrameEnd`, `fsync` partial
  archive и persist journal
- при checksum/length mismatch receiver отправляет `Error` и откатывает
  partial archive обратно к предыдущему committed `archive_len`
- при разрыве соединения до `Ack` sender считает frame незакоммиченным,
  а receiver не продвигает committed state

## Finish Semantics

`Finish` закрывает dump по последней уже committed boundary, а не по "частично
отправленному" состоянию.

Receiver перед `FinishAck`:

- валидирует `final_frame_no`, `final_file_index` и `final_file_path`
  против journal
- дописывает два tar end blocks (`1024` нулевых байта) как отдельный локальный
  zstd-frame
- `fsync`-ает архив
- переименовывает `.tar.zst.part` в финальный `.tar.zst`
- удаляет journal

## Contract Versus Implementation Notes

Стабильной частью v1 contract считаются:

- packet header layout
- integer endianness
- message kinds and body layout
- role model
- порядок `Hello -> HelloAck -> ResumeQuery -> ResumeState -> Frame* -> Ack -> Finish -> FinishAck`
- frame commit semantics через `Ack`
- лимиты размера `FrameChunk` и control messages

Implementation notes, которые важны сейчас, но могут быть переоформлены без
смены wire format:

- точные logging details
- конкретные timeout values в transport bootstrap
- внутренняя организация sender pipeline
- конкретный layout receiver-side journal file
