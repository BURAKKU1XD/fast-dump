# System Audit

Дата аудита: 2026-03-31

## Executive Summary

Проект находится в хорошем инженерном состоянии по архитектуре и тестируемости.
Основной v1 pipeline уже выглядит как цельный продукт, а не как прототип:

- есть единый CLI с двумя подкомандами
- sender и receiver разведены по ролям и слоям
- wire protocol централизован
- resume model реализована через frame commit и journal
- итоговый archive contract подтверждается integration-тестами

Главные слабые места на момент аудита не в core semantics, а в сопровождении:

- quality gates (`fmt`, `clippy`) не зелёные
- одна из ключевых markdown-спецификаций устарела
- в нескольких production-инвариантах остались panic-based failure paths

## Command Summary

```text
cargo test                                      -> PASS
cargo fmt --check                               -> FAIL
cargo clippy --all-targets --all-features ...   -> FAIL
```

## Layer Audit

### 1. CLI and App Layer

Состояние: good

Что хорошо:

- `src/cli/mod.rs` явно моделирует runtime options sender/receiver
- транспортный selector (`--listen` xor `--connect`) проверяется на уровне CLI
- есть тесты на defaults, invalid bounds и push/pull combinations
- `src/app.rs` держит orchestration отдельно от низкоуровневых модулей

Риски:

- публичный surface уже заметно вырос; без явного API policy он может начать расползаться

Оценка:

- слой CLI выглядит достаточно стабильным для v1

### 2. Protocol Codec

Состояние: good, but documentation drift exists

Что хорошо:

- `src/network/protocol.rs` централизует весь codec
- есть размерные лимиты для control bodies и `FrameChunk`
- path normalization и decode-side validation встроены в codec
- покрытие тестами хорошее: roundtrip, magic/version, invalid kind, oversized body, path validation

Риски:

- `src/network/protocol.md` больше не описывает текущее состояние runtime и не может считаться источником истины

Оценка:

- код протокола выглядит надёжнее документации о нём

### 3. Transport Layer

Состояние: good

Что хорошо:

- `src/network/transport.rs` отделяет socket setup от higher-level session logic
- handshake timeout и очистка timeout-параметров после bootstrap оформлены отдельно
- `Session` содержит ровно тот runtime context, который нужен остальному коду
- есть тесты на handshake, bad token, timeout reset, connect/listen behavior

Риски:

- transport уже знает о `dump_id` и роли, поэтому при расширении v2 важно не размыть границу между transport и protocol/session

Оценка:

- слой выглядит аккуратно и достаточно узко сфокусированным

### 4. Sender Pipeline

Состояние: good with maintainability debt

Что хорошо:

- `src/sender/mod.rs` действительно остаётся тонким фасадом
- `source`, `readers`, `frame`, `session` разнесены в соответствии с планом
- production path после последнего рефакторинга ушёл:
  - от полного materialized file map
  - от полного materialized compressed frame
- `PendingFileWalker` и `PendingFrameFiles` делают memory profile заметно лучше
- adaptive readers и bounded buffering изолированы в `src/sender/readers.rs`
- buffered compatibility path (`prepare_dump`) сохранён для тестов и диагностики

Что настораживает:

- `prepare_frame_build` в `src/sender/frame.rs` слишком широк по сигнатуре и уже подсвечен `clippy`
- в streaming path есть `expect` на внутренних инвариантах
- sender теперь имеет две модели работы:
  - compatibility buffered path
  - production streaming path

Это правильно для миграции, но со временем потребует явного maintenance decision:

- либо buffered path остаётся сознательно поддерживаемым test helper
- либо он должен быть дополнительно обёрнут/упрощён, чтобы не дублировать evolving logic

Оценка:

- по архитектуре sender сейчас один из самых сильных участков проекта
- по maintainability ещё есть несколько мест, которые стоит дочистить

### 5. Receiver Pipeline

Состояние: good

Что хорошо:

- `journal`, `resume`, `frame`, `finish`, `session`, `listener` разделены разумно
- `src/receiver/journal.rs` держит state persistence отдельно от runtime loop
- `src/receiver/frame.rs` реализует commit semantics в правильной последовательности:
  - принять chunk bytes
  - проверить `FrameEnd`
  - fsync
  - обновить journal
  - только потом отправить `Ack`
- `src/receiver/finish.rs` закрывает archive отдельно от frame commit, что соответствует модели протокола

Что особенно хорошо:

- тесты receiver закрывают rollback paths, checksum mismatch, journal replacement failure, restart/resume behavior

Риски:

- listener/session слой уже начал накапливать operational detail
- недавний фикс `max_sessions` rejection улучшил детерминизм, но показывает, что transport/listener edge cases надо и дальше держать под тестами

Оценка:

- receiver-side durability model выглядит убедительно для v1

### 6. Observability and Runtime Diagnostics

Состояние: acceptable

Что хорошо:

- есть `src/telemetry.rs` с общей инициализацией логирования
- integration-тесты проверяют debug logging behavior
- в sender и receiver достаточно runtime logging для разборов frame/session flow

Что слабее:

- нет формализованной стратегии метрик, только logs и aggregate sender report
- нет отдельного machine-readable audit/report artifact для receiver-side sessions

Оценка:

- для v1 достаточно
- для production эксплуатации на больших объёмах позже захочется richer telemetry

### 7. Tests, Benches, and Verification Strategy

Состояние: good with one notable gap

Что хорошо:

- unit-тесты широкие и предметные
- integration suite покрывает:
  - push/pull transport
  - handshake
  - frame flow
  - resume after partial frame loss
  - archive validity
  - observability
- в `Cargo.toml` уже заведены criterion benches

Гэп:

- `tests/connectivity/stress.rs` остаётся ignored, то есть самый тяжёлый scenario не входит в регулярный прогон

Оценка:

- функциональное покрытие сильное
- долговременное scaling/perf покрытие пока остаётся ручным

### 8. Dependency and Repo Hygiene

Состояние: needs attention

Что видно:

- `jwalk` заявлен, но не используется
- `cargo fmt --check` красный
- `cargo clippy --all-targets --all-features -- -D warnings` красный

Это не ломает runtime semantics, но ухудшает engineering discipline.

Оценка:

- hygiene отстаёт от уровня остальной архитектуры

## Overall Assessment

Если смотреть именно на продуктовую логику, проект уже выглядит зрелым для v1:

- contract `.tar.zst` соблюдён
- frame-based resume реализован
- sender streaming path существенно улучшен
- receiver commit semantics выглядят корректными
- тестовая сетка действительно полезная, а не номинальная

Если смотреть на инженерное сопровождение, есть понятный короткий backlog:

- довести `fmt` и `clippy` до зелёного состояния
- обновить protocol markdown
- убрать panic-based инварианты из production path
- подчистить dependencies

## Recommended Priority Order

### Priority 1

- починить `cargo fmt --check`
- починить `cargo clippy --all-targets --all-features -- -D warnings`

### Priority 2

- синхронизировать `src/network/protocol.md` с фактическим поведением
- убрать `expect` из production state machine

### Priority 3

- удалить `jwalk`
- решить, как долго будет жить buffered compatibility path sender

### Priority 4

- перевести heavy stress/perf scenarios в регулярно исполняемый pipeline
- при необходимости добавить richer telemetry beyond logs
