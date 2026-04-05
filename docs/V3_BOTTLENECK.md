# V3 Bottleneck Plan

Этот документ фиксирует текущее bottleneck-расследование по `v3` sender/receiver
пути с фокусом на `manySmallFiles`.

Цель документа:

- локализовать реальные узкие места в текущем `v3` runtime path;
- отделить подтвержденные факты от гипотез;
- зафиксировать несколько лучших направлений оптимизации;
- оценить ожидаемый выигрыш;
- разложить работу на этапы, которые можно внедрять без потери
  cross-platform совместимости на Windows и Linux.

## 1. Короткий вывод

Текущий bottleneck в `v3` для `manySmallFiles` находится не в `zstd` и не в
количестве compression threads.

Главный тормоз сейчас это manifest-first pipeline:

1. sender строит и seal'ит полный manifest spool до нормального старта archive
   frame loop;
2. sender затем повторно читает manifest spool как replay walker;
3. receiver persist'ит manifest temp через append + `sync_all()` на каждом
   manifest chunk;
4. только после этого first useful `FrameStart` начинает реально двигать
   payload path.

Для tiny-file workload это означает, что metadata/discovery/persist path
доминирует над compression path.

## 2. Что подтверждено измерениями

### 2.1 Focused `manySmallFiles` sweep по threads/frame/buffer

Локальный sweep показал:

- `buffer=64 KiB` стабильно лучше больших read buffers;
- `frame=4 MiB` хуже из-за роста количества frames и overhead;
- `threads=1..8` почти не меняют результат;
- значит главный рычаг сейчас не compression threads.

Лучшие single-run строки из focused sweep:

| threads | frame MiB | buffer KiB | total ms | total MiB/s |
| --- | ---: | ---: | ---: | ---: |
| 1 | 16 | 64 | 2713.62 | 32.39 |
| 8 | 64 | 64 | 2714.11 | 32.38 |
| 2 | 16 | 64 | 2754.31 | 31.91 |
| 4 | 16 | 64 | 2755.69 | 31.89 |

Практический смысл:

- рост threads до `8` не даёт убедимого выигрыша;
- `manySmallFiles` не упирается в zstd worker scaling;
- узкое место лежит раньше compression stage.

### 2.2 Stage breakdown по лучшей строке

Для строки `threads=1`, `frame=16 MiB`, `buffer=64 KiB`,
`manifest_chunk_kb=512`, `manifest_ack_records=65536`,
`manifest_ack_kb=8192`:

| metric | value |
| --- | ---: |
| total duration ms | 2713.62 |
| read duration ms | 1297.45 |
| compress duration ms | 218.28 |
| send duration ms | 1592.12 |
| ack wait ms | 64.47 |
| manifest build duration ms | 999.12 |
| manifest transfer duration ms | 960.55 |
| manifest spool read duration ms | 370.83 |
| time to first frame ms | 1104.63 |

Из этого видно:

- `compress_duration_ms` слишком мал, чтобы быть главным bottleneck;
- `ack_wait_ms` невелик и не объясняет общую стоимость;
- `manifest_build_ms` сам по себе съедает примерно треть total time;
- `time_to_first_frame_ms` больше одной секунды, что для tiny-file workload
  означает явный startup barrier перед payload path.

### 2.3 Focused sweep по `manifest_chunk_kb`

Более устойчивый mini-sweep с `repeat=3` на одном и том же `manySmallFiles`
profile:

- `threads=1`
- `frame=16 MiB`
- `buffer=64 KiB`
- `manifest_ack_records=65536`
- `manifest_ack_kb=8192`

Средние результаты:

| manifest chunk KiB | repeats | total ms | total MiB/s | manifest build ms | manifest transfer ms | time to first frame ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 3 | 3354.75 | 26.21 | 1202.25 | 1174.59 | 1341.16 |
| 512 | 3 | 3355.54 | 26.28 | 1144.13 | 1170.37 | 1269.81 |
| 768 | 3 | 3483.14 | 25.26 | 1259.13 | 1196.31 | 1380.02 |
| 896 | 3 | 3185.29 | 27.75 | 1131.20 | 1338.06 | 1247.78 |

Практический смысл:

- manifest chunking действительно влияет на throughput;
- текущий `512 KiB` не выглядит лучшим для `manySmallFiles`;
- `896 KiB` сейчас лучший из проверенных вариантов на `repeat=3`;
- но это всё ещё симптоматическая настройка, а не устранение root cause.

### 2.4 Есть баг в реальном chunk ceiling

CLI разрешает `manifest_chunk_kb=1024`, потому что
`MAX_MANIFEST_CHUNK_BYTES = 1024 * 1024`.

Но `ManifestChunk` body несёт служебные поля поверх payload, поэтому
`1024 KiB` payload реально не помещается в packet body и падает на runtime
валидации oversized body.

Следствие:

- текущий hard cap в CLI/validation не совпадает с реальным wire cap;
- часть пространства тюнинга сейчас формально разрешена, но practically
  broken;
- это нужно исправить даже если не трогать архитектуру.

## 3. Где именно узкое место в коде

### 3.1 Sender сначала делает полный sealed manifest

Горячий участок:

- [src/sender/source.rs:5367](D:/rep/cool_project/fast-dump/src/sender/source.rs#L5367)
- [src/sender/source.rs:5394](D:/rep/cool_project/fast-dump/src/sender/source.rs#L5394)
- [src/sender/source.rs:5432](D:/rep/cool_project/fast-dump/src/sender/source.rs#L5432)
- [src/sender/source.rs:5483](D:/rep/cool_project/fast-dump/src/sender/source.rs#L5483)

Текущее поведение:

1. `ensure_sealed_manifest_state()` делает полный `build_to_path_with_limit(...)`
2. затем читает log обратно целиком
3. затем строит sparse checkpoints вторым проходом
4. затем `PendingFileWalker::new_with_manifest(...)` начинает replay path

Итог:

- есть минимум два прохода по manifest path до полноценного payload streaming;
- `first_frame_started` наступает поздно;
- tiny-file workload сильно штрафуется за upfront metadata barrier.

### 3.2 Sender потом ещё раз читает manifest spool

Горячий участок:

- [src/sender/source.rs:2597](D:/rep/cool_project/fast-dump/src/sender/source.rs#L2597)

Replay walker читает spool record-by-record и заново восстанавливает путь до
следующего work item.

Итог:

- у нас не только build cost, но и повторный sequential read cost;
- на `manySmallFiles` это чистый metadata-heavy overhead.

### 3.3 Session loop ждёт durable manifest boundary перед frame window

Горячий участок:

- [src/sender/session.rs:2271](D:/rep/cool_project/fast-dump/src/sender/session.rs#L2271)
- [src/sender/session.rs:2364](D:/rep/cool_project/fast-dump/src/sender/session.rs#L2364)
- [src/sender/session.rs:1753](D:/rep/cool_project/fast-dump/src/sender/session.rs#L1753)
- [src/sender/session.rs:2516](D:/rep/cool_project/fast-dump/src/sender/session.rs#L2516)

Формально `v3` допускает overlap между manifest transfer и archive frames.
Но для `manySmallFiles` текущая реализация ведёт себя как manifest-first
pipeline с поздним `time_to_first_frame`.

Итог:

- overlap слишком слабый;
- payload path стартует позже, чем должен;
- threads и zstd тюнинг начинают иметь значение слишком поздно.

### 3.4 Receiver sync'ает manifest temp file слишком часто

Горячий участок:

- [src/receiver/journal.rs:408](D:/rep/cool_project/fast-dump/src/receiver/journal.rs#L408)
- [src/receiver/journal.rs:436](D:/rep/cool_project/fast-dump/src/receiver/journal.rs#L436)
- [src/receiver/journal.rs:447](D:/rep/cool_project/fast-dump/src/receiver/journal.rs#L447)
- [src/receiver/shared.rs:178](D:/rep/cool_project/fast-dump/src/receiver/shared.rs#L178)
- [src/receiver/shared.rs:226](D:/rep/cool_project/fast-dump/src/receiver/shared.rs#L226)

Текущее поведение:

- append chunk в temp file;
- `sync_all()` после append;
- journal persist после каждого commit boundary.

Итог:

- receiver durable path платится слишком часто;
- особенно болезненно это выглядит на Windows;
- manifest chunk sizing начинает компенсировать частоту sync instead of solving
  architecture.

## 4. Root cause

Root cause не один, а составной:

1. `v3` path пока слишком eager в построении полного durable manifest до
   активного payload streaming;
2. sender делает лишний reread/replay manifest spool;
3. receiver durable policy слишком granular для manifest temp path;
4. tuning knobs частично скрывают проблему, но не убирают её.

Именно поэтому `threads=8` не дают заметного выигрыша:

- threads улучшают не ту стадию;
- самый дорогой участок находится до полезной компрессии и полезного frame IO.

## 5. Лучшие решения

Ниже решения отсортированы по соотношению:

- ожидаемый выигрыш;
- сложность внедрения;
- риск сломать correctness/resume semantics;
- пригодность для Windows и Linux.

### Решение A. Receiver manifest group-commit

Идея:

- держать manifest temp file открытым в течение transfer window;
- append chunk'и без `sync_all()` на каждый кусок;
- делать durable sync только перед `ManifestAck` и `ManifestSeal`;
- journal boundary продвигать только на durable ack boundary.

Почему это хорошее решение:

- сохраняет текущую durable семантику;
- резко уменьшает количество дорогих fsync/sync_all;
- одинаково применимо на Windows и Linux;
- не требует сначала ломать весь sender pipeline.

Ожидаемый выигрыш:

- `manySmallFiles`: `8-20%`
- Windows: обычно ближе к верхней границе
- Linux: обычно `5-12%`

Риск:

- средний, но хорошо локализованный;
- нужен аккуратный rollback temp file на ack boundary и на error path.

### Решение B. Sender single-pass manifest build

Идея:

- во время build сразу считать sparse checkpoints;
- не читать spool обратно вторым полным проходом только ради checkpoint build;
- сразу собирать финальный build report и checkpoint state.

Почему это хорошее решение:

- устраняет чистый лишний I/O;
- не меняет wire protocol;
- не меняет correctness model;
- работает одинаково на Windows и Linux.

Ожидаемый выигрыш:

- `manySmallFiles`: `5-12%`

Риск:

- низкий или средний;
- в основном локальный refactor sender manifest builder.

### Решение C. Убрать disk replay для fresh run

Идея:

- после fresh manifest build не перечитывать тот же spool для walker;
- хранить compact leaf index или memory-backed replay cursor;
- disk replay оставить только для reconnect/restart path.

Почему это хорошее решение:

- это прямой удар по tiny-file bottleneck;
- reconnect semantics можно сохранить;
- fresh run и restart run имеют разные perf priorities и не обязаны идти одним
  и тем же медленным путём.

Ожидаемый выигрыш:

- `manySmallFiles`: `8-15%`

Риск:

- средний;
- нужно аккуратно разделить fresh path и restart path.

### Решение D. Настоящий progressive manifest/data overlap

Идея:

- перестать требовать full sealed manifest перед нормальным payload path;
- discovery/build manifest должен кормить durable boundary incrementally;
- frame loop должен ждать только boundary, покрывающий конкретный next work
  ordinal, а не весь upfront manifest barrier.

Почему это хорошее решение:

- это root-fix архитектурной природы;
- именно это превращает `v3` из manifest-first в genuinely pipelined path;
- это самый большой потенциальный win для `manySmallFiles`.

Ожидаемый выигрыш:

- `manySmallFiles`: `20-35%`
- при удачном выполнении поверх A/B/C суммарный выигрыш может быть выше

Риск:

- высокий;
- это уже не просто tuning, а перепрошивка sender flow.

### Решение E. Small-file fast lane

Идея:

- для tiny regular files объединить discovery metadata и file scheduling;
- уменьшить число повторных `open/read` и path reconstruction шагов;
- по возможности строить compact file descriptors/prefix structures один раз.

Почему это хорошее решение:

- tiny-file workload выигрывает именно от снижения per-file overhead;
- это совместимо с cross-platform целью, если abstraction сделать над
  платформенными открывалками.

Ожидаемый выигрыш:

- `manySmallFiles`: `10-25%`

Риск:

- высокий;
- легко ухудшить code complexity;
- нужно не потерять best-effort live-file semantics.

### Решение F. Починить manifest chunk ceiling и defaults

Идея:

- синхронизировать CLI/runtime cap с реальным packet body ceiling;
- не разрешать заведомо невалидный `1024 KiB` payload;
- поднять safe practical default ближе к верхней границе, но внутри реального
  wire budget.

Почему это хорошее решение:

- это дешёвый и правильный cleanup;
- улучшает predictability tuning;
- убирает фальшивую зону конфигурации.

Ожидаемый выигрыш:

- прямой throughput win сам по себе небольшой: `1-5%`
- но это важный prerequisite для нормального tuning.

Риск:

- низкий.

## 6. Что делать в первую очередь

Если цель именно blazing speed на `manySmallFiles`, а не минимальный diff, то
лучший порядок такой:

1. Решение A: receiver manifest group-commit
2. Решение B: sender single-pass manifest build
3. Решение C: убрать disk replay на fresh run
4. Решение F: починить effective chunk ceiling и defaults
5. Решение D: progressive overlap
6. Решение E: small-file fast lane

Почему именно так:

- A/B/C дают высокий выигрыш за умеренный engineering risk;
- D даёт самый большой upside, но его лучше делать после того, как убраны
  самые очевидные лишние sync и reread;
- E лучше делать уже после стабилизации manifest pipeline, иначе будет трудно
  понять, какой выигрыш дал именно small-file fast lane.

## 7. Ожидаемый совокупный выигрыш

Это не сумма процентов, а реалистичный engineering estimate для
`manySmallFiles`.

### Консервативный сценарий

- A + B + F
- ожидаемый итог: `10-20%`

### Рабочий сценарий

- A + B + C + F
- ожидаемый итог: `20-35%`

### Агрессивный сценарий

- A + B + C + D
- ожидаемый итог: `30-50%`

### Максимальный сценарий

- A + B + C + D + E
- ожидаемый итог: `40-70%`

Последний диапазон уже очень зависит от качества реализации и от того,
насколько удастся реально сделать `v3` не manifest-first, а pipeline-first.

## 8. Cross-platform ограничения

Все решения должны держать следующие ограничения:

- Windows и Linux одинаково поддерживаются;
- correctness важнее speed;
- resume semantics не деградируют;
- durable manifest boundary не может врать;
- ordinary reconnect не должен скатываться обратно в полный live rescan.

Особые замечания:

- Windows сильнее чувствителен к частым `sync_all()` и temp file churn;
- Linux сильнее выигрывает от хорошего overlap и снижения syscall volume;
- sender-side fast path нельзя делать через platform-specific hack, который
  ломает parity между Windows и Linux.

## 9. Рекомендуемый пошаговый план внедрения

### Phase 1. Low-risk throughput cleanup

Сделать:

- receiver manifest group-commit;
- реальный safe ceiling для `ManifestChunk`;
- benchmark matrix для `manifest_chunk_kb` рядом с ceiling.

Критерий успеха:

- `manySmallFiles` улучшается минимум на `8-12%`;
- все tests/clippy зелёные;
- no regression по resume и manifest durability.

### Phase 2. Sender лишние проходы

Сделать:

- single-pass build + sparse checkpoints;
- убрать full reread после build;
- сохранить restart/recovery semantics.

Критерий успеха:

- `manifest_build_duration_ms` заметно падает;
- `time_to_first_frame_ms` падает минимум на `10-15%`.

### Phase 3. Fresh-run fast path

Сделать:

- fresh run без disk replay того же manifest spool;
- reconnect/restart оставить на durable spool/checkpoint path.

Критерий успеха:

- `manifest_spool_read_duration_ms` на fresh run почти исчезает;
- `manySmallFiles` даёт ещё один заметный шаг вниз по total time.

### Phase 4. Real overlap

Сделать:

- incremental manifest build feeding frame loop;
- durable boundary gating только на нужный `work_ordinal`;
- убрать фактический manifest-first startup barrier.

Критерий успеха:

- `time_to_first_frame_ms` перестаёт быть около секунды;
- `overlap_efficiency_pct` становится реально положительным и стабильным;
- payload path стартует существенно раньше.

### Phase 5. Small-file specialization

Сделать:

- отдельный fast lane для tiny files;
- уменьшить per-file metadata and path reconstruction overhead;
- удержать bounded memory and portable semantics.

Критерий успеха:

- `manySmallFiles` получает финальный большой win;
- `mixed` не деградирует;
- `bigFiles` не страдает от новой сложности fast path.

## 10. Что измерять после каждого этапа

После каждого phase нужен один и тот же минимальный пакет проверки:

1. `cargo test --quiet`
2. `cargo clippy --quiet --all-targets --all-features`
3. `manySmallFiles`:
   - `threads=1`
   - `frame=16 MiB`
   - `buffer=64 KiB`
   - `repeat>=3`
4. сравнение:
   - `total_duration_ms`
   - `total_mib_s`
   - `manifest_build_duration_ms`
   - `manifest_transfer_duration_ms`
   - `manifest_spool_read_duration_ms`
   - `time_to_first_frame_ms`
   - `ack_wait_ms`
5. sanity check на `mixed` и `bigFiles`

Иначе слишком легко "оптимизировать" одну synthetic строку, но не улучшить
реальный pipeline.

## 11. Решение на сейчас

Если нужно выбрать одно направление прямо сейчас, без дальнейшего обсуждения,
то лучший следующий engineering шаг:

1. внедрить receiver manifest group-commit;
2. затем убрать sender-side second pass после manifest build;
3. затем повторить `manySmallFiles` bench matrix.

Это самый прагматичный путь к заметному speed win без немедленного риска
сломать весь `v3` session model.
