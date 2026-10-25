> Historical document. See the [current runtime](../runtime.md) and
> [protocol v4](../../src/network/protocol.md) for the active contract.

# SAFETY Plan for v3

Этот документ фиксирует план по тестам на надежность для текущего `v3` runtime
в `fast-dump`.

Цель документа: дать рабочую программу усиления доверия к корректности,
стабильности и предсказуемости sender/receiver для manifest-first протокола без
смешения этой задачи с профилированием производительности.

Это planning doc, а не runtime contract. Источник истины по текущему поведению:
код, `README.md`, `AGENTS.md` и `src/network/protocol.md`.

## 1. Что именно нужно защищать в текущем v3

Текущий mainline runtime уже целиком работает по `v3` и больше не опирается на
path-based resume как на главный источник прогресса.

Критичные для надежности свойства теперь такие:

- transport establishment и handshake корректно валидируют роли, `dump_id`,
  `token` и пару `push`/`pull`;
- resume строится из двух независимых durable boundaries:
  - manifest boundary:
    `manifest_bytes_persisted`, `last_record_seq`, `last_work_ordinal`,
    `sealed`;
  - archive boundary:
    `last_committed_frame`, `last_committed_work_ordinal`, `archive_len`;
- sender может начинать архивные кадры только в пределах durably acked
  manifest boundary;
- archive progress определяется `work_ordinal`, а не путем файла и не
  `file_index`;
- receiver коммитит кадр только после проверки длины, checksum, `fsync` и
  обновления journal;
- после disconnect может быть потерян максимум один unacked frame;
- typed issue tail передается отдельно через `IssueChunk` и не изменяет resume
  state;
- `Finish` валиден только после sealed manifest и завершенного frame loop;
- финальный `.tar.zst` и `.issues.tsv` должны появляться только после успешной
  финализации.

Ни один одиночный roundtrip test не доказывает все эти свойства сразу.
Надежность в `v3` должна подтверждаться набором инвариантов на разных слоях.

## 2. Что надежность означает для проекта в v3

Для текущего `v3` надежность состоит минимум из шести независимых гарантий:

1. Корректный transport bootstrap для `push` и `pull`.
2. Корректный manifest-first wire protocol и строгая валидация ordering rules.
3. Корректная фиксация durable manifest boundary и recovery после restart или
   reconnect.
4. Корректная фиксация committed archive frame и rollback к последнему `Ack`.
5. Корректная финализация архива, issue tail и cleanup journal/temp state.
6. Корректное содержимое итогового `.tar.zst` с учетом live-file best-effort
   semantics, terminal skips и deterministic traversal.

## 3. Что уже есть в репозитории

Текущая база для safety в `v3` уже не нулевая.

- `tests/connectivity/transport.rs`
  Проверяет establishment, role mismatch, `dump_id` mismatch, `token` и
  `max_sessions`.
- `tests/connectivity/protocol_flow.rs`
  Проверяет invalid message order для `v3`, typed issue tail и часть
  negative-path поведения.
- `tests/connectivity/resume.rs`
  Проверяет resume по committed `work_ordinal`, journal state и quota/error
  paths вокруг manifest boundary.
- `tests/connectivity/roundtrip_fast.rs`
  Проверяет бинарный push/pull roundtrip, boundary fixture и issue journal для
  terminal skip.
- `tests/connectivity/observability.rs`
  Проверяет aggregate diagnostics и метрики overlap/durable boundaries.
- `tests/connectivity/support.rs`
  Содержит harness, binary helpers и archive/hash utilities.

С точки зрения backlog это означает:

- закрыты `safety-01`..`safety-04`:
  - стабилизация harness;
  - failure matrix;
  - protocol negative coverage;
  - receiver state integrity coverage;
- открыты `safety-05`..`safety-10`:
  - archive invariants beyond payload hashes;
  - live-file retry/best-effort coverage;
  - расширенная binary CLI matrix;
  - явное tiering reliability suite;
  - настоящий soak mode;
  - финальный safety definition of done.

## 4. Что все еще уязвимо

Несмотря на уже существующее `v3` покрытие, остаются реальные зоны риска:

- часть readiness в harness все еще использует polling с `sleep`, а не чистое
  event-based подтверждение готовности;
- archive correctness в основном подтверждается через распаковку и payload
  hashes, но не закрывает системно tar-level invariants;
- live-file best-effort semantics пока не покрыты как полный fault matrix;
- binary matrix еще не эквивалентна полному поддерживаемому CLI contract;
- репозиторий пока не дает четкого разделения между быстрым PR gate,
  расширенным reliability suite и soak mode;
- текущий scheduled stress workflow нельзя считать завершенным soak solution,
  потому что он еще не соответствует требованиям этого документа как отдельный,
  диагностируемый и воспроизводимый режим.

## 5. Главные риски ложной уверенности именно в v3

### 5.1 Happy-path roundtrip не доказывает manifest correctness

Успешный loopback roundtrip еще не доказывает, что:

- sender не выйдет за пределы durably acked manifest boundary;
- receiver корректно переживет потерю части manifest tail;
- `ManifestSeal` и финальный `ManifestAck` не допускают скрытого рассинхрона;
- reconnect не возродит уже отброшенный manifest suffix.

### 5.2 Архив может выглядеть правильным по payload hash и все равно нарушать контракт

Даже если распакованные байты совпали, тест все еще может пропустить:

- не-нормализованные tar paths;
- недетерминированный порядок записей;
- лишние entries, которые не должны попадать в архив;
- отсутствие tar end blocks после финализации;
- неправильное поведение `.issues.tsv`.

### 5.3 Typed issue tail легко недооценить

`IssueChunk` в `v3` больше не просто декоративный хвост. Нужно проверять, что:

- он допускается только после sealed manifest и после frame loop;
- он не меняет manifest/archive durable boundaries;
- partial `.issues.tsv.part` безопасно удаляется или перезапускается;
- финальный `.issues.tsv` создается только после успешного `FinishAck`.

### 5.4 Старые path-based ментальные модели теперь опасны

В `v3` нельзя оценивать корректность через `last_committed_file_path` или
`file_index` как primary boundary. Проверки должны быть завязаны на:

- `record_seq` для manifest progress;
- `work_ordinal` для archive progress;
- `archive_len` как единственную authoritative archive boundary на диске.

## 6. Целевая структура v3 safety tests

### 6.1 Layer A: transport and protocol invariants

Назначение:

- быстро ловить нарушения wire contract;
- проверять deterministic protocol failures без heavy roundtrip.

Минимальные сценарии:

- reject invalid role pairing;
- reject `dump_id` mismatch after handshake;
- reject bad `token`;
- reject `FrameStart` до `ManifestStart`;
- reject archive frame beyond durably acked manifest boundary;
- reject `IssueChunk` до `ManifestSeal`;
- reject `Finish` до корректного завершения frame loop;
- reject invalid message after typed issue tail;
- reject invalid body lengths и unknown packet kinds.

Текущий статус:

- core negative cases уже есть;
- missing work теперь не в базовой protocol safety, а в доведении полного
  failure matrix до закрытия всех незакрытых дыр.

### 6.2 Layer B: durable state integrity on receiver

Назначение:

- доказать, что состояние на диске всегда объяснимо через journal и durable
  boundaries;
- доказать, что receiver никогда не коммитит непроверенный frame или issue
  state.

Минимальные сценарии:

- rollback при checksum mismatch;
- rollback при bad compressed length;
- discard unacked frame после disconnect;
- сохранение committed archive boundary при ошибке manifest persistence;
- loud failure или безопасный truncate при несогласованном `.part`/journal;
- cleanup journal только после successful finalize-and-rename;
- cleanup stale `.issues.tsv.part` и `.manifest.part` без порчи committed state.

Текущий статус:

- базовый journal/rollback контур уже закрыт;
- дополнительные archive-level и temp-file assertions еще нужно добрать.

### 6.3 Layer C: sender-side live-file and best-effort behavior

Назначение:

- проверить current best-effort contract на реально нестабильных файлах;
- не допустить скрытого нарушения archive rules и progress semantics.

Минимальные сценарии:

- file grows after `open`, но sender читает ровно `size_at_open`;
- file shrinks mid-read и корректно retry-ится с начала;
- open failure или share violation классифицируется как `open_failed`;
- отправка после 1-2 retry отражается как `best_effort`, а не `clean`;
- exhausted retry budget дает terminal skip без endless retry across reconnects;
- terminal skip корректно продвигает progress через `work_ordinal` gap и
  правильно отражается в issue tail.

Текущий статус:

- часть warning/terminal skip coverage уже есть;
- системный набор именно по live-file semantics все еще отсутствует.

### 6.4 Layer D: binary end-to-end CLI matrix

Назначение:

- проверять реальный CLI contract, transport bootstrap и финальный архив;
- ловить расхождения между sender, receiver, protocol glue и docs.

Обязательная матрица:

- `push`: `sender --connect`, `receiver --listen`;
- `pull`: `sender --listen`, `receiver --connect`;
- с `token`;
- без `token`;
- с явным `dump-id`;
- без `dump-id`, с проверкой fallback;
- clean run;
- completed-with-warnings run;
- boundary fixture;
- mixed fixture.

Текущий статус:

- push/pull и warning journal уже частично закрыты;
- matrix как целое еще не замкнута.

### 6.5 Layer E: observability and diagnostics

Назначение:

- убедиться, что safety-critical diagnostics не ломаются молча;
- оставить инженеру достаточно данных для triage.

Минимальные сценарии:

- debug logs включаются и выключаются ожидаемо;
- sender stdout сохраняет aggregate-only contract;
- observability отражает overlap manifest/archive и durable boundaries;
- error path сообщает, какая именно readiness или protocol expectation
  нарушилась.

### 6.6 Layer F: soak and repeated stress

Назначение:

- искать редкие race conditions, flaky failures и cross-test contamination,
  которые не видны по одиночному прогону.

Требования к режиму:

- отдельный entrypoint или script;
- configurable repeat count;
- логирование iteration number и scenario label;
- сохранение артефактов и stdout/stderr при падении;
- возможность nightly или scheduled CI запуска.

Текущий статус:

- идея присутствует, полноценный режим еще нет.

## 7. Критичные v3 failure points

Ниже минимальная failure matrix, которую нужно держать явной.

- Handshake role/token mismatch:
  session отвергается до runtime loop, локальное состояние receiver не должно
  появляться или изменяться.
- `ResumeQuery.dump_id` mismatch:
  sender должен получить `Error`, receiver не должен открыть чужое dump state.
- Disconnect до первого durable `ManifestAck`:
  receiver может отбросить unacked manifest suffix, archive boundary остается
  прежней.
- `FrameStart` выше durably acked manifest boundary:
  receiver должен вернуть `Error`, archive state не меняется.
- Disconnect в середине `FrameChunk` серии:
  незавершенный frame не должен стать committed.
- Disconnect после `FrameEnd`, но до `Ack`:
  sender обязан считать frame uncommitted; receiver после restart должен
  вернуться к предыдущему committed `archive_len`.
- Bad checksum или bad compressed length в `FrameEnd`:
  receiver откатывается к последнему committed boundary и не продвигает
  `last_committed_work_ordinal`.
- Invalid `IssueChunk` ordering:
  typed issue temp state не должно влиять на resume boundary.
- Unexpected message after issue tail:
  partial issue temp state отбрасывается безопасно.
- `Finish` с несовместимым final boundary:
  receiver не должен финализировать архив.

## 8. Приоритет remaining work для текущего проекта

С учетом уже закрытых `safety-01`..`safety-04` следующий порядок работ такой:

1. `safety-05`: добрать archive correctness assertions.
2. `safety-06`: закрыть live-file retry, best-effort и skip semantics.
3. `safety-07`: расширить binary CLI matrix до полного поддерживаемого контракта.
4. `safety-08`: разделить быстрый PR gate и расширенный reliability suite.
5. `safety-09`: ввести настоящий soak mode.
6. `safety-10`: зафиксировать final definition of done и doc-sync правила.

Этот порядок лучше соответствует текущему состоянию, чем старый план "сначала
stabilize harness": harness foundation уже сделан, а bottleneck сместился в
archive/live-file/matrix/soak зону.

## 9. Рекомендуемое разделение по времени выполнения

### Tier 1: обязательный PR gate

Должен быть быстрым и стабильным.

Состав:

- ключевые transport tests;
- protocol negative cases для manifest/frame/issue ordering;
- один push binary roundtrip;
- один pull binary roundtrip;
- один resume regression;
- один observability regression.

Назначение:

- ловить типовые регрессии за минимальное время;
- гарантировать, что базовый `v3` session flow не сломан.

### Tier 2: расширенный reliability suite

Состав:

- весь `tests/connectivity`;
- archive correctness cases;
- live-file fault cases;
- journal/temp-state integrity cases.

Назначение:

- запуск перед merge крупных изменений;
- запуск перед релизом;
- запуск на nightly CI.

### Tier 3: soak / stress reliability

Состав:

- многократный запуск избранных критичных сценариев;
- вариации sender runtime knobs:
  `threads`, `read_buffer`, `frame_target`, при необходимости manifest ack
  cadence;
- артефакты для triage flaky failures.

Назначение:

- ловить редкие timing bugs и race conditions;
- валидировать, что harness не создает ложного green.

## 10. Что не надо смешивать с safety tests

Следующие вещи не должны считаться основным критерием reliability:

- средняя скорость в MiB/s;
- CPU usage;
- memory peak;
- time-to-first-frame как самостоятельная цель;
- loopback throughput;
- criterion latency reports.

Это относится к performance investigation, а не к safety.

Если тест в первую очередь измеряет скорость, он не должен считаться
доказательством надежности, даже если попутно проверяет успешный статус.

## 11. Practical repo changes для закрытия плана

### 11.1 Что менять следующим

1. Усилить `tests/connectivity/support.rs` archive helpers:
   проверять tar-level invariants, а не только распакованные hashes.
2. Добрать live-file fault scenarios в `tests/connectivity/roundtrip_fast.rs`
   и/или `tests/connectivity/resume.rs`.
3. Довести `tests/connectivity/roundtrip_fast.rs` до полной поддерживаемой
   binary matrix.
4. Ввести явные entrypoints для Tier 1 и Tier 2.
5. Добавить отдельный soak entrypoint, не смешивая его с обычным
   `cargo test`.

### 11.2 Что менять осторожно

- не ломать stable sender stdout fields;
- не подменять `work_ordinal` старой path-based логикой в тестах и документах;
- не смешивать perf-specific asserts с correctness asserts;
- не делать heavy soak обязательным для каждого PR;
- не строить reliability на timing assumptions конкретной машины.

## 12. Acceptance criteria для завершения safety-плана

План можно считать реализованным, когда выполнены все условия:

- есть явная `v3` failure matrix для transport, manifest, frame, issue tail и
  finish;
- ключевые helper-level race points не завязаны на фиксированные `sleep` как на
  primary synchronization mechanism;
- archive rules покрыты явными assertions, а не только payload hash checks;
- live-file retry, best-effort и terminal skip semantics покрыты
  fault-oriented tests;
- поддерживаемая binary CLI matrix закрыта тестами;
- есть отдельный расширенный reliability suite;
- есть отдельный soak mode;
- `README.md`, `AGENTS.md`, `src/network/protocol.md` и `docs/SAFETY.md`
  синхронны по текущим `v3` semantics;
- repeated runs не показывают флаков на базовой машине разработки и в CI.

## 13. Краткий порядок внедрения отсюда

1. Закрыть archive correctness gaps.
2. Закрыть live-file и best-effort gaps.
3. Замкнуть binary CLI matrix.
4. Выделить Tier 1 и Tier 2 как отдельные, документированные режимы.
5. Добавить настоящий soak mode.
6. Зафиксировать safety definition of done и правило sync всех ключевых docs.

Итоговая идея простая: для текущего `v3` надежность должна доказываться не
одним "успешным дампом", а комбинацией manifest invariants, archive boundary
checks, typed issue-tail rules, fault scenarios и повторяемых регрессионных
прогонов.
