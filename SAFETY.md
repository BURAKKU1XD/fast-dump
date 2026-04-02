# SAFETY Plan

Этот документ фиксирует план по тестам на надежность для `fast-dump`.
Цель документа: дать рабочую программу усиления доверия к корректности,
стабильности и предсказуемости sender/receiver без смешения этой задачи с
профилированием производительности.

## 1. Цели

- Подтвердить, что sender и receiver корректно передают и собирают архив во
  всех поддерживаемых транспортных схемах.
- Подтвердить, что resume-модель не приводит к потере уже подтвержденных кадров
  и не оставляет битый архив.
- Подтвердить, что итоговый `.tar.zst` валиден и соответствует фактически
  переданным файлам.
- Подтвердить, что ошибки протокола, обрывы соединения и частичные записи
  обрабатываются детерминированно и без скрытой порчи состояния.
- Снизить риск флаков и ложного чувства надежности из-за тестовой методики.

## 2. Что надежность означает для этого проекта

Для `fast-dump` надежность состоит из пяти независимых гарантий:

1. Корректный transport establishment для `push` и `pull`.
2. Корректный wire protocol и строгая валидация сообщений.
3. Корректная фиксация committed frame и recovery после разрыва.
4. Корректная финализация и восстановимый receiver journal.
5. Корректное содержимое итогового архива и issue journal.

Ни один одинарный roundtrip test не закрывает все пять гарантий сразу. Поэтому
контур надежности должен быть многослойным.

## 3. Что уже есть в репозитории

Текущая база хорошая, но это именно база, а не финальная safety strategy.

- `tests/connectivity/transport.rs`
  Проверяет транспорт, роли и ограничения сессий.
- `tests/connectivity/protocol_flow.rs`
  Проверяет последовательность сообщений и protocol-level ошибки.
- `tests/connectivity/resume.rs`
  Проверяет восстановление после обрыва и поведение журнала.
- `tests/connectivity/roundtrip_fast.rs`
  Проверяет бинарный roundtrip и итоговые хэши архива.
- `tests/connectivity/observability.rs`
  Проверяет пользовательские и диагностические сигналы.
- `tests/connectivity/support.rs`
  Содержит test harness, helpers и fault-style сценарии.

Это покрывает основную функциональность, но оставляет несколько уязвимых зон:

- readiness местами основан на `sleep`, а не на событии готовности;
- нет явного soak-режима для поиска редких гонок;
- не все критические точки обрыва сессии покрыты системно;
- не разделены обязательные CI smoke checks и расширенный reliability suite;
- нет явной матрицы отказов, чтобы видеть, что уже покрыто, а что нет.

## 4. Главные риски ложной уверенности

### 4.1 Флаки из-за таймингов

Тесты, которые используют фиксированные ожидания вместо явных сигналов
готовности, могут:

- случайно падать на медленной машине;
- случайно проходить на быстрой машине, скрывая race;
- создавать иллюзию, что логика надежна, хотя надежен только конкретный timing.

### 4.2 Переоценка надежности из одного happy-path roundtrip

Если один roundtrip на loopback прошел успешно, это еще не доказывает:

- корректность rollback на середине кадра;
- корректность resume после checksum mismatch;
- корректность journal recovery после trailing bytes;
- корректность поведения на нестабильных живых файлах;
- корректность финализации при наличии terminal skips.

### 4.3 Слишком слабая модель ошибок

Если fault scenarios ограничены только "соединение оборвалось", можно пропустить
целый класс ошибок:

- некорректный `FrameEnd`;
- лишние байты после committed archive length;
- невалидный `Ack`;
- недопустимая последовательность packet kinds;
- несовпадение `last_committed_file_path` при resume.

## 5. Принципы safety test strategy

План строится на следующих принципах:

- Тесты надежности должны проверять инварианты, а не только успешный исход.
- Каждый тест должен фиксировать, что именно обязано сохраниться после ошибки.
- Ошибочные сценарии важнее happy path, если они касаются journal и resume.
- CI должен быстро отлавливать регрессии, но полный reliability suite может быть
  тяжелее и запускаться отдельно.
- Бинарные end-to-end проверки нужны, но helper-level tests тоже нужны, потому
  что они позволяют точнее целиться в fault boundaries.

## 6. Целевая структура тестов

### 6.1 Layer A: быстрые protocol/unit-style инварианты

Назначение:
- быстро отлавливать нарушения wire contract;
- проверять детерминированные ошибки без запуска полноценных бинарников.

Сценарии:
- reject unknown/invalid packet kind;
- reject invalid body length;
- reject message out of order;
- reject invalid role pairing;
- reject invalid resume state transitions;
- reject `IssueJournalChunk` вне разрешенного окна;
- reject `Finish` до корректного завершения frame loop.

Статус:
- часть уже есть;
- нужно системно добрать отсутствующие negative cases.

### 6.2 Layer B: receiver state integrity tests

Назначение:
- проверить, что состояние на диске всегда можно объяснить журналом;
- проверить, что receiver никогда не коммитит то, что не подтверждено.

Сценарии:
- trailing bytes beyond `archive_len` успешно отбрасываются, если это допустимый
  незакоммиченный хвост;
- irreconcilable trailing bytes вызывают loud failure;
- checksum mismatch приводит к truncate назад к committed `archive_len`;
- disconnect до `Ack` приводит к discard незавершенного frame;
- finalization удаляет journal только после успешного rename и fsync;
- issue journal записывается только после успешного завершения dump.

### 6.3 Layer C: sender live-file behavior tests

Назначение:
- проверить best-effort semantics на реальных изменяющихся файлах;
- не допустить скрытого нарушения archive rules.

Сценарии:
- файл растет после `open`, но текущая попытка читает ровно `size_at_open`;
- файл укорачивается во время чтения и корректно retry-ится;
- файл не открывается из-за share violation и считается `open failure`;
- после исчерпания retry бюджетов файл уходит в skip и не зацикливает resume;
- skipped file корректно отражается в session report и issue journal.

### 6.4 Layer D: binary end-to-end roundtrips

Назначение:
- проверять реальный CLI, transport establishment и итоговый архив;
- ловить интеграционные расхождения между sender, receiver и protocol glue.

Обязательная матрица:
- `push`: `sender --connect`, `receiver --listen`;
- `pull`: `sender --listen`, `receiver --connect`;
- с `token`;
- без `token`;
- с `dump-id`;
- без `dump-id`, с проверкой fallback;
- с terminal skips;
- с boundary dataset;
- с mixed dataset.

### 6.5 Layer E: reliability soak tests

Назначение:
- искать редкие race conditions и флаки, которые не ловятся одиночным прогоном.

Подход:
- повторять избранный набор критичных интеграционных тестов десятки или сотни
  раз;
- запускать на разных thread configurations sender;
- отдельно запускать на загруженной машине и на "чистой" машине;
- хранить историю flaky failures отдельно от обычного test log.

## 7. План усиления reliability suite

### Этап 1. Стабилизировать harness

Приоритет: самый высокий.

Нужно сделать:

- по возможности заменить фиксированные `sleep` в test helpers на event-based
  readiness;
- там, где `sleep` остается, ограничить его роль только backoff/polling loop, а
  не логическую синхронизацию;
- унифицировать helper, который ждет "receiver готов принимать соединение";
- унифицировать helper, который ждет "архив или journal появился на диске".

Причина:
- без этого нельзя доверять ни green CI, ни редким красным падениям.

Критерий завершения:
- ключевые connectivity tests проходят многоразовый прогон без флаков на одной
  машине.

### Этап 2. Зафиксировать матрицу отказов

Нужно завести явную таблицу сценариев, где для каждого failure point будет
отмечено:

- место отказа;
- ожидаемое поведение sender;
- ожидаемое поведение receiver;
- ожидаемое состояние `.part`, journal и issue journal;
- существующий тест или отсутствие теста.

Failure points, которые должны быть минимум в первой версии:

- disconnect до первого `FrameChunk`;
- disconnect в середине `FrameChunk` серии;
- disconnect после всех `FrameChunk`, но до `FrameEnd`;
- disconnect после `FrameEnd`, но до `Ack`;
- bad checksum в `FrameEnd`;
- bad compressed length в `FrameEnd`;
- resume с trailing bytes;
- resume с несовместимым journal;
- finish после terminal skips;
- invalid protocol order.

Критерий завершения:
- по каждому critical failure point есть либо тест, либо осознанный пробел с
  описанной причиной.

### Этап 3. Усилить archive correctness checks

Нужно проверить не только наличие архива, но и его инварианты:

- tar содержит только regular files;
- пути в tar относительные и нормализованы в `/`;
- порядок файлов детерминирован;
- skipped files отсутствуют в архиве;
- tar end blocks всегда присутствуют после финализации;
- `.tar.zst.issues.tsv` создается только при наличии terminal skips.

Критерий завершения:
- есть явные assertions на archive rules, а не только на хэши полезной
  нагрузки.

### Этап 4. Добавить sender-side fault tests для live files

Нужно системно проверить best-effort политику:

- успешная отправка после одного или двух retry;
- корректная классификация `clean` vs `best_effort`;
- корректный terminal skip после исчерпания retry;
- отсутствие endless retry across reconnects;
- корректное продвижение `last_committed_file_index` через skip gaps.

Критерий завершения:
- sender report и receiver resume согласованы на наборах с нестабильными
  файлами.

### Этап 5. Ввести soak режим

Нужно добавить отдельный запуск reliability suite, который:

- повторяет минимальный набор критичных тестов много раз;
- легко включается локально и в CI по расписанию;
- логирует номер итерации и seed;
- оставляет артефакты при падении.

Минимальный soak-набор:

- push roundtrip boundary fixture;
- pull roundtrip mixed fixture;
- resume after interrupted frame;
- checksum mismatch rollback;
- max-sessions transport gate.

Критерий завершения:
- есть документированный способ запускать soak locally и в CI.

## 8. Рекомендуемое разделение по времени выполнения

### Tier 1: обязательный PR gate

Должен быть быстрым и стабильным.

Состав:
- unit/protocol negative cases;
- ключевые transport tests;
- один push roundtrip;
- один pull roundtrip;
- один resume regression;
- один observability regression.

Назначение:
- ловить типовые регрессии за минимальное время.

### Tier 2: расширенный reliability suite

Состав:
- весь `tests/connectivity`;
- archive correctness cases;
- live-file fault cases;
- journal integrity cases.

Назначение:
- запуск перед merge крупных изменений, перед релизом и на nightly CI.

### Tier 3: soak / stress reliability

Состав:
- многократный запуск избранных критичных сценариев;
- возможный запуск на разных thread/read-buffer/frame-target конфигурациях.

Назначение:
- ловить редкие гонки, timing bugs и cross-test contamination.

## 9. Что не надо смешивать с safety tests

Следующие вещи не должны быть основным критерием reliability:

- средняя скорость в MiB/s;
- CPU usage;
- memory peak;
- loopback throughput;
- criterion latency reports.

Это относится к performance investigation, а не к safety.

Если тест в первую очередь меряет скорость, он не должен считаться доказательством
надежности, даже если попутно проверяет успешный статус.

## 10. Практические изменения в репозитории

### 10.1 Что менять сначала

1. Усилить `tests/connectivity/support.rs`:
   убрать зависимость от грубых `sleep`, где это возможно.
2. Добрать missing negative cases в `tests/connectivity/protocol_flow.rs`.
3. Добрать journal/rollback cases в `tests/connectivity/resume.rs`.
4. Расширить binary roundtrip coverage в `tests/connectivity/roundtrip_fast.rs`.
5. Добавить отдельный soak entrypoint, не смешивая его с обычным `cargo test`.

### 10.2 Что менять осторожно

- не ломать стабильный CLI output;
- не смешивать perf-specific asserts в correctness tests;
- не делать heavy soak обязательным для каждого PR;
- не завязывать надежность на timing assumptions конкретной машины.

## 11. Предлагаемые acceptance criteria

План можно считать реализованным, когда выполнены все условия:

- есть явная failure matrix для critical scenarios;
- ключевые helper-level race points не завязаны на фиксированные `sleep`;
- все правила archive/journal/finalization покрыты assertions;
- есть отдельный расширенный reliability suite;
- есть отдельный soak режим;
- repeated runs не показывают флаков на базовой машине разработки и в CI.

## 12. Краткий порядок внедрения

1. Стабилизировать test harness.
2. Закрыть protocol и receiver-state negative cases.
3. Закрыть live-file и skip/resume edge cases.
4. Усилить binary roundtrip matrix.
5. Добавить soak режим и выделить его в отдельный запуск.

Итоговая идея простая: надежность здесь должна доказываться не одним
"успешным дампом", а комбинацией инвариантов, fault scenarios и повторяемых
регрессионных прогонов.
