# PERFORMANCE V3 Plan

Этот документ фиксирует план `v3` для `fast-dump` с приоритетом на
максимальную производительность без потери корректности resume и recovery.

Главный продуктовый приоритет `v3`:

- передавать файлы целыми;
- передавать их максимально быстро;
- не сожрать при этом всю RAM;
- не превратить sender-side spool и receiver-side manifest storage в
  безразмерный consumer локального диска.

`v3` не должен быть просто "новым протоколом". Сначала нужна правильная
локальная manifest-модель, затем быстрый execution layer, и только потом
wire-level `v3`.

## 1. Цели

- Убрать фатальную зависимость dump-сессии от повторного live rescan дерева.
- Сохранить или улучшить throughput production send path.
- Не вводить staging copy и двойной I/O как основной сценарий.
- Держать RAM ограниченной даже на миллионах мелких файлов.
- Держать локальный диск под контролем даже при длинных или аварийных сессиях.
- Сохранить текущую best-effort политику для regular files, где истина для
  размера и retry берётся с открытого file handle.
- Иметь быстрый Unix fast path через `openat/dirfd`.
- Иметь корректный fallback executor для остальных платформ.

## 2. Что в `v2` мешает производительности и устойчивости

Сейчас sender:

- лениво обходит живое дерево;
- присваивает `file_index` по факту текущего обхода;
- повторно открывает файлы по абсолютным путям;
- опирается на `last_committed_file_index + last_committed_file_path` как на
  resume boundary;
- после disconnect вынужден снова доверять live rescan.

Следствия:

- исчезновение директории source tree делает ошибку фатальной;
- reconnect и resume зависят от того, что namespace снова выглядит так же;
- `abs_path` мешает дешёвому execution path через `openat/dirfd`;
- directory-level recovery и file-level recovery смешаны;
- throughput и correctness слишком сильно зависят от состояния live source.

## 3. Неподвижные инварианты

Ради производительности `v3` не должен размывать базовые инварианты проекта.

- Sender передаёт только целые file work items.
- Frame boundary остаётся только на границе завершённого file work item.
- Traversal order остаётся deterministic.
- Deterministic order остаётся depth-first с сортировкой внутри директории по
  нормализованному relative path.
- Никакой reorder ради throughput не допускается, если он ломает deterministic
  resume или whole-file semantics.
- Regular file size для tar header по-прежнему определяется по открытому handle,
  а не по discovery-time metadata.

## 4. Принципы `v3`

`v3` не должен строиться на следующих идеях:

- полный manifest как `Vec` в RAM;
- полный `rel_path` на каждый entry как базовый runtime format;
- повторный live rescan для восстановления source truth;
- path-based resume как единственный механизм прогресса;
- protocol redesign до того, как локальная manifest-модель стала понятной;
- fast path только для Unix без fallback story;
- legacy-совместимость любой ценой, если она тормозит новую модель.

`v3` должен строиться на следующих идеях:

- disk-backed append-only manifest spool;
- отдельные понятия `record_seq` и `work_ordinal`;
- компактная запись `depth + name`, а не full path;
- sparse checkpoints для быстрого seek без reread всего manifest;
- жёсткое разделение `discovery truth` и `file-content truth`;
- `openat/dirfd` как preferred execution layer;
- fallback executor с той же manifest-моделью;
- protocol `v3` только после стабилизации локальной manifest-модели.

## 5. `P3`: локальная manifest-модель

`P3` означает новую sender-side модель discovery, storage и execution, но ещё
не обязательную смену wire protocol.

### 5.1 Разделение source-of-truth

Manifest отвечает только за:

- deterministic traversal order;
- восстановление относительного пути;
- stable mapping от discovery к `work_ordinal`;
- фиксацию discovery-time проблем;
- resume без повторного live rescan.

Открытый file handle отвечает только за:

- `size_at_open`;
- tar header sizing;
- retry/skip decisions;
- best-effort classification;
- фактическое содержимое file payload.

Правила:

- manifest не становится источником истины для размера regular file;
- manifest не отменяет текущую live-file policy;
- scan-time metadata не возвращается в runtime как authoritative source для tar.

### 5.2 Канонический discovery log

`v3` не должен оперировать просто "списком файлов". Нужен канонический
discovery log.

Каждая запись имеет:

- `record_seq`
- `kind`
- `depth`
- `name`
- optional payload:
  - `symlink_target`
  - `skip_reason`
  - `subtree_skip_reason`

Канонический набор записей:

- `DirectoryEnter`
- `DirectoryLeave`
- `File`
- `Symlink`
- `DiscoverySkip`
- `SubtreeSkip`

`DirectoryEnter/DirectoryLeave` обязательны, а не optional:

- они восстанавливают path stack;
- фиксируют DFS-порядок;
- делают checkpoint replay однозначным;
- готовят execution layer к `dirfd`-стеку.

### 5.3 `record_seq` и `work_ordinal`

Нужно жёстко развести два счётчика.

`record_seq`:

- растёт на каждой manifest-записи;
- нужен для spool durability и checkpoint replay;
- отражает discovery log целиком.

`work_ordinal`:

- растёт только на sendable leaf work items;
- нужен для frame boundaries, Ack, Finish и file-level reporting;
- не зависит от служебных directory events.

Правила:

- `File` и `Symlink` получают `work_ordinal`;
- `DirectoryEnter`, `DirectoryLeave`, `DiscoverySkip` и `SubtreeSkip` не
  получают `work_ordinal`;
- terminal skip regular file или symlink продвигает committed
  `work_ordinal`, даже если tar payload не появился;
- `SubtreeSkip` является discovery issue, а не file work item.

### 5.4 Sender-side spool storage

Manifest spool должен быть полноценным sender-side persistent state.

Минимальный набор sender-side файлов:

- `manifest.log.part`
- `manifest.ckpt.part`
- `manifest.meta`

`manifest.meta` должен фиксировать:

- `dump_id`
- версию spool format
- source identity
- root source path
- build state
- `last_record_seq`
- `last_work_ordinal`
- `last_checkpoint_seq`
- состояние `clean` или `dirty`
- состояние `sealed` или `unsealed`

`source identity` нужна, чтобы sender не переиспользовал spool от другого
источника.

Минимум identity должен включать:

- нормализованный root path;
- platform-specific identity root directory;
- версию manifest builder.

### 5.5 Durability и recovery sender spool

Для spool нужна явная durability-модель.

Требования:

- append в `manifest.log.part` строго последовательный;
- torn tail можно обнаружить и безопасно обрезать;
- checkpoints обновляются атомарно;
- `manifest.meta` не рекламирует progress, который ещё не durably сохранён.

Recovery после sender restart:

- открыть `manifest.meta`;
- проверить `clean/dirty`;
- восстановить последний валидный checkpoint;
- обрезать `manifest.log.part` до последней валидной записи.

Критически важное правило:

- если spool был `unsealed`, продолжать его build после рестарта можно только
  для pinned/snapshotted source;
- если source является обычным live path source, `unsealed` spool нельзя тихо
  доращивать;
- такой spool инвалидируется и строится заново;
- событие invalidation обязательно попадает в sender discovery log;
- при успешном завершении новой сессии это событие отправляется receiver-у в
  итоговый discovery journal.

Cleanup policy:

- после успешного `FinishAck` spool удаляется;
- после фатального отказа spool может быть сохранён до следующего запуска для
  диагностики или recovery decision;
- нужна GC policy по возрасту и размеру sender-side state dir.

### 5.6 Integrity framing для spool

Spool recovery не должен опираться только на "файл читается".

Нужно сразу закладывать:

- per-record length prefix;
- per-record checksum или CRC;
- checkpoint checksum;
- возможность обнаружить corruption не только в torn tail, но и в середине
  spool file.

Если integrity check провалился:

- spool инвалидируется;
- событие попадает в sender discovery log;
- при успешном завершении новой сессии это событие отправляется receiver-у в
  итоговый discovery journal.

### 5.7 Quota, backpressure и pipeline

Spool storage может стать новым bottleneck, если не ограничить его поведение.

Нужно зафиксировать:

- максимальный размер sender-side spool;
- поведение при исчерпании quota;
- метрики по bytes written/read;
- backpressure между discovery и frame builder.

Эта quota-модель не должна оставаться sender-only. Симметричные disk guardrails и
backpressure для receiver-side manifest storage фиксируются отдельно в `8.4`.

Обязательный pipeline-инвариант:

- discovery, spool append, manifest upload и archive transfer должны уметь
  перекрываться;
- sender не должен ждать полного `ManifestSeal`, чтобы начать data transfer;
- sender может начинать frame production только по тому manifest prefix, который
  уже durably записан локально;
- при protocol `v3` sender может идти только до той manifest boundary, которую
  receiver уже durably подтвердил.

### 5.8 Checkpoints

Чтобы manifest на диске не был "медленным", нужны sparse checkpoints.

Каждый checkpoint фиксирует:

- `record_seq`
- `work_ordinal`
- смещение в `manifest.log.part`
- snapshot path stack
- snapshot execution state, достаточный для быстрого replay

Checkpoint stride задаётся явно, например `4096` или `16384` записей.

Это позволяет:

- быстро перейти к месту resume через `seek`;
- переигрывать только небольшой хвост после checkpoint;
- не перечитывать весь manifest после reconnect;
- держать replay latency предсказуемой.

### 5.9 Memory envelope и path materialization

Наивный manifest в памяти опасен: миллионы entry с `PathBuf` и `String` быстро
уходят в гигабайты.

Disk-backed manifest должен держать в RAM только:

- текущий DFS stack;
- небольшой chunk buffer writer/reader;
- sparse checkpoint index;
- ограниченный execution cache;
- небольшое prefetch window.

Отдельный performance-инвариант:

- hot path не должен материализовывать новый full relative path `String` для
  каждого file work item;
- нужен reusable path buffer, segment arena или эквивалентный механизм;
- path materialization должна быть амортизированной, а не allocation-heavy.

### 5.10 Subtree skip и root failure

Нужно жёстко разделить:

- `root source failure`
- `subtree discovery failure`

Правила:

- потеря root source до фиксации достаточного manifest state остаётся фатальной;
- `SubtreeSkip` не получает `work_ordinal`;
- `SubtreeSkip` попадает в отдельный discovery issue log;
- subtree-level issues влияют на session status, но не ломают file-level
  ordinal progress.

Каноническая grammar для directory failure:

- если directory entry принят в DFS и для него выписан `DirectoryEnter`, то
  закрытие subtree всегда оформляется парой `DirectoryEnter ... DirectoryLeave`;
- если ошибка произошла уже после `DirectoryEnter`, но до полного обхода subtree,
  canonical sequence выглядит как `DirectoryEnter -> SubtreeSkip -> DirectoryLeave`;
- `SubtreeSkip` без соответствующего открытого `DirectoryEnter` не допускается;
- если entry не удалось даже корректно классифицировать как проходимую директорию
  на уровне parent directory, это не `SubtreeSkip`, а `DiscoverySkip`.

Discovery issue log должен уметь хранить не только проблемы source tree, но и
события recovery path:

- `manifest_spool_invalidated`
- `manifest_spool_corrupted`
- `manifest_spool_rebuilt`

### 5.11 Discovery-time file type policy

`v3` не должен терять текущую file-type policy ради новой manifest-модели.

Правила:

- Linux filesystem-type lookup остаётся частью discovery state и по-прежнему
  должен использовать cached `/proc/self/mountinfo`, а не повторный expensive
  probe на каждый entry;
- virtual filesystems (`procfs`, `sysfs`, `cgroup2`, `debugfs`, `tracefs`) не
  превращаются в file work items, а фиксируются как `DiscoverySkip` с fs-type
  reason;
- sockets, devices, FIFOs и другие unsupported special entries фиксируются как
  kind-specific `DiscoverySkip`;
- symlink получает `work_ordinal` только если его `link target` удалось
  корректно прочитать и зафиксировать в manifest;
- unreadable symlink target во время discovery не должен создавать leaf work
  item и фиксируется как `DiscoverySkip` с symlink-specific reason.

## 6. Execution layer

### 6.1 Unix fast path: `openat/dirfd`

`openat/dirfd` не заменяет `P3`, а делает его быстрым на Unix-подобных
системах.

После перехода на manifest sender исполняет entries не через абсолютные пути,
а через parent directory handle и leaf name.

Нужные операции:

- `openat` для regular files;
- `readlinkat` для symlink;
- `fstatat` или metadata с уже открытого handle;
- directory stack по глубине manifest.

Плюсы:

- меньше повторного path resolution;
- меньше строковой работы и аллокаций;
- лучше locality на deep tiny-file workloads;
- меньше race на rename/unlink между discovery и execution;
- естественная интеграция с `depth + name`.

### 6.2 Cross-platform fallback executor

`v3` не должен стать "Unix-only by accident".

Для платформ без `openat/dirfd` нужен fallback executor с той же manifest-моделью.

Fallback executor должен:

- использовать тот же spool format;
- использовать те же `record_seq` и `work_ordinal`;
- восстанавливать full path лениво из path stack;
- не требовать хранения полного набора `abs_path` в памяти;
- использовать ту же reusable path materialization strategy;
- оставаться корректным даже если он медленнее Unix fast path.

Fallback executor не должен становиться скрытым legacy path. Для него нужны те
же benchmark-ы и telemetry, что и для Unix fast path.

### 6.3 FD budget

`openat/dirfd` полезен только при жёстком управлении файловыми дескрипторами.

Нужна bounded policy:

- stack открытых каталогов только по текущему пути;
- небольшой LRU cache для горячих parent directories;
- отдельный лимит для file handles reader pool;
- телеметрия по FD pressure;
- стратегия eviction при росте FD usage.

### 6.4 Source adapter boundary

Даже при наличии fallback executor полезно держать отдельную границу
`source backend`.

Нужно оставить место для fast path:

- pinned source
- snapshotted source
- обычный path source

Это не отменяет `P3`, а даёт возможность ускорять mount-backed источники без
переписывания manifest-модели.

## 7. Reporting и session semantics

`v3` меняет не только storage model, но и то, как считаются progress и issues.

Нужно явно разделить:

- file terminal issues
- discovery issues

File terminal issues:

- относятся к `work_ordinal`;
- участвуют в file-level skip/best-effort counters;
- могут продвигать committed progress без tar payload.

Discovery issues:

- относятся к `record_seq`, а не к `work_ordinal`;
- не должны насильно сводиться к `files_skipped_*`, если размер subtree
  неизвестен;
- хранятся отдельно от file terminal issues на sender-side;
- влияют на session status как warning.

Дополнительные правила:

- sender-side события invalidation/corruption/rebuild spool входят в discovery
  issue journal;
- receiver при успешном завершении dump сохраняет file terminal issues и
  discovery issues в единый typed итоговый issue journal с явным `scope`, чтобы
  не плодить параллельные финальные форматы.

Legacy-совместимость `v2` не является ограничением для `v3`.

`v3` может вводить новые агрегаты:

- `subtrees_skipped`
- `discovery_issues_total`
- `manifest_resets_total`

если это позволяет честно описывать поведение без дорогостоящего rescan только
ради старых счётчиков.

## 8. `P4`: Wire Protocol `v3`

`P4` строится только после того, как `P3` доказал свою пригодность по памяти,
throughput и recovery semantics.

### 8.1 Зачем нужен `P4`

Пока manifest знает только sender, receiver всё ещё зависит от старой
path-based модели resume.

Полноценный `v3` должен сделать manifest частью distributed source truth.

### 8.2 Manifest transfer как commit-поток

`ManifestStart/Chunk/Seal` без commit-семантики недостаточны.

Receiver-side state должен включать минимум:

- `manifest_bytes_persisted`
- `last_manifest_record_seq`
- `last_manifest_work_ordinal`
- `manifest_sealed`

Sender должен получать durable boundary manifest-а.

Допустимые варианты:

- `ManifestAck` после chunk batch;
- `ManifestAck` после explicit barrier;
- commit только после `ManifestSeal`, если bench докажет приемлемую latency.

Предпочтителен batch/barrier model, а не "Ack только на seal".

### 8.3 Runtime tuning для manifest transfer

Размеры и cadence manifest transfer нельзя оставлять "на потом". Они должны
быть сразу вынесены в runtime tuning и прогнаны на bench.

Нужны sender-side параметры:

- `--manifest-chunk-kb`
- `--manifest-ack-records`
- `--manifest-ack-kb`

Смысл:

- не зашивать размер manifest chunk до benchmark-данных;
- не зашивать cadence durable ack до benchmark-данных;
- иметь возможность подобрать баланс между RTT/fsync overhead и latency.

При этом protocol-level safety limits всё равно должны существовать и не
зависеть от CLI.

Стартовые hard caps для `v3`, которые benchmark может только ужесточить или
подтвердить, но не отменить через CLI:

- `ManifestChunk` body не больше `1 MiB`;
- `ManifestAck` и другие control-body для manifest path не больше `64 KiB`;
- total unacked manifest window ограничен фиксированным верхним пределом, чтобы
  tuning knobs не превращались в unbounded RAM/RTT spike;
- sender-side CLI параметры валидируются против этих protocol-level caps и не
  могут их перепрыгнуть.

### 8.4 Receiver-side manifest storage

Receiver тоже должен иметь disk-backed manifest state.

Минимальный набор receiver-side файлов:

- `<dump_id>.manifest.part`
- receiver journal с manifest progress

Recovery rules:

- manifest trailing tail beyond committed boundary отбрасывается;
- sender никогда не считает manifest chunk committed без receiver-side durable
  ack;
- data frame transfer не стартует выше той manifest boundary, которую receiver
  уже durably принял.

Receiver-side disk policy должна быть такой же жёсткой, как sender-side:

- нужен явный quota или upper bound для `<dump_id>.manifest.part` и связанных
  временных issue-tail файлов;
- `ENOSPC` или quota exhaustion на receiver-side не должны приводить к
  полузакоммиченному manifest boundary;
- при исчерпании receiver disk budget sender получает `Error`, а committed
  manifest/archive boundary остаются на последнем durable ack;
- нужны cleanup/GC rules для брошенных `.manifest.part` и `.issues.part`
  артефактов после abort/restart.

Receiver-side manifest storage тоже должен иметь integrity framing:

- checksum/CRC per persisted block или per record group;
- explicit truncation rule до последней валидной boundary;
- явное событие corruption в discovery issue journal.

### 8.5 Новая модель resume state

`ResumeStateV3` должна описывать две независимые границы:

- committed manifest boundary;
- committed archive boundary.

Минимальные поля:

- `last_manifest_record_seq`
- `last_manifest_work_ordinal`
- `manifest_bytes_persisted`
- `last_committed_frame`
- `last_committed_work_ordinal`
- `archive_len`

Прямой перенос `last_file_path` в новую версию не нужен как основная граница.

### 8.6 Frame semantics поверх `work_ordinal`

Data path после manifest transfer использует:

- `first_work_ordinal`
- `last_work_ordinal`

Правила:

- frame закрывается только на завершённом file work item;
- file terminal skip может продвинуть `last_work_ordinal` без tar payload;
- discovery-only records не участвуют во frame boundaries;
- `Finish` валидируется против committed `work_ordinal`, а не против path.

### 8.7 Discovery issues в protocol

`SubtreeSkip` и другие discovery issues нельзя прятать в file issue journal.

Для `v3` фиксируем решение:

- file terminal issues и discovery issues остаются раздельными по семантике;
- wire-level tail использует единый typed issue stream с явным `scope`, вместо
  двух параллельных tail-механизмов;
- receiver не держит весь issue tail в RAM: typed issue chunks пишутся
  инкрементально в `<dump_id>.tar.zst.issues.part`;
- issue tail по-прежнему не влияет на resume boundary, но его partial state
  после reconnect/abort должен быть либо безопасно отброшен, либо начат заново
  без влияния на committed archive/manifest state.

Отдельно нужно передавать discovery-события типа:

- `manifest_spool_invalidated`
- `manifest_spool_corrupted`
- `manifest_spool_rebuilt`

чтобы итоговый receiver-side журнал отражал не только проблемы source tree, но
и проблемы recovery/sender-state.

## 9. Benchmark и observability matrix

Одного throughput baseline для `v3` недостаточно.

Нужно мерить:

- throughput;
- CPU sender;
- CPU receiver;
- RSS sender;
- RSS receiver;
- sender spool bytes written;
- sender spool bytes read;
- receiver manifest bytes persisted;
- time-to-first-frame;
- reconnect-to-resume latency;
- checkpoint replay latency;
- FD usage;
- manifest build latency;
- manifest transfer latency;
- archive transfer latency отдельно от manifest phase;
- path materialization allocations или proxy metric;
- manifest ack cadence;
- manifest fsync/barrier cost;
- receiver manifest fsync/barrier cost;
- issue-tail persistence cost;
- manifest/data overlap efficiency;
- spool invalidation/rebuild frequency.

Нужны datasets минимум для:

- миллионов мелких файлов;
- глубины `9-12`;
- mixed workloads;
- unstable subtree workloads;
- mount-backed источников;
- datasets, где fallback executor сравнивается с Unix fast path.

Отдельно нужны network profiles для bench:

- low-latency LAN;
- умеренный WAN RTT;
- высокий RTT с заметной ценой каждого durable ack;
- bandwidth-limited канал;
- reconnect-prone или lossy сеть.

## 10. Поэтапный roadmap

### Этап 0. Baseline и guardrails

- Зафиксировать benchmark matrix.
- Зафиксировать baseline по throughput, CPU, RSS, FD usage и latency.
- Добавить отдельные метрики для manifest phase.
- Добавить network profiles для ack/fsync-sensitive bench.
- Зафиксировать whole-file invariants и deterministic traversal как
  непереговорные ограничения.

### Этап 1. Local spool format

- Зафиксировать канонический набор manifest records.
- Ввести `record_seq` и `work_ordinal`.
- Определить `source identity`.
- Спроектировать `manifest.log.part`, `manifest.ckpt.part`, `manifest.meta`.
- Спроектировать per-record integrity framing.
- Зафиксировать deterministic DFS ordering rules в manifest builder.
- Зафиксировать canonical grammar для `SubtreeSkip`.
- Перенести file-type policy в discovery log semantics.

### Этап 2. Sender spool durability

- Реализовать append/recovery semantics.
- Реализовать torn-tail detection и truncate.
- Реализовать cleanup и GC policy.
- Добавить quota и backpressure.
- Зафиксировать rule, что `unsealed` spool нельзя доращивать на обычном live
  path source после рестарта.
- Добавить discovery events для invalidate/corruption/rebuild.

### Этап 3. Sender replay и resume

- Сделать checkpointed seek.
- Убрать необходимость reread всего manifest при reconnect.
- Убедиться, что sender restart может переиспользовать spool только в
  разрешённых сценариях.
- Убедиться, что replay не требует allocation-heavy path rebuild на каждый file
  work item.

### Этап 4. Execution layer split

- Реализовать Unix fast path через `openat/dirfd`.
- Реализовать cross-platform fallback executor.
- Зафиксировать общий interface для обоих execution paths.
- Добавить reusable path materialization strategy для fallback executor.

### Этап 5. Reporting и discovery issues

- Отделить file issue journal от discovery issue journal.
- Решить, какие новые aggregate counters нужны.
- Обновить session status semantics для discovery issues.
- Зафиксировать typed итоговый issue journal и export discovery issues в него.

### Этап 6. Receiver-side manifest durability

- Спроектировать receiver-side manifest store.
- Спроектировать journal fields для manifest boundary.
- Зафиксировать recovery rules для partial manifest state.
- Добавить integrity framing и corruption detection.
- Добавить receiver-side quota, ENOSPC behavior и cleanup policy.

### Этап 7. Protocol `v3`

- Спроектировать `ManifestStart/Chunk/Seal`.
- Спроектировать `ManifestAck` или equivalent barrier/ack flow.
- Зафиксировать hard caps для manifest messages и unacked manifest window.
- Зафиксировать pipelined transfer model: manifest prefix transfer перекрывается
  с archive transfer.
- Вывести manifest chunk и ack cadence в sender CLI tuning knobs.
- Перевести frame boundaries и finish validation на `work_ordinal`.
- Спроектировать typed issue tail без full in-memory buffering на receiver-side.
- Синхронизировать `src/network/protocol.rs` и `src/network/protocol.md`.

### Этап 8. Cutover

- Сравнить `v2` и `v3` на одинаковой benchmark matrix.
- Отдельно сравнить Unix fast path и fallback executor.
- Решить, нужен ли dual-stack period.
- Обновить `README.md` и `AGENTS.md`, если внешний контракт поменяется.

## 11. Критерии успеха

`v3` считается удачным, если выполняются все условия:

- RAM sender не растёт линейно от общего числа entry.
- Локальный диск sender и receiver не растёт без ограничений.
- Deep tiny-file workloads не деградируют относительно `v2`, а в целевом
  сценарии ускоряются.
- Resume после reconnect не требует повторного полного live rescan.
- Sender больше не зависит от `abs_path` как от основной execution model.
- Manifest spool читается и пишется последовательно без того, чтобы стать новым
  bottleneck.
- Sender spool и receiver manifest store имеют явную recovery model.
- Receiver-side manifest storage и issue-tail persistence не ломают disk budget.
- Discovery issues и file terminal issues разведены семантически.
- Частично построенный spool не может быть тихо доращён на изменившемся live
  source.
- Manifest transfer и archive transfer реально перекрываются по времени, а не
  превращаются в три последовательные фазы.
- Path materialization не становится allocation bottleneck на tiny-file
  workloads.
- Network-sensitive tuning не деградирует на WAN/RTT-heavy профилях относительно
  заявленной throughput-цели.
- Protocol `v3` появляется только после того, как локальная manifest-модель уже
  доказала практическую жизнеспособность.
- Fallback executor остаётся корректным и не требует отдельной discovery logic.

## 12. Что этот план пока не обещает

- План сам по себе не решает потерю root source до того, как sender успел
  зафиксировать достаточный manifest state или получить pinned/snapshotted
  source.
- План не заменяет snapshot/pin fast path для mount-backed источников.
- План не означает, что `openat/dirfd` автоматически даст выигрыш на всех
  datasets; это должно быть подтверждено benchmark-ами.
- План не обещает, что discovery issues можно честно пересчитать в file-level
  skip counters без отдельной семантики.

## 13. Рекомендуемый порядок принятия решений

1. Сначала утвердить `P3` как disk-backed manifest model с явной durability
   model.
2. Затем утвердить разделение `record_seq` и `work_ordinal`.
3. После этого утвердить execution split: Unix fast path плюс fallback executor.
4. Затем утвердить reporting model для discovery issues.
5. И только после этого фиксировать wire protocol `v3`.
