> Historical document. See the [current runtime](../runtime.md) and
> [protocol v4](../../src/network/protocol.md) for the active contract.

# Rewrite Performance Plan

Status on 2026-04-14: the rewrite and cutover are already complete. Keep this
document as bundle-first perf rationale/backlog; references to `v3`, side-by-side
execution, and the old `src/v4/` subtree describe the historical path that led
to the current runtime, not the current repository layout.

План полной переписи `fast-dump`, если цель не "ещё немного улучшить `v3`", а
резко поднять потолок производительности и убрать архитектурные bottleneck-и,
которые уже почти не лечатся локальным тюнингом.

Это planning doc, а не runtime contract.

Источник истины по текущему поведению: код, `README.md`, `AGENTS.md` и
`src/network/protocol.md`.

## Scope

- считать, что mainline `v3` остаётся рабочим и поддерживаемым, пока rewrite не
  даст измеримый выигрыш и не закроет safety/integration matrix;
- не смешивать safe-тюнинг текущего `v3` runtime с идеями, которые требуют
  новой wire/runtime модели;
- по умолчанию сохранять пользовательский contract:
  - один бинарь `fast-dump`;
  - `sender` и `receiver`;
  - режимы `--listen` / `--connect`;
  - валидный `.tar.zst` на стороне receiver;
  - resumability после disconnect/restart;
  - Linux/Windows compatibility;
- разрешать смену wire protocol, внутреннего session flow и durable state
  layout, если это даёт большой throughput win.

## Why A Rewrite Is Justified

Текущий `v3` runtime уже сильно подтянут, но бенчи показывают, что потолок
теперь задают не случайные micro-cost, а сама форма pipeline и protocol flow.

Representative localhost numbers на подтверждённом `v3` baseline
(`target/bench-blazing-manifest-records-confirm-20260408/results_summary.csv`):

| dataset | total ms | dominant cost |
| --- | ---: | --- |
| `manySmallFiles` | `4185.19` | `manifest_transfer_duration_ms = 4063.32` |
| `mixed` | `2168.71` | `manifest_transfer_duration_ms = 2007.01` |
| `bigFiles` | `839.64` | main cost уже в data path / receiver frame commit |

Representative attribution baseline
(`target/bench-attribution-quick-20260408-f16/results_summary.csv`):

| dataset | network | total ms | bottleneck |
| --- | --- | ---: | --- |
| `manySmallFiles` | `loopback-localhost` | `4689.75` | sender-side manifest transfer |
| `manySmallFiles` | `wan-high-rtt` | `689193.54` | sender-side manifest transfer |
| `mixed` | `loopback-localhost` | `2403.16` | sender-side manifest transfer |
| `mixed` | `wan-high-rtt` | `244044.22` | sender-side manifest transfer |
| `bigFiles` | `loopback-localhost` | `950.28` | sender send / receiver frame path |
| `bigFiles` | `wan-high-rtt` | `136036.47` | sender send / receiver network-decode path |

Вывод:

- `manySmallFiles` и `mixed` упираются не в `zstd`, а в отдельный
  manifest-first critical path;
- `bigFiles` упирается не в discovery, а в stop-and-wait frame model и в
  receiver-side durable commit per frame;
- incremental `v3`-safe fixes ещё дадут win, но уже не поменяют саму форму
  кривой.

## What Must Stay True

Если rewrite не меняет эти инварианты явно, считать их обязательными:

- receiver output остаётся валидным `.tar.zst`;
- archive entries сохраняют current semantics:
  - regular files, которые удалось захватить;
  - readable symlinks without dereference;
  - relative tar paths with `/`;
  - deterministic traversal order;
- live-file policy остаётся best-effort и restart-from-scratch on failure;
- sender/receiver pairings `push` и `pull` сохраняются;
- cross-process mismatch всё ещё режется во время transport/session bootstrap;
- resume после disconnect/restart остаётся bounded и deterministic;
- sender aggregate stdout не ломается без отдельной задачи;
- новый runtime должен жить рядом с `v3`, пока migration не завершена.

## Rewrite Thesis

Если переписывать "ради сильно выше perf", то главная идея такая:

1. Убрать отдельный manifest-first hot path.
2. Разделить control plane и data plane.
3. Разрешить несколько data units in flight вместо stop-and-wait per frame.
4. Сдвинуть receiver durability с "sync everything per frame" к append-only
   journal/WAL + group commit.
5. Держать sender hot path streaming-first и memory-first, без обязательного
   disk spool на fresh path.
6. Сделать wire format компактнее и дешевле для file-heavy workloads.
7. Вынести resumability на уровень committed bundle, а не на уровень длинного
   отдельного manifest stream.

## Proposed Target Architecture

### A1. Bundle-First Protocol Instead Of Manifest-First

Вместо текущей схемы:

`Hello -> Resume -> ManifestStart/Chunk/Ack/Seal -> FrameStart/Chunk/End/Ack -> Finish`

целевая схема должна быть bundle-first:

- sender строит небольшой descriptor для очередной пачки файлов;
- descriptor описывает только то, что нужно для commit/resume этого bundle;
- сразу после descriptor идёт compressed tar payload того же bundle;
- receiver коммитит bundle как одно durable unit;
- resume идёт по committed bundle boundary, а не по длинному предварительному
  manifest хвосту.

Что это убирает:

- отдельный manifest critical path перед первым полезным payload;
- долгий RTT-sensitive prelude для file-heavy деревьев;
- отдельную durable историю discovery, которая сейчас живёт впереди данных.

### A2. Separate Control Plane And Data Plane

Текущий один поток сообщений перегружен и control traffic, и bulk payload.

Целевая модель:

- control channel:
  - handshake;
  - capabilities;
  - resume negotiation;
  - bundle descriptors;
  - durable commit acknowledgements;
  - errors / finish;
- data channel:
  - большие payload chunks без постоянного interleave с control packets.

Рекомендуемый порядок реализации:

1. сначала dual TCP channels как самый простой и предсказуемый вариант;
2. потом отдельная оценка QUIC streams, только если dual TCP не даёт нужного
   WAN win или слишком усложняет pairing/recovery.

### A3. Windowed In-Flight Bundles

Текущий `FrameStart -> FrameChunk* -> FrameEnd -> Ack` делает sender
stop-and-wait по durable boundary.

Целевой runtime должен уметь:

- держать несколько bundle-ов in flight;
- отдельно ограничивать:
  - bytes in flight;
  - bundles in flight;
  - raw source bytes in flight;
  - receiver durable lag;
- коммитить bundle-ы строго по порядку, даже если data path ahead уже заполнил
  окно;
- возвращать cumulative ack boundary.

Это особенно важно для:

- high RTT links;
- file-heavy trees, где descriptor и data path должны перекрываться;
- больших файлов, где sender не должен простаивать между frame commit-ами.

### A4. Receiver WAL + Group Commit

Главный архитектурный тормоз `bigFiles` на current path: durable commit на
каждый frame с `sync_all()` archive file и отдельным journal persist.

Целевой receiver path:

- append payload в archive temp file;
- append compact commit-intent / checksum / boundary record в receiver WAL;
- делать group commit по времени/объёму/queue pressure;
- после group fsync отвечать cumulative durable ack;
- recovery читать WAL и откатывать archive к последнему durable boundary.

Рекомендуемая durable модель для rewrite:

- после disconnect может быть потерян максимум весь незафиксированный commit
  group, а не только один bundle;
- размер commit group должен быть bounded и явно измеряем;
- этот tradeoff должен быть осознанным и configurable.

Если product contract не разрешит ослабить semantics "максимум один unacked
frame", тогда rewrite всё равно полезен, но выигрыш на receiver side будет
заметно меньше.

### A5. Streaming-First Sender Pipeline

Новый sender должен быть не большим session loop с условными ветками, а набором
простых bounded stages:

1. traversal + ordering
2. metadata normalize + path dictionary encode
3. file open/read
4. tar encode
5. compress
6. data send
7. commit/ack bookkeeping

Принципы:

- bounded queues между stage-ами;
- preallocated reusable buffers/slabs;
- без temp files и без обязательного manifest spool на fresh path;
- disk spill только как controlled fallback, а не как default pipeline;
- small files можно агрегировать пачками заранее;
- large files идут direct-stream path без попадания в heap-heavy buffering.

### A6. Compact Metadata Encoding

Current `v3` metadata path слишком дорогой для `manySmallFiles`.

В rewrite metadata wire format должен:

- кодировать пути через directory stack deltas;
- использовать varint для ordinal/length/count fields;
- передавать file kind и small flags битовыми масками;
- уметь dictionary reuse для path prefixes;
- не тащить повторно поля, которые можно вынести в bundle header.

Отдельно важно:

- metadata format должен быть cheap to write;
- cheap to validate;
- cheap to replay for resume;
- cheap to skip on receiver, если payload bundle ещё не стал durable.

### A7. Resume State Based On Bundle Boundaries

Новая resume model должна быть проще текущего split между manifest boundary и
archive boundary.

Рекомендуемая durable координата:

- `last_committed_bundle_no`
- `last_committed_work_ordinal`
- `archive_len`
- `bundle_catalog_offset` или `wal_seq`

Опционально:

- compact sidecar index для ускоренного restart/replay;
- rebuild-from-wal path, если sidecar отсутствует или повреждён.

Важный принцип:

- resume state должен описывать только уже durable data, а не speculative
  discovery ahead.

## Proposed Runtime Model

Ниже не контракт, а рекомендуемый target flow.

1. `Hello`
2. `HelloAck + Capabilities`
3. `ResumeQuery`
4. `ResumeState { bundle boundary, archive_len, wal_seq }`
5. optional `DataChannelOpen`
6. repeated:
   - `BundlePlan`
   - `BundleDataChunk*`
   - `BundleCommit`
   - `BundleAck`
7. `IssueTail` or `IssueSummary`
8. `Finish`
9. `FinishAck`

Где `BundlePlan` включает:

- bundle number;
- first/last `work_ordinal`;
- descriptor checksum;
- expected compressed length;
- raw/tar byte counters;
- enough metadata to validate resume and archive order.

## Proposed Repository Shape

Если rewrite делать рядом с `v3`, лучше не пытаться вживлять его в старые файлы.

Рекомендуемая структура:

- `src/network/protocol.rs`
- `src/network/transport.rs`
- `src/sender/scan.rs`
- `src/sender/plan.rs`
- `src/sender/read.rs`
- `src/sender/tar.rs`
- `src/sender/compress.rs`
- `src/sender/network.rs`
- `src/receiver/session.rs`
- `src/receiver/wal.rs`
- `src/receiver/catalog.rs`
- `src/receiver/archive.rs`
- `src/receiver/recovery.rs`
- `src/tests/`

Смысл такого разделения:

- не тащить `v3` branching в каждый hot path;
- держать migration reversible;
- сравнивать `v3` и rewrite side by side на одинаковом bench harness.

## Phase Plan

### Phase 0. Freeze Targets And Fix Measurement

Цель:

- не переписать всё вокруг ложного bottleneck-а;
- сначала получить measurements, которым можно верить.

Что делаем:

- формализуем target contract, который rewrite обязан сохранить;
- отдельно фиксируем, какие semantics можно ослабить ради perf;
- обновляем bench harness так, чтобы shaped network моделировал transfer,
  а не RTT на каждый `read`/`write`;
- добавляем stage split для:
  - sender planning;
  - sender data production;
  - sender transport wait;
  - receiver wal append;
  - receiver durable commit;
  - receiver replay/recovery.

Exit criteria:

- benchmark report показывает sender/receiver stage split без двусмысленности;
- есть зафиксированный perf target по трём workload classes:
  - `manySmallFiles`
  - `mixed`
  - `bigFiles`

### Phase 1. Bundle Format Prototype Without Networking

Цель:

- доказать, что bundle-first model вообще закрывает archive correctness и
  resumability.

Что делаем:

- локальный sender/receiver prototype без TCP;
- sender собирает bundle descriptor + tar.zst payload;
- receiver пишет archive temp file + WAL;
- restart/recovery симулируется локально.

Что проверяем:

- tar correctness;
- resume after crash;
- path ordering;
- large/small file mix;
- skipped-file reporting;
- symlink semantics.

Exit criteria:

- offline prototype проходит integration matrix на файловой системе;
- bundle boundary достаточно информативна для deterministic recovery;
- descriptor format не требует отдельного global manifest.

### Phase 2. Single-Channel Rewrite MVP

Цель:

- получить первый real runtime win без усложнения transport model.

Что делаем:

- новый wire protocol поверх одного TCP connection;
- но уже без manifest-first phase;
- один in-flight bundle;
- новый receiver WAL path;
- новый sender streaming pipeline.

Зачем такой этап нужен:

- он изолирует пользу от bundle-first architecture;
- не смешивает её с параллельным вводом dual-channel transport.

Exit criteria:

- `manySmallFiles` и `mixed` быстрее current `v3` на localhost;
- `bigFiles` не regress-ится;
- recovery и crash semantics уже закрыты тестами.

### Phase 3. Multi-Bundle Window

Цель:

- убрать stop-and-wait effect на durable boundary.

Что делаем:

- sender умеет держать window из нескольких bundle-ов;
- receiver принимает ahead, но ack остаётся cumulative и ordered;
- bounded inflight memory и bounded inflight disk lag;
- scheduler отделяет small-file aggregation от large-file streaming.

Exit criteria:

- measurable WAN win на `mixed` и `bigFiles`;
- sender не простаивает между durable acks;
- RSS не уходит в неуправляемый рост.

### Phase 4. Split Control/Data Channels

Цель:

- убрать interference между metadata/control и payload traffic.

Что делаем:

- dual TCP channels или альтернативный transport с independent streams;
- отдельные failure rules:
  - data channel reset;
  - control channel reset;
  - partial session teardown;
- rebind/reconnect semantics during active dump.

Exit criteria:

- `manySmallFiles` на WAN больше не похож на control-bound workload;
- sender control path не блокируется bulk payload flush-ами;
- transport establishment всё ещё поддерживает `push` и `pull`.

### Phase 5. Receiver Group Commit Tuning

Цель:

- получить major win на `bigFiles` и смешанных потоках.

Что делаем:

- tuning commit group size;
- tuning fsync cadence;
- tuning WAL record size;
- rollback policy and recovery checks;
- explicit config guards against unbounded durability lag.

Exit criteria:

- measurable localhost win на `bigFiles`;
- measurable shaped-network win без corrupt/replay regressions;
- crash tests подтверждают bounded data loss ровно в рамках нового contract.

### Phase 6. Migration, Compatibility, And Hardening

Цель:

- довести rewrite до production-ready состояния.

Что делаем:

- side-by-side runtime selection:
  - `v3` current mainline;
  - rewrite runtime behind explicit flag or negotiated version;
- soak tests;
- chaos/restart matrix;
- docs for wire protocol and operational semantics;
- upgrade/downgrade policy between protocol versions.

Exit criteria:

- rewrite проходит `cargo test --quiet` и targeted perf matrix;
- `src/network/protocol.md` или новый protocol doc отражает actual behavior;
- можно принять решение о default switch.

## Suggested Order Of Execution

Если делать rewrite прагматично, порядок должен быть такой:

1. measurement fix
2. offline bundle prototype
3. single-channel rewrite MVP
4. multi-bundle window
5. split control/data channels
6. receiver group commit retune
7. migration and hardening

Если выбирать ровно один самый важный first step, это не dual-channel
transport, а `bundle-first offline prototype`.

Именно он отвечает на главный вопрос: можно ли вообще выкинуть отдельный
manifest critical path без потери resumability и archive correctness.

## Expected Gains

Это не обещания, а рабочие target ranges, которые оправдывают rewrite.

| workload | target gain | main source of win |
| --- | --- | --- |
| `manySmallFiles` localhost | `2x..5x` | removal of manifest-first critical path + compact metadata |
| `manySmallFiles` WAN | `3x..10x` | split channels + windowed bundles + less RTT-sensitive control flow |
| `mixed` localhost | `1.5x..3x` | better overlap + cheaper metadata + less stop-and-wait |
| `mixed` WAN | `2x..5x` | transport window + control/data separation |
| `bigFiles` localhost | `1.2x..2x` | receiver group commit + better sender steady-state |
| `bigFiles` WAN | `1.5x..3x` | inflight window + reduced ack stalls |

Если rewrite не даёт хотя бы нижнюю границу этих диапазонов на representative
bench suite, переключать default runtime смысла нет.

## Verification Plan

Минимум, который должен быть у rewrite с первого дня:

- loopback benchmark matrix against `v3`;
- shaped WAN matrix after harness fix;
- crash/restart tests:
  - before durable ack;
  - after durable ack but before sender receives it;
  - during multi-bundle inflight window;
  - during receiver recovery;
- archive equivalence tests against `v3` fixtures;
- correctness tests for:
  - long paths;
  - symlinks;
  - unstable live files;
  - skipped entries;
  - deterministic ordering.

Отдельно нужны performance guardrails:

- bytes copied per stage;
- allocations per file class;
- queue depth histograms;
- inflight window occupancy;
- fsync cadence;
- ack batch size;
- sender idle time vs receiver commit time.

## Risks And Open Questions

### R1. Durability Contract Might Need To Change

Самый большой perf win на receiver side почти наверняка потребует отказаться от
"после disconnect теряется максимум один unacked frame" в пользу
"теряется максимум один uncommitted group".

Это надо решить в начале, а не посередине реализации.

### R2. QUIC May Be More Expensive Than It Looks

QUIC streams красивы на бумаге, но могут принести:

- больше complexity;
- труднее debug;
- хуже Windows/Linux parity;
- неожиданные CPU costs.

Поэтому первая transport split implementation должна быть проще: dual TCP.

### R3. Compact Metadata Can Accidentally Become Hard To Recover

Слишком агрессивная компрессия metadata может сделать recovery и validation
дороже, чем текущий explicit format.

Нужно оптимизировать не "минимум байт", а "минимум байт при дешёвом recovery".

### R4. Overlap Can Blow Up RSS

Windowed bundles и aggressive prefetch легко превращаются в memory balloon.

Нужны жёсткие caps:

- raw bytes queued;
- tar bytes queued;
- compressed bytes queued;
- descriptors queued;
- open file handles;
- large direct-stream concurrency.

### R5. Migration Complexity Can Eat The Win

Если пытаться слишком рано глубоко переиспользовать `v3` internals, rewrite
потеряет ясность и начнёт тащить старые компромиссы.

Лучше иметь новый isolated runtime tree и потом переносить только реально
удачные helper-ы.

## Explicit Non-Goals

Этот rewrite-план не про:

- cosmetic refactoring текущих `v3` файлов;
- сохранение wire compatibility с `v3`;
- сохранение backward compatibility и legacy-ограничений только потому, что они есть в
  старом runtime; если очень нужен старый путь, его всегда можно взять из git, а не
  тащить как постоянный груз в rewrite;
- преждевременный переход на QUIC без benchmark proof;
- усложнение CLI contract ради внутренних удобств;
- абстракции "на будущее", если они не дают perf или correctness win сразу.

## Definition Of Success

Rewrite считается успешным, если одновременно выполнены все условия:

- `manySmallFiles` и `mixed` перестают быть преимущественно manifest/control
  bound;
- `bigFiles` получает measurable win на receiver durable path;
- resume/recovery остаётся deterministic и bounded;
- итоговый `.tar.zst` корректен и стабилен на Linux/Windows;
- bench harness даёт интерпретируемые sender-vs-receiver attribution numbers;
- rewrite runtime можно сравнивать с `v3` side by side и осознанно выбирать как
  default.
