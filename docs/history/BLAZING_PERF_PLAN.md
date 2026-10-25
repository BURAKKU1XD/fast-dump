> Historical document. See the [current runtime](../runtime.md) and
> [protocol v4](../../src/network/protocol.md) for the active contract.

# BLAZING PERF Plan

План ускорения `fast-dump` после полного cutover на текущий runtime `v3`.

Этот файл описывает порядок perf-работ и критерии успеха. Это planning doc, а не runtime contract.

## Scope

- не ломать wire contract `v3`, resumability и bounded durability semantics;
- бить сначала по bottleneck-ам с самым большим `ROI`, а не по косметическим micro-cost;
- держать benchmark truth в артефактах, а не в архитектурных предположениях;
- отдельно помечать идеи, которые уже требуют `v4`, а не безопасного тюнинга внутри `v3`.

## Current Attribution Baseline

Актуальный baseline: `2026-04-08`.

Основной attribution-run:

```powershell
$env:FAST_DUMP_BENCH_MATRIX_DIR='target\bench-attribution-quick-20260408-f16'
cargo bench --bench loopback_send_dump -- --dataset manySmallFiles,mixed,bigFiles --network-profile loopback-localhost,wan-high-rtt,bandwidth-limited --threads 1 --frames 16 --buffers 1024 --manifest-chunk-kb 512 --manifest-ack-records 1024 --manifest-ack-kb 8192 --repeats 1
```

Артефакты:

- `target/bench-attribution-quick-20260408-f16/benchmark_report.md`
- `target/bench-attribution-quick-20260408-f16/results_summary.csv`

Representative rows:

| dataset | network | total ms | bottleneck | key signal |
| --- | --- | ---: | --- | --- |
| `bigFiles` | `loopback-localhost` | `950.28` | `sender: send` | `receiver frame local ms=363.84`, `receiver frame network/decode ms=576.11` |
| `bigFiles` | `wan-high-rtt` | `136036.47` | `sender: send` | `receiver frame network/decode ms=132315.05` |
| `manySmallFiles` | `loopback-localhost` | `4689.75` | `sender: manifest transfer` | `manifest tail ms=4471.99` |
| `manySmallFiles` | `wan-high-rtt` | `689193.54` | `sender: manifest transfer` | `manifest tail ms=589557.07` |
| `mixed` | `loopback-localhost` | `2403.16` | `sender: manifest transfer` | `manifest tail ms=2113.49` |
| `mixed` | `wan-high-rtt` | `244044.22` | `sender: manifest transfer` | `manifest tail ms=196516.20` |

Вывод из baseline:

- `manySmallFiles` и `mixed` сейчас проигрывают не в receiver finalize и не в replay, а в sender-side `manifest` critical path.
- `bigFiles` почти не страдает от manifest path; там следующий bottleneck уже в `send`/transport/data path.
- receiver local commit виден и теперь измеряется, но для `manySmallFiles`/`mixed` это не первая цель.

## Current Landed State

Актуальный localhost-confirmed tuning snapshot: `2026-04-08`.

Подтверждённый localhost baseline после быстрого `manifest_ack_records` sweep:

- `threads=1`
- `frame_target_mb=64`
- `read_buffer_kb=1024`
- `manifest_chunk_kb=512`
- `manifest_ack_records=4096`
- `manifest_ack_kb=8192`

Артефакты:

- `target/bench-blazing-manifest-records-confirm-20260408/benchmark_report.md`
- `target/bench-blazing-manifest-records-confirm-20260408/results_summary.csv`

Representative localhost rows:

| dataset | old total ms (`ack_records=1024`) | new total ms (`ack_records=4096`) | delta |
| --- | ---: | ---: | ---: |
| `bigFiles` | `853.55` | `839.64` | `+1.63%` |
| `manySmallFiles` | `4322.75` | `4185.19` | `+3.18%` |
| `mixed` | `2431.80` | `2168.71` | `+10.82%` |

Что это значит сейчас:

- `P2` уже даёт измеримый localhost win и не показывает regression на `bigFiles`;
- `manySmallFiles` и `mixed` всё ещё остаются преимущественно `manifest transfer` bound;
- shaped-network confirmation для новых defaults всё ещё нужна, но менять current default обратно на `1024` уже нет причин.

Incremental follow-up landed on `2026-04-09`:

- sender manifest send path больше не клонирует payload каждого `ManifestChunk`
  перед wire-write;
- fixed packet header (`magic/version/kind/body_len`) теперь уходит одним
  write вместо четырёх отдельных `write_all` вызовов на каждый packet;
- это safe `P6` cleanup внутри текущего `v3` wire contract;
- отдельный throughput win от одной этой правки пока не изолирован: на текущей
  машине localhost bench-ы шумят сильнее, чем ожидаемая micro-delta.

Adaptive sender defaults also landed on `2026-04-09`:

- sender теперь делает bounded deterministic probe по раннему DFS-префиксу
  source tree и классифицирует workload как `tiny-heavy`, `mixed` или
  `large-heavy`;
- adaptive logic трогает только `threads`, `read_buffer_kb` и
  `frame_target_mb`, если соответствующий CLI knob не был задан явно;
- стартовые runtime profiles сейчас такие:
  - `tiny-heavy`: `threads=2`, `frame_target_mb=64`, `read_buffer_kb=1024`
  - `mixed`: `threads=2`, `frame_target_mb=64`, `read_buffer_kb=64`
  - `large-heavy`: `threads=4`, `frame_target_mb=64`, `read_buffer_kb=1024`
- явные CLI overrides остаются сильнее adaptive logic, включая явный
  `--threads auto`;

Adaptive localhost confirmation landed on `2026-04-12`:

- targeted localhost sweep for `manySmallFiles` and `mixed` showed that the
  earlier `16 MiB / 64 KiB` adaptive candidates were too aggressive;
- confirmed current localhost winners are:
  - `tiny-heavy`: `threads=2`, `frame_target_mb=64`, `read_buffer_kb=1024`
  - `mixed`: `threads=2`, `frame_target_mb=64`, `read_buffer_kb=64`
  - `large-heavy`: `threads=4`, `frame_target_mb=64`, `read_buffer_kb=1024`
- explicit CLI overrides still remain stronger than adaptive logic, including
  explicit `--threads auto`;
- shaped-network confirmation for the full adaptive step is still pending, but
  localhost confirmation is now done.

Artifacts:

- `target/bench-p4-adaptive-confirm-localhost-20260412/benchmark_report.md`
- `target/bench-p4-adaptive-confirm-localhost-20260412/results_summary.csv`

Large-file profile tuning landed on localhost on `2026-04-12`:

- targeted `bigFiles` confirmation showed that the best current large-heavy
  profile is `threads=4`, `frame_target_mb=64`, `read_buffer_kb=1024`;
- older `large-heavy` candidate with `read_buffer_kb=4096` lost on clean
  localhost repeats and also did not beat `1024 KiB` on the shaped
  `bandwidth-limited` sanity check;
- separate threshold confirm showed that the current
  `large_file_single_frame_threshold=16 MiB` should stay unchanged for now.

Artifacts:

- `target/bench-p5-profile-confirm-localhost-20260412/benchmark_report.md`
- `target/bench-p5-profile-confirm-localhost-20260412/results_summary.csv`
- `target/bench-p5-profile-sanity-bandwidth-20260412/benchmark_report.md`
- `target/bench-p5-profile-sanity-bandwidth-20260412/results_summary.csv`
- `target/bench-p5-threshold-confirm-localhost-20260412/benchmark_report.md`
- `target/bench-p5-threshold-confirm-localhost-20260412/results_summary.csv`

## Benchmark Caveat

Текущий shaped bench harness моделирует RTT и bandwidth на каждую `read`/`write` операцию, а не на один агрегированный сетевой packet transfer.

Это значит:

- `loopback-localhost` остается главным reality anchor;
- `wan-high-rtt` и `bandwidth-limited` полезны для относительной attribution и сравнения candidate fixes между собой;
- shaped WAN numbers пока не надо трактовать как точный production forecast в абсолютных секундах.

Пока этот caveat не исправлен, perf-решения надо принимать по комбинации:

- localhost throughput;
- shaped-network relative deltas;
- receiver-aware stage split;
- стабильность результата на повторных прогонах.

## Prioritized Improvements

| priority | change | scope | expected gain | status |
| --- | --- | --- | --- | --- |
| `P0` | Validate benchmark interpretation and shaping caveat | harness/docs | точнее attribution, без прямого throughput gain | completed |
| `P1` | Startup frontier fix | `v3`-safe | `manySmallFiles +15..35%`, `mixed +10..25%` | landed, more tuning pending |
| `P2` | Manifest ack cadence retune | `v3`-safe | `manySmallFiles +10..25%`, `mixed +5..15%` | landed on localhost |
| `P3` | Real steady-state overlap | `v3`-safe | `manySmallFiles +15..30%`, `mixed +10..25%` | partially landed |
| `P4` | Adaptive sender defaults | `v3`-safe | `bigFiles +15..35%`, `mixed +5..15%` | landed on localhost |
| `P5` | Large-file profile tuning | `v3`-safe | `bigFiles +10..25%` | landed on localhost |
| `P6` | Hot-path manifest/data micro-cost cleanup | `v3`-safe | `localhost +3..10%`, shaped links `+1..3%` | partially landed |
| `P7` | Receiver frame local commit cleanup | `v3`-safe | `bigFiles localhost +5..15%` | planned |
| `D1` | Compact manifest wire format | `v4`-leaning | `manySmallFiles +20..40%`, `mixed +10..25%` | deferred |
| `D2` | Split control/data channels | `v4`-leaning | `manySmallFiles WAN +25..50%`, `mixed WAN +15..35%` | deferred |

## Phase Plan

### Phase 0. Measurement Guardrails

Цель:

- не оптимизировать benchmark artifact вместо runtime bottleneck.

Что делаем:

- зафиксировать caveat про per-operation shaping прямо в perf-docs и backlog;
- держать `loopback-localhost` как обязательный anchor для каждого perf PR;
- сохранять receiver-aware metrics как обязательную часть report;
- сравнивать candidate fixes по одним и тем же knobs и datasets.

Выход:

- понятная интерпретация shaped runs;
- отсутствие ложных выводов вида "5 MiB manifest payload сам по себе стоит минуты".

### Phase 1. Startup Frontier Fix

Цель:

- резко снизить `time_to_first_frame_ms` для file-heavy cases.

Что меняем:

- в progressive fresh-path sender покрывает durable manifest только до минимально нужного `first frame frontier`;
- startup target и steady-state target больше не должны совпадать;
- первый frame стартует сразу после минимального безопасного frontier.

Где меняем:

- `src/sender/session.rs`
- `src/sender/frame.rs`
- `benches/pipeline/stats.rs`

Критерии успеха:

- `manySmallFiles`: `time_to_first_frame_ms` минимум в `2x` ниже;
- `manySmallFiles`: `total_duration_ms` минимум на `20%` ниже;
- `mixed`: `time_to_first_frame_ms` минимум в `2x` ниже.

### Phase 2. Manifest Ack Cadence and Chunk Sweep

Цель:

- уменьшить RTT-sensitive stop-and-wait behavior в manifest path без ломки bounded durability.

Что меняем:

- отдельно тюним `manifest_ack_records`, `manifest_ack_kb`, `manifest_chunk_kb`;
- не даем `manifest_ack_records` душить tiny-heavy tree раньше byte-window;
- фиксируем candidate defaults только после измерения на `manySmallFiles` и `mixed`.

Где меняем:

- `src/sender/session.rs`
- `src/cli/mod.rs`
- `src/app.rs`
- `benches/pipeline/loopback.rs`
- `benches/pipeline/results.rs`

Критерии успеха:

- `manySmallFiles`: заметное падение `manifest_transfer_duration_ms` на localhost и shaped runs;
- `mixed`: падение `manifest tail ms` без роста RSS и без regression в `bigFiles`;
- выбранные knobs остаются внутри protocol caps.

### Phase 3. Real Steady-State Overlap

Цель:

- сделать overlap не номинальным, а реальным на `manySmallFiles` и `mixed`.

Что меняем:

- не раздувать discovery window раньше времени;
- держать manifest немного впереди archive pipeline, но не на длинный хвост очереди;
- стабилизировать running lookahead policy после первого frame.

Где меняем:

- `src/sender/session.rs`
- `src/sender/frame.rs`
- `benches/pipeline/stats.rs`
- `tests/connectivity/observability.rs`

Критерии успеха:

- `mixed`: `overlap_efficiency_pct > 20`;
- `manySmallFiles`: manifest cost частично перекрывается archive send path;
- RSS не выходит за текущий bench budget.

### Phase 4. Adaptive Defaults and Large-File Profile

Цель:

- перестать использовать один static sender profile для всех workloads.

Что меняем:

- вводим простой classifier: `tiny-heavy`, `mixed`, `large-heavy`;
- выбираем один из заранее измеренных runtime profiles;
- explicit CLI overrides остаются сильнее adaptive logic.

Стартовые candidate profiles:

- `tiny-heavy`: `threads=2`, `frame_target_mb=64`, `read_buffer_kb=1024`
- `mixed`: `threads=2`, `frame_target_mb=64`, `read_buffer_kb=64`
- `large-heavy`: `threads=4`, `frame_target_mb=64`, `read_buffer_kb=1024`

Где меняем:

- `src/app.rs`
- `src/cli/mod.rs`
- `src/sender/session.rs`
- `benches/pipeline/shared.rs`

Критерии успеха:

- `bigFiles`: `total_duration_ms` минимум на `15%` ниже на localhost;
- `mixed`: дополнительное снижение `total_duration_ms` после Phase 1-3;
- explicit knobs from CLI keep precedence.

### Phase 5. Micro-Cost Cleanup

Цель:

- добить локальные лишние расходы только после закрытия больших bottleneck.

Что можно трогать:

- лишние `clone()` в manifest send path;
- batching/coalescing write path;
- receiver frame local commit cost;
- checksum/update path, если это подтверждено profiling-ом.

Что не делаем в первой волне:

- не меняем `v3` wire contract;
- не вводим второй runtime stack;
- не трогаем `v4`-класс изменений до закрытия Phase 1-4.

## Recommended Backlog Order

Порядок реализации:

1. measurement guardrails
2. startup frontier fix
3. manifest ack cadence sweep
4. steady-state overlap fix
5. adaptive defaults
6. large-file profile tuning
7. micro-cost cleanup
8. только потом `v4`-level ideas

Если выбирать только один самый выгодный шаг прямо сейчас, это `startup frontier fix`.

## Minimal Bench Set After Every Perf PR

Обязательный quick set:

```powershell
$env:FAST_DUMP_BENCH_MATRIX_DIR='target\bench-attribution-current'
cargo bench --bench loopback_send_dump -- --dataset manySmallFiles,mixed,bigFiles --network-profile loopback-localhost,wan-high-rtt,bandwidth-limited --threads 1 --frames 64 --buffers 1024 --manifest-chunk-kb 512 --manifest-ack-records 4096 --manifest-ack-kb 8192 --repeats 1
```

Отдельный sweep для manifest-path изменений:

```powershell
$env:FAST_DUMP_BENCH_MATRIX_DIR='target\bench-manifest-sweep'
cargo bench --bench loopback_send_dump -- --dataset manySmallFiles,mixed --network-profile loopback-localhost,wan-high-rtt --threads 1 --frames 64 --buffers 1024 --manifest-chunk-kb 512,1024 --manifest-ack-records 1024,4096,16384 --manifest-ack-kb 8192 --repeats 3
```

Отдельный sweep для large-file tuning:

```powershell
$env:FAST_DUMP_BENCH_MATRIX_DIR='target\bench-large-file-sweep'
cargo bench --bench loopback_send_dump -- --dataset bigFiles --network-profile loopback-localhost,bandwidth-limited --threads 1,2,4 --frames 16,64 --buffers 1024,4096 --manifest-chunk-kb 512 --manifest-ack-records 4096 --manifest-ack-kb 8192 --repeats 3
```

## Definition of Success

План считается успешным, если:

- `manySmallFiles` и `mixed` перестают быть почти полностью `manifest transfer` bound;
- `bigFiles` получает отдельный measurable gain на sender/data path;
- benchmarks различают sender-vs-receiver costs и остаются интерпретируемыми;
- docs и backlog остаются синхронизированы с текущим perf priority order.
