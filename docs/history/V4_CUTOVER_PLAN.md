# V4 Cutover Plan

Status on 2026-04-14: cutover completed. This document is kept as the planning
record and completion checklist for the work that moved the project from the
mixed transition tree to the current single-runtime canonical layout.

План полного cutover с текущего mixed/transition workspace на single-stack `v4`
с последующим удалением active `v3` runtime-кода.

Это planning doc, а не runtime contract.

Источник истины по текущему поведению: код, `README.md`, `AGENTS.md` и
`src/network/protocol.md`.

## Goal

- оставить в проекте один runtime: `v4`;
- вернуть один canonical code path в `src/network/`, `src/sender/` и
  `src/receiver/`;
- сохранить пользовательский контракт CLI, transport pairings, resumability и
  валидный выходной `.tar.zst`;
- убрать `v3` как active runtime из кода, тестов, bench harness и docs.

## Non-Goals

- не делаем `v3`/`v4` wire negotiation;
- не сохраняем mixed-runtime поддержку после cutover;
- не мигрируем `v3` partial state в `v4`;
- не меняем transport pairings `push`/`pull`;
- не меняем базовый продуктовый shape CLI кроме тех флагов, которые были
  привязаны только к `v3` manifest-first pipeline.

## Locked Decisions

Эти решения считаются принятыми для cutover и не должны оставаться открытыми к
моменту реализации.

1. Canonical runtime.
   После cutover в бинарнике остаётся только protocol/runtime `4`. Флаг
   `--runtime` удаляется.
2. Output naming.
   Mainline naming после cutover возвращается к canonical именам без `v4`
   suffix:
   - `<dump_id>.tar.zst.part`
   - `<dump_id>.tar.zst`
   - `<dump_id>.wal.part`
   - `<dump_id>.catalog.part`
   - `<dump_id>.tar.zst.issues.tsv.part`
   - `<dump_id>.tar.zst.issues.tsv`
3. On-disk migration.
   `v3` partial state не поддерживается и не мигрируется. Experimental `v4`
   state с именами `*.v4.*` должен получить one-shot rename/migration в новые
   canonical имена при открытии receiver state.
4. CLI knobs.
   Внешняя sender-форма сохраняет привычные contract knobs:
   - оставить `--level`
   - оставить `--threads`
   - оставить `--min-readers` / `--max-readers`
   - оставить `--read-buffer-kb`
   - оставить `--frame-target-mb`, но использовать его как public alias для
     `v4 bundle_target`
   - удалить `--manifest-chunk-kb`
   - удалить `--manifest-ack-records`
   - удалить `--manifest-ack-kb`
   На receiver убрать `manifest_ack_*`, так как у `v4` нет manifest-first
   фазы.
5. Stdout compatibility.
   Sender CLI stdout остаётся aggregate-only и сохраняет stable mainline shape.
   Если `v4` internally считает `bundles`, публичный summary всё равно обязан
   сохранить текущие aggregate counters или их совместимые aliases; silent
   churn недопустим.
6. Bench artifacts.
   Генерация bench artifacts после cutover становится single-runtime. Reader
   compatibility для legacy CSV можно временно сохранить в `benches/pipeline/`,
   но только как history-bridge, не как признак active `v3` поддержки.
7. Canonical file layout.
   После cutover active protocol/transport снова живут в canonical местах:
   - `src/network/protocol.rs`
   - `src/network/protocol.md`
   - `src/network/transport.rs`
   - `src/sender/`
   - `src/receiver/`
   `src/v4/` после завершения cutover удаляется.

## Workspace Reality On 2026-04-14

Срез важен, потому что план должен опираться на фактическое состояние дерева, а
не на старое описание из docs.

- `src/v4/` уже содержит рабочий experimental stack:
  - dual-channel transport в `src/v4/transport/`
  - sender/receiver runtime в `src/v4/sender/` и `src/v4/receiver/`
  - protocol reference в `src/v4/protocol.md`
- `v4` уже имеет unit/integration-like coverage для:
  - offline roundtrip
  - `push` network roundtrip
  - `pull` network roundtrip
  - resume после disconnect
  - terminal skips и issue persistence
  - streamed large-file retry/skip
  - `receiver --listen --max-sessions`
- текущий workspace уже transition-only и не является green baseline:
  - `git status --short` показывает dirty tree
  - `src/cli/mod.rs` удалён из worktree
  - `src/app.rs` уже переписан под `v4`
  - `cargo test --quiet` на этом snapshot падает с `E0583` по `pub mod cli`
- docs и bench harness ещё описывают side-by-side world:
  - `README.md` и `AGENTS.md` всё ещё называют `v3` mainline runtime
  - `benches/pipeline/loopback.rs` и `benches/pipeline/stats.rs` всё ещё гоняют
    matrix `v3` vs `v4-experimental`
  - `benches/pipeline/results.rs` пишет и читает `protocol_runtime`
  - `src/v4/receiver/mod.rs` всё ещё использует `*.v4.*` naming

Следствие: кодовый cutover нельзя начинать с удаления `v3`. Сначала нужен
собираемый и тестируемый baseline.

## Phase 0: Restore A Green Baseline

Это обязательная нулевая фаза. Без неё все следующие шаги невалидны, потому что
нечем доказать regression-free cutover.

### Tasks

- привести workspace в компилируемое состояние;
- восстановить согласованность между `src/lib.rs`, `src/app.rs` и actual CLI
  module layout;
- договориться, какие текущие изменения пользователя уже считаются частью
  pre-cutover branch, а какие нет;
- снова добиться прохождения:
  - `cargo test --quiet`
  - `cargo clippy --all-targets --all-features`

### Concrete Files

- `src/lib.rs`
- `src/app.rs`
- `src/cli/mod.rs` или его замена в новом layout

### Exit

- сборка green;
- validation stack снова пригоден как gate для всех следующих фаз.

## Phase 1: Reach Full V4 Contract Parity

Прежде чем делать `v4` единственным runtime, он должен закрыть весь current
external contract, который пользователю сегодня обещают `AGENTS.md` и
`README.md`.

### Tasks

- проверить каждый пункт hard contract из `AGENTS.md` против `v4`;
- закрыть user-facing gaps между current `v4` и current mainline contract;
- довести sender/receiver summaries до stable aggregate shape;
- убрать `v4`-specific output naming из runtime semantics и заменить его на
  canonical naming плюс migration с legacy `*.v4.*`;
- подтвердить CLI/runtime validation:
  - `--threads auto`
  - zero/invalid bounds rejection
  - `--max-sessions`
  - required `--source` / `--data-dir`
  - `--dump-id` default от last path component
- подтвердить transport establishment invariants:
  - pairing mismatch rejection
  - role mismatch rejection
  - token validation
  - `push` и `pull`
- подтвердить resumability и issue-tail semantics на real runtime path;
- подтвердить Linux/Windows behavior, включая path normalization, symlinks и
  rename/truncate/fsync semantics.

### Required Validation Additions

- binary-level connectivity tests для `fast-dump sender` / `fast-dump receiver`,
  а не только direct calls into `src/v4`;
- receiver migration tests:
  - resume с legacy `*.v4.*`
  - отсутствие resume с `v3` state
- stdout contract tests для sender/receiver summary fields;
- tests на canonical output naming без `v4` suffix.

### Concrete Files

- `src/app.rs`
- `src/cli/mod.rs`
- `src/v4/sender/`
- `src/v4/receiver/`
- `src/v4/transport/`
- `src/v4/tests/mod.rs`
- `tests/connectivity/`

### Exit

- `v4` покрывает обязательное поведение mainline;
- остаются только mechanical switch и code relocation, а не runtime gaps.

## Phase 2: Switch Mainline To V4

После contract parity нужно переключить entrypoints, CLI и docs на single
runtime world.

### Tasks

- удалить runtime branching из orchestration layer;
- удалить hidden `--runtime v4-experimental` из CLI;
- сделать `v4` единственным execution path для sender и receiver;
- привязать public `--frame-target-mb` к `v4 bundle_target`;
- удалить `manifest_*` knobs из CLI, runtime validation и help;
- обновить user-facing logs/errors под post-cutover naming и vocabulary;
- обновить protocol reference так, чтобы mainline contract ссылался уже на `v4`.

### Concrete Files

- `src/app.rs`
- `src/cli/mod.rs`
- `README.md`
- `AGENTS.md`
- `src/v4/protocol.md`
- `src/network/protocol.md`

### Exit

- у бинарника больше нет выбора runtime;
- default и actual runtime совпадают и это `v4`.

## Phase 3: Collapse The Tree Into Canonical Locations

После переключения mainline нужно убрать side-by-side layout, иначе в проекте
останется ложное ощущение временного coexistence.

### Tasks

- перенести active protocol codec из `src/v4/protocol/` в `src/network/`;
- перенести active transport из `src/v4/transport/` в `src/network/transport.rs`;
- перенести active sender implementation из `src/v4/sender/` в `src/sender/`;
- перенести active receiver implementation из `src/v4/receiver/` в
  `src/receiver/`;
- сохранить thin facade shape у `src/sender/mod.rs` и `src/receiver/mod.rs`;
- обновить все imports, benches и tests на canonical paths;
- затем удалить `src/v4/`.

### Concrete Files

- `src/network/protocol.rs`
- `src/network/protocol.md`
- `src/network/transport.rs`
- `src/sender/`
- `src/receiver/`
- `src/v4/` -> удалить после переноса

### Exit

- в `src/` больше нет отдельного experimental subtree;
- canonical navigation снова соответствует runtime reality.

## Phase 4: Remove V3 Runtime And Compatibility Layers

Только после того, как `v4` стал mainline и переехал в canonical locations,
можно удалять `v3`.

### Tasks

- удалить `v3` packet codec и manifest-first protocol model;
- удалить `v3` sender session flow, manifest spool и frame pipeline;
- удалить `v3` receiver journal/resume/frame handling, которые больше не нужны
  `v4`;
- удалить `v3`-specific CLI validation, logs, helpers и terminology;
- оставить только history docs там, где они ещё полезны для архива.

### Concrete Files

- `src/network/protocol.rs` старый `v3` код
- `src/network/transport.rs` старый `v3` transport
- `src/sender/frame.rs`
- `src/sender/readers.rs`
- `src/sender/session.rs`
- `src/sender/source.rs`
- `src/receiver/journal.rs`
- `src/receiver/resume.rs`
- `src/receiver/frame.rs`
- `src/receiver/finish.rs`
- `src/receiver/session.rs`
- `src/receiver/shared.rs`
- `tests/connectivity/` `v3`-specific cases

### Exit

- active `v3` code удалён;
- в дереве больше нет двусмысленности, какой runtime является production.

## Phase 5: Rewrite Tests And Bench Harness For Single Runtime

После удаления `v3` validation stack тоже должен перестать моделировать
dual-runtime мир.

### Tasks

- переписать connectivity/protocol-flow tests так, чтобы они проверяли только
  current runtime;
- убрать ожидания строк вида `v3` / `v4-experimental` из observability tests;
- удалить runtime matrix `v3` vs `v4-experimental` из bench harness;
- сделать single-runtime benchmark plan/report generation;
- оставить legacy CSV parsing только если он нужен для чтения historical
  artifacts, а не для active result generation.

### Concrete Files

- `tests/connectivity/`
- `tests/bench_matrix.rs`
- `benches/pipeline/loopback.rs`
- `benches/pipeline/stats.rs`
- `benches/pipeline/results.rs`

### Exit

- tests и benches описывают только текущий production runtime;
- benchmark outputs больше не создают ложную поддержку `v3`.

## Phase 6: Documentation Cleanup

Docs должны обновляться в тех же change sets, что и runtime behavior.

### Tasks

- обновить `README.md`;
- обновить `AGENTS.md`;
- обновить canonical protocol reference;
- перенести устаревшие `v3`-specific planning docs в `docs/history/`, если их
  ещё нужно хранить;
- убрать формулировки `default is v3`, `v4-experimental`, `side-by-side`,
  `rewrite runtime`, если это уже неправда.

### Concrete Files

- `README.md`
- `AGENTS.md`
- `src/network/protocol.md`
- `docs/history/`

### Exit

- active docs совпадают с active code;
- history отделена от текущего контракта.

## Recommended Delivery Split

Самый безопасный порядок PR-ов:

1. `PR0`: восстановить green baseline, зафиксировать compiling tree и
   validation gates.
2. `PR1`: довести `v4` до contract parity, включая stdout compatibility,
   canonical naming и migration с `*.v4.*`.
3. `PR2`: переключить CLI/mainline на `v4`, удалить runtime selector, обновить
   docs.
4. `PR3`: перенести active `v4` в canonical locations и удалить `src/v4/`.
5. `PR4`: удалить `v3` runtime-код, затем зачистить tests, bench harness и
   history-only docs.

Не смешивать `PR0` и удаление `v3` в один change set.

## Done Criteria

Cutover считается завершённым только когда одновременно выполнены все пункты:

- `cargo test --quiet` проходит;
- `cargo clippy --all-targets --all-features` проходит;
- у CLI больше нет `--runtime`;
- production path использует только `v4`;
- receiver пишет canonical `.tar.zst` / `.issues.tsv` naming без `v4` suffix;
- legacy experimental `*.v4.*` state мигрируется или корректно подхватывается;
- `v3` partial state явно не поддерживается и это отражено в docs;
- в `src/` больше нет active `v3` runtime-кода;
- в `src/` больше нет отдельного `src/v4/` subtree;
- `README.md`, `AGENTS.md` и protocol docs синхронизированы;
- bench harness и tests больше не представляют `v3` как supported runtime.

## Main Risk

Главный риск cutover не в удалении файлов, а в ложном ощущении, что `v4` уже
можно сделать единственным runtime без доведения его до полного contract parity
и без green baseline.

Правильный порядок только такой:

1. сначала green baseline;
2. потом parity;
3. потом mainline switch;
4. потом relocation;
5. потом удаление `v3`;
6. потом cleanup хвостов.
