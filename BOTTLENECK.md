# BOTTLENECK Plan

Этот документ фиксирует план по поиску bottleneck в `fast-dump`.
Цель документа: получить данные, по которым можно уверенно оптимизировать
sender/receiver, не разрушая correctness и не принимая лабораторные измерения за
реальную эксплуатационную картину.

## 1. Цели

- Найти самые дорогие стадии в production send/receive path.
- Понять, где ограничение определяется CPU, диском, сетью, протоколом или
  синхронизацией sender/receiver.
- Отделить bottleneck sender от bottleneck receiver.
- Получить повторяемую методику сравнения изменений между ветками и коммитами.
- Не перепутать relative benchmark signal с absolute real-world throughput.

## 2. Главный принцип

Для bottleneck analysis нельзя использовать один универсальный тест.

Нужны минимум три разных класса измерений:

1. Stage microbench
   Чтобы понять стоимость отдельных стадий без сетевого шума.
2. Controlled end-to-end benchmark
   Чтобы понять взаимодействие sender и receiver как системы.
3. Realistic profile run
   Чтобы понять, как поведение меняется на реальном наборе файлов и реальном
   окружении.

Если эти три класса не разделены, результаты становятся трудноинтерпретируемыми.

## 3. Что уже есть в репозитории

### 3.1 Полезная база

- `benches/pipeline/scan_source.rs`
  Меряет обход дерева и подготовку source scan.
- `benches/pipeline/prepare_dump.rs`
  Меряет buffered path `prepare_dump`, который полезен для тестов и benchmark
  diagnostics.
- `benches/pipeline/receive_frame.rs`
  Меряет receiver-side commit одного подготовленного кадра.
- `benches/pipeline/stats.rs`
  Уже считает полезные стадии:
  - `read_duration`
  - `compress_duration`
  - `send_duration`
  - `total_duration`
  - `traversal_wait`
  - `ack_wait`
  - wire bytes / overhead
  - CPU/RSS/private memory
- `benches/pipeline/loopback.rs`
  Уже запускает matrix runner по datasets, threads, frame target и read buffer.

### 3.2 Что уже полезно для диагностики

Текущий loopback runner хорош тем, что он уже умеет сравнивать:

- `threads`;
- `frame_target_mb`;
- `read_buffer_kb`;
- synthetic datasets разных профилей;
- повторные прогоны;
- агрегированную summary таблицу.

Это хороший фундамент для optimization workflow.

## 4. Ограничения текущего bench harness

Именно эти ограничения нельзя забывать при интерпретации результатов.

### 4.1 Loopback не равен сети

Loopback benchmark отвечает на вопрос:
"как система ведет себя без реальной сети и удаленного хоста?"

Он не отвечает на вопрос:
"какую скорость увидит реальный пользователь через настоящую сеть?"

Следствия:

- `ack_wait` на loopback недооценивает сетевую задержку;
- влияние socket buffers и NIC почти не моделируется;
- взаимодействие с удаленным fsync и remote storage не видно.

### 4.2 Текущий процесс-монитор смешивает стороны

Когда sender и receiver запускаются внутри одного локального benchmark flow,
CPU и память могут отражать общую стоимость системы, а не чистый sender или
чистый receiver.

Следствия:

- трудно понять, кто именно съел CPU;
- трудно понять, чей RSS растет;
- сложно делать прицельную оптимизацию без дополнительных замеров.

### 4.3 Синтетические datasets не равны продовому corpus

Текущие datasets полезны для повторяемости, но они не обязательно отражают:

- реальное распределение размеров файлов;
- долю маленьких, средних и больших файлов;
- глубину каталогов;
- типичную compressibility;
- реальные патологические кейсы пользователей.

### 4.4 Warm cache и порядок прогонов искажают картину

При повторных чтениях по тем же данным результат может улучшаться не потому, что
код стал быстрее, а потому что:

- сработал page cache;
- файловая система уже "разогрета";
- allocator уже в выгодном состоянии;
- порядок сценариев помог следующим прогонам.

### 4.5 `prepare_dump` не тождествен production sender path

`prepare_dump` полезен как лабораторная стадия, но production path проходит
через `send_dump`, `PendingFileWalker`, `ReaderPool` и
`ProtocolChunkWriter`.

Следствие:
- оптимизация только по `prepare_dump` может улучшить benchmark и не дать
  заметного прироста реальному sender path.

## 5. Правильная рамка интерпретации метрик

Ниже зафиксировано, что означает доминирование той или иной стадии.

### Если доминирует `read_duration`

Вероятные причины:

- источник упирается в storage;
- слишком маленький `read_buffer_kb`;
- избыточные reopen/retry;
- узкое место в reader scheduling.

Что проверять:

- масштабирование по `read_buffer_kb`;
- масштабирование по `min_readers/max_readers`;
- поведение на warm/cold cache;
- IO counters и queue depth.

### Если доминирует `compress_duration`

Вероятные причины:

- слишком высокий zstd level;
- недостаточно/слишком много threads;
- плохая гранулярность работы для worker threads;
- данные имеют низкую compressibility, и CPU тратится без выигрыша.

Что проверять:

- matrix по `level`;
- matrix по `threads`;
- ratio vs throughput;
- CPU saturation sender side.

### Если доминирует `send_duration`

Вероятные причины:

- framing overhead;
- слишком мелкие `FrameChunk`;
- socket write path;
- протокольные накладные расходы.

Что проверять:

- wire overhead percent;
- средний размер payload на frame;
- размер protocol chunk;
- связь между `send_duration` и `wire_bytes`.

### Если доминирует `ack_wait`

Вероятные причины:

- receiver commit/fsync/journal;
- медленный storage на receiver side;
- сетевой RTT;
- receiver-side verification.

Что проверять:

- receiver isolated bench;
- отдельный storage benchmark для journal/finalize path;
- раздельные sender/receiver process metrics;
- влияние размера frame.

### Если растет `frame_aborts` или `frame_rebuilds`

Вероятные причины:

- слишком агрессивная политика isolation/retry;
- нестабильные файлы;
- неудачная конфигурация frame target.

Что проверять:

- частоту retry по dataset;
- вклад rebuild в total duration;
- отдельный benchmark для unstable file workloads.

## 6. Целевая структура bottleneck harness

### 6.1 Layer A: microbench по стадиям

Назначение:
- локализовать узкий участок без сетевых и межпроцессных шумов.

Нужно сохранить и развивать:

- `scan_source`;
- `receive_frame_commit`;
- stage-specific profiling helpers.

Нужно добавить:

- benchmark для production reader pipeline;
- benchmark для zstd-only path на уже подготовленных tar bytes;
- benchmark для journal append/fsync/finalize;
- benchmark для issue journal upload/persist path.

### 6.2 Layer B: controlled loopback E2E

Назначение:
- смотреть, как sender и receiver взаимодействуют как одна система при
  минимальном внешнем шуме.

Это хороший режим для:

- сравнения конфигураций;
- оценки относительного выигрыша от рефакторинга;
- выявления смещений между `read/compress/send/ack_wait`.

Но результаты должны явно помечаться как:

- synthetic;
- loopback;
- mostly warm/local.

### 6.3 Layer C: split-process E2E

Назначение:
- отделить sender bottleneck от receiver bottleneck.

Нужно получить:

- sender CPU/RSS отдельно;
- receiver CPU/RSS отдельно;
- sender stage timings отдельно;
- receiver commit/journal/finalize timings отдельно.

Предпочтительный подход:

- запускать sender и receiver как отдельные процессы;
- снимать метрики по PID каждой стороны;
- сохранять structured result artifact на каждый прогон.

### 6.4 Layer D: realistic corpus runs

Назначение:
- проверять, не обманывают ли synthetic datasets.

Нужно иметь минимум три класса реальных наборов:

- много маленьких файлов;
- смешанный проектный corpus;
- несколько очень больших слабо/сильно сжимаемых файлов.

Важно:
- эти прогоны не обязаны жить в обычном CI;
- они нужны как validation suite для решений по оптимизации.

## 7. Что именно нужно измерять

Минимальный набор метрик на каждый E2E прогон:

- raw read bytes;
- tar input bytes;
- compressed payload bytes;
- wire bytes;
- wire overhead bytes;
- read duration;
- compress duration;
- send duration;
- total duration;
- traversal wait;
- ack wait;
- sent frames;
- avg files per frame;
- frame aborts;
- frame rebuilds;
- sender CPU/RSS/private memory;
- receiver CPU/RSS/private memory;
- compression ratio;
- throughput в MiB/s по стадиям и total.

Дополнительно желательно:

- p50/p90/p99 вместо одного среднего;
- размер крупнейшего frame;
- средний/медианный размер `FrameChunk`;
- число fsync на receiver side;
- размер и количество journal writes;
- retry count и skip count;
- доля времени, пока sender blocked на receiver ack.

## 8. Какие datasets нужны

### 8.1 Synthetic baseline datasets

Их надо оставить, потому что они:

- детерминированы;
- удобны для сравнения веток;
- легко воспроизводятся на другой машине.

Нужны минимум:

- many small files;
- mixed entropy mixed size;
- big files.

### 8.2 Realistic datasets

Нужно добавить механизм для запуска на внешнем corpus:

- через путь, переданный аргументом;
- без коммита самих данных в репозиторий;
- с записью metadata manifest рядом с результатом.

Manifest должен содержать:

- количество файлов;
- суммарный размер;
- гистограмму размеров;
- долю расширений/типов файлов;
- отметку warm/cold run.

### 8.3 Pathological datasets

Отдельно нужны наборы для stress-analysis:

- очень много файлов по 1-8 KiB;
- глубоко вложенные каталоги;
- несколько гигантских файлов;
- нестабильные live files;
- плохо сжимаемые данные;
- очень хорошо сжимаемые данные.

## 9. Методика измерений

### 9.1 Для сравнений между коммитами

Использовать:

- одинаковую машину;
- одинаковую power profile конфигурацию;
- одинаковые datasets;
- `repeats >= 5`;
- shuffle порядка сценариев;
- отдельные warm и cold серии;
- median как основную цифру;
- p90 как индикатор нестабильности.

### 9.2 Для абсолютных пользовательских ожиданий

Использовать:

- split-process режим;
- по возможности раздельные хосты;
- реальную сеть или ее явную эмуляцию;
- реальный corpus;
- повторные прогоны в несколько временных окон;
- раздельную отчетность sender/receiver/storage/network.

### 9.3 Для локальной разработки

Допускается:

- loopback;
- synthetic datasets;
- warm cache;
- сокращенные repeats.

Но в отчете результат должен быть явно помечен как developer-local signal.

## 10. План внедрения

### Этап 1. Навести порядок в текущем loopback matrix

Приоритет: высокий.

Нужно сделать:

- включить более явную маркировку режима измерений;
- сохранять raw results с достаточным контекстом окружения;
- добавить median/p90 рядом со средним;
- включить shuffle сценариев по умолчанию для длинных прогонов;
- увеличить стандартный `repeats` для анализов, где нужен вывод, а не smoke.

Критерий завершения:
- loopback matrix дает воспроизводимые comparative results.

### Этап 2. Разделить sender и receiver метрики

Приоритет: самый высокий для реальной оптимизации.

Нужно сделать:

- добавить split-process bench harness;
- снимать метрики по PID sender и PID receiver отдельно;
- разнести sender stage timings и receiver commit timings;
- сохранять result artifact в structured формате.

Критерий завершения:
- можно сказать "уперлись в sender compression" или
  "уперлись в receiver fsync/journal", а не просто "система медленная".

### Этап 3. Добрать недостающие stage benches

Нужно добавить/усилить:

- reader pipeline bench;
- zstd-only bench;
- receiver finalize bench;
- journal persistence bench;
- protocol chunk write bench.

Критерий завершения:
- каждая дорогая стадия production path имеет собственный изолированный benchmark.

### Этап 4. Добавить realistic corpus mode

Нужно сделать:

- аргумент или отдельный runner для внешнего dataset path;
- запись manifest и environment metadata;
- сохранение результатов рядом с raw/summary CSV;
- явное разделение warm/cold run.

Критерий завершения:
- можно верифицировать, что оптимизация помогает не только synthetic datasets.

### Этап 5. Встроить optimization workflow

Нужно договориться о рабочем процессе:

1. Сначала safety tests.
2. Потом stage bench для гипотезы.
3. Потом loopback matrix для system-level сравнения.
4. Потом realistic corpus validation.
5. Только после этого принимать решение об оптимизации.

Критерий завершения:
- изменения в производительности обсуждаются на основе одинаковой методики, а
  не разовых локальных запусков.

## 11. Что не надо делать

- Не использовать обычные connectivity tests как основной источник bottleneck
  данных.
- Не делать вывод по одному прогону.
- Не оптимизировать только `prepare_dump`, если проблема наблюдается в
  production sender path.
- Не сравнивать цифры с разных машин без пометки.
- Не принимать loopback throughput за пользовательский throughput.
- Не делать perf regression gate слишком тяжелым для каждого PR без tiered
  режима.

## 12. Практические изменения в репозитории

### Что менять сначала

1. `benches/pipeline/stats.rs`
   Усилить структуру метрик и форму отчетности.
2. `benches/pipeline/loopback.rs`
   Усилить matrix methodology, repeats, shuffle и metadata.
3. `benches/pipeline/shared.rs`
   Добавить режим realistic corpus и manifest metadata.
4. Новый split-process benchmark runner
   Для раздельного профиля sender/receiver.
5. Новые stage benches
   Для receiver finalize, journal и production reader path.

### Что можно отложить

- сетевую эмуляцию;
- multi-host automation;
- тяжелые corpus runs в CI.

## 13. Предлагаемые acceptance criteria

План можно считать реализованным, когда выполнены все условия:

- для sender и receiver есть раздельные process-level метрики;
- для production path есть stage-level benchmark coverage;
- есть repeatable loopback matrix для comparative analysis;
- есть realistic corpus mode;
- результаты сохраняются в structured artifacts;
- по результатам можно однозначно локализовать хотя бы один bottleneck class:
  `scan`, `read`, `compress`, `send`, `ack_wait`, `receiver commit`,
  `journal`, `finalize`.

## 14. Краткий порядок внедрения

1. Укрепить текущий loopback matrix.
2. Разнести sender и receiver в измерениях.
3. Добавить недостающие stage benches.
4. Добавить realistic corpus mode.
5. Формализовать optimization workflow.

Итоговая идея простая: bottleneck analysis должен отвечать не на вопрос
"быстро или медленно?", а на вопрос "какая конкретная стадия ограничивает
систему, в каком режиме и на каком классе данных?".
