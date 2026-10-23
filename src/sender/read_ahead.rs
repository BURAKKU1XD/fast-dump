use std::collections::{BTreeSet, VecDeque};
use std::thread::{self, JoinHandle};

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, bounded};

use super::read::{
    PrefetchBudget, READ_BATCH_ENTRIES, ReadBatch, ReadDecision, finish_materializing,
    materialize_budgeted_batch, materialize_owned_entry,
};
use crate::network::protocol::SkippedEntry;

use super::scan::{ScanProgress, ScannedEntry, StreamingScan};

// Окно ограничивает handles; отдельный общий byte budget ограничивает payload
// независимо от числа readers. При нехватке памяти planner дочитает открытый handle.
const JOBS_PER_READER: usize = 4;

type Job = (usize, [Option<ScannedEntry>; READ_BATCH_ENTRIES]);
type Outcome = (usize, Result<ReadBatch>);

pub(crate) struct OrderedReader {
    entries: EntrySource,
    lookahead: VecDeque<ScannedEntry>,
    stream_threshold: u64,
    parallel: Option<ParallelReader>,
}

enum EntrySource {
    #[cfg(any(test, feature = "bench-internals"))]
    Buffered(std::vec::IntoIter<ScannedEntry>),
    Streaming {
        scan: StreamingScan,
        terminal_paths: BTreeSet<String>,
    },
}

impl EntrySource {
    fn next(&mut self) -> Result<Option<ScannedEntry>> {
        match self {
            #[cfg(any(test, feature = "bench-internals"))]
            Self::Buffered(entries) => Ok(entries.next()),
            Self::Streaming {
                scan,
                terminal_paths,
            } => {
                while let Some(entry) = scan.next_pending_entry()? {
                    if !terminal_paths.contains(&entry.relative_path) {
                        return Ok(Some(entry));
                    }
                }
                Ok(None)
            }
        }
    }
}

struct ParallelReader {
    jobs: Option<Sender<Job>>,
    results: Option<Receiver<Outcome>>,
    workers: Vec<JoinHandle<()>>,
    ready: Vec<Option<Result<ReadDecision>>>,
    submitted: usize,
    consumed: usize,
}

impl OrderedReader {
    #[cfg(any(test, feature = "bench-internals"))]
    pub(crate) fn new(
        entries: Vec<ScannedEntry>,
        readers: usize,
        stream_threshold: u64,
    ) -> Result<Self> {
        anyhow::ensure!(readers > 0, "reader count must be greater than zero");
        let count = entries.len();
        Self::start(
            EntrySource::Buffered(entries.into_iter()),
            VecDeque::new(),
            count,
            readers,
            stream_threshold,
        )
    }

    pub(crate) fn from_stream(
        scan: StreamingScan,
        terminal_paths: BTreeSet<String>,
        readers: usize,
        stream_threshold: u64,
    ) -> Result<Self> {
        anyhow::ensure!(readers > 0, "reader count must be greater than zero");
        let limit = readers.saturating_mul(32);
        let mut entries = EntrySource::Streaming {
            scan,
            terminal_paths,
        };
        let mut lookahead = VecDeque::new();
        // До запуска IO узнаём ровно столько pending entries, сколько нужно
        // для прежнего ceil(pending / 32), не собирая весь manifest в памяти.
        while lookahead.len() < limit {
            let Some(entry) = entries.next()? else {
                break;
            };
            lookahead.push_back(entry);
        }
        let count = lookahead.len();
        Self::start(entries, lookahead, count, readers, stream_threshold)
    }

    fn start(
        entries: EntrySource,
        lookahead: VecDeque<ScannedEntry>,
        count: usize,
        readers: usize,
        stream_threshold: u64,
    ) -> Result<Self> {
        // Поток должен обработать достаточно файлов, чтобы окупить свой запуск.
        // Лимит окна остаётся отдельным: только четыре результата на worker.
        let readers = readers.min(count.div_ceil(32)).max(1);
        let mut reader = Self {
            entries,
            lookahead,
            stream_threshold,
            parallel: None,
        };
        // Для нескольких больших файлов запуск пула дороже полезной работы.
        if readers == 1 {
            return Ok(reader);
        }
        let window = readers * JOBS_PER_READER;
        let (job_tx, job_rx) = bounded::<Job>(readers);
        let (result_tx, result_rx) = bounded::<Outcome>(readers);
        let mut parallel = ParallelReader {
            jobs: Some(job_tx),
            results: Some(result_rx),
            workers: Vec::with_capacity(readers),
            ready: (0..window).map(|_| None).collect(),
            submitted: 0,
            consumed: 0,
        };
        let budget = PrefetchBudget::new();
        for _ in 0..readers {
            let jobs = job_rx.clone();
            let results = result_tx.clone();
            let budget = budget.clone();
            parallel.workers.push(
                thread::Builder::new()
                    .name("dump-reader".into())
                    .spawn(move || {
                        while let Ok((index, entries)) = jobs.recv() {
                            let result = std::panic::catch_unwind(|| {
                                materialize_budgeted_batch(entries, stream_threshold, &budget)
                            })
                            .map_err(|_| anyhow::anyhow!("file reader panicked"));
                            if results.send((index, result)).is_err() {
                                break;
                            }
                        }
                    })
                    .context("failed to start file reader")?,
            );
        }
        drop(job_rx);
        drop(result_tx);
        reader.parallel = Some(parallel);
        reader.fill_window()?;
        Ok(reader)
    }

    fn fill_window(&mut self) -> Result<()> {
        loop {
            let Some(parallel) = &self.parallel else {
                return Ok(());
            };
            let available = parallel.ready.len() - (parallel.submitted - parallel.consumed);
            // Ждём место для целой четвёрки, иначе после initial window каждое
            // next породит single-entry batch. Частичный batch допустим только в EOF.
            if available < READ_BATCH_ENTRIES {
                break;
            }
            let mut entries = std::array::from_fn(|_| None);
            let mut count = 0;
            for slot in &mut entries {
                let Some(entry) = self.next_entry()? else {
                    break;
                };
                *slot = Some(entry);
                count += 1;
            }
            if count == 0 {
                break;
            }
            let parallel = self.parallel.as_mut().unwrap();
            parallel
                .jobs
                .as_ref()
                .unwrap()
                .send((parallel.submitted, entries))
                .context("file reader stopped before receiving work")?;
            parallel.submitted += count;
        }
        Ok(())
    }

    fn next_entry(&mut self) -> Result<Option<ScannedEntry>> {
        match self.lookahead.pop_front() {
            Some(entry) => Ok(Some(entry)),
            None => self.entries.next(),
        }
    }

    pub(crate) fn take_scan_skips(&mut self) -> Vec<SkippedEntry> {
        match &mut self.entries {
            EntrySource::Streaming { scan, .. } => scan.take_skips(),
            #[cfg(any(test, feature = "bench-internals"))]
            EntrySource::Buffered(_) => Vec::new(),
        }
    }

    pub(crate) fn scan_progress(&self) -> ScanProgress {
        match &self.entries {
            EntrySource::Streaming { scan, .. } => scan.progress(),
            #[cfg(any(test, feature = "bench-internals"))]
            EntrySource::Buffered(_) => ScanProgress::default(),
        }
    }

    pub(crate) fn next(&mut self) -> Result<Option<ReadDecision>> {
        let Some(parallel) = &mut self.parallel else {
            return self
                .next_entry()?
                .map(|entry| materialize_owned_entry(entry, self.stream_threshold))
                .transpose();
        };
        if parallel.consumed == parallel.submitted {
            return Ok(None);
        }
        let slot = parallel.consumed % parallel.ready.len();
        let decision = loop {
            if let Some(result) = parallel.ready[slot].take() {
                break result?;
            }
            let (index, result) = parallel
                .results
                .as_ref()
                .unwrap()
                .recv()
                .context("file readers stopped before completing work")?;
            for (offset, result) in result?.into_iter().enumerate() {
                if let Some(result) = result {
                    let result_slot = (index + offset) % parallel.ready.len();
                    parallel.ready[result_slot] = Some(result);
                }
            }
        };
        if let ReadDecision::Accepted(entry) = &decision {
            entry.contents.delivered_from_prefetch();
        }
        parallel.consumed += 1;
        self.fill_window()?;
        match decision {
            ReadDecision::Streamed(file)
                if file.descriptor.size_at_open <= self.stream_threshold =>
            {
                finish_materializing(file, self.stream_threshold).map(Some)
            }
            decision => Ok(Some(decision)),
        }
    }
}

impl Drop for ParallelReader {
    fn drop(&mut self) {
        // При обрыве сети снимаем обе стороны backpressure до join, иначе
        // worker может навсегда остаться в send результата.
        self.jobs.take();
        self.results.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sender::read::EntryContents;
    use crate::sender::read::test_hooks;
    use crate::sender::scan::scan_source_tree;
    use crate::tests::TestDir;
    use std::fs;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn streamed_source_keeps_worker_cap_and_filters_known_terminal_paths() {
        let dir = TestDir::new();
        for index in 0..129 {
            fs::write(dir.path().join(format!("{index:03}")), [index as u8; 17]).unwrap();
        }
        for pending in [0_usize, 1, 32, 33, 65, 129] {
            let terminal = (pending..129).map(|index| format!("{index:03}")).collect();
            let scan = StreamingScan::new(dir.path()).unwrap();
            let mut reader = OrderedReader::from_stream(scan, terminal, 4, 1024).unwrap();
            let expected_workers = 4.min(pending.div_ceil(32)).max(1);
            assert_eq!(
                reader
                    .parallel
                    .as_ref()
                    .map_or(1, |parallel| parallel.workers.len()),
                expected_workers
            );
            assert!(reader.lookahead.len() <= 4 * 32);
            for index in 0..pending {
                let Some(ReadDecision::Accepted(entry)) = reader.next().unwrap() else {
                    panic!("streamed scan lost a pending entry");
                };
                assert_eq!(entry.descriptor.work_ordinal, index as u64 + 1);
                assert_eq!(entry.contents, [index as u8; 17]);
            }
            assert!(reader.next().unwrap().is_none());
            assert_eq!(reader.scan_progress().discovered_entries, 129);
        }
    }

    #[test]
    fn streamed_lookahead_does_not_collect_the_complete_tree() {
        let dir = TestDir::new();
        for index in 0..193 {
            fs::write(dir.path().join(format!("{index:03}")), []).unwrap();
        }
        let scan = StreamingScan::new(dir.path()).unwrap();
        let reader = OrderedReader::from_stream(scan, BTreeSet::new(), 2, 1024).unwrap();
        assert_eq!(reader.scan_progress().discovered_entries, 64);
        assert_eq!(reader.lookahead.len(), 64 - 2 * JOBS_PER_READER);
        assert_eq!(reader.parallel.as_ref().unwrap().workers.len(), 2);
        drop(reader);
        fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn streamed_tiny_source_caps_even_maximum_requested_reader_count() {
        let dir = TestDir::new();
        for index in 0..3 {
            fs::write(dir.path().join(format!("{index}")), [index as u8; 17]).unwrap();
        }
        let scan = StreamingScan::new(dir.path()).unwrap();
        let mut reader =
            OrderedReader::from_stream(scan, BTreeSet::new(), usize::MAX, 1024).unwrap();
        assert!(reader.parallel.is_none());
        for index in 0..3 {
            let Some(ReadDecision::Accepted(entry)) = reader.next().unwrap() else {
                panic!("large requested reader count lost a tiny source entry");
            };
            assert_eq!(entry.descriptor.work_ordinal, index + 1);
            assert_eq!(entry.contents, [index as u8; 17]);
        }
        assert!(reader.next().unwrap().is_none());
    }

    #[cfg(unix)]
    #[test]
    fn streamed_reader_never_opens_a_file_colliding_with_late_scan_skip() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        fs::create_dir(dir.path().join("a")).unwrap();
        let suppressed = dir.path().join("a/b");
        fs::write(&suppressed, b"must not open").unwrap();
        fs::write(dir.path().join("z"), b"keep").unwrap();
        let _socket = std::os::unix::net::UnixListener::bind(dir.path().join("a\\b")).unwrap();
        test_hooks::fail_next_open(&suppressed);
        let scan = StreamingScan::new(dir.path()).unwrap();
        let mut reader = OrderedReader::from_stream(scan, BTreeSet::new(), 4, 1024).unwrap();
        let Some(ReadDecision::Accepted(entry)) = reader.next().unwrap() else {
            panic!("expected the non-colliding file");
        };
        assert_eq!(entry.descriptor.relative_path, "z");
        assert_eq!(entry.descriptor.work_ordinal, 2);
        assert!(reader.next().unwrap().is_none());
        assert_eq!(reader.scan_progress().discovered_entries, 2);
        let skips = reader.take_scan_skips();
        assert_eq!(skips.len(), 1);
        assert_eq!(skips[0].relative_path, "a/b");
        assert!(test_hooks::before_open(&suppressed).is_err());
        test_hooks::clear();
    }

    #[test]
    fn parallel_reads_preserve_every_byte_and_dfs_order_across_windows() {
        let dir = TestDir::new();
        for index in 0..160 {
            let size = if index % 31 == 0 {
                300_000
            } else {
                257 + index
            };
            fs::write(
                dir.path().join(format!("{index:03}.bin")),
                vec![index as u8; size],
            )
            .unwrap();
        }
        let scan = scan_source_tree(dir.path()).unwrap();
        let mut parallel = OrderedReader::new(scan.entries.clone(), 8, 1_048_576).unwrap();
        let mut serial = OrderedReader::new(scan.entries, 1, 1_048_576).unwrap();
        let mut count = 0;
        while let Some(ReadDecision::Accepted(expected)) = serial.next().unwrap() {
            let Some(ReadDecision::Accepted(actual)) = parallel.next().unwrap() else {
                panic!("parallel reader lost a regular file");
            };
            assert_eq!(actual.descriptor, expected.descriptor);
            assert_eq!(actual.contents, expected.contents);
            count += 1;
        }
        assert_eq!(count, 160);
        assert!(parallel.next().unwrap().is_none());
    }

    #[test]
    fn dropping_read_ahead_with_full_queues_joins_workers() {
        let dir = TestDir::new();
        for index in 0..64 {
            fs::write(dir.path().join(format!("{index}.bin")), [42; 512]).unwrap();
        }
        let scan = scan_source_tree(dir.path()).unwrap();
        let reader = OrderedReader::new(scan.entries, 4, 1024).unwrap();
        drop(reader);
        // После drop не остаётся открытых worker handles на исходные файлы.
        fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn read_ahead_keeps_terminal_failures_in_order() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        for index in 0..64 {
            fs::write(
                dir.path().join(format!("{index:02}.bin")),
                [index as u8; 128],
            )
            .unwrap();
        }
        let failed = dir.path().join("17.bin");
        for _ in 0..3 {
            test_hooks::fail_next_open(&failed);
        }
        let scan = scan_source_tree(dir.path()).unwrap();
        let mut reader = OrderedReader::new(scan.entries, 4, 1024).unwrap();
        for index in 0..64 {
            let decision = reader.next().unwrap().unwrap();
            if index == 17 {
                let ReadDecision::Skipped(entry) = decision else {
                    panic!("missing terminal skip");
                };
                assert_eq!(entry.work_ordinal, 18);
            } else {
                let ReadDecision::Accepted(entry) = decision else {
                    panic!("file omitted");
                };
                assert_eq!(entry.contents, vec![index as u8; 128]);
            }
        }
        assert!(reader.next().unwrap().is_none());
        drop(reader);
        test_hooks::clear();
    }

    #[test]
    fn partial_final_batch_preserves_every_entry() {
        let dir = TestDir::new();
        for index in 0..35 {
            fs::write(
                dir.path().join(format!("{index:02}.bin")),
                [index as u8; 17],
            )
            .unwrap();
        }
        let scan = scan_source_tree(dir.path()).unwrap();
        let mut reader = OrderedReader::new(scan.entries, 8, 1024).unwrap();
        for index in 0..35 {
            let Some(ReadDecision::Accepted(entry)) = reader.next().unwrap() else {
                panic!("file omitted from partial batch");
            };
            assert_eq!(entry.descriptor.work_ordinal, index + 1);
            assert_eq!(entry.contents, [index as u8; 17]);
        }
        assert!(reader.next().unwrap().is_none());
    }

    #[test]
    fn window_refills_whole_batches_and_keeps_partial_tail_in_order() {
        let dir = TestDir::new();
        for index in 0..67 {
            fs::write(dir.path().join(format!("{index:03}")), [index as u8; 17]).unwrap();
        }
        let scan = scan_source_tree(dir.path()).unwrap();
        let mut reader = OrderedReader::new(scan.entries, 2, 1024).unwrap();
        assert_eq!(reader.parallel.as_ref().unwrap().submitted, 8);
        let mut owner = None;
        for index in 0..67 {
            let Some(ReadDecision::Accepted(entry)) = reader.next().unwrap() else {
                panic!();
            };
            assert_eq!(entry.descriptor.work_ordinal, index + 1);
            assert_eq!(entry.contents, [index as u8; 17]);
            let EntryContents::Shared { bytes, .. } = &entry.contents else {
                panic!("parallel batch fell back to per-file allocations");
            };
            if index % READ_BATCH_ENTRIES as u64 == 0 {
                owner = Some(bytes.clone());
            } else {
                assert!(Arc::ptr_eq(owner.as_ref().unwrap(), bytes));
            }
            assert_eq!(bytes.capacity(), if index < 64 { 4 * 17 } else { 3 * 17 });
            let parallel = reader.parallel.as_ref().unwrap();
            assert!(parallel.submitted - parallel.consumed <= parallel.ready.len());
            if index < 3 {
                assert_eq!(parallel.submitted, 8);
            }
            if index == 3 {
                assert_eq!(parallel.submitted, 12);
            }
        }
        assert!(reader.next().unwrap().is_none());
    }

    #[test]
    fn shared_payload_survives_full_queue_cancellation_until_last_owner_drops() {
        let dir = TestDir::new();
        for index in 0..64 {
            fs::write(dir.path().join(format!("{index:03}")), [index as u8; 31]).unwrap();
        }
        let scan = scan_source_tree(dir.path()).unwrap();
        let mut reader = OrderedReader::new(scan.entries, 2, 1024).unwrap();
        let parallel = reader.parallel.as_ref().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while parallel.results.as_ref().unwrap().len() != parallel.workers.len() {
            assert!(
                Instant::now() < deadline,
                "workers did not fill bounded results queue"
            );
            thread::yield_now();
        }
        let Some(ReadDecision::Accepted(retained)) = reader.next().unwrap() else {
            panic!();
        };
        let EntryContents::Shared { bytes, .. } = &retained.contents else {
            panic!();
        };
        let weak = Arc::downgrade(bytes);
        drop(reader);
        assert_eq!(retained.contents, [0; 31]);
        assert_eq!(weak.strong_count(), 1);
        drop(retained);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn serial_reader_keeps_owned_payload_without_shared_batch_allocation() {
        let dir = TestDir::new();
        fs::write(dir.path().join("one"), b"owned").unwrap();
        let scan = scan_source_tree(dir.path()).unwrap();
        let mut reader = OrderedReader::new(scan.entries, 1, 1024).unwrap();
        let Some(ReadDecision::Accepted(entry)) = reader.next().unwrap() else {
            panic!();
        };
        assert!(matches!(entry.contents, EntryContents::Owned(bytes) if bytes == b"owned"));
        assert!(reader.next().unwrap().is_none());
    }
}
