#[cfg(test)]
use super::PREFETCH_FILE_BYTES;
use super::{
    MaterializedEntry, ReadDecision, append_contents, finish_materializing_controlled,
    materialize_owned_entry_controlled, reopen_after_short_read, skipped_regular,
};
use crate::network::protocol::BundleEntryDescriptor;
use crate::runtime::SessionControl;
use crate::sender::scan::ScannedEntry;
use anyhow::Result;
use std::ops::{Deref, Range};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub(crate) const READ_BATCH_ENTRIES: usize = 4;
pub(crate) const PREFETCH_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct PrefetchBudget {
    pub(super) available: AtomicUsize,
}

impl PrefetchBudget {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            available: AtomicUsize::new(PREFETCH_PAYLOAD_BYTES),
        })
    }

    fn reserve_batch(
        self: &Arc<Self>,
        sizes: [usize; READ_BATCH_ENTRIES],
    ) -> ([bool; READ_BATCH_ENTRIES], Option<PrefetchReservation>) {
        let mut available = self.available.load(Ordering::Relaxed);
        loop {
            let mut reserved = 0;
            let selected = sizes.map(|size| {
                let fits = size != 0 && size <= available - reserved;
                if fits {
                    reserved += size;
                }
                fits
            });
            if reserved == 0 {
                return (selected, None);
            }
            match self.available.compare_exchange_weak(
                available,
                available - reserved,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return (
                        selected,
                        Some(PrefetchReservation {
                            budget: self.clone(),
                            bytes: reserved,
                            delivered: AtomicBool::new(false),
                        }),
                    );
                }
                Err(current) => available = current,
            }
        }
    }
}

#[derive(Debug)]
struct PrefetchReservation {
    budget: Arc<PrefetchBudget>,
    bytes: usize,
    delivered: AtomicBool,
}

impl PrefetchReservation {
    fn release(&mut self, bytes: usize) {
        self.bytes -= bytes;
        self.budget.available.fetch_add(bytes, Ordering::Relaxed);
    }

    fn deliver_slab(&self) {
        // Planner отдельно ограничивает собираемый bundle. После последней
        // выдачи slab больше не занимает read-ahead budget, даже если ещё живёт
        // в planner. Не ждём сжатия 64 MiB bundle при бюджете очереди 8 MiB.
        if !self.delivered.swap(true, Ordering::Relaxed) {
            self.budget
                .available
                .fetch_add(self.bytes, Ordering::Relaxed);
        }
    }
}

impl Drop for PrefetchReservation {
    fn drop(&mut self) {
        if !*self.delivered.get_mut() {
            self.budget
                .available
                .fetch_add(self.bytes, Ordering::Relaxed);
        }
    }
}

#[derive(Debug)]
pub(crate) struct SharedContents {
    bytes: Vec<u8>,
    // Пока последний элемент slab не передан planner, весь slab входит в budget,
    // включая уже выданный префикс. При отмене Drop освобождает reservation.
    _reservation: Option<PrefetchReservation>,
}

impl Deref for SharedContents {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

#[derive(Debug, Clone)]
pub(crate) enum EntryContents {
    Owned(Vec<u8>),
    Shared {
        bytes: Arc<SharedContents>,
        range: Range<usize>,
    },
}

impl EntryContents {
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared { bytes, range } => &bytes[range.clone()],
        }
    }

    pub(crate) fn delivered_from_prefetch(&self) {
        // Slab заполняется в порядке обхода и выдаётся в том же порядке.
        // Последний непустой slice освобождает budget одной атомарной операцией
        // на batch; предыдущим slices не нужен отдельный atomic counter.
        if let Self::Shared { bytes, range } = self
            && range.end == bytes.len()
            && let Some(reservation) = &bytes._reservation
        {
            reservation.deliver_slab();
        }
    }
}

impl From<Vec<u8>> for EntryContents {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Owned(bytes)
    }
}

impl Deref for EntryContents {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for EntryContents {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<T: AsRef<[u8]> + ?Sized> PartialEq<T> for EntryContents {
    fn eq(&self, other: &T) -> bool {
        self.as_slice() == other.as_ref()
    }
}

impl Eq for EntryContents {}

pub(crate) type ReadBatch = [Option<Result<ReadDecision>>; READ_BATCH_ENTRIES];

#[cfg(test)]
pub(crate) fn materialize_batch(
    entries: [Option<ScannedEntry>; READ_BATCH_ENTRIES],
    stream_threshold_bytes: u64,
) -> ReadBatch {
    finish_read_batch(open_read_batch(entries, None), stream_threshold_bytes)
}

#[cfg(test)]
pub(crate) fn materialize_budgeted_batch(
    entries: [Option<ScannedEntry>; READ_BATCH_ENTRIES],
    stream_threshold_bytes: u64,
    budget: &Arc<PrefetchBudget>,
) -> ReadBatch {
    materialize_budgeted_batch_controlled(entries, stream_threshold_bytes, budget, None)
}

pub(crate) fn materialize_budgeted_batch_controlled(
    entries: [Option<ScannedEntry>; READ_BATCH_ENTRIES],
    stream_threshold_bytes: u64,
    budget: &Arc<PrefetchBudget>,
    control: Option<&SessionControl>,
) -> ReadBatch {
    finish_read_batch_with_budget(
        open_read_batch(entries, control),
        stream_threshold_bytes,
        Some(budget),
        control,
    )
}

fn open_read_batch(
    entries: [Option<ScannedEntry>; READ_BATCH_ENTRIES],
    control: Option<&SessionControl>,
) -> ReadBatch {
    // Порог 0 сохраняет открытый handle каждого непустого файла. Суммируем
    // размеры именно этих handles; отдельный stat и повторный open не нужны.
    entries.map(|entry| entry.map(|entry| materialize_owned_entry_controlled(entry, 0, control)))
}

#[cfg(test)]
fn finish_read_batch(outcomes: ReadBatch, stream_threshold_bytes: u64) -> ReadBatch {
    finish_read_batch_with_budget(
        outcomes,
        stream_threshold_bytes.min(PREFETCH_FILE_BYTES),
        None,
        None,
    )
}

fn finish_read_batch_with_budget(
    mut outcomes: ReadBatch,
    stream_threshold_bytes: u64,
    budget: Option<&Arc<PrefetchBudget>>,
    control: Option<&SessionControl>,
) -> ReadBatch {
    struct SharedEntry {
        descriptor: BundleEntryDescriptor,
        mode: u32,
        range: Range<usize>,
    }

    let sizes = std::array::from_fn(|index| match &outcomes[index] {
        Some(Ok(ReadDecision::Streamed(file)))
            if file.descriptor.size_at_open <= stream_threshold_bytes =>
        {
            usize::try_from(file.descriptor.size_at_open).unwrap_or(usize::MAX)
        }
        _ => 0,
    });
    // Одна reservation на batch. Никогда не ждём память: planner может держать
    // предыдущие slabs до сжатия bundle; ожидание здесь создало бы взаимную блокировку.
    let (selected, mut reservation) = match budget {
        Some(budget) => budget.reserve_batch(sizes),
        None => (sizes.map(|size| size != 0), None),
    };
    let mut reserved: usize = sizes
        .iter()
        .zip(selected)
        .filter_map(|(&size, selected)| selected.then_some(size))
        .sum();
    let mut bytes = Vec::with_capacity(reserved);
    let mut shared_entries: [Option<SharedEntry>; READ_BATCH_ENTRIES] =
        std::array::from_fn(|_| None);
    for (index, outcome) in outcomes.iter_mut().enumerate() {
        if !selected[index] {
            continue;
        }
        if let Err(error) = crate::sender::check_control(control) {
            *outcome = Some(Err(error));
            reserved -= sizes[index];
            bytes.shrink_to(reserved);
            if let Some(reservation) = &mut reservation {
                reservation.release(sizes[index]);
            }
            continue;
        }
        let Some(Ok(ReadDecision::Streamed(mut file))) = outcome.take() else {
            unreachable!();
        };
        let start = bytes.len();
        let forced_limit = file.take_forced_short_read_limit();
        match append_contents(
            &mut file.file,
            file.descriptor.size_at_open,
            forced_limit,
            &mut bytes,
        ) {
            Ok(range) => {
                shared_entries[index] = Some(SharedEntry {
                    descriptor: file.descriptor,
                    mode: file.mode,
                    range,
                });
            }
            Err(_) => {
                bytes.truncate(start);
                reserved -= file.descriptor.size_at_open as usize;
                // Освобождаем reservation до Owned retry, чтобы оба payload
                // одновременно не удваивали ограниченное окно prefetch.
                bytes.shrink_to(reserved);
                if let Some(reservation) = &mut reservation {
                    reservation.release(file.descriptor.size_at_open as usize);
                }
                // Retry не меняет уже заполненный slab. Его новый size_at_open
                // может вырасти; редкий повтор читает в отдельный Owned payload.
                *outcome = Some(match reopen_after_short_read(&mut file, control) {
                    Err(error) => Err(error),
                    Ok(Some(reason)) => Ok(skipped_regular(file, reason)),
                    // После изменения размера planner продолжит уже открытую
                    // попытку; worker не выходит за общую reservation новым payload.
                    Ok(None) if budget.is_some() => Ok(ReadDecision::Streamed(file)),
                    Ok(None) => {
                        finish_materializing_controlled(file, stream_threshold_bytes, control)
                    }
                });
            }
        }
    }
    if bytes.len() != reserved {
        // После ошибок маленький успешный файл не должен удерживать reservation
        // больших неудачных соседей через границы bundles. Только этот путь
        // допускает перемещение данных при уменьшении allocation.
        bytes.shrink_to_fit();
    }
    if !bytes.is_empty() {
        // Arc<Vec> переносит allocation без копии payload; Arc<[u8]> скопировал бы её.
        let bytes = Arc::new(SharedContents {
            bytes,
            _reservation: reservation,
        });
        for (outcome, entry) in outcomes.iter_mut().zip(shared_entries) {
            if let Some(entry) = entry {
                *outcome = Some(Ok(ReadDecision::Accepted(MaterializedEntry {
                    descriptor: entry.descriptor,
                    mode: entry.mode,
                    contents: EntryContents::Shared {
                        bytes: bytes.clone(),
                        range: entry.range,
                    },
                })));
            }
        }
    }
    outcomes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::protocol::{BundleEntryKind, SkipReason};
    use crate::sender::read::{PREFETCH_FILE_BYTES, test_hooks};
    use crate::sender::read::{finish_materializing, read_contents, scanned_batch};
    use crate::tests::TestDir;
    use std::fs;
    use std::io::Write;
    fn accepted(outcome: Option<Result<ReadDecision>>) -> MaterializedEntry {
        let ReadDecision::Accepted(entry) = outcome.unwrap().unwrap() else {
            panic!("batch omitted a readable file");
        };
        entry
    }

    #[test]
    fn medium_prefetch_budget_transfers_to_planner_and_releases_on_cancel() {
        let dir = TestDir::new();
        const SIZE: usize = 300_000;
        for index in 0..4 {
            fs::write(dir.path().join(format!("{index}")), vec![index as u8; SIZE]).unwrap();
        }
        let budget = Arc::new(PrefetchBudget {
            available: AtomicUsize::new(2 * SIZE),
        });
        let [first, second, third, fourth] =
            materialize_budgeted_batch(scanned_batch(dir.path()), 1024 * 1024, &budget);
        let first = accepted(first);
        let second = accepted(second);
        assert_eq!(first.contents, vec![0; SIZE]);
        assert_eq!(second.contents, vec![1; SIZE]);
        assert_eq!(budget.available.load(Ordering::Relaxed), 0);
        let ReadDecision::Streamed(third) = third.unwrap().unwrap() else {
            panic!("prefetch exceeded its shared byte budget");
        };
        assert_eq!(third.open_attempts, 1);
        assert!(matches!(fourth, Some(Ok(ReadDecision::Streamed(_)))));
        fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("2"))
            .unwrap()
            .write_all(b"appended-tail")
            .unwrap();
        first.contents.delivered_from_prefetch();
        assert_eq!(budget.available.load(Ordering::Relaxed), 0);
        second.contents.delivered_from_prefetch();
        assert_eq!(budget.available.load(Ordering::Relaxed), 2 * SIZE);
        assert_eq!(first.contents, vec![0; SIZE]);
        assert_eq!(second.contents, vec![1; SIZE]);
        drop(first);
        drop(second);
        assert_eq!(budget.available.load(Ordering::Relaxed), 2 * SIZE);
        let ReadDecision::Accepted(third) = finish_materializing(third, 1024 * 1024).unwrap()
        else {
            panic!("fallback lost the previously opened file");
        };
        assert_eq!(third.contents, vec![2; SIZE]);

        let pending = materialize_budgeted_batch(scanned_batch(dir.path()), 1024 * 1024, &budget);
        assert_eq!(budget.available.load(Ordering::Relaxed), 0);
        drop(pending);
        assert_eq!(budget.available.load(Ordering::Relaxed), 2 * SIZE);
    }

    #[test]
    fn budgeted_short_read_releases_payload_without_resetting_attempts() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        let path = dir.path().join("file");
        fs::write(&path, vec![42; 300_000]).unwrap();
        for _ in 0..3 {
            test_hooks::force_short_read_after_next_open(&path, 2);
        }
        let budget = PrefetchBudget::new();
        let [file, _, _, _] =
            materialize_budgeted_batch(scanned_batch(dir.path()), 1024 * 1024, &budget);
        let ReadDecision::Streamed(file) = file.unwrap().unwrap() else {
            panic!("failed prefetch must defer the reopened attempt");
        };
        assert_eq!(file.open_attempts, 2);
        assert_eq!(
            budget.available.load(Ordering::Relaxed),
            PREFETCH_PAYLOAD_BYTES
        );
        let ReadDecision::Skipped(skipped) = finish_materializing(file, 1024 * 1024).unwrap()
        else {
            panic!("prefetch reset the shared three-attempt limit");
        };
        assert_eq!(skipped.reason, SkipReason::UnstableAfterRetries);
        test_hooks::clear();
    }

    #[test]
    fn batch_shares_exact_payload_allocation_and_outlives_other_entries() {
        let dir = TestDir::new();
        for (name, contents) in [
            ("a", b"abc".as_slice()),
            ("b", b""),
            ("c", b"12345"),
            ("d", b"tail"),
        ] {
            fs::write(dir.path().join(name), contents).unwrap();
        }
        let mut entries = materialize_batch(scanned_batch(dir.path()), 1024).map(accepted);
        let EntryContents::Shared { bytes, range } = &entries[0].contents else {
            panic!("nonempty batch did not share payload");
        };
        assert_eq!(range, &(0..3));
        assert_eq!(bytes.len(), 12);
        assert_eq!(bytes.capacity(), 12);
        let weak = Arc::downgrade(bytes);
        for (index, expected) in [(2, b"12345".as_slice()), (3, b"tail")] {
            let EntryContents::Shared {
                bytes: other,
                range,
            } = &entries[index].contents
            else {
                panic!("batch allocated per-file payload");
            };
            assert!(Arc::ptr_eq(bytes, other));
            assert_eq!(&other[range.clone()], expected);
        }
        assert!(matches!(&entries[1].contents, EntryContents::Owned(bytes) if bytes.is_empty()));
        let retained = std::mem::replace(&mut entries[2].contents, Vec::new().into());
        drop(entries);
        assert_eq!(retained, b"12345");
        assert_eq!(weak.strong_count(), 1);
        drop(retained);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn batch_snapshot_ignores_append_and_retry_growth_uses_fresh_open_size() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        for name in ["a", "b", "c", "d"] {
            fs::write(dir.path().join(name), b"original").unwrap();
        }
        test_hooks::force_short_read_after_next_open(&dir.path().join("b"), 2);
        let opened = open_read_batch(scanned_batch(dir.path()), None);
        fs::OpenOptions::new()
            .append(true)
            .open(dir.path().join("a"))
            .unwrap()
            .write_all(b"-ignored-tail")
            .unwrap();
        fs::write(dir.path().join("b"), vec![0x5a; 2048]).unwrap();
        fs::write(dir.path().join("c"), b"tiny").unwrap();
        let [first, grown, shrunk, last] = finish_read_batch(opened, 1024);
        let first = accepted(first);
        let last = accepted(last);
        assert_eq!(first.contents, b"original");
        assert_eq!(first.descriptor.size_at_open, 8);
        assert_eq!(last.contents, b"original");
        let EntryContents::Shared { bytes, .. } = &first.contents else {
            panic!();
        };
        assert_eq!(bytes.capacity(), 16);
        let ReadDecision::Streamed(mut grown) = grown.unwrap().unwrap() else {
            panic!("retry growth must defer large payload");
        };
        assert_eq!(grown.open_attempts, 2);
        assert_eq!(grown.remaining_attempts(), 2);
        assert_eq!(grown.descriptor.size_at_open, 2048);
        assert!(grown.descriptor.best_effort);
        assert_eq!(
            read_contents(&mut grown.file, 2048, None).unwrap(),
            vec![0x5a; 2048]
        );
        let shrunk = accepted(shrunk);
        assert_eq!(shrunk.contents, b"tiny");
        assert_eq!(shrunk.descriptor.size_at_open, 4);
        assert!(shrunk.descriptor.best_effort);
        assert!(matches!(shrunk.contents, EntryContents::Owned(_)));
        test_hooks::clear();
    }

    #[test]
    fn batch_errors_preserve_three_attempt_budget_and_release_unused_capacity() {
        let _guard = test_hooks::lock();
        test_hooks::clear();
        let dir = TestDir::new();
        for name in ["a", "b", "c"] {
            fs::write(
                dir.path().join(name),
                vec![0xa5; PREFETCH_FILE_BYTES as usize],
            )
            .unwrap();
        }
        fs::write(dir.path().join("d"), b"ok").unwrap();
        let first = dir.path().join("a");
        test_hooks::fail_next_open(&first);
        test_hooks::fail_next_open(&first);
        test_hooks::force_short_read_after_next_open(&first, 2);
        let second = dir.path().join("b");
        for limit in [2, 3, 4, 99] {
            test_hooks::force_short_read_after_next_open(&second, limit);
        }
        let third = dir.path().join("c");
        for _ in 0..3 {
            test_hooks::fail_next_open(&third);
        }
        let [first, second_result, third, last] =
            materialize_batch(scanned_batch(dir.path()), PREFETCH_FILE_BYTES);
        for (outcome, reason) in [
            (first, SkipReason::UnstableAfterRetries),
            (second_result, SkipReason::UnstableAfterRetries),
            (third, SkipReason::OpenFailed),
        ] {
            let ReadDecision::Skipped(skipped) = outcome.unwrap().unwrap() else {
                panic!("batch retried an exhausted file");
            };
            assert_eq!(skipped.reason, reason);
        }
        assert_eq!(test_hooks::forced_short_read_after_open(&second), Some(99));
        let last = accepted(last);
        assert_eq!(last.contents, b"ok");
        let EntryContents::Shared { bytes, .. } = last.contents else {
            panic!();
        };
        assert_eq!(bytes.capacity(), 2);
        test_hooks::clear();
    }

    #[test]
    fn batch_prefetch_cap_defers_large_handles_and_preserves_empty_entries() {
        let dir = TestDir::new();
        fs::write(dir.path().join("a"), []).unwrap();
        fs::write(
            dir.path().join("b"),
            vec![0x31; PREFETCH_FILE_BYTES as usize],
        )
        .unwrap();
        fs::write(
            dir.path().join("c"),
            vec![0x32; PREFETCH_FILE_BYTES as usize + 1],
        )
        .unwrap();
        let [empty, small, large, tail] = materialize_batch(scanned_batch(dir.path()), u64::MAX);
        assert!(accepted(empty).contents.is_empty());
        let small = accepted(small);
        assert_eq!(small.contents.len(), PREFETCH_FILE_BYTES as usize);
        let EntryContents::Shared { bytes, .. } = small.contents else {
            panic!();
        };
        assert_eq!(bytes.capacity(), PREFETCH_FILE_BYTES as usize);
        let ReadDecision::Streamed(file) = large.unwrap().unwrap() else {
            panic!();
        };
        assert_eq!(file.open_attempts, 1);
        assert_eq!(file.descriptor.size_at_open, PREFETCH_FILE_BYTES + 1);
        assert!(tail.is_none());
    }

    #[test]
    fn batch_keeps_symlink_target_without_reading_its_contents() {
        let dir = TestDir::new();
        fs::write(dir.path().join("a"), b"target bytes").unwrap();
        let link = dir.path().join("b-link");
        #[cfg(unix)]
        std::os::unix::fs::symlink("a", &link).unwrap();
        #[cfg(windows)]
        if std::os::windows::fs::symlink_file("a", &link).is_err() {
            return;
        }
        let [file, symlink, tail, last] = materialize_batch(scanned_batch(dir.path()), 1024);
        assert_eq!(accepted(file).contents, b"target bytes");
        let symlink = accepted(symlink);
        assert_eq!(
            symlink.descriptor.kind,
            BundleEntryKind::Symlink { target: "a".into() }
        );
        assert_eq!(symlink.descriptor.size_at_open, 0);
        assert!(matches!(&symlink.contents, EntryContents::Owned(bytes) if bytes.is_empty()));
        assert!(tail.is_none());
        assert!(last.is_none());
    }
}
