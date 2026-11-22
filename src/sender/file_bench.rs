//! Изолированные измерения реального обхода и reader без cfg(test) hooks.

use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail, ensure};

use crate::checksum::Xxh3;

use super::read::{MaterializedEntry, ReadDecision};
use super::read_ahead::OrderedReader;
use super::scan::{ScannedEntry, ScannedEntryKind, scan_source_tree};
use crate::network::protocol::BundleEntryKind;

const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_FIXTURE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone, Copy, Default)]
pub struct AllocationCounts {
    pub allocs: u64,
    pub alloc_bytes: u64,
    pub reallocs: u64,
    pub deallocs: u64,
    pub alloc_successes: u64,
    pub alloc_success_bytes: u64,
    pub realloc_successes: u64,
    pub realloc_success_bytes: u64,
    pub alloc_failures: u64,
    pub realloc_failures: u64,
    pub dealloc_bytes: u64,
    pub zeroed_bytes: u64,
}

#[derive(Clone, Copy)]
pub struct AllocationHooks {
    pub start: fn(),
    pub stop: fn() -> AllocationCounts,
}

#[derive(Default)]
pub struct Measurement {
    pub entries: u64,
    pub files: u64,
    pub bytes: u64,
    pub elapsed_ns: u64,
    pub work_ns: u64,
    pub drop_ns: u64,
    pub user_ns: u64,
    pub kernel_ns: u64,
    pub voluntary_switches: Option<u64>,
    pub involuntary_switches: Option<u64>,
    pub minor_faults: Option<u64>,
    pub major_faults: Option<u64>,
    pub page_faults: Option<u64>,
    pub cpu_cycles: Option<u64>,
    pub cycle_source: &'static str,
    pub io_source: &'static str,
    pub io_read_ops: Option<u64>,
    pub io_write_ops: Option<u64>,
    pub io_other_ops: Option<u64>,
    pub io_read_bytes: Option<u64>,
    pub io_write_bytes: Option<u64>,
    pub io_other_bytes: Option<u64>,
    pub io_read_ops_raw: Option<u64>,
    pub io_read_bytes_raw: Option<u64>,
    pub io_observer_read_ops: Option<u64>,
    pub io_observer_read_bytes: Option<u64>,
    pub storage_read_bytes: Option<u64>,
    pub storage_write_bytes: Option<u64>,
    pub process_peak_rss_bytes: u64,
    pub fingerprint: u64,
    pub allocations: Option<AllocationCounts>,
}

#[derive(Default)]
struct ProcessSnapshot {
    user_ns: u64,
    kernel_ns: u64,
    voluntary_switches: Option<u64>,
    involuntary_switches: Option<u64>,
    minor_faults: Option<u64>,
    major_faults: Option<u64>,
    page_faults: Option<u64>,
    cpu_cycles: Option<u64>,
    io_read_ops: Option<u64>,
    io_write_ops: Option<u64>,
    io_other_ops: Option<u64>,
    io_read_bytes: Option<u64>,
    io_write_bytes: Option<u64>,
    io_other_bytes: Option<u64>,
    io_observer_read_ops: Option<u64>,
    io_observer_read_bytes: Option<u64>,
    storage_read_bytes: Option<u64>,
    storage_write_bytes: Option<u64>,
    peak_rss_bytes: u64,
}

struct Interval {
    wall_ns: u64,
    before: ProcessSnapshot,
    after: ProcessSnapshot,
    allocations: Option<AllocationCounts>,
}

#[derive(Clone, Copy)]
enum SnapshotEdge {
    Before,
    After,
}

struct AllocationGuard(Option<AllocationHooks>);

impl AllocationGuard {
    fn start(hooks: Option<AllocationHooks>) -> Self {
        if let Some(hooks) = hooks {
            (hooks.start)();
        }
        Self(hooks)
    }

    fn stop(&mut self) -> Option<AllocationCounts> {
        self.0.take().map(|hooks| (hooks.stop)())
    }
}

impl Drop for AllocationGuard {
    fn drop(&mut self) {
        // Ошибка или panic не должны оставлять instrumentation включённым.
        self.stop();
    }
}

fn timed<T>(
    hooks: Option<AllocationHooks>,
    work: impl FnOnce() -> Result<T>,
) -> Result<(T, Interval)> {
    let before = process_snapshot(SnapshotEdge::Before)?;
    let mut allocation_guard = AllocationGuard::start(hooks);
    let started = Instant::now();
    let result = work();
    let wall_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
    let allocations = allocation_guard.stop();
    let after = process_snapshot(SnapshotEdge::After)?;
    Ok((
        result?,
        Interval {
            wall_ns,
            before,
            after,
            allocations,
        },
    ))
}

fn sum_delta(
    first: &Interval,
    second: &Interval,
    value: fn(&ProcessSnapshot) -> Option<u64>,
) -> Option<u64> {
    value(&first.after)?
        .checked_sub(value(&first.before)?)?
        .checked_add(value(&second.after)?.checked_sub(value(&second.before)?)?)
}

fn page_fault_delta(before: Option<u64>, after: Option<u64>) -> Option<u64> {
    #[cfg(windows)]
    {
        // Win32 PageFaultCount имеет ширину DWORD и может обернуться между снимками.
        Some((u32::try_from(after?).ok()?).wrapping_sub(u32::try_from(before?).ok()?) as u64)
    }
    #[cfg(not(windows))]
    after?.checked_sub(before?)
}

fn finish_measurement(mut measurement: Measurement, work: Interval, drop: Interval) -> Measurement {
    measurement.elapsed_ns = work.wall_ns + drop.wall_ns;
    measurement.work_ns = work.wall_ns;
    measurement.drop_ns = drop.wall_ns;
    measurement.user_ns = work.after.user_ns.saturating_sub(work.before.user_ns)
        + drop.after.user_ns.saturating_sub(drop.before.user_ns);
    measurement.kernel_ns = work.after.kernel_ns.saturating_sub(work.before.kernel_ns)
        + drop.after.kernel_ns.saturating_sub(drop.before.kernel_ns);
    measurement.voluntary_switches = sum_delta(&work, &drop, |value| value.voluntary_switches);
    measurement.involuntary_switches = sum_delta(&work, &drop, |value| value.involuntary_switches);
    measurement.minor_faults = sum_delta(&work, &drop, |value| value.minor_faults);
    measurement.major_faults = sum_delta(&work, &drop, |value| value.major_faults);
    measurement.page_faults = page_fault_delta(work.before.page_faults, work.after.page_faults)
        .zip(page_fault_delta(
            drop.before.page_faults,
            drop.after.page_faults,
        ))
        .and_then(|(a, b)| a.checked_add(b));
    measurement.cpu_cycles = sum_delta(&work, &drop, |value| value.cpu_cycles);
    measurement.cycle_source = if cfg!(windows) {
        "windows-process-cycle-time"
    } else {
        "unavailable"
    };
    measurement.io_source = if cfg!(windows) {
        "windows-process-io"
    } else if cfg!(target_os = "linux") {
        "linux-proc-io-corrected"
    } else {
        "unavailable"
    };
    measurement.io_read_ops_raw = sum_delta(&work, &drop, |value| value.io_read_ops);
    measurement.io_read_bytes_raw = sum_delta(&work, &drop, |value| value.io_read_bytes);
    // Linux snapshot ещё не содержит свой read: в delta входит только read начального снимка.
    measurement.io_observer_read_ops = work
        .before
        .io_observer_read_ops
        .zip(drop.before.io_observer_read_ops)
        .and_then(|(a, b)| a.checked_add(b));
    measurement.io_observer_read_bytes = work
        .before
        .io_observer_read_bytes
        .zip(drop.before.io_observer_read_bytes)
        .and_then(|(a, b)| a.checked_add(b));
    measurement.io_read_ops = measurement
        .io_read_ops_raw
        .zip(measurement.io_observer_read_ops)
        .and_then(|(raw, observer)| raw.checked_sub(observer));
    measurement.io_read_bytes = measurement
        .io_read_bytes_raw
        .zip(measurement.io_observer_read_bytes)
        .and_then(|(raw, observer)| raw.checked_sub(observer));
    measurement.io_write_ops = sum_delta(&work, &drop, |value| value.io_write_ops);
    measurement.io_other_ops = sum_delta(&work, &drop, |value| value.io_other_ops);
    measurement.io_write_bytes = sum_delta(&work, &drop, |value| value.io_write_bytes);
    measurement.io_other_bytes = sum_delta(&work, &drop, |value| value.io_other_bytes);
    measurement.storage_read_bytes = sum_delta(&work, &drop, |value| value.storage_read_bytes);
    measurement.storage_write_bytes = sum_delta(&work, &drop, |value| value.storage_write_bytes);
    // Lifetime high-water до verification; это не изолированный RSS самой стадии.
    measurement.process_peak_rss_bytes = work.after.peak_rss_bytes;
    measurement.allocations =
        work.allocations
            .zip(drop.allocations)
            .map(|(a, b)| AllocationCounts {
                allocs: a.allocs + b.allocs,
                alloc_bytes: a.alloc_bytes + b.alloc_bytes,
                reallocs: a.reallocs + b.reallocs,
                deallocs: a.deallocs + b.deallocs,
                alloc_successes: a.alloc_successes + b.alloc_successes,
                alloc_success_bytes: a.alloc_success_bytes + b.alloc_success_bytes,
                realloc_successes: a.realloc_successes + b.realloc_successes,
                realloc_success_bytes: a.realloc_success_bytes + b.realloc_success_bytes,
                alloc_failures: a.alloc_failures + b.alloc_failures,
                realloc_failures: a.realloc_failures + b.realloc_failures,
                dealloc_bytes: a.dealloc_bytes + b.dealloc_bytes,
                zeroed_bytes: a.zeroed_bytes + b.zeroed_bytes,
            });
    measurement
}

fn hash_entry(hash: &mut Xxh3, entry: &ScannedEntry, size: u64) {
    hash.update(&entry.work_ordinal.to_le_bytes());
    hash.update(&(entry.relative_path.len() as u64).to_le_bytes());
    hash.update(entry.relative_path.as_bytes());
    hash.update(&[match entry.kind {
        ScannedEntryKind::Regular => 0,
        ScannedEntryKind::Symlink => 1,
    }]);
    hash.update(&size.to_le_bytes());
}

fn fixture_counts(entries: &[ScannedEntry], enforce_read_limit: bool) -> Result<(u64, u64)> {
    let mut files = 0;
    let mut bytes = 0_u64;
    for entry in entries {
        if entry.kind == ScannedEntryKind::Regular {
            let metadata = fs::symlink_metadata(&entry.abs_path)?;
            ensure!(metadata.is_file(), "fixture regular file changed type");
            files += 1;
            bytes = bytes
                .checked_add(metadata.len())
                .context("fixture byte count overflow")?;
            if enforce_read_limit {
                ensure!(
                    metadata.len() <= MAX_FILE_BYTES,
                    "reader fixture file exceeds 64 MiB"
                );
                ensure!(
                    bytes <= MAX_FIXTURE_BYTES,
                    "reader fixture exceeds 512 MiB retained payload cap"
                );
            }
        }
    }
    Ok((files, bytes))
}

pub fn measure(
    source: &Path,
    stage: &str,
    readers: usize,
    hooks: Option<AllocationHooks>,
) -> Result<Measurement> {
    ensure!(readers > 0, "reader count must be positive");
    match stage {
        "noop" => {
            let (_, work) = timed(hooks, || Ok(()))?;
            let (_, dropped) = timed(hooks, || Ok(()))?;
            Ok(finish_measurement(Measurement::default(), work, dropped))
        }
        "scan" => {
            let (report, work) = timed(hooks, || scan_source_tree(source))?;
            ensure!(
                report.skipped.is_empty(),
                "static fixture contains skipped entries"
            );
            let (files, bytes) = fixture_counts(&report.entries, false)?;
            let mut hash = Xxh3::new();
            for entry in &report.entries {
                // Для scanner fingerprint проверяем только результат обхода, без content I/O.
                hash_entry(&mut hash, entry, 0);
            }
            let measurement = Measurement {
                entries: report.entries.len() as u64,
                files,
                bytes,
                fingerprint: hash.digest(),
                ..Measurement::default()
            };
            let (_, dropped) = timed(hooks, || {
                drop(report);
                Ok(())
            })?;
            Ok(finish_measurement(measurement, work, dropped))
        }
        "read" => measure_reads(source, readers, hooks),
        other => bail!("unknown file benchmark stage: {other}"),
    }
}

fn measure_reads(
    source: &Path,
    readers: usize,
    hooks: Option<AllocationHooks>,
) -> Result<Measurement> {
    let scan = scan_source_tree(source)?;
    ensure!(
        scan.skipped.is_empty(),
        "static fixture contains skipped entries"
    );
    // Snapshot и проверка cap вне таймера; reader по-прежнему берёт size_at_open сам.
    let (files, bytes) = fixture_counts(&scan.entries, true)?;
    let expected = scan.entries.clone();
    let (materialized, work) = timed(hooks, || {
        let mut reader = OrderedReader::new(scan.entries, readers, MAX_FILE_BYTES)?;
        let mut entries = Vec::with_capacity(expected.len());
        let mut retained_bytes = 0_u64;
        while let Some(decision) = reader.next()? {
            match decision {
                ReadDecision::Accepted(entry) => {
                    retained_bytes = retained_bytes.saturating_add(entry.contents.len() as u64);
                    ensure!(
                        retained_bytes <= MAX_FIXTURE_BYTES,
                        "fixture grew beyond retained payload cap"
                    );
                    entries.push(entry);
                }
                ReadDecision::Skipped(entry) => {
                    bail!("reader skipped fixture entry: {}", entry.relative_path)
                }
                ReadDecision::Streamed(_) => {
                    bail!("fixture grew beyond the 64 MiB materialization cap")
                }
            }
        }
        // Закрытие каналов, join workers и освобождение input Vec входят в измерение.
        drop(reader);
        Ok(entries)
    })?;
    let fingerprint = verify_reads(&expected, &materialized)?;
    let actual_bytes: u64 = materialized
        .iter()
        .map(|entry| entry.contents.len() as u64)
        .sum();
    ensure!(actual_bytes == bytes, "static fixture byte count changed");
    let measurement = Measurement {
        entries: materialized.len() as u64,
        files,
        bytes,
        fingerprint,
        ..Measurement::default()
    };
    // Verification не входит в измерение; освобождение всех payload/path allocations входит.
    let (_, dropped) = timed(hooks, || {
        drop(materialized);
        Ok(())
    })?;
    Ok(finish_measurement(measurement, work, dropped))
}

fn verify_reads(expected: &[ScannedEntry], actual: &[MaterializedEntry]) -> Result<u64> {
    ensure!(
        expected.len() == actual.len(),
        "reader omitted fixture entries"
    );
    let mut hash = Xxh3::new();
    let mut buffer = vec![0_u8; 256 * 1024];
    for (expected, actual) in expected.iter().zip(actual) {
        let descriptor = &actual.descriptor;
        ensure!(
            expected.work_ordinal == descriptor.work_ordinal
                && expected.relative_path == descriptor.relative_path,
            "reader changed deterministic order"
        );
        ensure!(!descriptor.best_effort, "static fixture required a retry");
        hash_entry(&mut hash, expected, descriptor.size_at_open);
        match (&expected.kind, &descriptor.kind) {
            (ScannedEntryKind::Regular, BundleEntryKind::Regular) => {
                ensure!(
                    descriptor.size_at_open == actual.contents.len() as u64,
                    "reader size mismatch"
                );
                let mut source = File::open(&expected.abs_path)?;
                for chunk in actual.contents.chunks(buffer.len()) {
                    source.read_exact(&mut buffer[..chunk.len()])?;
                    ensure!(chunk == &buffer[..chunk.len()], "reader content mismatch");
                }
                ensure!(
                    source.read(&mut buffer[..1])? == 0,
                    "source grew after reading"
                );
                hash.update(&actual.contents);
            }
            (ScannedEntryKind::Symlink, BundleEntryKind::Symlink { target }) => {
                ensure!(
                    fs::read_link(&expected.abs_path)?.to_string_lossy() == target.as_str(),
                    "symlink target changed"
                );
                hash.update(&(target.len() as u64).to_le_bytes());
                hash.update(target.as_bytes());
            }
            _ => bail!("reader changed fixture entry kind"),
        }
    }
    Ok(hash.digest())
}

#[cfg(unix)]
fn process_snapshot(edge: SnapshotEdge) -> Result<ProcessSnapshot> {
    let mut snapshot = ProcessSnapshot::default();
    if matches!(edge, SnapshotEdge::Before) {
        read_process_io(&mut snapshot);
    }
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // getrusage заполняет весь struct и учитывает CPU всех threads процесса.
    ensure!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } == 0,
        "getrusage failed: {}",
        std::io::Error::last_os_error()
    );
    let usage = unsafe { usage.assume_init() };
    let ns =
        |time: libc::timeval| (time.tv_sec as u64) * 1_000_000_000 + (time.tv_usec as u64) * 1000;
    snapshot.user_ns = ns(usage.ru_utime);
    snapshot.kernel_ns = ns(usage.ru_stime);
    snapshot.voluntary_switches = Some(usage.ru_nvcsw as u64);
    snapshot.involuntary_switches = Some(usage.ru_nivcsw as u64);
    snapshot.minor_faults = Some(usage.ru_minflt as u64);
    snapshot.major_faults = Some(usage.ru_majflt as u64);
    snapshot.page_faults = snapshot
        .minor_faults
        .zip(snapshot.major_faults)
        .and_then(|(minor, major)| minor.checked_add(major));
    snapshot.peak_rss_bytes =
        usage.ru_maxrss as u64 * if cfg!(target_os = "macos") { 1 } else { 1024 };
    if matches!(edge, SnapshotEdge::After) {
        read_process_io(&mut snapshot);
    }
    Ok(snapshot)
}

#[cfg(all(unix, not(target_os = "linux")))]
fn read_process_io(_snapshot: &mut ProcessSnapshot) {}

#[cfg(target_os = "linux")]
fn parse_process_io(bytes: &[u8], snapshot: &mut ProcessSnapshot) -> Option<()> {
    let text = std::str::from_utf8(bytes).ok()?;
    // Нет allocation и EOF-read; отсутствие любого обязательного поля означает unavailable.
    let field = |key: &str| -> Option<u64> {
        text.lines()
            .find_map(|line| line.strip_prefix(key))?
            .trim()
            .parse()
            .ok()
    };
    let rchar = field("rchar:")?;
    let wchar = field("wchar:")?;
    let syscr = field("syscr:")?;
    let syscw = field("syscw:")?;
    let read_bytes = field("read_bytes:")?;
    let write_bytes = field("write_bytes:")?;
    snapshot.io_read_ops = Some(syscr);
    snapshot.io_write_ops = Some(syscw);
    snapshot.io_read_bytes = Some(rchar);
    snapshot.io_write_bytes = Some(wchar);
    snapshot.storage_read_bytes = Some(read_bytes);
    snapshot.storage_write_bytes = Some(write_bytes);
    snapshot.io_observer_read_ops = Some(1);
    snapshot.io_observer_read_bytes = Some(bytes.len() as u64);
    Some(())
}

#[cfg(target_os = "linux")]
fn read_process_io(snapshot: &mut ProcessSnapshot) {
    use std::os::fd::AsRawFd;
    let Ok(file) = File::open("/proc/self/io") else {
        return;
    };
    let mut buffer = [0_u8; 4096];
    // Один read: vfs_read добавляет rchar/syscr ПОСЛЕ формирования proc-содержимого.
    // Не повторяем EINTR и не угадываем partial snapshot: такие метрики останутся None.
    let count = unsafe { libc::read(file.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
    if count > 0 && (count as usize) < buffer.len() && buffer[count as usize - 1] == b'\n' {
        parse_process_io(&buffer[..count as usize], snapshot);
    }
}

#[cfg(windows)]
fn process_snapshot(_edge: SnapshotEdge) -> Result<ProcessSnapshot> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetProcessIoCounters, GetProcessTimes, IO_COUNTERS,
    };
    use windows_sys::Win32::System::WindowsProgramming::QueryProcessCycleTime;
    // Win32 APIs пишут только в переданные POD buffers; pseudo-handle закрывать нельзя.
    unsafe {
        let process = GetCurrentProcess();
        let mut creation = std::mem::zeroed::<FILETIME>();
        let mut exit = std::mem::zeroed::<FILETIME>();
        let mut kernel = std::mem::zeroed::<FILETIME>();
        let mut user = std::mem::zeroed::<FILETIME>();
        ensure!(
            GetProcessTimes(process, &mut creation, &mut exit, &mut kernel, &mut user) != 0,
            "GetProcessTimes failed: {}",
            std::io::Error::last_os_error()
        );
        let mut memory = std::mem::zeroed::<PROCESS_MEMORY_COUNTERS>();
        memory.cb = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
        ensure!(
            GetProcessMemoryInfo(process, &mut memory, memory.cb) != 0,
            "GetProcessMemoryInfo failed: {}",
            std::io::Error::last_os_error()
        );
        let ns = |time: FILETIME| {
            (((time.dwHighDateTime as u64) << 32) | time.dwLowDateTime as u64) * 100
        };
        let mut cycles = 0;
        let cpu_cycles = (QueryProcessCycleTime(process, &mut cycles) != 0).then_some(cycles);
        let mut io = std::mem::zeroed::<IO_COUNTERS>();
        let io_available = GetProcessIoCounters(process, &mut io) != 0;
        Ok(ProcessSnapshot {
            user_ns: ns(user),
            kernel_ns: ns(kernel),
            peak_rss_bytes: memory.PeakWorkingSetSize as u64,
            page_faults: Some(memory.PageFaultCount as u64),
            cpu_cycles,
            io_read_ops: io_available.then_some(io.ReadOperationCount),
            io_write_ops: io_available.then_some(io.WriteOperationCount),
            io_other_ops: io_available.then_some(io.OtherOperationCount),
            io_read_bytes: io_available.then_some(io.ReadTransferCount),
            io_write_bytes: io_available.then_some(io.WriteTransferCount),
            io_other_bytes: io_available.then_some(io.OtherTransferCount),
            io_observer_read_ops: io_available.then_some(0),
            io_observer_read_bytes: io_available.then_some(0),
            ..ProcessSnapshot::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    fn interval(before: u64, after: u64, observer_bytes: u64) -> Interval {
        Interval {
            wall_ns: 1,
            before: ProcessSnapshot {
                cpu_cycles: Some(before),
                io_read_ops: Some(before),
                io_read_bytes: Some(before),
                io_observer_read_ops: Some(1),
                io_observer_read_bytes: Some(observer_bytes),
                ..ProcessSnapshot::default()
            },
            after: ProcessSnapshot {
                cpu_cycles: Some(after),
                io_read_ops: Some(after),
                io_read_bytes: Some(after),
                ..ProcessSnapshot::default()
            },
            allocations: None,
        }
    }

    #[test]
    fn verification_gap_and_observer_reads_are_excluded() {
        // Большой gap моделирует verification I/O, который нельзя включать в результат.
        let result = finish_measurement(
            Measurement::default(),
            interval(10, 120, 100),
            interval(10000, 10200, 150),
        );
        assert_eq!(result.cpu_cycles, Some(310));
        assert_eq!(result.io_read_ops_raw, Some(310));
        assert_eq!(result.io_read_ops, Some(308));
        assert_eq!(result.io_read_bytes_raw, Some(310));
        assert_eq!(result.io_observer_read_bytes, Some(250));
        assert_eq!(result.io_read_bytes, Some(60));
        assert_eq!(result.minor_faults, None);
        let empty = finish_measurement(
            Measurement::default(),
            interval(0, 1, 1),
            interval(99, 100, 1),
        );
        assert_eq!(empty.io_read_ops, Some(0));
        assert_eq!(empty.io_read_bytes, Some(0));
    }

    #[test]
    fn unsupported_or_backwards_counters_are_not_reported_as_zero() {
        let a = interval(20, 19, 0);
        let b = interval(30, 40, 0);
        assert_eq!(sum_delta(&a, &b, |s| s.cpu_cycles), None);
        assert_eq!(sum_delta(&a, &b, |s| s.major_faults), None);
        let overflow = interval(0, u64::MAX, 0);
        assert_eq!(sum_delta(&overflow, &b, |s| s.cpu_cycles), None);
        assert_eq!(page_fault_delta(None, Some(5)), None);
        assert_eq!(page_fault_delta(Some(2), Some(9)), Some(7));
        #[cfg(windows)]
        assert_eq!(
            page_fault_delta(Some(u32::MAX as u64 - 1), Some(2)),
            Some(4)
        );
        #[cfg(not(windows))]
        assert_eq!(page_fault_delta(Some(9), Some(2)), None);
    }

    #[test]
    fn allocation_scope_stops_on_success_error_and_panic() {
        static ACTIVE: AtomicBool = AtomicBool::new(false);
        static STOPS: AtomicU64 = AtomicU64::new(0);
        let hooks = AllocationHooks {
            start: || assert!(!ACTIVE.swap(true, Ordering::SeqCst)),
            stop: || {
                assert!(ACTIVE.swap(false, Ordering::SeqCst));
                STOPS.fetch_add(1, Ordering::SeqCst);
                AllocationCounts::default()
            },
        };
        timed(Some(hooks), || Ok(())).unwrap();
        assert!(timed(Some(hooks), || -> Result<()> { bail!("expected") }).is_err());
        assert!(
            std::panic::catch_unwind(|| timed(Some(hooks), || -> Result<()> {
                panic!("expected")
            }))
            .is_err()
        );
        assert!(!ACTIVE.load(Ordering::SeqCst));
        assert_eq!(STOPS.load(Ordering::SeqCst), 3);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn proc_io_requires_complete_fields_and_keeps_storage_separate() {
        let bytes = b"rchar: 2000\nwchar: 0\nsyscr: 12\nsyscw: 0\nread_bytes: 4096\nwrite_bytes: 0\ncancelled_write_bytes: 0\n";
        let mut snapshot = ProcessSnapshot::default();
        assert_eq!(parse_process_io(bytes, &mut snapshot), Some(()));
        assert_eq!(snapshot.io_read_bytes, Some(2000));
        assert_eq!(snapshot.storage_read_bytes, Some(4096));
        assert_eq!(snapshot.io_observer_read_bytes, Some(bytes.len() as u64));
        assert_eq!(snapshot.io_write_bytes, Some(0));
        let mut invalid = ProcessSnapshot::default();
        assert_eq!(parse_process_io(b"rchar: 5\n", &mut invalid), None);
        assert_eq!(invalid.io_read_bytes, None);
        assert_eq!(parse_process_io(b"rchar: invalid\n", &mut invalid), None);
    }
}
