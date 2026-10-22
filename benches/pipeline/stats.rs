use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;
use std::time::Instant;

#[cfg(target_os = "windows")]
use std::mem::{size_of, zeroed};

use criterion::black_box;
use fast_dump::network::transport::{BoundListener, CloneableStream, connect_once};
use fast_dump::receiver::{ReceiverProfile, receive_dump_profile as receive_dump_v4_profile};
use fast_dump::role::Role;
use fast_dump::sender::{
    BundlePlanOptions as V4BundlePlanOptions, SendReport as V4SendReport,
    SenderProfile as V4SenderProfile, StageProfile, send_dump_profile as send_dump_v4_profile,
};
#[cfg(target_os = "windows")]
use windows_sys::Win32::Foundation::FILETIME;
#[cfg(target_os = "windows")]
use windows_sys::Win32::System::ProcessStatus::{
    GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
};
#[cfg(target_os = "windows")]
use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};

#[cfg(target_os = "windows")]
use super::shared::available_logical_cpus;
use super::shared::{
    BenchDir, BenchmarkCoverage, SendOptions, benchmark_network_shape, loopback_addr,
    loopback_profile_sample_count, monitor_interval,
};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SummaryStats {
    pub(crate) avg: f64,
    pub(crate) min: f64,
    pub(crate) p50: f64,
    pub(crate) p90: f64,
    pub(crate) p99: f64,
    pub(crate) max: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ProcessResourceSummary {
    pub(crate) sample_count: usize,
    pub(crate) avg_cpu_percent: f64,
    pub(crate) max_cpu_percent: f64,
    pub(crate) avg_working_set_bytes: u64,
    pub(crate) max_working_set_bytes: u64,
    pub(crate) avg_private_bytes: u64,
    pub(crate) max_private_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct ProcessResourceSample {
    cpu_percent: f64,
    working_set_bytes: u64,
    private_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct LoopbackRunMetrics {
    pub(crate) files_count: u64,
    pub(crate) skipped_files: u64,
    pub(crate) sent_frames: u64,
    pub(crate) frame_aborts: u64,
    pub(crate) frame_rebuilds: u64,
    pub(crate) avg_files_per_frame: f64,
    pub(crate) raw_read_bytes: u64,
    pub(crate) tar_input_bytes: u64,
    pub(crate) compressed_payload_bytes: u64,
    pub(crate) wire_bytes: u64,
    pub(crate) wire_overhead_bytes: u64,
    pub(crate) read_duration_ms: f64,
    pub(crate) compress_duration_ms: f64,
    pub(crate) send_duration_ms: f64,
    pub(crate) total_duration_ms: f64,
    pub(crate) manifest_build_bytes: u64,
    pub(crate) manifest_build_duration_ms: f64,
    pub(crate) manifest_transfer_bytes: u64,
    pub(crate) manifest_transfer_duration_ms: f64,
    pub(crate) manifest_spool_read_bytes: u64,
    pub(crate) manifest_spool_read_duration_ms: f64,
    pub(crate) manifest_replay_bytes: u64,
    pub(crate) manifest_replay_duration_ms: f64,
    pub(crate) manifest_replay_records: u64,
    pub(crate) receiver_manifest_bytes: u64,
    pub(crate) receiver_issue_tail_bytes: u64,
    pub(crate) receiver_total_duration_ms: f64,
    pub(crate) receiver_manifest_stage_duration_ms: f64,
    pub(crate) receiver_manifest_commit_duration_ms: f64,
    pub(crate) receiver_manifest_ack_flush_duration_ms: f64,
    pub(crate) receiver_frame_duration_ms: f64,
    pub(crate) receiver_frame_write_duration_ms: f64,
    pub(crate) receiver_frame_sync_duration_ms: f64,
    pub(crate) receiver_frame_journal_persist_duration_ms: f64,
    pub(crate) receiver_frame_ack_flush_duration_ms: f64,
    pub(crate) receiver_issue_tail_duration_ms: f64,
    pub(crate) receiver_finish_duration_ms: f64,
    pub(crate) receiver_archive_finalize_duration_ms: f64,
    pub(crate) receiver_issue_finalize_duration_ms: f64,
    pub(crate) receiver_finish_ack_flush_duration_ms: f64,
    pub(crate) disk_budget_wait_ms: f64,
    pub(crate) time_to_first_frame_ms: f64,
    pub(crate) overlap_efficiency_pct: f64,
    pub(crate) traversal_wait_ms: f64,
    pub(crate) ack_wait_ms: f64,
    pub(crate) flush_count: u64,
    pub(crate) max_inflight_dirs: u64,
    pub(crate) max_dirfd_depth: u64,
    pub(crate) read_mib_s: f64,
    pub(crate) compress_mib_s: f64,
    pub(crate) send_mib_s: f64,
    pub(crate) total_mib_s: f64,
    pub(crate) avg_cpu_pct: f64,
    pub(crate) max_cpu_pct: f64,
    pub(crate) avg_rss_mib: f64,
    pub(crate) max_rss_mib: f64,
    pub(crate) avg_private_mib: f64,
    pub(crate) max_private_mib: f64,
    pub(crate) compression_ratio_pct: f64,
    pub(crate) wire_pct_of_read: f64,
    pub(crate) wire_overhead_pct: f64,
}

pub(crate) const PROTOCOL_RUNTIME_LABEL: &str = "v4";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct PipelineProfile {
    pub(crate) scan: StageProfile,
    pub(crate) read: StageProfile,
    pub(crate) tar: StageProfile,
    pub(crate) compress: StageProfile,
    pub(crate) send: StageProfile,
    pub(crate) ack_wait: StageProfile,
    pub(crate) total: StageProfile,
    pub(crate) file_reads: Vec<StageProfile>,
    pub(crate) files_read: u64,
    pub(crate) read_buffers: u64,
    pub(crate) manifest_build: StageProfile,
    pub(crate) manifest_transfer: StageProfile,
    pub(crate) manifest_spool_read: StageProfile,
    pub(crate) manifest_replay: StageProfile,
    pub(crate) manifest_replay_records: u64,
    pub(crate) disk_budget_wait: StageProfile,
    pub(crate) traversal_wait: StageProfile,
    pub(crate) time_to_first_frame: Duration,
    pub(crate) max_inflight_directory_tasks: u64,
    pub(crate) max_dirfd_depth: u64,
}

impl From<V4SenderProfile> for PipelineProfile {
    fn from(profile: V4SenderProfile) -> Self {
        Self {
            scan: profile.scan.clone(),
            read: profile.read.clone(),
            tar: profile.tar.clone(),
            compress: profile.compress.clone(),
            send: profile.send.clone(),
            ack_wait: profile.ack_wait.clone(),
            total: profile.total.clone(),
            files_read: profile.discovered_entries,
            read_buffers: profile.flush_count,
            manifest_build: profile.scan.clone(),
            traversal_wait: profile.scan.clone(),
            time_to_first_frame: profile.time_to_first_bundle,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LoopbackScenarioReport {
    pub(crate) name: String,
    pub(crate) options: SendOptions,
    pub(crate) total_bytes: u64,
    pub(crate) sample_count: usize,
    pub(crate) read_speed: SummaryStats,
    pub(crate) compress_speed: SummaryStats,
    pub(crate) send_speed: SummaryStats,
    pub(crate) total_speed: SummaryStats,
    pub(crate) file_read_speed: Option<SummaryStats>,
    pub(crate) files_read: SummaryStats,
    pub(crate) read_buffers: SummaryStats,
    pub(crate) resources: ProcessResourceSummary,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BenchmarkMetricPlan {
    pub(crate) id: &'static str,
    pub(crate) title: &'static str,
    pub(crate) coverage: BenchmarkCoverage,
    pub(crate) current_source: &'static str,
    pub(crate) notes: &'static str,
}

const BENCHMARK_METRIC_PLAN: &[BenchmarkMetricPlan] = &[
    BenchmarkMetricPlan {
        id: "total-throughput-mib-s",
        title: "Total throughput MiB/s",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.total_mib_s",
        notes: "Successfully transferred logical payload bytes divided by sender session duration through FinishAck, including scan, retries, and durable commits. Repeated attempt reads do not inflate the numerator. This is not wire throughput; transport establishment is outside the timer.",
    },
    BenchmarkMetricPlan {
        id: "stage-durations-and-speeds",
        title: "Read/compress/send durations and speeds",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics read/compress/send fields",
        notes: "Read time includes ordered read-ahead waits and planner materialization, not summed reader CPU. Streamed read/compression runs in a producer while the main thread sends; streamed compression duration excludes source reads and producer queue waits, but includes tar work. Stages overlap and are not an additive wall-time breakdown.",
    },
    BenchmarkMetricPlan {
        id: "traversal-and-ack-wait",
        title: "Source scan and durable-ack wait",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.traversal_wait_ms and ack_wait_ms",
        notes: "traversal_wait_ms and manifest_build_duration_ms are aliases of the same source scan duration. ack_wait_ms measures explicit sender waits on both localhost and shaped profiles.",
    },
    BenchmarkMetricPlan {
        id: "wire-overhead",
        title: "Wire bytes and overhead",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.wire_bytes and wire_overhead_bytes",
        notes: "wire_bytes counts both directions across control and data channels after transport establishment. Overhead subtracts successfully sent compressed bundle payload, so retries can also contribute discarded attempt payload. TCP/IP headers and transport handshake bytes are excluded.",
    },
    BenchmarkMetricPlan {
        id: "executor-fd-budget",
        title: "Legacy executor and directory-handle fields",
        coverage: BenchmarkCoverage::Partial,
        current_source: "results_raw.csv executor plus LoopbackRunMetrics.max_dirfd_depth",
        notes: "The v4 sender uses one traversal implementation. The legacy executor selector has no runtime effect; max_dirfd_depth and max_inflight_dirs are zero placeholders, not measured handle counts.",
    },
    BenchmarkMetricPlan {
        id: "sender-cpu-rss",
        title: "Combined benchmark-process CPU and RSS",
        coverage: BenchmarkCoverage::Partial,
        current_source: "Current ProcessMonitor samples the combined loopback benchmark process",
        notes: "Windows samples the combined sender/receiver process; CPU is normalized by logical CPU count. Other targets emit zero placeholders. Archive verification starts after sampling stops; retained allocations from previous runs can affect later RSS.",
    },
    BenchmarkMetricPlan {
        id: "receiver-cpu-rss",
        title: "Receiver CPU and RSS",
        coverage: BenchmarkCoverage::Planned,
        current_source: "planned split-process bench harness",
        notes: "Required before the team can attribute bottlenecks to sender compression vs receiver commit paths.",
    },
    BenchmarkMetricPlan {
        id: "sender-spool-bytes",
        title: "Legacy sender manifest columns",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.manifest_build_bytes and manifest_spool_read_bytes",
        notes: "v4 has no manifest spool: manifest_build_bytes contains discovered entry count, not bytes. Transfer, spool-read, and replay columns are zero compatibility placeholders.",
    },
    BenchmarkMetricPlan {
        id: "receiver-manifest-bytes",
        title: "Legacy receiver manifest column",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.receiver_manifest_bytes",
        notes: "v4 has no persisted manifest. This field remains zero for CSV compatibility and does not measure the bundle WAL or archive size.",
    },
    BenchmarkMetricPlan {
        id: "issue-tail-cost",
        title: "Issue-tail persistence cost",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.receiver_issue_tail_bytes and receiver_issue_tail_duration_ms",
        notes: "Reports final issue-file size and receiver skip-catalog processing duration. These are zero for stable fixtures without terminal skips.",
    },
    BenchmarkMetricPlan {
        id: "time-to-first-frame",
        title: "Time until first bundle is ready",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.time_to_first_frame_ms",
        notes: "Legacy frame name maps to time_to_first_bundle: elapsed sender time when the first prepared or streamed task is received, before that bundle is sent or acknowledged.",
    },
    BenchmarkMetricPlan {
        id: "reconnect-and-replay-latency",
        title: "Reconnect-to-resume and checkpoint replay latency",
        coverage: BenchmarkCoverage::Planned,
        current_source: "planned reconnect/replay benchmarks",
        notes: "Not measured by the fresh-dump matrix. The current sender rescans the source and resumes after the durable committed work ordinal; dedicated reconnect latency runs remain planned.",
    },
    BenchmarkMetricPlan {
        id: "manifest-phase-latency",
        title: "Legacy manifest timing aliases",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics manifest_build_* / manifest_transfer_* / manifest_spool_read_* fields",
        notes: "Only manifest_build_duration_ms is populated, as an alias of source scan duration. Transfer, spool-read, and replay durations remain zero; they are not active v4 stages.",
    },
    BenchmarkMetricPlan {
        id: "overlap-efficiency",
        title: "Unmeasured overlap efficiency",
        coverage: BenchmarkCoverage::Planned,
        current_source: "LoopbackRunMetrics.overlap_efficiency_pct",
        notes: "This compatibility column is always zero in v4. No overlap-efficiency measurement is implemented; zero does not imply serial execution.",
    },
    BenchmarkMetricPlan {
        id: "receiver-stage-latency",
        title: "Receiver bundle commit and finalization latency",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics receiver_*_duration_ms fields",
        notes: "Legacy frame columns map to bundle write, sync, WAL append, and ACK flush. Manifest stages are zero. The report's network/decode residual is not an independent measurement and cannot attribute network delay.",
    },
];

pub(crate) fn benchmark_metric_plan() -> &'static [BenchmarkMetricPlan] {
    BENCHMARK_METRIC_PLAN
}

struct ProcessMonitor {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<ProcessResourceSummary>,
}

struct CountingStream<T> {
    inner: T,
    read_bytes: u64,
    written_bytes: u64,
}

struct ShapedStream<T> {
    inner: T,
    read_shaper: Option<TrafficShaper>,
    write_shaper: Option<TrafficShaper>,
    shared_read_pacer: Option<SharedReadPacer>,
}

const LINK_BURST_BYTES: usize = 8 * 1024 * 1024;
const LINK_MAX_READ_BYTES: usize = 4 * 1024 * 1024;
const NANOS_PER_SECOND: u128 = 1_000_000_000;

// Один бюджет входящих bytes разделяют control/data и все клоны endpoint.
// Обратное направление получает отдельный бюджет: модель остаётся full duplex.
#[derive(Clone)]
struct SharedReadPacer {
    started: Instant,
    budget: Arc<Mutex<RateBudget>>,
}

struct RateBudget {
    bytes_per_second: u64,
    burst_bytes: usize,
    available_scaled: u128,
    updated_at: Duration,
}

impl RateBudget {
    fn new(bytes_per_second: u64, burst_bytes: usize) -> Self {
        assert!(bytes_per_second > 0 && burst_bytes > 0);
        Self {
            bytes_per_second,
            burst_bytes,
            available_scaled: burst_bytes as u128 * NANOS_PER_SECOND,
            updated_at: Duration::ZERO,
        }
    }

    fn acquire_or_delay(&mut self, now: Duration, bytes: usize) -> Option<Duration> {
        assert!(bytes <= self.burst_bytes);
        let earned = now
            .saturating_sub(self.updated_at)
            .as_nanos()
            .saturating_mul(self.bytes_per_second as u128);
        self.available_scaled = self
            .available_scaled
            .saturating_add(earned)
            .min(self.burst_bytes as u128 * NANOS_PER_SECOND);
        self.updated_at = now;
        let requested = bytes as u128 * NANOS_PER_SECOND;
        if self.available_scaled >= requested {
            self.available_scaled -= requested;
            return None;
        }
        // Не списываем будущие bytes: после sleep другой канал мог занять budget.
        // Повторная проверка также сохраняет credit от oversleep, но не больше burst.
        let wait_ns = (requested - self.available_scaled).div_ceil(self.bytes_per_second as u128);
        Some(Duration::from_nanos(wait_ns as u64))
    }
}

impl SharedReadPacer {
    fn for_profile(network_profile: &str) -> Option<Self> {
        let rate = benchmark_network_shape(network_profile)?.shared_receive_bytes_per_second?;
        Some(Self {
            started: Instant::now(),
            budget: Arc::new(Mutex::new(RateBudget::new(rate, LINK_BURST_BYTES))),
        })
    }

    fn acquire(&self, bytes: usize) -> io::Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        loop {
            let delay = self
                .budget
                .lock()
                .map_err(|_| io::Error::other("benchmark link budget poisoned"))?
                .acquire_or_delay(self.started.elapsed(), bytes);
            let Some(delay) = delay else {
                return Ok(());
            };
            thread::sleep(delay);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TrafficShaper {
    startup_latency: Duration,
    burst_restart_gap: Duration,
    bytes_per_second: Option<u64>,
    next_ready_at: Instant,
    primed: bool,
}

impl ProcessMonitor {
    fn start(interval: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let monitor_stop = stop.clone();
        let handle = thread::spawn(move || monitor_current_process(interval, monitor_stop));
        Self { stop, handle }
    }

    fn finish(self) -> ProcessResourceSummary {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap_or_default()
    }
}

impl<T> CountingStream<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            read_bytes: 0,
            written_bytes: 0,
        }
    }

    fn total_wire_bytes(&self) -> u64 {
        self.read_bytes.saturating_add(self.written_bytes)
    }
}

impl<T> ShapedStream<T> {
    fn new(inner: T, network_profile: &str, shared_read_pacer: Option<SharedReadPacer>) -> Self {
        let Some(shape) = benchmark_network_shape(network_profile) else {
            return Self {
                inner,
                read_shaper: None,
                write_shaper: None,
                shared_read_pacer,
            };
        };
        assert_eq!(
            shape.shared_receive_bytes_per_second.is_some(),
            shared_read_pacer.is_some(),
            "rate-only profiles require the endpoint's shared receive budget"
        );
        let startup_latency = Duration::from_nanos(shape.rtt_ms.saturating_mul(1_000_000) / 2);
        let bytes_per_second = shape
            .bandwidth_mib_per_sec
            .map(|mib_per_sec| mib_per_sec.saturating_mul(1024 * 1024));
        let shaper = if startup_latency.is_zero() && bytes_per_second.is_none() {
            None
        } else {
            Some(TrafficShaper {
                startup_latency,
                burst_restart_gap: startup_latency,
                bytes_per_second,
                next_ready_at: Instant::now(),
                primed: false,
            })
        };
        Self {
            inner,
            read_shaper: shaper,
            write_shaper: shaper,
            shared_read_pacer,
        }
    }
}

impl<T> CloneableStream for ShapedStream<T>
where
    T: CloneableStream,
{
    fn try_clone_stream(&self) -> anyhow::Result<Self> {
        Ok(Self {
            inner: self.inner.try_clone_stream()?,
            read_shaper: self.read_shaper,
            write_shaper: self.write_shaper,
            shared_read_pacer: self.shared_read_pacer.clone(),
        })
    }
}

impl TrafficShaper {
    fn scheduled_ready_at(&mut self, now: Instant, transferred_bytes: usize) -> Instant {
        let start_at = if self.next_ready_at > now {
            self.next_ready_at
        } else {
            now
        };

        // RTT добавляем только на старт нового burst после простоя канала.
        // Иначе shaped bench наказывал бы любой syscall-sized read/write
        // отдельной сетевой задержкой и искажал бы WAN attribution.
        let needs_startup_latency = !self.primed
            || (self.next_ready_at <= now
                && now.saturating_duration_since(self.next_ready_at) >= self.burst_restart_gap);

        let mut ready_at = start_at;
        if needs_startup_latency {
            ready_at += self.startup_latency;
        }
        if let Some(bytes_per_second) = self.bytes_per_second {
            let transfer_duration =
                Duration::from_secs_f64(transferred_bytes as f64 / bytes_per_second as f64);
            ready_at += transfer_duration;
        }

        self.next_ready_at = ready_at;
        self.primed = true;
        ready_at
    }

    fn apply(&mut self, transferred_bytes: usize) {
        if transferred_bytes == 0 {
            return;
        }

        let now = Instant::now();
        let ready_at = self.scheduled_ready_at(now, transferred_bytes);
        let sleep_for = ready_at.saturating_duration_since(now);
        if !sleep_for.is_zero() {
            thread::sleep(sleep_for);
        }
    }
}

impl<T> Read for CountingStream<T>
where
    T: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.read_bytes = self.read_bytes.saturating_add(read as u64);
        Ok(read)
    }
}

impl<T> Read for ShapedStream<T>
where
    T: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Половина burst остаётся под oversleep credit даже при полном read 4 MiB.
        let limit = if self.shared_read_pacer.is_some() {
            buf.len().min(LINK_MAX_READ_BYTES)
        } else {
            buf.len()
        };
        let read = self.inner.read(&mut buf[..limit])?;
        if let Some(shaper) = &mut self.read_shaper {
            shaper.apply(read);
        }
        if let Some(pacer) = &self.shared_read_pacer {
            // TCP уже принял bytes, но protocol parser увидит их только после accounting.
            pacer.acquire(read)?;
        }
        Ok(read)
    }
}

impl<T> Write for CountingStream<T>
where
    T: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.written_bytes = self.written_bytes.saturating_add(written as u64);
        Ok(written)
    }

    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        let written = self.inner.write_vectored(bufs)?;
        self.written_bytes = self.written_bytes.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<T> Write for ShapedStream<T>
where
    T: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        if let Some(shaper) = &mut self.write_shaper {
            shaper.apply(written);
        }
        Ok(written)
    }

    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        let written = self.inner.write_vectored(bufs)?;
        if let Some(shaper) = &mut self.write_shaper {
            shaper.apply(written);
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, Copy)]
struct ProcessCounters {
    captured_at: Instant,
    cpu_time_100ns: u64,
    working_set_bytes: u64,
    private_bytes: u64,
}

fn throughput_mib_per_sec(bytes: u64, duration: Duration) -> Option<f64> {
    if bytes == 0 || duration.is_zero() {
        return None;
    }
    Some(bytes as f64 / duration.as_secs_f64() / (1024.0 * 1024.0))
}

fn percentile(sorted_values: &[f64], percentile: f64) -> Option<f64> {
    if sorted_values.is_empty() {
        return None;
    }
    let rank = ((sorted_values.len() - 1) as f64 * percentile).round() as usize;
    sorted_values.get(rank).copied()
}

fn summarize_f64(values: &[f64]) -> Option<SummaryStats> {
    if values.is_empty() {
        return None;
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    Some(SummaryStats {
        avg: sorted.iter().sum::<f64>() / sorted.len() as f64,
        min: *sorted.first().unwrap(),
        p50: percentile(&sorted, 0.50).unwrap(),
        p90: percentile(&sorted, 0.90).unwrap(),
        p99: percentile(&sorted, 0.99).unwrap(),
        max: *sorted.last().unwrap(),
    })
}

fn summarize_u64(values: &[u64]) -> Option<SummaryStats> {
    let float_values = values.iter().map(|value| *value as f64).collect::<Vec<_>>();
    summarize_f64(&float_values)
}

fn stage_speed_or_zero(stage: &StageProfile) -> f64 {
    throughput_mib_per_sec(stage.bytes, stage.duration).unwrap_or(0.0)
}

fn percentage_of(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    numerator as f64 * 100.0 / denominator as f64
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn print_summary_line(label: &str, unit: &str, summary: SummaryStats) {
    eprintln!(
        "  {label}: avg={:.2} {unit}, p50={:.2} {unit}, p90={:.2} {unit}, p99={:.2} {unit}, max={:.2} {unit}",
        summary.avg, summary.p50, summary.p90, summary.p99, summary.max
    );
}

fn stage_speed_samples<F>(profiles: &[PipelineProfile], selector: F) -> Vec<f64>
where
    F: Fn(&PipelineProfile) -> &StageProfile,
{
    profiles
        .iter()
        .filter_map(|profile| {
            let stage = selector(profile);
            throughput_mib_per_sec(stage.bytes, stage.duration)
        })
        .collect()
}

fn file_read_speed_samples(profiles: &[PipelineProfile]) -> Vec<f64> {
    profiles
        .iter()
        .flat_map(|profile| profile.file_reads.iter())
        .filter_map(|file| throughput_mib_per_sec(file.bytes, file.duration))
        .collect()
}

pub(crate) fn print_profile_summary(name: &str, profiles: &[PipelineProfile]) {
    eprintln!("[bench-profile] {name}: samples={}", profiles.len());

    if let Some(summary) = summarize_f64(&stage_speed_samples(profiles, |profile| &profile.read)) {
        print_summary_line("read speed", "MiB/s", summary);
    }
    if let Some(summary) =
        summarize_f64(&stage_speed_samples(profiles, |profile| &profile.compress))
    {
        print_summary_line("compress speed", "MiB/s", summary);
    }
    if let Some(summary) = summarize_f64(&stage_speed_samples(profiles, |profile| &profile.send)) {
        print_summary_line("send speed", "MiB/s", summary);
    }
    if let Some(summary) = summarize_f64(&stage_speed_samples(profiles, |profile| &profile.total)) {
        print_summary_line("total speed", "MiB/s", summary);
    }
    if let Some(summary) = summarize_u64(
        &profiles
            .iter()
            .map(|profile| profile.files_read)
            .collect::<Vec<_>>(),
    ) {
        print_summary_line("files read", "files", summary);
    }
    if let Some(summary) = summarize_u64(
        &profiles
            .iter()
            .map(|profile| profile.read_buffers)
            .collect::<Vec<_>>(),
    ) {
        print_summary_line("buffers read", "buffers", summary);
    }
    if let Some(summary) = summarize_f64(&file_read_speed_samples(profiles)) {
        print_summary_line("file read speed", "MiB/s", summary);
    }
}

pub(crate) fn print_loopback_report(report: &LoopbackScenarioReport) {
    eprintln!(
        "[bench-loopback] {}: samples={} total_bytes={} threads={} frame={} MiB buffer={} KiB",
        report.name,
        report.sample_count,
        report.total_bytes,
        report.options.threads,
        report.options.frame_target_bytes / (1024 * 1024),
        report.options.read_buffer_bytes / 1024
    );
    print_summary_line("read speed", "MiB/s", report.read_speed);
    print_summary_line("compress speed", "MiB/s", report.compress_speed);
    print_summary_line("send speed", "MiB/s", report.send_speed);
    print_summary_line("total speed", "MiB/s", report.total_speed);
    if let Some(summary) = report.file_read_speed {
        print_summary_line("file read speed", "MiB/s", summary);
    }
    print_summary_line("files read", "files", report.files_read);
    print_summary_line("buffers read", "buffers", report.read_buffers);
    if report.resources.sample_count != 0 {
        eprintln!(
            "  cpu: avg={:.2} %, max={:.2} %",
            report.resources.avg_cpu_percent, report.resources.max_cpu_percent
        );
        eprintln!(
            "  mem: avg_rss={:.2} MiB, max_rss={:.2} MiB, avg_private={:.2} MiB, max_private={:.2} MiB",
            bytes_to_mib(report.resources.avg_working_set_bytes),
            bytes_to_mib(report.resources.max_working_set_bytes),
            bytes_to_mib(report.resources.avg_private_bytes),
            bytes_to_mib(report.resources.max_private_bytes)
        );
    }
}

struct V4ReceiverLoopbackReport {
    final_archive_path: std::path::PathBuf,
    issue_journal_path: Option<std::path::PathBuf>,
    receiver_profile: ReceiverProfile,
}

fn receive_v4_loopback_dump<TControl, TData>(
    control_stream: TControl,
    data_stream: TData,
    receiver_path: std::path::PathBuf,
) -> anyhow::Result<V4ReceiverLoopbackReport>
where
    TControl: CloneableStream,
    TData: Read + Send + 'static,
{
    let (report, profile) =
        receive_dump_v4_profile(control_stream, data_stream, receiver_path, "bench-dump")?;
    Ok(V4ReceiverLoopbackReport {
        final_archive_path: report.final_archive_path,
        issue_journal_path: report.issue_report_path,
        receiver_profile: profile,
    })
}

fn send_v4_loopback_profile<TControl, TData>(
    control_stream: &mut TControl,
    data_stream: &mut TData,
    source_dir: &Path,
    options: &SendOptions,
) -> anyhow::Result<(V4SendReport, V4SenderProfile)>
where
    TControl: Read + Write,
    TData: Write,
{
    send_dump_v4_profile(
        control_stream,
        data_stream,
        source_dir.to_path_buf(),
        "bench-dump",
        V4BundlePlanOptions {
            bundle_target_bytes: options.frame_target_bytes as usize,
            compression_level: options.level,
            compression_threads: options.threads,
            readers: std::thread::available_parallelism()
                .map_or(1, |n| n.get())
                .clamp(options.min_readers, options.max_readers),
        },
    )
}

fn sample_loopback_send_profile(source_dir: &Path, options: &SendOptions) -> PipelineProfile {
    let receiver_dir = BenchDir::new("loopback-v4-receiver-profile");
    let listener = BoundListener::bind(loopback_addr()).unwrap();
    let addr = listener.local_addr();
    let receiver_path = receiver_dir.path().to_path_buf();

    let receiver = thread::spawn(move || {
        let session = listener.accept(Role::Receiver, None, None).unwrap();
        receive_dump_v4_profile(
            session.control_stream,
            session.data_stream,
            receiver_path,
            "bench-dump",
        )
        .unwrap()
    });

    let mut sender_session = connect_once(Role::Sender, addr, Some("bench-dump"), None).unwrap();
    let (_, profile) = send_dump_v4_profile(
        &mut sender_session.control_stream,
        &mut sender_session.data_stream,
        source_dir.to_path_buf(),
        "bench-dump",
        V4BundlePlanOptions {
            bundle_target_bytes: options.frame_target_bytes as usize,
            compression_level: options.level,
            compression_threads: options.threads,
            readers: std::thread::available_parallelism()
                .map_or(1, |n| n.get())
                .clamp(options.min_readers, options.max_readers),
        },
    )
    .unwrap();
    let (receive_report, _) = receiver.join().unwrap();
    black_box(receive_report.final_archive_path);
    profile.into()
}

pub(crate) fn collect_profile_samples<F>(sample_count: usize, mut sample: F) -> Vec<PipelineProfile>
where
    F: FnMut() -> PipelineProfile,
{
    let _ = sample();
    (0..sample_count).map(|_| sample()).collect()
}

// Для matrix runner нужен один детерминированный прогон без criterion warmup:
// здесь считаем ровно те байты и скорости, которые потом пишем в raw CSV.
pub(crate) fn run_loopback_scenario(
    source_dir: &Path,
    options: &SendOptions,
    network_profile: &str,
) -> LoopbackRunMetrics {
    run_v4_loopback_scenario(source_dir, options, network_profile)
}

fn run_v4_loopback_scenario(
    source_dir: &Path,
    options: &SendOptions,
    network_profile: &str,
) -> LoopbackRunMetrics {
    let receiver_dir = BenchDir::new("loopback-v4-receiver-run");
    let listener = BoundListener::bind(loopback_addr()).unwrap();
    let addr = listener.local_addr();
    let receiver_path = receiver_dir.path().to_path_buf();
    let receiver_network_profile = network_profile.to_owned();

    let receiver = thread::spawn(move || {
        let session = listener.accept(Role::Receiver, None, None).unwrap();
        let receive_pacer = SharedReadPacer::for_profile(&receiver_network_profile);
        let receiver_control_stream = ShapedStream::new(
            session.control_stream,
            &receiver_network_profile,
            receive_pacer.clone(),
        );
        let receiver_data_stream = ShapedStream::new(
            session.data_stream,
            &receiver_network_profile,
            receive_pacer,
        );
        receive_v4_loopback_dump(receiver_control_stream, receiver_data_stream, receiver_path)
            .unwrap()
    });

    let sender_session = connect_once(Role::Sender, addr, Some("bench-dump"), None).unwrap();
    let receive_pacer = SharedReadPacer::for_profile(network_profile);
    let mut sender_control_stream = CountingStream::new(ShapedStream::new(
        sender_session.control_stream,
        network_profile,
        receive_pacer.clone(),
    ));
    let mut sender_data_stream = CountingStream::new(ShapedStream::new(
        sender_session.data_stream,
        network_profile,
        receive_pacer,
    ));
    let monitor = ProcessMonitor::start(monitor_interval());
    let (report, profile) = send_v4_loopback_profile(
        &mut sender_control_stream,
        &mut sender_data_stream,
        source_dir,
        options,
    )
    .unwrap();
    let resources = monitor.finish();

    let receive_report = receiver.join().unwrap();
    let logical_bytes = profile.total.bytes;
    let raw_read_bytes = profile.read.bytes;
    let tx_bytes = sender_control_stream
        .written_bytes
        .saturating_add(sender_data_stream.written_bytes);
    let rx_bytes = sender_control_stream
        .read_bytes
        .saturating_add(sender_data_stream.read_bytes);
    let elapsed_seconds = profile.total.duration.as_secs_f64();
    let tx_gbps = if elapsed_seconds > 0.0 {
        tx_bytes as f64 * 8.0 / elapsed_seconds / 1_000_000_000.0
    } else {
        0.0
    };
    eprintln!(
        "[bench-link] profile={network_profile} tx_bytes={tx_bytes} rx_bytes={rx_bytes} elapsed_ms={:.6} tx_gbps={tx_gbps:.6} logical_bytes={logical_bytes} raw_read_bytes={raw_read_bytes}",
        duration_ms(profile.total.duration),
    );
    black_box(receiver_dir.path().to_path_buf());
    super::verify::verify_if_requested(source_dir, &receive_report.final_archive_path)
        .unwrap_or_else(|error| panic!("benchmark archive verification failed: {error:#}"));
    black_box(receive_report.final_archive_path);

    let compressed_payload_bytes = profile.send.bytes;
    let wire_bytes = sender_control_stream
        .total_wire_bytes()
        .saturating_add(sender_data_stream.total_wire_bytes());
    let wire_overhead_bytes = wire_bytes.saturating_sub(compressed_payload_bytes);
    let sent_payload_files = profile.bundled_entries;
    let receiver_issue_tail_bytes = receive_report
        .issue_journal_path
        .as_ref()
        .and_then(|path| fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .unwrap_or(0);

    LoopbackRunMetrics {
        files_count: sent_payload_files,
        skipped_files: report.terminal_skip_count as u64,
        sent_frames: report.bundles_sent as u64,
        frame_aborts: profile.bundle_aborts,
        frame_rebuilds: profile.bundle_rebuilds,
        avg_files_per_frame: if report.bundles_sent == 0 {
            0.0
        } else {
            sent_payload_files as f64 / report.bundles_sent as f64
        },
        raw_read_bytes,
        tar_input_bytes: profile.tar.bytes,
        compressed_payload_bytes,
        wire_bytes,
        wire_overhead_bytes,
        read_duration_ms: duration_ms(profile.read.duration),
        compress_duration_ms: duration_ms(profile.compress.duration),
        send_duration_ms: duration_ms(profile.send.duration),
        total_duration_ms: duration_ms(profile.total.duration),
        manifest_build_bytes: profile.discovered_entries,
        manifest_build_duration_ms: duration_ms(profile.scan.duration),
        manifest_transfer_bytes: 0,
        manifest_transfer_duration_ms: 0.0,
        manifest_spool_read_bytes: 0,
        manifest_spool_read_duration_ms: 0.0,
        manifest_replay_bytes: 0,
        manifest_replay_duration_ms: 0.0,
        manifest_replay_records: 0,
        receiver_manifest_bytes: 0,
        receiver_issue_tail_bytes,
        receiver_total_duration_ms: duration_ms(receive_report.receiver_profile.total_duration),
        receiver_manifest_stage_duration_ms: 0.0,
        receiver_manifest_commit_duration_ms: 0.0,
        receiver_manifest_ack_flush_duration_ms: 0.0,
        receiver_frame_duration_ms: duration_ms(
            receive_report.receiver_profile.bundle_commit.duration,
        ),
        receiver_frame_write_duration_ms: duration_ms(
            receive_report.receiver_profile.bundle_write.duration,
        ),
        receiver_frame_sync_duration_ms: duration_ms(
            receive_report.receiver_profile.bundle_sync.duration,
        ),
        receiver_frame_journal_persist_duration_ms: duration_ms(
            receive_report.receiver_profile.wal_append.duration,
        ),
        receiver_frame_ack_flush_duration_ms: duration_ms(
            receive_report.receiver_profile.bundle_ack_flush.duration,
        ),
        receiver_issue_tail_duration_ms: duration_ms(
            receive_report.receiver_profile.skip_catalog.duration,
        ),
        receiver_finish_duration_ms: duration_ms(receive_report.receiver_profile.finalize_duration),
        receiver_archive_finalize_duration_ms: duration_ms(
            receive_report.receiver_profile.archive_finalize_duration,
        ),
        receiver_issue_finalize_duration_ms: duration_ms(
            receive_report.receiver_profile.issue_finalize_duration,
        ),
        receiver_finish_ack_flush_duration_ms: duration_ms(
            receive_report.receiver_profile.finish_ack_flush_duration,
        ),
        disk_budget_wait_ms: 0.0,
        time_to_first_frame_ms: duration_ms(profile.time_to_first_bundle),
        overlap_efficiency_pct: 0.0,
        traversal_wait_ms: duration_ms(profile.scan.duration),
        ack_wait_ms: duration_ms(profile.ack_wait.duration),
        flush_count: profile.flush_count,
        max_inflight_dirs: 0,
        max_dirfd_depth: 0,
        read_mib_s: throughput_mib_per_sec(raw_read_bytes, profile.read.duration).unwrap_or(0.0),
        compress_mib_s: throughput_mib_per_sec(profile.tar.bytes, profile.compress.duration)
            .unwrap_or(0.0),
        send_mib_s: throughput_mib_per_sec(compressed_payload_bytes, profile.send.duration)
            .unwrap_or(0.0),
        total_mib_s: throughput_mib_per_sec(logical_bytes, profile.total.duration).unwrap_or(0.0),
        avg_cpu_pct: resources.avg_cpu_percent,
        max_cpu_pct: resources.max_cpu_percent,
        avg_rss_mib: bytes_to_mib(resources.avg_working_set_bytes),
        max_rss_mib: bytes_to_mib(resources.max_working_set_bytes),
        avg_private_mib: bytes_to_mib(resources.avg_private_bytes),
        max_private_mib: bytes_to_mib(resources.max_private_bytes),
        compression_ratio_pct: 100.0 - percentage_of(compressed_payload_bytes, raw_read_bytes),
        wire_pct_of_read: percentage_of(wire_bytes, raw_read_bytes),
        wire_overhead_pct: percentage_of(wire_overhead_bytes, raw_read_bytes),
    }
}

pub(crate) fn collect_loopback_scenario_report(
    name: &str,
    source_dir: &Path,
    total_bytes: u64,
    options: &SendOptions,
) -> LoopbackScenarioReport {
    let _ = sample_loopback_send_profile(source_dir, options);

    let monitor = ProcessMonitor::start(monitor_interval());
    let profiles = (0..loopback_profile_sample_count())
        .map(|_| sample_loopback_send_profile(source_dir, options))
        .collect::<Vec<_>>();
    let resources = monitor.finish();

    LoopbackScenarioReport {
        name: name.to_owned(),
        options: options.clone(),
        total_bytes,
        sample_count: profiles.len(),
        read_speed: summarize_f64(&stage_speed_samples(&profiles, |profile| &profile.read))
            .unwrap(),
        compress_speed: summarize_f64(&stage_speed_samples(&profiles, |profile| &profile.compress))
            .unwrap(),
        send_speed: summarize_f64(&stage_speed_samples(&profiles, |profile| &profile.send))
            .unwrap(),
        total_speed: summarize_f64(&stage_speed_samples(&profiles, |profile| &profile.total))
            .unwrap(),
        file_read_speed: summarize_f64(&file_read_speed_samples(&profiles)),
        files_read: summarize_u64(
            &profiles
                .iter()
                .map(|profile| profile.files_read)
                .collect::<Vec<_>>(),
        )
        .unwrap(),
        read_buffers: summarize_u64(
            &profiles
                .iter()
                .map(|profile| profile.read_buffers)
                .collect::<Vec<_>>(),
        )
        .unwrap(),
        resources,
    }
}

#[cfg(target_os = "windows")]
fn filetime_to_u64(time: FILETIME) -> u64 {
    ((time.dwHighDateTime as u64) << 32) | time.dwLowDateTime as u64
}

#[cfg(target_os = "windows")]
fn current_process_counters() -> Option<ProcessCounters> {
    unsafe {
        let process = GetCurrentProcess();

        let mut creation = zeroed::<FILETIME>();
        let mut exit = zeroed::<FILETIME>();
        let mut kernel = zeroed::<FILETIME>();
        let mut user = zeroed::<FILETIME>();
        if GetProcessTimes(process, &mut creation, &mut exit, &mut kernel, &mut user) == 0 {
            return None;
        }

        let mut counters = zeroed::<PROCESS_MEMORY_COUNTERS_EX>();
        counters.cb = size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32;
        if GetProcessMemoryInfo(
            process,
            &mut counters as *mut _ as *mut PROCESS_MEMORY_COUNTERS,
            counters.cb,
        ) == 0
        {
            return None;
        }

        Some(ProcessCounters {
            captured_at: Instant::now(),
            cpu_time_100ns: filetime_to_u64(kernel) + filetime_to_u64(user),
            working_set_bytes: counters.WorkingSetSize as u64,
            private_bytes: counters.PrivateUsage as u64,
        })
    }
}

#[cfg(target_os = "windows")]
fn diff_process_resource_sample(
    previous: ProcessCounters,
    current: ProcessCounters,
) -> Option<ProcessResourceSample> {
    let elapsed = current
        .captured_at
        .saturating_duration_since(previous.captured_at);
    if elapsed.is_zero() {
        return None;
    }

    let cpu_delta_100ns = current
        .cpu_time_100ns
        .saturating_sub(previous.cpu_time_100ns) as f64;
    let cpu_percent =
        (cpu_delta_100ns / 10_000_000.0) / elapsed.as_secs_f64() / available_logical_cpus() * 100.0;

    Some(ProcessResourceSample {
        cpu_percent,
        working_set_bytes: current.working_set_bytes,
        private_bytes: current.private_bytes,
    })
}

fn summarize_resource_samples(samples: &[ProcessResourceSample]) -> ProcessResourceSummary {
    if samples.is_empty() {
        return ProcessResourceSummary::default();
    }

    let avg_cpu_percent =
        samples.iter().map(|sample| sample.cpu_percent).sum::<f64>() / samples.len() as f64;
    let max_cpu_percent = samples
        .iter()
        .map(|sample| sample.cpu_percent)
        .fold(0.0, f64::max);
    let avg_working_set_bytes = samples
        .iter()
        .map(|sample| sample.working_set_bytes as f64)
        .sum::<f64>()
        / samples.len() as f64;
    let max_working_set_bytes = samples
        .iter()
        .map(|sample| sample.working_set_bytes)
        .max()
        .unwrap_or(0);
    let avg_private_bytes = samples
        .iter()
        .map(|sample| sample.private_bytes as f64)
        .sum::<f64>()
        / samples.len() as f64;
    let max_private_bytes = samples
        .iter()
        .map(|sample| sample.private_bytes)
        .max()
        .unwrap_or(0);

    ProcessResourceSummary {
        sample_count: samples.len(),
        avg_cpu_percent,
        max_cpu_percent,
        avg_working_set_bytes: avg_working_set_bytes.round() as u64,
        max_working_set_bytes,
        avg_private_bytes: avg_private_bytes.round() as u64,
        max_private_bytes,
    }
}

fn monitor_current_process(interval: Duration, stop: Arc<AtomicBool>) -> ProcessResourceSummary {
    #[cfg(target_os = "windows")]
    {
        let mut samples = Vec::new();
        let mut previous = current_process_counters();

        while !stop.load(Ordering::Relaxed) {
            thread::sleep(interval);
            let Some(current) = current_process_counters() else {
                continue;
            };
            if let Some(previous_counters) = previous
                && let Some(sample) = diff_process_resource_sample(previous_counters, current)
            {
                samples.push(sample);
            }
            previous = Some(current);
        }

        summarize_resource_samples(&samples)
    }

    #[cfg(not(target_os = "windows"))]
    {
        while !stop.load(Ordering::Relaxed) {
            thread::sleep(interval);
        }
        ProcessResourceSummary::default()
    }
}

pub(crate) fn bytes_to_mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[cfg(test)]
#[allow(unused_imports)]
mod link_rate_tests {
    use std::io::Cursor;
    use std::net::{Shutdown, TcpListener, TcpStream};
    use std::sync::{Barrier, OnceLock};

    use super::*;
    use crate::pipeline::shared::{LINK_1GBPS_PROFILE, LINK_10GBPS_PROFILE};

    #[test]
    fn link_profiles_use_decimal_rates_without_legacy_rtt_shaping() {
        for (profile, expected_rate) in [
            (LINK_1GBPS_PROFILE, 125_000_000),
            (LINK_10GBPS_PROFILE, 1_250_000_000),
        ] {
            let shape = benchmark_network_shape(profile).unwrap();
            assert_eq!(shape.shared_receive_bytes_per_second, Some(expected_rate));
            assert_eq!(shape.rtt_ms, 0);
            assert_eq!(shape.bandwidth_mib_per_sec, None);
        }
    }

    #[test]
    fn byte_budget_is_independent_of_read_fragmentation() {
        let total_bytes = 16 * 1024 * 1024;
        for rate in [125_000_000, 1_250_000_000] {
            let expected_ns = ((total_bytes - LINK_BURST_BYTES) as u128 * NANOS_PER_SECOND)
                .div_ceil(rate as u128);
            for chunk_bytes in [1_024, 65_536, LINK_MAX_READ_BYTES] {
                let mut budget = RateBudget::new(rate, LINK_BURST_BYTES);
                let mut elapsed = Duration::ZERO;
                for _ in 0..total_bytes / chunk_bytes {
                    while let Some(delay) = budget.acquire_or_delay(elapsed, chunk_bytes) {
                        elapsed += delay;
                    }
                }
                // Дробный credit сохраняется и при максимальном read, который меньше burst.
                assert!(elapsed.as_nanos() >= expected_ns);
                assert!(
                    elapsed.as_nanos() - expected_ns <= (total_bytes / LINK_BURST_BYTES) as u128
                );
            }
        }
    }

    #[test]
    fn oversleep_credit_is_preserved_and_idle_credit_is_bounded() {
        let mut budget = RateBudget::new(125_000_000, LINK_BURST_BYTES);
        assert_eq!(
            budget.acquire_or_delay(Duration::ZERO, LINK_BURST_BYTES),
            None
        );
        let byte_delay = Some(Duration::from_nanos(8));
        assert_eq!(budget.acquire_or_delay(Duration::ZERO, 1), byte_delay);
        assert_eq!(budget.acquire_or_delay(Duration::ZERO, 1), byte_delay);

        // Sleep на 8 ns может фактически длиться 1 ms: эти bytes доступны сразу.
        let late_wakeup = Duration::from_millis(1);
        assert_eq!(budget.acquire_or_delay(late_wakeup, 125_000), None);
        assert_eq!(budget.acquire_or_delay(late_wakeup, 1), byte_delay);

        let idle_wakeup = Duration::from_secs(60);
        assert_eq!(budget.acquire_or_delay(idle_wakeup, LINK_BURST_BYTES), None);
        assert_eq!(budget.acquire_or_delay(idle_wakeup, 1), byte_delay);
    }

    #[test]
    fn maximum_read_keeps_oversleep_credit_at_ten_gigabits() {
        let mut budget = RateBudget::new(1_250_000_000, LINK_BURST_BYTES);
        assert_eq!(
            budget.acquire_or_delay(Duration::ZERO, LINK_BURST_BYTES),
            None
        );
        let scheduled_wait = budget
            .acquire_or_delay(Duration::ZERO, LINK_MAX_READ_BYTES)
            .unwrap();
        let late_wakeup = scheduled_wait + Duration::from_millis(1);
        assert_eq!(
            budget.acquire_or_delay(late_wakeup, LINK_MAX_READ_BYTES),
            None
        );
        // При burst == read limit этот credit терялся на каждом полном read.
        assert_eq!(budget.acquire_or_delay(late_wakeup, 1_250_000), None);
    }

    #[test]
    fn channels_and_clones_share_budget_but_reverse_direction_does_not() {
        let endpoint = SharedReadPacer::for_profile(LINK_1GBPS_PROFILE).unwrap();
        let clone = endpoint.clone();
        let reverse = SharedReadPacer::for_profile(LINK_1GBPS_PROFILE).unwrap();
        assert_eq!(
            endpoint
                .budget
                .lock()
                .unwrap()
                .acquire_or_delay(Duration::ZERO, LINK_BURST_BYTES),
            None
        );
        assert_eq!(
            clone
                .budget
                .lock()
                .unwrap()
                .acquire_or_delay(Duration::ZERO, 1),
            Some(Duration::from_nanos(8))
        );
        assert_eq!(
            reverse
                .budget
                .lock()
                .unwrap()
                .acquire_or_delay(Duration::ZERO, LINK_BURST_BYTES),
            None
        );
    }

    #[test]
    fn rate_only_read_caps_a_single_grant_below_the_burst() {
        let pacer = SharedReadPacer::for_profile(LINK_10GBPS_PROFILE);
        let mut stream = ShapedStream::new(
            Cursor::new(vec![0xa5; LINK_MAX_READ_BYTES + 1]),
            LINK_10GBPS_PROFILE,
            pacer,
        );
        let mut bytes = vec![0; LINK_MAX_READ_BYTES + 1];
        assert_eq!(stream.read(&mut bytes).unwrap(), LINK_MAX_READ_BYTES);
        assert_eq!(bytes[LINK_MAX_READ_BYTES - 1], 0xa5);
        assert_eq!(bytes[LINK_MAX_READ_BYTES], 0);
        assert!(stream.read_shaper.is_none());
        assert!(stream.write_shaper.is_none());
    }

    #[test]
    #[ignore = "manual TCP calibration: 1 GiB per direction/rate with 1 MiB and 4 MiB read buffers"]
    fn shared_link_rate_calibration_over_tcp() {
        for profile in [LINK_1GBPS_PROFILE, LINK_10GBPS_PROFILE] {
            for read_buffer_bytes in [1024 * 1024, 4 * 1024 * 1024] {
                calibrate_shared_link_rate(profile, read_buffer_bytes);
            }
        }
    }

    fn calibrate_shared_link_rate(profile: &str, read_buffer_bytes: usize) {
        const BYTES_PER_CHANNEL: u64 = 512 * 1024 * 1024;
        const IO_BYTES: usize = 256 * 1024;
        let rate = benchmark_network_shape(profile)
            .unwrap()
            .shared_receive_bytes_per_second
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint_pacers = [
            SharedReadPacer::for_profile(profile).unwrap(),
            SharedReadPacer::for_profile(profile).unwrap(),
        ];
        let mut streams = Vec::new();
        for _ in 0..2 {
            let sender = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (receiver, _) = listener.accept().unwrap();
            streams.push((0, receiver));
            streams.push((1, sender));
        }
        let gate = Arc::new(Barrier::new(streams.len() * 2 + 1));
        let started = OnceLock::<Instant>::new();
        let measurements = thread::scope(|scope| {
            let mut readers = Vec::new();
            let mut writers = Vec::new();
            for (direction, stream) in streams {
                stream.set_nodelay(true).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(60)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(60)))
                    .unwrap();
                let mut read_stream =
                    ShapedStream::new(stream, profile, Some(endpoint_pacers[direction].clone()));
                let mut write_stream = read_stream.try_clone_stream().unwrap();
                assert!(Arc::ptr_eq(
                    &read_stream.shared_read_pacer.as_ref().unwrap().budget,
                    &write_stream.shared_read_pacer.as_ref().unwrap().budget,
                ));
                let read_gate = gate.clone();
                let read_started = &started;
                readers.push((
                    direction,
                    scope.spawn(move || {
                        let mut buffer = vec![0_u8; read_buffer_bytes];
                        let mut received = 0_u64;
                        let mut read_calls = 0_u64;
                        let mut full_reads = 0_u64;
                        let mut max_read_bytes = 0_usize;
                        read_gate.wait();
                        loop {
                            let read = read_stream.read(&mut buffer).unwrap();
                            if read == 0 {
                                break;
                            }
                            received += read as u64;
                            read_calls += 1;
                            full_reads += u64::from(read == read_buffer_bytes);
                            max_read_bytes = max_read_bytes.max(read);
                        }
                        assert_eq!(received, BYTES_PER_CHANNEL);
                        (
                            received,
                            read_started.get().unwrap().elapsed(),
                            read_calls,
                            full_reads,
                            max_read_bytes,
                        )
                    }),
                ));
                let write_gate = gate.clone();
                writers.push(scope.spawn(move || {
                    let buffer = vec![0xa5; IO_BYTES];
                    write_gate.wait();
                    for _ in 0..BYTES_PER_CHANNEL / IO_BYTES as u64 {
                        write_stream.write_all(&buffer).unwrap();
                    }
                    write_stream.inner.shutdown(Shutdown::Write).unwrap();
                }));
            }
            started.set(Instant::now()).unwrap();
            gate.wait();
            for writer in writers {
                writer.join().unwrap();
            }
            let mut measurements = [(0_u64, Duration::ZERO, 0_u64, 0_u64, 0_usize); 2];
            for (direction, reader) in readers {
                let (bytes, elapsed, read_calls, full_reads, max_read_bytes) =
                    reader.join().unwrap();
                measurements[direction].0 += bytes;
                measurements[direction].1 = measurements[direction].1.max(elapsed);
                measurements[direction].2 += read_calls;
                measurements[direction].3 += full_reads;
                measurements[direction].4 = measurements[direction].4.max(max_read_bytes);
            }
            measurements
        });
        for (direction, (bytes, elapsed, read_calls, full_reads, max_read_bytes)) in
            measurements.into_iter().enumerate()
        {
            let achieved = bytes as f64 / elapsed.as_secs_f64();
            eprintln!(
                "[bench-link-calibration] profile={profile} direction={} bytes={bytes} elapsed_ms={:.3} achieved_gbps={:.6} configured_gbps={:.3} burst_bytes={LINK_BURST_BYTES} max_grant_bytes={LINK_MAX_READ_BYTES} read_buffer_bytes={read_buffer_bytes} read_calls={read_calls} full_reads={full_reads} max_read_bytes={max_read_bytes}",
                ["sender-to-receiver", "receiver-to-sender"][direction],
                duration_ms(elapsed),
                achieved * 8.0 / 1_000_000_000.0,
                rate as f64 * 8.0 / 1_000_000_000.0,
            );
            assert_eq!(bytes, 2 * BYTES_PER_CHANNEL);
            let byte_bound = rate as f64 * elapsed.as_secs_f64() + LINK_BURST_BYTES as f64;
            assert!(
                bytes as f64 <= byte_bound,
                "shared direction exceeded its rate plus burst"
            );
            assert!(
                achieved >= rate as f64 * 0.90,
                "host/shaper sustained less than 90% of {profile}; do not use this calibration as a link-speed baseline"
            );
        }
    }
}
