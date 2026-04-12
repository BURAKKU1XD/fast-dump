use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;
use std::time::Instant;

#[cfg(target_os = "windows")]
use std::mem::{size_of, zeroed};

use criterion::black_box;
use fast_dump::network::transport::{BoundListener, connect_once};
use fast_dump::receiver::{ReceiverProfile, receive_dump};
use fast_dump::role::Role;
use fast_dump::sender::{PipelineProfile, SendOptions, StageProfile, send_dump_profile};
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
    BenchDir, BenchmarkCoverage, benchmark_network_shape, loopback_addr,
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
        notes: "Current harness records this for every loopback scenario and uses it in summary ranking.",
    },
    BenchmarkMetricPlan {
        id: "stage-durations-and-speeds",
        title: "Read/compress/send durations and speeds",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics read/compress/send fields",
        notes: "Useful for locating whether CPU, protocol, or storage dominates the current v3 path.",
    },
    BenchmarkMetricPlan {
        id: "traversal-and-ack-wait",
        title: "Traversal wait and ack wait",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.traversal_wait_ms and ack_wait_ms",
        notes: "These waits are already visible, but only on localhost without shaped network delay.",
    },
    BenchmarkMetricPlan {
        id: "wire-overhead",
        title: "Wire bytes and overhead",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.wire_bytes and wire_overhead_bytes",
        notes: "This is the current baseline for frame and chunk overhead on the wire.",
    },
    BenchmarkMetricPlan {
        id: "executor-fd-budget",
        title: "Executor mode and max dirfd depth",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "results_raw.csv executor plus LoopbackRunMetrics.max_dirfd_depth",
        notes: "Unix matrix runs now compare the default dirfd executor against the forced abs-path fallback and record the observed bounded directory-handle depth.",
    },
    BenchmarkMetricPlan {
        id: "sender-cpu-rss",
        title: "Sender CPU and RSS",
        coverage: BenchmarkCoverage::Partial,
        current_source: "Current ProcessMonitor samples the combined loopback benchmark process",
        notes: "Useful for local regression hints, but not yet split into dedicated sender and receiver process metrics.",
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
        title: "Sender spool bytes written/read",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.manifest_build_bytes and manifest_spool_read_bytes",
        notes: "Loopback runner now exports sender manifest spool build and spool-read bytes directly from PipelineProfile.",
    },
    BenchmarkMetricPlan {
        id: "receiver-manifest-bytes",
        title: "Receiver manifest bytes persisted",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.receiver_manifest_bytes",
        notes: "Receiver loopback runs now report the durable manifest bytes persisted in ReceiverState journal.",
    },
    BenchmarkMetricPlan {
        id: "issue-tail-cost",
        title: "Issue-tail persistence cost",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.receiver_issue_tail_bytes and receiver_issue_tail_duration_ms",
        notes: "Loopback runner now reports both persisted typed issue-tail bytes and the receiver-side wall time spent consuming the typed tail.",
    },
    BenchmarkMetricPlan {
        id: "time-to-first-frame",
        title: "Time to first frame",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.time_to_first_frame_ms",
        notes: "Derived from v3 sender session timings so manifest-first regressions are visible in raw and summary artifacts.",
    },
    BenchmarkMetricPlan {
        id: "reconnect-and-replay-latency",
        title: "Reconnect-to-resume and checkpoint replay latency",
        coverage: BenchmarkCoverage::Planned,
        current_source: "planned reconnect/replay benchmarks",
        notes: "This is the measurable proof that ordinary v3 reconnect no longer depends on a full live rescan.",
    },
    BenchmarkMetricPlan {
        id: "manifest-phase-latency",
        title: "Manifest build and transfer latency",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics manifest_build_* / manifest_transfer_* / manifest_spool_read_* fields",
        notes: "Loopback raw results now split sender manifest build, transfer, spool-read, and replay costs.",
    },
    BenchmarkMetricPlan {
        id: "overlap-efficiency",
        title: "Manifest/data overlap efficiency",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics.overlap_efficiency_pct",
        notes: "Computed from v3 manifest-transfer duration versus time-to-first-frame so sequential fallback is visible in results.",
    },
    BenchmarkMetricPlan {
        id: "receiver-stage-latency",
        title: "Receiver manifest/frame/finish stage latency",
        coverage: BenchmarkCoverage::Implemented,
        current_source: "LoopbackRunMetrics receiver_*_duration_ms fields",
        notes: "Loopback raw results now split receiver-side manifest staging, frame local commit, issue-tail handling, and finish/finalization latency so sender-vs-receiver bottlenecks are distinguishable.",
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
}

#[derive(Debug, Clone, Copy)]
struct TrafficShaper {
    latency_per_operation: Duration,
    bytes_per_second: Option<u64>,
    next_ready_at: Instant,
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
    fn new(inner: T, network_profile: &str) -> Self {
        let Some(shape) = benchmark_network_shape(network_profile) else {
            return Self {
                inner,
                read_shaper: None,
                write_shaper: None,
            };
        };
        let latency_per_operation =
            Duration::from_nanos(shape.rtt_ms.saturating_mul(1_000_000) / 2);
        let bytes_per_second = shape
            .bandwidth_mib_per_sec
            .map(|mib_per_sec| mib_per_sec.saturating_mul(1024 * 1024));
        let shaper = if latency_per_operation.is_zero() && bytes_per_second.is_none() {
            None
        } else {
            Some(TrafficShaper {
                latency_per_operation,
                bytes_per_second,
                next_ready_at: Instant::now(),
            })
        };
        Self {
            inner,
            read_shaper: shaper,
            write_shaper: shaper,
        }
    }
}

impl TrafficShaper {
    fn apply(&mut self, transferred_bytes: usize) {
        if transferred_bytes == 0 {
            return;
        }

        let now = Instant::now();
        let start_at = if self.next_ready_at > now {
            self.next_ready_at
        } else {
            now
        };
        let mut ready_at = start_at + self.latency_per_operation;
        if let Some(bytes_per_second) = self.bytes_per_second {
            let transfer_duration =
                Duration::from_secs_f64(transferred_bytes as f64 / bytes_per_second as f64);
            ready_at += transfer_duration;
        }
        self.next_ready_at = ready_at;
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
        let read = self.inner.read(buf)?;
        if let Some(shaper) = &mut self.read_shaper {
            shaper.apply(read);
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

struct ReceiverLoopbackReport {
    final_archive_path: std::path::PathBuf,
    issue_journal_path: std::path::PathBuf,
    receiver_manifest_bytes: u64,
    receiver_profile: ReceiverProfile,
}

fn receive_loopback_dump<T>(
    stream: &mut T,
    receiver_path: std::path::PathBuf,
    options: &SendOptions,
) -> anyhow::Result<ReceiverLoopbackReport>
where
    T: Read + Write,
{
    let report = receive_dump(
        stream,
        receiver_path,
        options.manifest_ack_records,
        options.manifest_ack_bytes,
    )?;
    Ok(ReceiverLoopbackReport {
        final_archive_path: report.final_archive_path,
        issue_journal_path: report.state.paths.issue_journal_path().to_path_buf(),
        receiver_manifest_bytes: report.state.journal.manifest_bytes_persisted,
        receiver_profile: report.profile,
    })
}

fn send_loopback_profile<T>(
    stream: &mut T,
    source_dir: &Path,
    options: &SendOptions,
) -> anyhow::Result<(fast_dump::sender::SendReport, PipelineProfile)>
where
    T: Read + Write,
{
    send_dump_profile(stream, source_dir.to_path_buf(), "bench-dump", options)
}

fn sample_loopback_send_profile(source_dir: &Path, options: &SendOptions) -> PipelineProfile {
    let receiver_dir = BenchDir::new("loopback-receiver-profile");
    let listener = BoundListener::bind(loopback_addr()).unwrap();
    let addr = listener.local_addr();
    let receiver_path = receiver_dir.path().to_path_buf();
    let manifest_ack_records = options.manifest_ack_records;
    let manifest_ack_bytes = options.manifest_ack_bytes;

    let receiver = thread::spawn(move || {
        let mut session = listener.accept(Role::Receiver, None, None).unwrap();
        receive_dump(
            &mut session.stream,
            receiver_path,
            manifest_ack_records,
            manifest_ack_bytes,
        )
        .unwrap()
    });

    let mut sender_session = connect_once(Role::Sender, addr, Some("bench-dump"), None).unwrap();
    let (_, profile) = send_dump_profile(
        &mut sender_session.stream,
        source_dir.to_path_buf(),
        "bench-dump",
        options,
    )
    .unwrap();
    let receive_report = receiver.join().unwrap();
    black_box(receive_report.final_archive_path);
    profile
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
    let receiver_dir = BenchDir::new("loopback-receiver-run");
    let listener = BoundListener::bind(loopback_addr()).unwrap();
    let addr = listener.local_addr();
    let receiver_path = receiver_dir.path().to_path_buf();
    let receiver_options = options.clone();
    let receiver_network_profile = network_profile.to_owned();

    let receiver = thread::spawn(move || {
        let session = listener.accept(Role::Receiver, None, None).unwrap();
        let mut receiver_stream = ShapedStream::new(session.stream, &receiver_network_profile);
        receive_loopback_dump(&mut receiver_stream, receiver_path, &receiver_options).unwrap()
    });

    let sender_session = connect_once(Role::Sender, addr, Some("bench-dump"), None).unwrap();
    let mut sender_stream =
        CountingStream::new(ShapedStream::new(sender_session.stream, network_profile));
    let monitor = ProcessMonitor::start(monitor_interval());
    let (report, profile) = send_loopback_profile(&mut sender_stream, source_dir, options).unwrap();
    let resources = monitor.finish();

    let receive_report = receiver.join().unwrap();
    black_box(receiver_dir.path().to_path_buf());
    black_box(receive_report.final_archive_path);
    let receiver_profile = &receive_report.receiver_profile;

    let raw_read_bytes = profile.read.bytes;
    let compressed_payload_bytes = profile.send.bytes;
    let wire_bytes = sender_stream.total_wire_bytes();
    let wire_overhead_bytes = wire_bytes.saturating_sub(compressed_payload_bytes);
    let skipped_files = report
        .files_skipped_open_failed
        .saturating_add(report.files_skipped_unstable_after_retries);
    let sent_payload_files = report
        .files_sent_clean
        .saturating_add(report.files_sent_best_effort);
    let receiver_issue_tail_bytes = fs::metadata(&receive_report.issue_journal_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let overlap_efficiency_pct = if profile.manifest_transfer.duration.is_zero() {
        0.0
    } else {
        profile
            .manifest_transfer
            .duration
            .saturating_sub(profile.time_to_first_frame)
            .as_secs_f64()
            * 100.0
            / profile.manifest_transfer.duration.as_secs_f64()
    };

    LoopbackRunMetrics {
        files_count: profile.files_read,
        skipped_files,
        sent_frames: report.sent_frames as u64,
        frame_aborts: report.frame_aborts,
        frame_rebuilds: report.frame_rebuilds,
        avg_files_per_frame: if report.sent_frames == 0 {
            0.0
        } else {
            sent_payload_files as f64 / report.sent_frames as f64
        },
        raw_read_bytes,
        tar_input_bytes: profile.compress.bytes,
        compressed_payload_bytes,
        wire_bytes,
        wire_overhead_bytes,
        read_duration_ms: duration_ms(profile.read.duration),
        compress_duration_ms: duration_ms(profile.compress.duration),
        send_duration_ms: duration_ms(profile.send.duration),
        total_duration_ms: duration_ms(profile.total.duration),
        manifest_build_bytes: profile.manifest_build.bytes,
        manifest_build_duration_ms: duration_ms(profile.manifest_build.duration),
        manifest_transfer_bytes: profile.manifest_transfer.bytes,
        manifest_transfer_duration_ms: duration_ms(profile.manifest_transfer.duration),
        manifest_spool_read_bytes: profile.manifest_spool_read.bytes,
        manifest_spool_read_duration_ms: duration_ms(profile.manifest_spool_read.duration),
        manifest_replay_bytes: profile.manifest_replay.bytes,
        manifest_replay_duration_ms: duration_ms(profile.manifest_replay.duration),
        manifest_replay_records: profile.manifest_replay_records,
        receiver_manifest_bytes: receive_report.receiver_manifest_bytes,
        receiver_issue_tail_bytes,
        receiver_total_duration_ms: duration_ms(receiver_profile.total_duration),
        receiver_manifest_stage_duration_ms: duration_ms(receiver_profile.manifest_stage_duration),
        receiver_manifest_commit_duration_ms: duration_ms(
            receiver_profile.manifest_commit_duration,
        ),
        receiver_manifest_ack_flush_duration_ms: duration_ms(
            receiver_profile.manifest_ack_flush_duration,
        ),
        receiver_frame_duration_ms: duration_ms(receiver_profile.frame_duration),
        receiver_frame_write_duration_ms: duration_ms(receiver_profile.frame_write_duration),
        receiver_frame_sync_duration_ms: duration_ms(receiver_profile.frame_sync_duration),
        receiver_frame_journal_persist_duration_ms: duration_ms(
            receiver_profile.frame_journal_persist_duration,
        ),
        receiver_frame_ack_flush_duration_ms: duration_ms(
            receiver_profile.frame_ack_flush_duration,
        ),
        receiver_issue_tail_duration_ms: duration_ms(receiver_profile.issue_tail_duration),
        receiver_finish_duration_ms: duration_ms(receiver_profile.finish_duration),
        receiver_archive_finalize_duration_ms: duration_ms(
            receiver_profile.archive_finalize_duration,
        ),
        receiver_issue_finalize_duration_ms: duration_ms(receiver_profile.issue_finalize_duration),
        receiver_finish_ack_flush_duration_ms: duration_ms(
            receiver_profile.finish_ack_flush_duration,
        ),
        disk_budget_wait_ms: duration_ms(profile.disk_budget_wait.duration),
        time_to_first_frame_ms: duration_ms(profile.time_to_first_frame),
        overlap_efficiency_pct,
        traversal_wait_ms: duration_ms(profile.traversal_wait.duration),
        ack_wait_ms: duration_ms(profile.ack_wait.duration),
        flush_count: profile.flush_count,
        max_inflight_dirs: profile.max_inflight_directory_tasks,
        max_dirfd_depth: profile.max_dirfd_depth,
        read_mib_s: stage_speed_or_zero(&profile.read),
        compress_mib_s: stage_speed_or_zero(&profile.compress),
        send_mib_s: stage_speed_or_zero(&profile.send),
        total_mib_s: stage_speed_or_zero(&profile.total),
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
