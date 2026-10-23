use std::fs;
use std::io::{Cursor, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendOptions {
    pub(crate) level: i32,
    pub(crate) threads: u32,
    pub(crate) frame_target_bytes: u64,
    pub(crate) min_readers: usize,
    pub(crate) max_readers: usize,
    pub(crate) read_buffer_bytes: usize,
    pub(crate) manifest_chunk_bytes: usize,
    pub(crate) manifest_ack_records: u64,
    pub(crate) manifest_ack_bytes: usize,
}

pub(crate) const MANY_SMALL_FILES_DATASET: &str = "manySmallFiles";
pub(crate) const BIG_FILES_DATASET: &str = "bigFiles";
pub(crate) const MIXED_DATASET: &str = "mixed";
pub(crate) const DEFAULT_MEMORY_BUDGET_MIB: u64 = 512;
pub(crate) const LOOPBACK_LOCALHOST_PROFILE: &str = "loopback-localhost";
pub(crate) const LINK_1GBPS_PROFILE: &str = "link-1gbps";
pub(crate) const LINK_10GBPS_PROFILE: &str = "link-10gbps";
pub(crate) const LAN_LOW_RTT_PROFILE: &str = "lan-low-rtt";
pub(crate) const WAN_MODERATE_RTT_PROFILE: &str = "wan-moderate-rtt";
pub(crate) const WAN_HIGH_RTT_PROFILE: &str = "wan-high-rtt";
pub(crate) const BANDWIDTH_LIMITED_PROFILE: &str = "bandwidth-limited";
pub(crate) const LOSSY_RECONNECT_PRONE_PROFILE: &str = "lossy-reconnect-prone";
pub(crate) const PREPARE_SMALL_FILE_COUNT: usize = 40_000;
pub(crate) const MAX_SMALL_FILE_DEPTH: usize = 7;
pub(crate) const PREPARE_FRAME_TARGET_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const PREPARE_OVERSIZED_FILE_BYTES: u64 = PREPARE_FRAME_TARGET_BYTES * 2;
const DATASET_READY_MARKER: &str = ".ready";
const MIB: u64 = 1024 * 1024;
const MIXED_SMALL_FILE_COUNT: usize = 10_000;
const MIXED_MEDIUM_FILE_COUNT: usize = 128;
const ENTROPY_TEMPLATE_BYTES: usize = 64 * 1024;
const BIG_FILE_CASES: &[(&str, u64)] = &[
    ("payloads/big/alpha/huge-256mib.bin", 256 * MIB),
    ("payloads/big/beta/huge-512mib.bin", 512 * MIB),
    ("payloads/big/gamma/archive-512mib.bin", 512 * MIB),
];
const MIXED_LARGE_FILE_CASES: &[(&str, u64)] = &[
    ("payloads/large/alpha/deep/large-032mib.bin", 32 * MIB),
    ("payloads/large/beta/deep/large-064mib.bin", 64 * MIB),
    ("payloads/large/gamma/deep/large-064mib.bin", 64 * MIB),
    ("payloads/large/delta/deep/large-128mib.bin", 128 * MIB),
    ("payloads/large/epsilon/deep/large-128mib.bin", 128 * MIB),
    ("payloads/large/zeta/deep/large-256mib.bin", 256 * MIB),
];

pub(crate) struct BenchDir {
    path: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct MatrixDataset {
    pub(crate) name: &'static str,
    pub(crate) path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BenchmarkCoverage {
    Implemented,
    Partial,
    Planned,
}

impl BenchmarkCoverage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Implemented => "implemented",
            Self::Partial => "partial",
            Self::Planned => "planned",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BenchmarkDatasetPlan {
    pub(crate) id: &'static str,
    pub(crate) title: &'static str,
    pub(crate) coverage: BenchmarkCoverage,
    pub(crate) current_fixture: &'static str,
    pub(crate) primary_goal: &'static str,
    pub(crate) notes: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BenchmarkNetworkProfile {
    pub(crate) id: &'static str,
    pub(crate) title: &'static str,
    pub(crate) coverage: BenchmarkCoverage,
    pub(crate) rtt_ms: &'static str,
    pub(crate) bandwidth_limit: &'static str,
    pub(crate) loss_or_disconnects: &'static str,
    pub(crate) primary_goal: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BenchmarkNetworkShape {
    pub(crate) rtt_ms: u64,
    pub(crate) bandwidth_mib_per_sec: Option<u64>,
    pub(crate) shared_receive_bytes_per_second: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BenchmarkComparisonRule {
    pub(crate) id: &'static str,
    pub(crate) rule: &'static str,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BenchmarkSuccessGate {
    pub(crate) id: &'static str,
    pub(crate) title: &'static str,
    pub(crate) rule: &'static str,
    pub(crate) evidence: &'static str,
}

const BENCHMARK_DATASET_PLAN: &[BenchmarkDatasetPlan] = &[
    BenchmarkDatasetPlan {
        id: "million-tiny-files",
        title: "Million tiny files",
        coverage: BenchmarkCoverage::Planned,
        current_fixture: "planned dedicated dataset; current manySmallFiles is only a developer-scale proxy",
        primary_goal: "Stress deterministic traversal, bundle metadata limits, and RAM growth on extreme file counts.",
        notes: "The sender streams ordinary scans through queues bounded by entry count. This case must measure metadata growth from directory width, DFS depth, path lengths, and ambiguous-name subtrees beyond the payload read-ahead budget; public collecting scan helpers still retain their full report.",
    },
    BenchmarkDatasetPlan {
        id: "deep-tree",
        title: "Deep tree tiny files",
        coverage: BenchmarkCoverage::Partial,
        current_fixture: "manySmallFiles depth=7, files=40000",
        primary_goal: "Measure path handling, directory traversal, and scan cost on nested trees.",
        notes: "Current synthetic dataset exercises nesting, but not the target depth 9-12 or million-entry scale.",
    },
    BenchmarkDatasetPlan {
        id: "mixed-working-set",
        title: "Mixed working set",
        coverage: BenchmarkCoverage::Implemented,
        current_fixture: "mixed",
        primary_goal: "Track general-purpose throughput, compression ratio, and stage balance on realistic size and entropy mixes.",
        notes: "This is the main baseline for comparing reader, compression, and bundle-size settings.",
    },
    BenchmarkDatasetPlan {
        id: "unstable-subtree",
        title: "Unstable subtree",
        coverage: BenchmarkCoverage::Planned,
        current_fixture: "planned fault-style dataset with live churn and subtree skips",
        primary_goal: "Measure retry cost, terminal skips, and durable resume behavior under partial subtree instability.",
        notes: "Live-file correctness has regression tests; the fresh static-fixture matrix does not measure churn or reconnect latency.",
    },
    BenchmarkDatasetPlan {
        id: "mount-backed-source",
        title: "Mount-backed source",
        coverage: BenchmarkCoverage::Planned,
        current_fixture: "planned external corpus or snapshot-backed source adapter run",
        primary_goal: "Validate source-backend boundaries, mount-aware fast paths, and realistic storage latency effects.",
        notes: "This remains outside committed synthetic fixtures but is part of the frozen acceptance matrix.",
    },
    BenchmarkDatasetPlan {
        id: "fallback-vs-unix",
        title: "Cross-platform source traversal",
        coverage: BenchmarkCoverage::Planned,
        current_fixture: "planned paired Linux and Windows runs on equivalent fixtures",
        primary_goal: "Compare correctness and throughput of supported platform paths on the same corpus.",
        notes: "The legacy executor selector does not change v4 runtime behavior; separate operating-system runs are needed for this comparison.",
    },
    BenchmarkDatasetPlan {
        id: "large-payload-baseline",
        title: "Large payload baseline",
        coverage: BenchmarkCoverage::Implemented,
        current_fixture: "bigFiles",
        primary_goal: "Keep a control case for compression throughput, frame isolation, and receiver fsync costs on large files.",
        notes: "This complements tiny-file and mixed workloads; reconnect and live-churn performance remain separate planned cases.",
    },
];

const BENCHMARK_NETWORK_PROFILES: &[BenchmarkNetworkProfile] = &[
    BenchmarkNetworkProfile {
        id: LOOPBACK_LOCALHOST_PROFILE,
        title: "Loopback localhost baseline",
        coverage: BenchmarkCoverage::Implemented,
        rtt_ms: "<1",
        bandwidth_limit: "host-limited",
        loss_or_disconnects: "none",
        primary_goal: "Fast developer-local signal for stage balance before shaped-network validation.",
    },
    BenchmarkNetworkProfile {
        id: LINK_1GBPS_PROFILE,
        title: "1 Gbit/s shared application-byte rate",
        coverage: BenchmarkCoverage::Implemented,
        rtt_ms: "0 added",
        bandwidth_limit: "125000000 B/s per direction, shared across channels",
        loss_or_disconnects: "none",
        primary_goal: "Rate-only full-duplex model with a bounded 8 MiB burst and at most 4 MiB per read. No TCP RTT, congestion, or Ethernet overhead emulation.",
    },
    BenchmarkNetworkProfile {
        id: LINK_10GBPS_PROFILE,
        title: "10 Gbit/s shared application-byte rate",
        coverage: BenchmarkCoverage::Implemented,
        rtt_ms: "0 added",
        bandwidth_limit: "1250000000 B/s per direction, shared across channels",
        loss_or_disconnects: "none",
        primary_goal: "Rate-only full-duplex model with a bounded 8 MiB burst and at most 4 MiB per read. Calibrate host throughput before interpreting results.",
    },
    BenchmarkNetworkProfile {
        id: LAN_LOW_RTT_PROFILE,
        title: "Legacy approximate low-latency LAN",
        coverage: BenchmarkCoverage::Implemented,
        rtt_ms: "1",
        bandwidth_limit: "250 MiB/s shaper setting",
        loss_or_disconnects: "none",
        primary_goal: "Historical per-I/O delay/pacing comparison only; independent channel budgets do not model one physical link.",
    },
    BenchmarkNetworkProfile {
        id: WAN_MODERATE_RTT_PROFILE,
        title: "Legacy approximate moderate WAN RTT",
        coverage: BenchmarkCoverage::Implemented,
        rtt_ms: "30",
        bandwidth_limit: "125 MiB/s shaper setting",
        loss_or_disconnects: "none",
        primary_goal: "Historical application-delay stress case; TCP ACKs and congestion still run on localhost.",
    },
    BenchmarkNetworkProfile {
        id: WAN_HIGH_RTT_PROFILE,
        title: "Legacy approximate high-RTT durable-ack cost",
        coverage: BenchmarkCoverage::Implemented,
        rtt_ms: "100",
        bandwidth_limit: "125 MiB/s shaper setting",
        loss_or_disconnects: "none",
        primary_goal: "Historical application-delay stress case, not a physical 1 Gbit/s link or true TCP RTT model.",
    },
    BenchmarkNetworkProfile {
        id: BANDWIDTH_LIMITED_PROFILE,
        title: "Legacy approximate bandwidth-limited link",
        coverage: BenchmarkCoverage::Implemented,
        rtt_ms: "30",
        bandwidth_limit: "25 MiB/s",
        loss_or_disconnects: "none",
        primary_goal: "Historical per-I/O delay/pacing comparison; preserve old results without equating this model to a physical link.",
    },
    BenchmarkNetworkProfile {
        id: LOSSY_RECONNECT_PRONE_PROFILE,
        title: "Lossy or reconnect-prone link",
        coverage: BenchmarkCoverage::Planned,
        rtt_ms: "60",
        bandwidth_limit: "25 MiB/s",
        loss_or_disconnects: "0.5% loss or forced disconnect every 256 MiB",
        primary_goal: "Measure replay latency, durable resume boundaries, and issue-tail restart behavior.",
    },
];

const BENCHMARK_COMPARISON_RULES: &[BenchmarkComparisonRule] = &[
    BenchmarkComparisonRule {
        id: "compare-like-for-like-configs",
        rule: "Compare benchmark runs only on the same machine, dataset, network profile, sender knobs, and warm/cold cache mode.",
    },
    BenchmarkComparisonRule {
        id: "summary-means-and-repeat-counts",
        rule: "Summary CSV values are arithmetic means across completed repeats, including the per-run maxima columns. Keep raw results and repeat counts; median, p90, and an overall peak require separate calculations from raw rows.",
    },
    BenchmarkComparisonRule {
        id: "loopback-is-not-cutover",
        rule: "Loopback localhost is a developer baseline only; it cannot by itself justify a production default tuning change.",
    },
    BenchmarkComparisonRule {
        id: "full-matrix-before-cutover",
        rule: "Cutover requires the full required dataset and network-profile matrix, not isolated wins on a single workload.",
    },
];

const BENCHMARK_SUCCESS_GATES: &[BenchmarkSuccessGate] = &[
    BenchmarkSuccessGate {
        id: "whole-file-correctness",
        title: "Whole-file correctness remains non-negotiable",
        rule: "Every compared run must pass the relevant safety and archive-integrity suites before any performance number counts.",
        evidence: "Connectivity and archive-correctness tests stay green on the measured revision. Matrix archives are verified by default after timing and resource sampling; FAST_DUMP_BENCH_VERIFY_ARCHIVE=0 explicitly disables that check.",
    },
    BenchmarkSuccessGate {
        id: "bounded-ram",
        title: "RAM stays explicitly bounded",
        rule: "Inspect per-run peak combined-process RSS against the declared budget; separate sender and receiver memory attribution is not implemented.",
        evidence: "Windows raw rows contain sampled process peaks; summary max_rss_mib is their mean. Other targets emit zero placeholders. Sender scan queues bound entry counts, not metadata bytes; active sorted directories and ambiguous-name subtrees can still grow, and public collecting scan helpers retain their full report.",
    },
    BenchmarkSuccessGate {
        id: "bounded-disk",
        title: "Disk growth stays explicitly bounded",
        rule: "Review receiver archive, WAL, and issue-file growth and cleanup across aborts and resumes; v4 does not use sender or receiver manifest spools.",
        evidence: "The current matrix records final issue-file size but does not measure total disk growth or enforce disk quotas. Abort and resume correctness are covered separately by tests.",
    },
    BenchmarkSuccessGate {
        id: "resume-from-durable-bundles",
        title: "Reconnect preserves durable bundle boundaries",
        rule: "Resume from the last durable committed bundle without duplicate archive entries; the current sender rescans the live source and skips committed work ordinals.",
        evidence: "Disconnect/resume correctness tests verify complete archives. Fresh-dump matrix rows do not measure reconnect or replay latency.",
    },
    BenchmarkSuccessGate {
        id: "target-throughput-non-regression",
        title: "Target scenarios do not regress throughput",
        rule: "The current runtime must match or improve the recorded baseline throughput on the required target scenarios after accounting for the same correctness and resource budgets.",
        evidence: "Per-scenario mean elapsed time and raw repeat rows are stored with matching configuration and compared against the baseline artifacts.",
    },
];

impl BenchDir {
    pub(crate) fn new(prefix: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // Для тяжёлых bench-прогонов разрешаем увести временные каталоги с системного диска.
        let temp_root = std::env::var_os("FAST_DUMP_BENCH_TEMP_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let path = temp_root.join(format!(
            "fast-dump-bench-{prefix}-{}-{}",
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for BenchDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

struct DeterministicBytes {
    state: u64,
}

impl DeterministicBytes {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        value
    }

    fn fill(&mut self, buffer: &mut [u8]) {
        for chunk in buffer.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

pub(crate) struct BenchStream<'a> {
    read: Cursor<&'a [u8]>,
    pub(crate) written: Vec<u8>,
}

impl<'a> BenchStream<'a> {
    pub(crate) fn new(read_bytes: &'a [u8]) -> Self {
        Self {
            read: Cursor::new(read_bytes),
            written: Vec::new(),
        }
    }
}

impl Read for BenchStream<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read.read(buf)
    }
}

impl Write for BenchStream<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.written.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn env_usize(name: &str, default: usize, minimum: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value >= minimum)
        .unwrap_or(default.max(minimum))
}

fn env_u64(name: &str, default: u64, minimum: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value >= minimum)
        .unwrap_or(default.max(minimum))
}

pub(crate) fn prepare_profile_sample_count() -> usize {
    env_usize("FAST_DUMP_BENCH_PREPARE_PROFILE_SAMPLES", 100, 1)
}

pub(crate) fn loopback_profile_sample_count() -> usize {
    env_usize("FAST_DUMP_BENCH_LOOPBACK_PROFILE_SAMPLES", 100, 1)
}

pub(crate) fn prepare_group_sample_size() -> usize {
    env_usize("FAST_DUMP_BENCH_PREPARE_SAMPLE_SIZE", 100, 10)
}

pub(crate) fn loopback_group_sample_size() -> usize {
    env_usize("FAST_DUMP_BENCH_LOOPBACK_SAMPLE_SIZE", 100, 10)
}

pub(crate) fn loopback_warm_up_time() -> Duration {
    Duration::from_millis(env_u64("FAST_DUMP_BENCH_LOOPBACK_WARMUP_MS", 3_000, 100))
}

pub(crate) fn loopback_measurement_time() -> Duration {
    Duration::from_millis(env_u64(
        "FAST_DUMP_BENCH_LOOPBACK_MEASUREMENT_MS",
        5_000,
        100,
    ))
}

pub(crate) fn scan_group_sample_size() -> usize {
    env_usize("FAST_DUMP_BENCH_SCAN_SAMPLE_SIZE", 1000, 10)
}

pub(crate) fn monitor_interval() -> Duration {
    Duration::from_millis(env_u64("FAST_DUMP_BENCH_MONITOR_INTERVAL_MS", 50, 1))
}

fn write_random_file(path: &Path, size: u64, seed: u64) {
    let mut file = fs::File::create(path).unwrap();
    let mut generator = DeterministicBytes::new(seed);
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut remaining = size;

    while remaining != 0 {
        let chunk_len = remaining.min(buffer.len() as u64) as usize;
        generator.fill(&mut buffer[..chunk_len]);
        file.write_all(&buffer[..chunk_len]).unwrap();
        remaining -= chunk_len as u64;
    }
}

fn fill_repeated_pattern(buffer: &mut [u8], pattern: &[u8]) {
    for chunk in buffer.chunks_mut(pattern.len()) {
        chunk.copy_from_slice(&pattern[..chunk.len()]);
    }
}

fn fill_mutated_pattern(buffer: &mut [u8], pattern: &[u8], seed: u64) {
    fill_repeated_pattern(buffer, pattern);
    let mut generator = DeterministicBytes::new(seed);
    let mutation_count = (buffer.len() / (32 * 1024)).max(1);
    for _ in 0..mutation_count {
        let index = (generator.next_u64() as usize) % buffer.len();
        buffer[index] ^= (generator.next_u64() & 0xFF) as u8;
    }
}

fn write_mixed_entropy_file(path: &Path, size: u64, seed: u64) {
    let mut file = fs::File::create(path).unwrap();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut random_generator = DeterministicBytes::new(seed);
    let mut template_generator = DeterministicBytes::new(seed ^ 0x9E37_79B9_7F4A_7C15);
    let mut template_a = vec![0_u8; ENTROPY_TEMPLATE_BYTES];
    let mut template_b = vec![0_u8; ENTROPY_TEMPLATE_BYTES];
    template_generator.fill(&mut template_a);
    template_generator.fill(&mut template_b);

    let mut remaining = size;
    let mut chunk_index = 0_u64;
    while remaining != 0 {
        let chunk_len = remaining.min(buffer.len() as u64) as usize;
        let chunk = &mut buffer[..chunk_len];
        match seed.wrapping_add(chunk_index.wrapping_mul(17)) % 8 {
            0..=2 => random_generator.fill(chunk),
            3 | 4 => fill_mutated_pattern(
                chunk,
                &template_a,
                seed ^ chunk_index.wrapping_mul(0xA5A5_A5A5_5A5A_5A5A),
            ),
            5 => fill_mutated_pattern(
                chunk,
                &template_b,
                seed ^ chunk_index.wrapping_mul(0xD6E8_FD50_93A4_4B29),
            ),
            6 => fill_repeated_pattern(chunk, &template_a),
            _ => fill_repeated_pattern(chunk, &template_b),
        }
        file.write_all(chunk).unwrap();
        remaining -= chunk_len as u64;
        chunk_index += 1;
    }
}

pub(crate) fn create_mixed_fixture_set(dir: &Path) -> u64 {
    let mut total =
        create_nested_small_files_fixture_set(dir, MIXED_SMALL_FILE_COUNT, MAX_SMALL_FILE_DEPTH);

    for index in 0..MIXED_MEDIUM_FILE_COUNT {
        let size = match index % 8 {
            0 => 256 * 1024,
            1 | 2 => 512 * 1024,
            3 | 4 => 1024 * 1024,
            5 | 6 => 2 * 1024 * 1024,
            _ => 4 * 1024 * 1024,
        };
        let path = dir.join(format!(
            "payloads/medium/group-{:02}/tier-{:02}/medium-{index:03}.bin",
            index % 11,
            index % 7
        ));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        write_mixed_entropy_file(
            &path,
            size,
            MIXED_SMALL_FILE_COUNT as u64 + index as u64 + 1,
        );
        total += size;
    }

    for (index, (name, size)) in MIXED_LARGE_FILE_CASES.iter().enumerate() {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        write_mixed_entropy_file(
            &path,
            *size,
            MIXED_SMALL_FILE_COUNT as u64 + MIXED_MEDIUM_FILE_COUNT as u64 + index as u64 + 1,
        );
        total += *size;
    }

    total
}

pub(crate) fn create_big_files_fixture_set(dir: &Path) -> u64 {
    let mut total = 0_u64;
    for (index, (name, size)) in BIG_FILE_CASES.iter().enumerate() {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        write_mixed_entropy_file(&path, *size, index as u64 + 1);
        total += *size;
    }
    total
}

fn nested_small_file_rel_path(index: usize, max_depth: usize) -> PathBuf {
    let depth = 1 + (index % max_depth.max(1));
    let mut relative = PathBuf::new();
    let mut cursor = index;

    for level in 0..depth {
        let bucket = cursor % 23;
        let segment = match level % 3 {
            0 => format!("shared-{bucket:02}"),
            1 => format!("tier-{:02}", bucket % 11),
            _ => format!("branch-{:02}", bucket % 17),
        };
        relative.push(segment);
        cursor = cursor / 23 + level + 1;
    }

    relative.push(format!("file-{index:05}.bin"));
    relative
}

pub(crate) fn create_nested_small_files_fixture_set(
    dir: &Path,
    file_count: usize,
    max_depth: usize,
) -> u64 {
    let mut total = 0_u64;
    for index in 0..file_count {
        let size = 512_u64 * (1 + (index % 8) as u64);
        let path = dir.join(nested_small_file_rel_path(index, max_depth));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        write_random_file(&path, size, index as u64 + 1);
        total += size;
    }
    total
}

pub(crate) fn create_single_large_file_fixture(dir: &Path, size: u64) -> u64 {
    write_random_file(&dir.join("payload.bin"), size, 1);
    size
}

pub(crate) fn available_sender_threads_limit() -> u32 {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get().saturating_sub(1).max(1) as u32)
        .unwrap_or(1)
}

pub(crate) fn available_logical_cpus() -> f64 {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get() as f64)
        .unwrap_or(1.0)
}

fn sample_send_options() -> SendOptions {
    SendOptions {
        level: 3,
        threads: 1,
        frame_target_bytes: 8 * 1024 * 1024,
        min_readers: 1,
        max_readers: fast_dump::sender::BundlePlanOptions::default().readers,
        read_buffer_bytes: 1024 * 1024,
        manifest_chunk_bytes: 512 * 1024,
        manifest_ack_records: 4_096,
        manifest_ack_bytes: 8 * 1024 * 1024,
    }
}

pub(crate) fn default_matrix_send_options() -> SendOptions {
    sample_send_options()
}

pub(crate) fn benchmark_dataset_plan() -> &'static [BenchmarkDatasetPlan] {
    BENCHMARK_DATASET_PLAN
}

pub(crate) fn benchmark_network_profiles() -> &'static [BenchmarkNetworkProfile] {
    BENCHMARK_NETWORK_PROFILES
}

pub(crate) fn benchmark_network_profile(
    profile_id: &str,
) -> Option<&'static BenchmarkNetworkProfile> {
    BENCHMARK_NETWORK_PROFILES
        .iter()
        .find(|profile| profile.id == profile_id)
}

pub(crate) fn benchmark_network_shape(profile_id: &str) -> Option<BenchmarkNetworkShape> {
    match profile_id {
        LOOPBACK_LOCALHOST_PROFILE => Some(BenchmarkNetworkShape {
            rtt_ms: 0,
            bandwidth_mib_per_sec: None,
            shared_receive_bytes_per_second: None,
        }),
        LINK_1GBPS_PROFILE => Some(BenchmarkNetworkShape {
            rtt_ms: 0,
            bandwidth_mib_per_sec: None,
            shared_receive_bytes_per_second: Some(125_000_000),
        }),
        LINK_10GBPS_PROFILE => Some(BenchmarkNetworkShape {
            rtt_ms: 0,
            bandwidth_mib_per_sec: None,
            shared_receive_bytes_per_second: Some(1_250_000_000),
        }),
        LAN_LOW_RTT_PROFILE => Some(BenchmarkNetworkShape {
            rtt_ms: 1,
            bandwidth_mib_per_sec: Some(250),
            shared_receive_bytes_per_second: None,
        }),
        WAN_MODERATE_RTT_PROFILE => Some(BenchmarkNetworkShape {
            rtt_ms: 30,
            bandwidth_mib_per_sec: Some(125),
            shared_receive_bytes_per_second: None,
        }),
        WAN_HIGH_RTT_PROFILE => Some(BenchmarkNetworkShape {
            rtt_ms: 100,
            bandwidth_mib_per_sec: Some(125),
            shared_receive_bytes_per_second: None,
        }),
        BANDWIDTH_LIMITED_PROFILE => Some(BenchmarkNetworkShape {
            rtt_ms: 30,
            bandwidth_mib_per_sec: Some(25),
            shared_receive_bytes_per_second: None,
        }),
        LOSSY_RECONNECT_PRONE_PROFILE => None,
        _ => None,
    }
}

pub(crate) fn default_matrix_network_profiles() -> Vec<&'static str> {
    BENCHMARK_NETWORK_PROFILES
        .iter()
        .filter(|profile| {
            profile.coverage == BenchmarkCoverage::Implemented
                && benchmark_network_shape(profile.id).is_some()
        })
        .map(|profile| profile.id)
        .collect()
}

pub(crate) fn benchmark_comparison_rules() -> &'static [BenchmarkComparisonRule] {
    BENCHMARK_COMPARISON_RULES
}

pub(crate) fn benchmark_success_gates() -> &'static [BenchmarkSuccessGate] {
    BENCHMARK_SUCCESS_GATES
}

pub(crate) fn matrix_thread_counts() -> Vec<u32> {
    let cores_limit = available_sender_threads_limit();
    let mut counts = vec![1_u32, 2, 4, 8, 16, cores_limit];
    counts.retain(|count| *count <= cores_limit);
    counts.sort_unstable();
    counts.dedup();
    counts
}

pub(crate) fn matrix_frame_target_values_mb() -> Vec<u64> {
    vec![4, 16, 64]
}

pub(crate) fn matrix_read_buffer_values_kb() -> Vec<u64> {
    vec![64, 256, 1024, 4096]
}

fn ensure_dataset<F>(root: &Path, name: &'static str, build: F) -> Result<MatrixDataset>
where
    F: FnOnce(&Path) -> String,
{
    let dataset_path = root.join(name);
    let ready_marker = root.join(format!("{name}{DATASET_READY_MARKER}"));
    if ready_marker.exists() && dataset_path.exists() {
        return Ok(MatrixDataset {
            name,
            path: dataset_path,
        });
    }

    if ready_marker.exists() {
        let _ = fs::remove_file(&ready_marker);
    }
    if dataset_path.exists() {
        fs::remove_dir_all(&dataset_path).with_context(|| {
            format!(
                "failed to remove incomplete dataset directory {}",
                dataset_path.display()
            )
        })?;
    }
    fs::create_dir_all(&dataset_path).with_context(|| {
        format!(
            "failed to create benchmark dataset directory {}",
            dataset_path.display()
        )
    })?;

    let ready_contents = build(&dataset_path);
    fs::write(&ready_marker, ready_contents).with_context(|| {
        format!(
            "failed to write dataset ready marker {}",
            ready_marker.display()
        )
    })?;

    Ok(MatrixDataset {
        name,
        path: dataset_path,
    })
}

pub(crate) fn ensure_many_small_files_dataset(root: &Path) -> Result<MatrixDataset> {
    ensure_dataset(root, MANY_SMALL_FILES_DATASET, |dataset_path| {
        let total_bytes = create_nested_small_files_fixture_set(
            dataset_path,
            PREPARE_SMALL_FILE_COUNT,
            MAX_SMALL_FILE_DEPTH,
        );
        format!(
            "dataset={}\nfiles={}\nraw_bytes={}\nmax_depth={}\n",
            MANY_SMALL_FILES_DATASET, PREPARE_SMALL_FILE_COUNT, total_bytes, MAX_SMALL_FILE_DEPTH
        )
    })
}

pub(crate) fn ensure_big_files_dataset(root: &Path) -> Result<MatrixDataset> {
    ensure_dataset(root, BIG_FILES_DATASET, |dataset_path| {
        let total_bytes = create_big_files_fixture_set(dataset_path);
        format!(
            "dataset={}\nfiles={}\nraw_bytes={}\nfile_sizes_mib=256,512,512\nentropy=mixed\n",
            BIG_FILES_DATASET,
            BIG_FILE_CASES.len(),
            total_bytes
        )
    })
}

pub(crate) fn ensure_mixed_dataset(root: &Path) -> Result<MatrixDataset> {
    ensure_dataset(root, MIXED_DATASET, |dataset_path| {
        let total_bytes = create_mixed_fixture_set(dataset_path);
        format!(
            "dataset={}\nsmall_files={}\nmedium_files={}\nlarge_files={}\nraw_bytes={}\nmax_depth={}\nentropy=mixed\n",
            MIXED_DATASET,
            MIXED_SMALL_FILE_COUNT,
            MIXED_MEDIUM_FILE_COUNT,
            MIXED_LARGE_FILE_CASES.len(),
            total_bytes,
            MAX_SMALL_FILE_DEPTH
        )
    })
}

pub(crate) fn loopback_addr() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}
