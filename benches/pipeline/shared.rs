use std::fs;
use std::io::{Cursor, Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use fast_dump::network::protocol::{self, FrameChunk, FrameEnd, FrameStart, Message, ResumeState};
use fast_dump::sender::{PreparedFrame, SendOptions};
use xxhash_rust::xxh3::Xxh3Default;

pub(crate) const MANY_SMALL_FILES_DATASET: &str = "manySmallFiles";
pub(crate) const BIG_FILES_DATASET: &str = "bigFiles";
pub(crate) const MIXED_DATASET: &str = "mixed";
pub(crate) const DEFAULT_MEMORY_BUDGET_MIB: u64 = 512;
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

impl BenchDir {
    pub(crate) fn new(prefix: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
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

pub(crate) fn receive_frame_group_sample_size() -> usize {
    env_usize("FAST_DUMP_BENCH_RECEIVE_FRAME_SAMPLE_SIZE", 100, 10)
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
            0 | 1 | 2 => random_generator.fill(chunk),
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

pub(crate) fn empty_resume_state() -> ResumeState {
    ResumeState {
        last_committed_frame: 0,
        last_committed_file_index: 0,
        last_committed_file_path: String::new(),
        archive_len: 0,
    }
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
        min_readers: 2,
        max_readers: 8,
        read_buffer_bytes: 1024 * 1024,
    }
}

pub(crate) fn default_matrix_send_options() -> SendOptions {
    sample_send_options()
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

pub(crate) fn encode_prepared_frame(frame: &PreparedFrame) -> Vec<u8> {
    let mut encoded = Vec::new();
    protocol::write_message(
        &mut encoded,
        &Message::FrameStart(FrameStart {
            frame_no: frame.outbound.frame_no,
            first_file_index: frame.outbound.first_file_index,
            first_file_path: frame.outbound.first_file_path.clone(),
            level: frame.outbound.level,
            threads: frame.outbound.threads,
        }),
    )
    .unwrap();

    let mut hasher = Xxh3Default::new();
    let mut compressed_len = 0_u64;
    for chunk in &frame.chunks {
        hasher.update(chunk);
        compressed_len += chunk.len() as u64;
        protocol::write_message(
            &mut encoded,
            &Message::FrameChunk(FrameChunk {
                bytes: chunk.clone(),
            }),
        )
        .unwrap();
    }

    protocol::write_message(
        &mut encoded,
        &Message::FrameEnd(FrameEnd {
            frame_no: frame.outbound.frame_no,
            last_file_index: frame.outbound.last_file_index,
            last_file_path: frame.outbound.last_file_path.clone(),
            compressed_len,
            compressed_xxh3_64: hasher.digest(),
        }),
    )
    .unwrap();

    encoded
}

pub(crate) fn total_compressed_bytes(frame: &PreparedFrame) -> u64 {
    frame.chunks.iter().map(|chunk| chunk.len() as u64).sum()
}
