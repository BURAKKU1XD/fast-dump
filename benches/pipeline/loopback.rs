use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fmt::Display;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::{CommandFactory, Parser, ValueEnum};
use fast_dump::{network::protocol, sender::SendOptions};

use super::results::{
    RawResultRow, ScenarioKey, aggregate_summary_rows, append_raw_result, completed_runs,
    dataset_summary_rows, load_raw_results, print_dataset_markdown_tables, reset_results,
    results_raw_path, results_summary_path, write_benchmark_plan, write_benchmark_report,
    write_summary_csv,
};
use super::shared::{
    BIG_FILES_DATASET, BenchmarkProtocolRuntime, DEFAULT_MEMORY_BUDGET_MIB,
    MANY_SMALL_FILES_DATASET, MIXED_DATASET, MatrixDataset, benchmark_network_profile,
    benchmark_network_shape, benchmark_protocol_runtimes, default_matrix_network_profiles,
    default_matrix_send_options, ensure_big_files_dataset, ensure_many_small_files_dataset,
    ensure_mixed_dataset, matrix_frame_target_values_mb, matrix_read_buffer_values_kb,
    matrix_thread_counts,
};
use super::stats::{LoopbackRunMetrics, run_loopback_scenario};

const DEFAULT_LARGE_FILE_SINGLE_FRAME_THRESHOLD_MIB: u64 = 16;
const MIB: u64 = 1024 * 1024;
const FORCE_ABSOLUTE_PATH_EXECUTOR_ENV: &str = "FAST_DUMP_FORCE_ABS_PATH_EXECUTOR";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum ExecutorMode {
    Default,
    AbsPath,
}

impl ExecutorMode {
    fn label(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::AbsPath => "abs_path",
        }
    }
}

impl Display for ExecutorMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label())
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "loopback_send_dump",
    about = "Run sender->receiver loopback matrix benchmarks without Criterion warmup/measurement"
)]
struct MatrixArgs {
    #[arg(long = "dataset", value_delimiter = ',', num_args = 1..)]
    datasets: Vec<String>,
    #[arg(long = "protocol-runtime", value_delimiter = ',', num_args = 1..)]
    protocol_runtimes: Vec<String>,
    #[arg(long = "network-profile", value_delimiter = ',', num_args = 1..)]
    network_profiles: Vec<String>,
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    threads: Vec<u32>,
    #[arg(long = "frames", value_delimiter = ',', num_args = 1..)]
    frames_mb: Vec<u64>,
    #[arg(long = "buffers", value_delimiter = ',', num_args = 1..)]
    buffers_kb: Vec<u64>,
    #[arg(long = "manifest-chunk-kb", value_delimiter = ',', num_args = 1..)]
    manifest_chunk_kb: Vec<usize>,
    #[arg(long = "manifest-ack-records", value_delimiter = ',', num_args = 1..)]
    manifest_ack_records: Vec<u64>,
    #[arg(long = "manifest-ack-kb", value_delimiter = ',', num_args = 1..)]
    manifest_ack_kb: Vec<usize>,
    #[arg(long)]
    resume: bool,
    #[arg(long, default_value_t = 1)]
    repeats: u32,
    #[arg(long, default_value_t = DEFAULT_MEMORY_BUDGET_MIB)]
    memory_budget_mib: u64,
    #[arg(long = "large-file-single-frame-thresholds-mib", value_delimiter = ',', num_args = 1.., hide = true)]
    large_file_single_frame_thresholds_mib: Vec<u64>,
    #[arg(long = "executor", value_enum, value_delimiter = ',', num_args = 1.., hide = true)]
    executors: Vec<ExecutorMode>,
    #[arg(long, hide = true)]
    shuffle_scenarios: bool,
    #[arg(long, hide = true)]
    shuffle_seed: Option<u64>,
    #[arg(long, hide = true)]
    bench: bool,
}

struct LargeFileThresholdEnvGuard {
    previous: Option<OsString>,
}

impl LargeFileThresholdEnvGuard {
    fn set(threshold_bytes: u64) -> Self {
        let previous = std::env::var_os("FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES");
        // В bench-runner меняем env только между завершёнными сценариями; worker threads
        // конкретного прогона уже завершены до восстановления переменной.
        unsafe {
            std::env::set_var(
                "FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES",
                threshold_bytes.to_string(),
            );
        }
        Self { previous }
    }
}

impl Drop for LargeFileThresholdEnvGuard {
    fn drop(&mut self) {
        // См. комментарий в set(): env живёт только как knob для следующего isolated run.
        unsafe {
            if let Some(previous) = &self.previous {
                std::env::set_var(
                    "FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES",
                    previous,
                );
            } else {
                std::env::remove_var("FAST_DUMP_LARGE_FILE_SINGLE_FRAME_THRESHOLD_BYTES");
            }
        }
    }
}

struct ExecutorEnvGuard {
    previous: Option<OsString>,
}

impl ExecutorEnvGuard {
    fn set(mode: ExecutorMode) -> Self {
        let previous = std::env::var_os(FORCE_ABSOLUTE_PATH_EXECUTOR_ENV);
        unsafe {
            match mode {
                ExecutorMode::Default => {
                    std::env::remove_var(FORCE_ABSOLUTE_PATH_EXECUTOR_ENV);
                }
                ExecutorMode::AbsPath => {
                    std::env::set_var(FORCE_ABSOLUTE_PATH_EXECUTOR_ENV, "1");
                }
            }
        }
        Self { previous }
    }
}

impl Drop for ExecutorEnvGuard {
    fn drop(&mut self) {
        unsafe {
            if let Some(previous) = &self.previous {
                std::env::set_var(FORCE_ABSOLUTE_PATH_EXECUTOR_ENV, previous);
            } else {
                std::env::remove_var(FORCE_ABSOLUTE_PATH_EXECUTOR_ENV);
            }
        }
    }
}

pub(crate) fn main() -> Result<()> {
    let has_explicit_args = std::env::args_os()
        .skip(1)
        .any(|arg| arg.to_string_lossy() != "--bench");
    if !has_explicit_args {
        let mut command = MatrixArgs::command();
        command
            .print_help()
            .context("failed to print matrix runner help")?;
        println!();
        println!();
        println!(
            "Use `cargo bench --bench loopback_send_dump -- --dataset {},{},{}`
to run the full dataset set, or pass one dataset name to focus a single case.",
            MANY_SMALL_FILES_DATASET, BIG_FILES_DATASET, MIXED_DATASET
        );
        return Ok(());
    }

    run_matrix(MatrixArgs::parse())
}

pub(crate) fn resolve_dataset_names(requested: &[String]) -> Result<Vec<&'static str>> {
    let requested_names = if requested.is_empty() {
        vec![MANY_SMALL_FILES_DATASET, BIG_FILES_DATASET, MIXED_DATASET]
    } else {
        let mut deduped = BTreeSet::new();
        for name in requested {
            deduped.insert(name.trim().to_owned());
        }
        deduped
            .into_iter()
            .map(|name| match name.as_str() {
                MANY_SMALL_FILES_DATASET => Ok(MANY_SMALL_FILES_DATASET),
                BIG_FILES_DATASET => Ok(BIG_FILES_DATASET),
                MIXED_DATASET => Ok(MIXED_DATASET),
                other => bail!(
                    "unsupported dataset `{other}`; currently implemented datasets: {}, {}, {}",
                    MANY_SMALL_FILES_DATASET,
                    BIG_FILES_DATASET,
                    MIXED_DATASET
                ),
            })
            .collect::<Result<Vec<_>>>()?
    };

    Ok(requested_names)
}

fn resolve_protocol_runtimes(requested: &[String]) -> Result<Vec<BenchmarkProtocolRuntime>> {
    if requested.is_empty() {
        return Ok(benchmark_protocol_runtimes().to_vec());
    }

    let mut selected = BTreeSet::new();
    for runtime in requested {
        match runtime.trim() {
            "v2" => {
                selected.insert(BenchmarkProtocolRuntime::V2);
            }
            "v3" => {
                selected.insert(BenchmarkProtocolRuntime::V3);
            }
            other => {
                bail!("unsupported protocol runtime `{other}`; supported runtimes: v2, v3");
            }
        }
    }

    Ok(selected.into_iter().collect())
}

fn resolve_network_profiles(requested: &[String]) -> Result<Vec<&'static str>> {
    if requested.is_empty() {
        return Ok(default_matrix_network_profiles());
    }

    let mut selected = BTreeSet::new();
    for profile_id in requested {
        let profile_id = profile_id.trim();
        let Some(profile) = benchmark_network_profile(profile_id) else {
            bail!("unsupported network profile `{profile_id}`");
        };
        if benchmark_network_shape(profile.id).is_none() {
            bail!(
                "network profile `{}` is documented, but the current loopback runner does not implement a deterministic shaping model for it yet",
                profile.id
            );
        }
        selected.insert(profile.id);
    }

    Ok(selected.into_iter().collect())
}

fn run_matrix(args: MatrixArgs) -> Result<()> {
    if args.repeats == 0 {
        bail!("--repeats must be at least 1");
    }

    let output_dir = std::env::var_os("FAST_DUMP_BENCH_MATRIX_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("target")
                .join("bench-matrix")
        });
    if !args.resume {
        reset_results(&output_dir)?;
    }
    let benchmark_plan_path = write_benchmark_plan(&output_dir)?;

    let raw_results_path = results_raw_path(&output_dir);
    let summary_results_path = results_summary_path(&output_dir);
    let mut raw_rows = load_raw_results(&raw_results_path)?;
    let mut completed = completed_runs(&raw_rows);

    let dataset_root = std::env::var_os("FAST_DUMP_BENCH_DATASET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("FAST_DUMP_BENCH_TEMP_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(std::env::temp_dir)
                .join("fast-dump-bench-datasets")
        });
    let datasets = resolve_datasets(&args.datasets, &dataset_root)?;
    let protocol_runtimes = resolve_protocol_runtimes(&args.protocol_runtimes)?;
    let network_profiles = resolve_network_profiles(&args.network_profiles)?;
    let threads = resolve_values("threads", &matrix_thread_counts(), &args.threads)?;
    let frames_mb = resolve_values("frames", &matrix_frame_target_values_mb(), &args.frames_mb)?;
    let buffers_kb = resolve_values("buffers", &matrix_read_buffer_values_kb(), &args.buffers_kb)?;
    let manifest_chunk_kb = resolve_manifest_chunk_values_kb(&args.manifest_chunk_kb)?;
    let manifest_ack_records = resolve_manifest_ack_record_values(&args.manifest_ack_records)?;
    let manifest_ack_kb = resolve_manifest_ack_values_kb(&args.manifest_ack_kb)?;
    let thresholds_mib = resolve_large_file_single_frame_thresholds_mib(
        &args.large_file_single_frame_thresholds_mib,
    )?;
    let executors = resolve_executor_modes(&args.executors)?;
    let shuffle_seed = args
        .shuffle_seed
        .unwrap_or_else(|| u64::from(std::process::id()) << 32 | u64::from(args.repeats));

    for dataset in datasets {
        eprintln!(
            "[bench-matrix] dataset={} path={} scenarios={} repeats={} thresholds={} runtimes={} networks={} executors={} resume={} shuffle={} plan={}",
            dataset.name,
            dataset.path.display(),
            protocol_runtimes.len()
                * network_profiles.len()
                * executors.len()
                * threads.len()
                * frames_mb.len()
                * buffers_kb.len()
                * manifest_chunk_kb.len()
                * manifest_ack_records.len()
                * manifest_ack_kb.len()
                * thresholds_mib.len(),
            args.repeats,
            join_values(&thresholds_mib),
            join_values(&protocol_runtimes),
            join_values(&network_profiles),
            join_values(&executors),
            args.resume,
            args.shuffle_scenarios,
            benchmark_plan_path.display()
        );

        let mut executed_runs = 0_usize;
        let mut skipped_runs = 0_usize;
        let mut planned_runs = Vec::new();
        for runtime in &protocol_runtimes {
            for network_profile in &network_profiles {
                for executor in &executors {
                    for threads in &threads {
                        for frame_target_mb in &frames_mb {
                            for read_buffer_kb in &buffers_kb {
                                for manifest_chunk_kb in &manifest_chunk_kb {
                                    for manifest_ack_records in &manifest_ack_records {
                                        for manifest_ack_kb in &manifest_ack_kb {
                                            if *manifest_ack_kb < *manifest_chunk_kb {
                                                bail!(
                                                    "manifest-ack-kb must not be smaller than manifest-chunk-kb: {} < {}",
                                                    manifest_ack_kb,
                                                    manifest_chunk_kb
                                                );
                                            }
                                            for threshold_mib in &thresholds_mib {
                                                for repeat_index in 1..=args.repeats {
                                                    planned_runs.push((
                                                        *runtime,
                                                        *network_profile,
                                                        *executor,
                                                        ScenarioKey {
                                                            dataset: dataset.name.to_owned(),
                                                            protocol_runtime: runtime
                                                                .id()
                                                                .to_owned(),
                                                            network_profile: (*network_profile)
                                                                .to_owned(),
                                                            executor: executor.label().to_owned(),
                                                            threads: *threads,
                                                            frame_target_mb: *frame_target_mb,
                                                            read_buffer_kb: *read_buffer_kb,
                                                            manifest_chunk_kb: *manifest_chunk_kb,
                                                            manifest_ack_records:
                                                                *manifest_ack_records,
                                                            manifest_ack_kb: *manifest_ack_kb,
                                                            large_file_single_frame_threshold_mib:
                                                                *threshold_mib,
                                                        },
                                                        repeat_index,
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if args.shuffle_scenarios {
            shuffle_planned_runs(
                &mut planned_runs,
                shuffle_seed ^ dataset_shuffle_salt(dataset.name),
            );
        }

        for (runtime, network_profile, executor_mode, scenario, repeat_index) in planned_runs {
            if args.resume && completed.contains_key(&(scenario.clone(), repeat_index)) {
                skipped_runs += 1;
                continue;
            }

            eprintln!(
                "[bench-matrix] run dataset={} runtime={} network={} executor={} threads={} frame={}MiB buffer={}KiB manifest_chunk={}KiB manifest_ack_records={} manifest_ack={}KiB isolate={}MiB repeat={}/{}",
                scenario.dataset,
                scenario.protocol_runtime,
                scenario.network_profile,
                scenario.executor,
                scenario.threads,
                scenario.frame_target_mb,
                scenario.read_buffer_kb,
                scenario.manifest_chunk_kb,
                scenario.manifest_ack_records,
                scenario.manifest_ack_kb,
                scenario.large_file_single_frame_threshold_mib,
                repeat_index,
                args.repeats
            );

            let _threshold_guard = LargeFileThresholdEnvGuard::set(
                scenario.large_file_single_frame_threshold_mib * MIB,
            );
            let _executor_guard = ExecutorEnvGuard::set(executor_mode);
            let options = scenario_options(
                scenario.threads,
                scenario.frame_target_mb,
                scenario.read_buffer_kb,
                scenario.manifest_chunk_kb,
                scenario.manifest_ack_records,
                scenario.manifest_ack_kb,
            );
            let metrics = run_loopback_scenario(&dataset.path, &options, runtime, network_profile);
            let row = raw_row(scenario.clone(), repeat_index, metrics);

            append_raw_result(&raw_results_path, &row)?;
            raw_rows.push(row.clone());
            completed.insert((scenario, repeat_index), row);

            let summaries = aggregate_summary_rows(&raw_rows);
            write_summary_csv(&summary_results_path, &summaries)?;
            write_benchmark_report(&output_dir, &summaries)?;
            executed_runs += 1;
        }

        let summaries = aggregate_summary_rows(&raw_rows);
        write_summary_csv(&summary_results_path, &summaries)?;
        write_benchmark_report(&output_dir, &summaries)?;
        let dataset_rows = dataset_summary_rows(&summaries, dataset.name);
        print_dataset_markdown_tables(dataset.name, &dataset_rows, args.memory_budget_mib);
        eprintln!(
            "[bench-matrix] dataset={} completed: executed_runs={} skipped_runs={} raw_results={} summary={}",
            dataset.name,
            executed_runs,
            skipped_runs,
            raw_results_path.display(),
            summary_results_path.display()
        );
    }

    Ok(())
}

fn resolve_datasets(requested: &[String], dataset_root: &Path) -> Result<Vec<MatrixDataset>> {
    let requested_names = resolve_dataset_names(requested)?;
    let mut datasets = Vec::with_capacity(requested_names.len());
    for name in requested_names {
        match name {
            MANY_SMALL_FILES_DATASET => {
                datasets.push(ensure_many_small_files_dataset(dataset_root)?);
            }
            BIG_FILES_DATASET => {
                datasets.push(ensure_big_files_dataset(dataset_root)?);
            }
            MIXED_DATASET => {
                datasets.push(ensure_mixed_dataset(dataset_root)?);
            }
            other => bail!("unsupported dataset `{other}`"),
        }
    }

    Ok(datasets)
}

fn resolve_values<T>(label: &str, defaults: &[T], requested: &[T]) -> Result<Vec<T>>
where
    T: Copy + Display + Ord,
{
    if requested.is_empty() {
        return Ok(defaults.to_vec());
    }

    let allowed = defaults.iter().copied().collect::<BTreeSet<_>>();
    let mut selected = BTreeSet::new();
    for value in requested {
        if !allowed.contains(value) {
            bail!(
                "unsupported {label} value `{value}`; allowed values: {}",
                join_values(defaults)
            );
        }
        selected.insert(*value);
    }

    Ok(selected.into_iter().collect())
}

fn resolve_executor_modes(requested: &[ExecutorMode]) -> Result<Vec<ExecutorMode>> {
    if requested.is_empty() {
        #[cfg(unix)]
        {
            return Ok(vec![ExecutorMode::Default, ExecutorMode::AbsPath]);
        }

        #[cfg(not(unix))]
        {
            return Ok(vec![ExecutorMode::Default]);
        }
    }

    let selected = requested.iter().copied().collect::<BTreeSet<_>>();

    #[cfg(not(unix))]
    if selected.contains(&ExecutorMode::AbsPath) {
        bail!("--executor abs-path is supported only on Unix targets");
    }

    Ok(selected.into_iter().collect())
}

fn resolve_large_file_single_frame_thresholds_mib(requested: &[u64]) -> Result<Vec<u64>> {
    if requested.is_empty() {
        return Ok(vec![DEFAULT_LARGE_FILE_SINGLE_FRAME_THRESHOLD_MIB]);
    }

    let mut selected = BTreeSet::new();
    for value in requested {
        if *value == 0 {
            bail!("large-file-single-frame-thresholds-mib must not contain 0");
        }
        selected.insert(*value);
    }

    Ok(selected.into_iter().collect())
}

fn resolve_manifest_chunk_values_kb(requested: &[usize]) -> Result<Vec<usize>> {
    let default_chunk_kb = default_matrix_send_options().manifest_chunk_bytes / 1024;
    if requested.is_empty() {
        return Ok(vec![default_chunk_kb]);
    }

    let max_chunk_kb = protocol::MAX_MANIFEST_CHUNK_BYTES / 1024;
    let mut selected = BTreeSet::new();
    for value in requested {
        if *value == 0 {
            bail!("manifest-chunk-kb must not contain 0");
        }
        if *value > max_chunk_kb {
            bail!("manifest-chunk-kb exceeds protocol cap: {value} > {max_chunk_kb}");
        }
        selected.insert(*value);
    }

    Ok(selected.into_iter().collect())
}

fn resolve_manifest_ack_record_values(requested: &[u64]) -> Result<Vec<u64>> {
    let default_ack_records = default_matrix_send_options().manifest_ack_records;
    if requested.is_empty() {
        return Ok(vec![default_ack_records]);
    }

    let mut selected = BTreeSet::new();
    for value in requested {
        if *value == 0 {
            bail!("manifest-ack-records must not contain 0");
        }
        if *value > protocol::MAX_UNACKED_MANIFEST_RECORDS {
            bail!(
                "manifest-ack-records exceeds protocol cap: {value} > {}",
                protocol::MAX_UNACKED_MANIFEST_RECORDS
            );
        }
        selected.insert(*value);
    }

    Ok(selected.into_iter().collect())
}

fn resolve_manifest_ack_values_kb(requested: &[usize]) -> Result<Vec<usize>> {
    let default_ack_kb = default_matrix_send_options().manifest_ack_bytes / 1024;
    if requested.is_empty() {
        return Ok(vec![default_ack_kb]);
    }

    let max_ack_kb = protocol::MAX_UNACKED_MANIFEST_BYTES / 1024;
    let mut selected = BTreeSet::new();
    for value in requested {
        if *value == 0 {
            bail!("manifest-ack-kb must not contain 0");
        }
        if *value > max_ack_kb {
            bail!("manifest-ack-kb exceeds protocol cap: {value} > {max_ack_kb}");
        }
        selected.insert(*value);
    }

    Ok(selected.into_iter().collect())
}

fn dataset_shuffle_salt(dataset: &str) -> u64 {
    dataset.bytes().fold(0_u64, |acc, byte| {
        acc.wrapping_mul(131).wrapping_add(u64::from(byte))
    })
}

fn shuffle_planned_runs<T>(runs: &mut [T], seed: u64) {
    if runs.len() < 2 {
        return;
    }

    let mut state = seed.max(1);
    for index in (1..runs.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let swap_index = (state as usize) % (index + 1);
        runs.swap(index, swap_index);
    }
}

fn join_values<T>(values: &[T]) -> String
where
    T: Display,
{
    values
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

// В raw CSV фиксируем уже нормализованные runtime knobs, чтобы resume и summary
// агрегировали именно тот sender-конфиг, который реально исполнялся.
fn scenario_options(
    threads: u32,
    frame_target_mb: u64,
    read_buffer_kb: u64,
    manifest_chunk_kb: usize,
    manifest_ack_records: u64,
    manifest_ack_kb: usize,
) -> SendOptions {
    SendOptions {
        threads,
        frame_target_bytes: frame_target_mb * 1024 * 1024,
        read_buffer_bytes: (read_buffer_kb * 1024) as usize,
        manifest_chunk_bytes: manifest_chunk_kb * 1024,
        manifest_ack_records,
        manifest_ack_bytes: manifest_ack_kb * 1024,
        ..default_matrix_send_options()
    }
}

fn raw_row(scenario: ScenarioKey, repeat_index: u32, metrics: LoopbackRunMetrics) -> RawResultRow {
    RawResultRow {
        scenario,
        repeat_index,
        files_count: metrics.files_count,
        skipped_files: metrics.skipped_files,
        sent_frames: metrics.sent_frames,
        frame_aborts: metrics.frame_aborts,
        frame_rebuilds: metrics.frame_rebuilds,
        avg_files_per_frame: metrics.avg_files_per_frame,
        raw_read_bytes: metrics.raw_read_bytes,
        tar_input_bytes: metrics.tar_input_bytes,
        compressed_payload_bytes: metrics.compressed_payload_bytes,
        wire_bytes: metrics.wire_bytes,
        wire_overhead_bytes: metrics.wire_overhead_bytes,
        read_duration_ms: metrics.read_duration_ms,
        compress_duration_ms: metrics.compress_duration_ms,
        send_duration_ms: metrics.send_duration_ms,
        total_duration_ms: metrics.total_duration_ms,
        traversal_wait_ms: metrics.traversal_wait_ms,
        ack_wait_ms: metrics.ack_wait_ms,
        flush_count: metrics.flush_count,
        max_inflight_dirs: metrics.max_inflight_dirs,
        max_dirfd_depth: metrics.max_dirfd_depth,
        read_mib_s: metrics.read_mib_s,
        compress_mib_s: metrics.compress_mib_s,
        send_mib_s: metrics.send_mib_s,
        total_mib_s: metrics.total_mib_s,
        avg_cpu_pct: metrics.avg_cpu_pct,
        max_cpu_pct: metrics.max_cpu_pct,
        avg_rss_mib: metrics.avg_rss_mib,
        max_rss_mib: metrics.max_rss_mib,
        avg_private_mib: metrics.avg_private_mib,
        max_private_mib: metrics.max_private_mib,
        compression_ratio_pct: metrics.compression_ratio_pct,
        wire_pct_of_read: metrics.wire_pct_of_read,
        wire_overhead_pct: metrics.wire_overhead_pct,
        manifest_build_bytes: metrics.manifest_build_bytes,
        manifest_build_duration_ms: metrics.manifest_build_duration_ms,
        manifest_transfer_bytes: metrics.manifest_transfer_bytes,
        manifest_transfer_duration_ms: metrics.manifest_transfer_duration_ms,
        manifest_spool_read_bytes: metrics.manifest_spool_read_bytes,
        manifest_spool_read_duration_ms: metrics.manifest_spool_read_duration_ms,
        manifest_replay_bytes: metrics.manifest_replay_bytes,
        manifest_replay_duration_ms: metrics.manifest_replay_duration_ms,
        manifest_replay_records: metrics.manifest_replay_records,
        receiver_manifest_bytes: metrics.receiver_manifest_bytes,
        receiver_issue_tail_bytes: metrics.receiver_issue_tail_bytes,
        disk_budget_wait_ms: metrics.disk_budget_wait_ms,
        time_to_first_frame_ms: metrics.time_to_first_frame_ms,
        overlap_efficiency_pct: metrics.overlap_efficiency_pct,
    }
}
