use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use super::shared::{
    LOOPBACK_LOCALHOST_PROFILE, benchmark_comparison_rules, benchmark_dataset_plan,
    benchmark_network_profiles, benchmark_protocol_runtimes, benchmark_success_gates,
    default_matrix_send_options,
};
use super::stats::benchmark_metric_plan;

const RAW_HEADER: &str = "dataset,executor,threads,frame_target_mb,read_buffer_kb,large_file_single_frame_threshold_mib,repeat_index,files_count,skipped_files,sent_frames,frame_aborts,frame_rebuilds,avg_files_per_frame,raw_read_bytes,tar_input_bytes,compressed_payload_bytes,wire_bytes,wire_overhead_bytes,read_duration_ms,compress_duration_ms,send_duration_ms,total_duration_ms,traversal_wait_ms,ack_wait_ms,flush_count,max_inflight_dirs,max_dirfd_depth,read_mib_s,compress_mib_s,send_mib_s,total_mib_s,avg_cpu_pct,max_cpu_pct,avg_rss_mib,max_rss_mib,avg_private_mib,max_private_mib,compression_ratio_pct,wire_pct_of_read,wire_overhead_pct,manifest_build_bytes,manifest_build_duration_ms,manifest_transfer_bytes,manifest_transfer_duration_ms,manifest_spool_read_bytes,manifest_spool_read_duration_ms,manifest_replay_bytes,manifest_replay_duration_ms,manifest_replay_records,receiver_manifest_bytes,receiver_issue_tail_bytes,disk_budget_wait_ms,time_to_first_frame_ms,overlap_efficiency_pct,protocol_runtime,network_profile,manifest_chunk_kb,manifest_ack_records,manifest_ack_kb";
const LEGACY_RAW_HEADER: &str = "dataset,threads,frame_target_mb,read_buffer_kb,large_file_single_frame_threshold_mib,repeat_index,files_count,skipped_files,sent_frames,frame_aborts,frame_rebuilds,avg_files_per_frame,raw_read_bytes,tar_input_bytes,compressed_payload_bytes,wire_bytes,wire_overhead_bytes,read_duration_ms,compress_duration_ms,send_duration_ms,total_duration_ms,traversal_wait_ms,ack_wait_ms,flush_count,max_inflight_dirs,read_mib_s,compress_mib_s,send_mib_s,total_mib_s,avg_cpu_pct,max_cpu_pct,avg_rss_mib,max_rss_mib,avg_private_mib,max_private_mib,compression_ratio_pct,wire_pct_of_read,wire_overhead_pct";
const SUMMARY_HEADER: &str = "dataset,executor,threads,frame_target_mb,read_buffer_kb,large_file_single_frame_threshold_mib,completed_repeats,files_count,skipped_files,sent_frames,frame_aborts,frame_rebuilds,avg_files_per_frame,raw_read_bytes,tar_input_bytes,compressed_payload_bytes,wire_bytes,wire_overhead_bytes,read_duration_ms,compress_duration_ms,send_duration_ms,total_duration_ms,traversal_wait_ms,ack_wait_ms,flush_count,max_inflight_dirs,max_dirfd_depth,read_mib_s,compress_mib_s,send_mib_s,total_mib_s,avg_cpu_pct,max_cpu_pct,avg_rss_mib,max_rss_mib,avg_private_mib,max_private_mib,compression_ratio_pct,wire_pct_of_read,wire_overhead_pct,manifest_build_bytes,manifest_build_duration_ms,manifest_transfer_bytes,manifest_transfer_duration_ms,manifest_spool_read_bytes,manifest_spool_read_duration_ms,manifest_replay_bytes,manifest_replay_duration_ms,manifest_replay_records,receiver_manifest_bytes,receiver_issue_tail_bytes,disk_budget_wait_ms,time_to_first_frame_ms,overlap_efficiency_pct,protocol_runtime,network_profile,manifest_chunk_kb,manifest_ack_records,manifest_ack_kb";
const MARKDOWN_TOP_ROWS: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ScenarioKey {
    pub(crate) dataset: String,
    pub(crate) protocol_runtime: String,
    pub(crate) network_profile: String,
    pub(crate) executor: String,
    pub(crate) threads: u32,
    pub(crate) frame_target_mb: u64,
    pub(crate) read_buffer_kb: u64,
    pub(crate) manifest_chunk_kb: usize,
    pub(crate) manifest_ack_records: u64,
    pub(crate) manifest_ack_kb: usize,
    pub(crate) large_file_single_frame_threshold_mib: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct RawResultRow {
    pub(crate) scenario: ScenarioKey,
    pub(crate) repeat_index: u32,
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
    pub(crate) disk_budget_wait_ms: f64,
    pub(crate) time_to_first_frame_ms: f64,
    pub(crate) overlap_efficiency_pct: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct SummaryRow {
    pub(crate) scenario: ScenarioKey,
    pub(crate) completed_repeats: u32,
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
    pub(crate) disk_budget_wait_ms: f64,
    pub(crate) time_to_first_frame_ms: f64,
    pub(crate) overlap_efficiency_pct: f64,
}

impl RawResultRow {
    fn run_key(&self) -> (ScenarioKey, u32) {
        (self.scenario.clone(), self.repeat_index)
    }

    fn to_csv_line(&self) -> String {
        [
            self.scenario.dataset.clone(),
            self.scenario.executor.clone(),
            self.scenario.threads.to_string(),
            self.scenario.frame_target_mb.to_string(),
            self.scenario.read_buffer_kb.to_string(),
            self.scenario
                .large_file_single_frame_threshold_mib
                .to_string(),
            self.repeat_index.to_string(),
            self.files_count.to_string(),
            self.skipped_files.to_string(),
            self.sent_frames.to_string(),
            self.frame_aborts.to_string(),
            self.frame_rebuilds.to_string(),
            format!("{:.6}", self.avg_files_per_frame),
            self.raw_read_bytes.to_string(),
            self.tar_input_bytes.to_string(),
            self.compressed_payload_bytes.to_string(),
            self.wire_bytes.to_string(),
            self.wire_overhead_bytes.to_string(),
            format!("{:.6}", self.read_duration_ms),
            format!("{:.6}", self.compress_duration_ms),
            format!("{:.6}", self.send_duration_ms),
            format!("{:.6}", self.total_duration_ms),
            format!("{:.6}", self.traversal_wait_ms),
            format!("{:.6}", self.ack_wait_ms),
            self.flush_count.to_string(),
            self.max_inflight_dirs.to_string(),
            self.max_dirfd_depth.to_string(),
            format!("{:.6}", self.read_mib_s),
            format!("{:.6}", self.compress_mib_s),
            format!("{:.6}", self.send_mib_s),
            format!("{:.6}", self.total_mib_s),
            format!("{:.6}", self.avg_cpu_pct),
            format!("{:.6}", self.max_cpu_pct),
            format!("{:.6}", self.avg_rss_mib),
            format!("{:.6}", self.max_rss_mib),
            format!("{:.6}", self.avg_private_mib),
            format!("{:.6}", self.max_private_mib),
            format!("{:.6}", self.compression_ratio_pct),
            format!("{:.6}", self.wire_pct_of_read),
            format!("{:.6}", self.wire_overhead_pct),
            self.manifest_build_bytes.to_string(),
            format!("{:.6}", self.manifest_build_duration_ms),
            self.manifest_transfer_bytes.to_string(),
            format!("{:.6}", self.manifest_transfer_duration_ms),
            self.manifest_spool_read_bytes.to_string(),
            format!("{:.6}", self.manifest_spool_read_duration_ms),
            self.manifest_replay_bytes.to_string(),
            format!("{:.6}", self.manifest_replay_duration_ms),
            self.manifest_replay_records.to_string(),
            self.receiver_manifest_bytes.to_string(),
            self.receiver_issue_tail_bytes.to_string(),
            format!("{:.6}", self.disk_budget_wait_ms),
            format!("{:.6}", self.time_to_first_frame_ms),
            format!("{:.6}", self.overlap_efficiency_pct),
            self.scenario.protocol_runtime.clone(),
            self.scenario.network_profile.clone(),
            self.scenario.manifest_chunk_kb.to_string(),
            self.scenario.manifest_ack_records.to_string(),
            self.scenario.manifest_ack_kb.to_string(),
        ]
        .join(",")
    }

    fn parse(line: &str) -> Result<Self> {
        let columns = line.split(',').collect::<Vec<_>>();
        if columns.len() != 38 && columns.len() != 40 && columns.len() != 54 && columns.len() != 59
        {
            bail!(
                "unexpected raw results column count: expected 38, 40, 54, or 59, got {}",
                columns.len()
            );
        }
        let has_executor_columns =
            columns.len() == 40 || columns.len() == 54 || columns.len() == 59;
        let has_v3_columns = columns.len() == 54 || columns.len() == 59;
        let has_extended_scenario_columns = columns.len() == 59;
        let column = |with_executor: usize, legacy: usize| -> &str {
            if has_executor_columns {
                columns[with_executor]
            } else {
                columns[legacy]
            }
        };
        let default_options = default_matrix_send_options();

        Ok(Self {
            scenario: ScenarioKey {
                dataset: columns[0].to_owned(),
                protocol_runtime: if has_extended_scenario_columns {
                    columns[54].to_owned()
                } else {
                    "v3".to_owned()
                },
                network_profile: if has_extended_scenario_columns {
                    columns[55].to_owned()
                } else {
                    LOOPBACK_LOCALHOST_PROFILE.to_owned()
                },
                executor: if has_executor_columns {
                    columns[1].to_owned()
                } else {
                    "default".to_owned()
                },
                threads: parse_u32(column(2, 1), "threads")?,
                frame_target_mb: parse_u64(column(3, 2), "frame_target_mb")?,
                read_buffer_kb: parse_u64(column(4, 3), "read_buffer_kb")?,
                manifest_chunk_kb: if has_extended_scenario_columns {
                    parse_usize(columns[56], "manifest_chunk_kb")?
                } else {
                    default_options.manifest_chunk_bytes / 1024
                },
                manifest_ack_records: if has_extended_scenario_columns {
                    parse_u64(columns[57], "manifest_ack_records")?
                } else {
                    default_options.manifest_ack_records
                },
                manifest_ack_kb: if has_extended_scenario_columns {
                    parse_usize(columns[58], "manifest_ack_kb")?
                } else {
                    default_options.manifest_ack_bytes / 1024
                },
                large_file_single_frame_threshold_mib: parse_u64(
                    column(5, 4),
                    "large_file_single_frame_threshold_mib",
                )?,
            },
            repeat_index: parse_u32(column(6, 5), "repeat_index")?,
            files_count: parse_u64(column(7, 6), "files_count")?,
            skipped_files: parse_u64(column(8, 7), "skipped_files")?,
            sent_frames: parse_u64(column(9, 8), "sent_frames")?,
            frame_aborts: parse_u64(column(10, 9), "frame_aborts")?,
            frame_rebuilds: parse_u64(column(11, 10), "frame_rebuilds")?,
            avg_files_per_frame: parse_f64(column(12, 11), "avg_files_per_frame")?,
            raw_read_bytes: parse_u64(column(13, 12), "raw_read_bytes")?,
            tar_input_bytes: parse_u64(column(14, 13), "tar_input_bytes")?,
            compressed_payload_bytes: parse_u64(column(15, 14), "compressed_payload_bytes")?,
            wire_bytes: parse_u64(column(16, 15), "wire_bytes")?,
            wire_overhead_bytes: parse_u64(column(17, 16), "wire_overhead_bytes")?,
            read_duration_ms: parse_f64(column(18, 17), "read_duration_ms")?,
            compress_duration_ms: parse_f64(column(19, 18), "compress_duration_ms")?,
            send_duration_ms: parse_f64(column(20, 19), "send_duration_ms")?,
            total_duration_ms: parse_f64(column(21, 20), "total_duration_ms")?,
            traversal_wait_ms: parse_f64(column(22, 21), "traversal_wait_ms")?,
            ack_wait_ms: parse_f64(column(23, 22), "ack_wait_ms")?,
            flush_count: parse_u64(column(24, 23), "flush_count")?,
            max_inflight_dirs: parse_u64(column(25, 24), "max_inflight_dirs")?,
            max_dirfd_depth: if has_executor_columns {
                parse_u64(columns[26], "max_dirfd_depth")?
            } else {
                0
            },
            read_mib_s: parse_f64(column(27, 25), "read_mib_s")?,
            compress_mib_s: parse_f64(column(28, 26), "compress_mib_s")?,
            send_mib_s: parse_f64(column(29, 27), "send_mib_s")?,
            total_mib_s: parse_f64(column(30, 28), "total_mib_s")?,
            avg_cpu_pct: parse_f64(column(31, 29), "avg_cpu_pct")?,
            max_cpu_pct: parse_f64(column(32, 30), "max_cpu_pct")?,
            avg_rss_mib: parse_f64(column(33, 31), "avg_rss_mib")?,
            max_rss_mib: parse_f64(column(34, 32), "max_rss_mib")?,
            avg_private_mib: parse_f64(column(35, 33), "avg_private_mib")?,
            max_private_mib: parse_f64(column(36, 34), "max_private_mib")?,
            compression_ratio_pct: parse_f64(column(37, 35), "compression_ratio_pct")?,
            wire_pct_of_read: parse_f64(column(38, 36), "wire_pct_of_read")?,
            wire_overhead_pct: parse_f64(column(39, 37), "wire_overhead_pct")?,
            manifest_build_bytes: if has_v3_columns {
                parse_u64(columns[40], "manifest_build_bytes")?
            } else {
                0
            },
            manifest_build_duration_ms: if has_v3_columns {
                parse_f64(columns[41], "manifest_build_duration_ms")?
            } else {
                0.0
            },
            manifest_transfer_bytes: if has_v3_columns {
                parse_u64(columns[42], "manifest_transfer_bytes")?
            } else {
                0
            },
            manifest_transfer_duration_ms: if has_v3_columns {
                parse_f64(columns[43], "manifest_transfer_duration_ms")?
            } else {
                0.0
            },
            manifest_spool_read_bytes: if has_v3_columns {
                parse_u64(columns[44], "manifest_spool_read_bytes")?
            } else {
                0
            },
            manifest_spool_read_duration_ms: if has_v3_columns {
                parse_f64(columns[45], "manifest_spool_read_duration_ms")?
            } else {
                0.0
            },
            manifest_replay_bytes: if has_v3_columns {
                parse_u64(columns[46], "manifest_replay_bytes")?
            } else {
                0
            },
            manifest_replay_duration_ms: if has_v3_columns {
                parse_f64(columns[47], "manifest_replay_duration_ms")?
            } else {
                0.0
            },
            manifest_replay_records: if has_v3_columns {
                parse_u64(columns[48], "manifest_replay_records")?
            } else {
                0
            },
            receiver_manifest_bytes: if has_v3_columns {
                parse_u64(columns[49], "receiver_manifest_bytes")?
            } else {
                0
            },
            receiver_issue_tail_bytes: if has_v3_columns {
                parse_u64(columns[50], "receiver_issue_tail_bytes")?
            } else {
                0
            },
            disk_budget_wait_ms: if has_v3_columns {
                parse_f64(columns[51], "disk_budget_wait_ms")?
            } else {
                0.0
            },
            time_to_first_frame_ms: if has_v3_columns {
                parse_f64(columns[52], "time_to_first_frame_ms")?
            } else {
                0.0
            },
            overlap_efficiency_pct: if has_v3_columns {
                parse_f64(columns[53], "overlap_efficiency_pct")?
            } else {
                0.0
            },
        })
    }
}

impl SummaryRow {
    fn to_csv_line(&self) -> String {
        [
            self.scenario.dataset.clone(),
            self.scenario.executor.clone(),
            self.scenario.threads.to_string(),
            self.scenario.frame_target_mb.to_string(),
            self.scenario.read_buffer_kb.to_string(),
            self.scenario
                .large_file_single_frame_threshold_mib
                .to_string(),
            self.completed_repeats.to_string(),
            self.files_count.to_string(),
            self.skipped_files.to_string(),
            self.sent_frames.to_string(),
            self.frame_aborts.to_string(),
            self.frame_rebuilds.to_string(),
            format!("{:.6}", self.avg_files_per_frame),
            self.raw_read_bytes.to_string(),
            self.tar_input_bytes.to_string(),
            self.compressed_payload_bytes.to_string(),
            self.wire_bytes.to_string(),
            self.wire_overhead_bytes.to_string(),
            format!("{:.6}", self.read_duration_ms),
            format!("{:.6}", self.compress_duration_ms),
            format!("{:.6}", self.send_duration_ms),
            format!("{:.6}", self.total_duration_ms),
            format!("{:.6}", self.traversal_wait_ms),
            format!("{:.6}", self.ack_wait_ms),
            self.flush_count.to_string(),
            self.max_inflight_dirs.to_string(),
            self.max_dirfd_depth.to_string(),
            format!("{:.6}", self.read_mib_s),
            format!("{:.6}", self.compress_mib_s),
            format!("{:.6}", self.send_mib_s),
            format!("{:.6}", self.total_mib_s),
            format!("{:.6}", self.avg_cpu_pct),
            format!("{:.6}", self.max_cpu_pct),
            format!("{:.6}", self.avg_rss_mib),
            format!("{:.6}", self.max_rss_mib),
            format!("{:.6}", self.avg_private_mib),
            format!("{:.6}", self.max_private_mib),
            format!("{:.6}", self.compression_ratio_pct),
            format!("{:.6}", self.wire_pct_of_read),
            format!("{:.6}", self.wire_overhead_pct),
            self.manifest_build_bytes.to_string(),
            format!("{:.6}", self.manifest_build_duration_ms),
            self.manifest_transfer_bytes.to_string(),
            format!("{:.6}", self.manifest_transfer_duration_ms),
            self.manifest_spool_read_bytes.to_string(),
            format!("{:.6}", self.manifest_spool_read_duration_ms),
            self.manifest_replay_bytes.to_string(),
            format!("{:.6}", self.manifest_replay_duration_ms),
            self.manifest_replay_records.to_string(),
            self.receiver_manifest_bytes.to_string(),
            self.receiver_issue_tail_bytes.to_string(),
            format!("{:.6}", self.disk_budget_wait_ms),
            format!("{:.6}", self.time_to_first_frame_ms),
            format!("{:.6}", self.overlap_efficiency_pct),
            self.scenario.protocol_runtime.clone(),
            self.scenario.network_profile.clone(),
            self.scenario.manifest_chunk_kb.to_string(),
            self.scenario.manifest_ack_records.to_string(),
            self.scenario.manifest_ack_kb.to_string(),
        ]
        .join(",")
    }
}

pub(crate) fn results_raw_path(output_dir: &Path) -> PathBuf {
    output_dir.join("results_raw.csv")
}

pub(crate) fn results_summary_path(output_dir: &Path) -> PathBuf {
    output_dir.join("results_summary.csv")
}

pub(crate) fn benchmark_plan_path(output_dir: &Path) -> PathBuf {
    output_dir.join("benchmark_plan.md")
}

pub(crate) fn benchmark_report_path(output_dir: &Path) -> PathBuf {
    output_dir.join("benchmark_report.md")
}

pub(crate) fn reset_results(output_dir: &Path) -> Result<()> {
    let raw_path = results_raw_path(output_dir);
    let summary_path = results_summary_path(output_dir);
    let benchmark_plan_path = benchmark_plan_path(output_dir);
    let benchmark_report_path = benchmark_report_path(output_dir);

    if raw_path.exists() {
        fs::remove_file(&raw_path).with_context(|| {
            format!(
                "failed to remove stale raw results file {}",
                raw_path.display()
            )
        })?;
    }
    if summary_path.exists() {
        fs::remove_file(&summary_path).with_context(|| {
            format!(
                "failed to remove stale summary results file {}",
                summary_path.display()
            )
        })?;
    }
    if benchmark_plan_path.exists() {
        fs::remove_file(&benchmark_plan_path).with_context(|| {
            format!(
                "failed to remove stale benchmark plan file {}",
                benchmark_plan_path.display()
            )
        })?;
    }
    if benchmark_report_path.exists() {
        fs::remove_file(&benchmark_report_path).with_context(|| {
            format!(
                "failed to remove stale benchmark report file {}",
                benchmark_report_path.display()
            )
        })?;
    }

    Ok(())
}

pub(crate) fn write_benchmark_plan(output_dir: &Path) -> Result<PathBuf> {
    fs::create_dir_all(output_dir).with_context(|| {
        format!(
            "failed to create results directory {}",
            output_dir.display()
        )
    })?;

    let path = benchmark_plan_path(output_dir);
    let mut output = String::new();

    output.push_str("# fast-dump v3 Benchmark Matrix\n\n");
    output.push_str(
        "This artifact is generated from the benchmark harness source. It freezes the required v3 benchmark scope and the current coverage state without pretending that planned profiles or metrics already exist.\n\n",
    );
    output.push_str("## Current Scope\n\n");
    let automated_profiles = benchmark_network_profiles()
        .iter()
        .filter(|profile| profile.coverage.as_str() == "implemented")
        .map(|profile| format!("`{}`", profile.id))
        .collect::<Vec<_>>()
        .join(", ");
    let automated_runtimes = benchmark_protocol_runtimes()
        .iter()
        .map(|runtime| format!("`{}`", runtime.id()))
        .collect::<Vec<_>>()
        .join(", ");
    output.push_str(&format!(
        "- Current automated protocol runtimes: {}.\n",
        automated_runtimes
    ));
    output.push_str(&format!(
        "- Current automated network profiles: {}.\n",
        automated_profiles
    ));
    output.push_str("- Current automated runner: synthetic sender->receiver loopback matrix.\n");
    output.push_str(
        "- Cutover decisions must use the full required dataset and shaped-network matrix below, not loopback-only numbers.\n\n",
    );

    output.push_str("## Dataset Matrix\n\n");
    output.push_str("| id | title | coverage | current fixture | primary goal | notes |\n");
    output.push_str("| --- | --- | --- | --- | --- | --- |\n");
    for dataset in benchmark_dataset_plan() {
        output.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            dataset.id,
            dataset.title,
            dataset.coverage.as_str(),
            dataset.current_fixture,
            dataset.primary_goal,
            dataset.notes
        ));
    }
    output.push('\n');

    output.push_str("## Network Profile Matrix\n\n");
    output.push_str(
        "| id | title | coverage | RTT ms | bandwidth | loss or disconnects | primary goal |\n",
    );
    output.push_str("| --- | --- | --- | --- | --- | --- | --- |\n");
    for profile in benchmark_network_profiles() {
        output.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} |\n",
            profile.id,
            profile.title,
            profile.coverage.as_str(),
            profile.rtt_ms,
            profile.bandwidth_limit,
            profile.loss_or_disconnects,
            profile.primary_goal
        ));
    }
    output.push('\n');

    output.push_str("## Required Metrics\n\n");
    output.push_str("| id | title | coverage | current source | notes |\n");
    output.push_str("| --- | --- | --- | --- | --- |\n");
    for metric in benchmark_metric_plan() {
        output.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            metric.id,
            metric.title,
            metric.coverage.as_str(),
            metric.current_source,
            metric.notes
        ));
    }
    output.push('\n');

    output.push_str("## Comparison Rules\n\n");
    for (index, rule) in benchmark_comparison_rules().iter().enumerate() {
        output.push_str(&format!("{}. `{}`: {}\n", index + 1, rule.id, rule.rule));
    }
    output.push('\n');

    output.push_str("## Success Gates\n\n");
    output.push_str("| id | title | rule | required evidence |\n");
    output.push_str("| --- | --- | --- | --- |\n");
    for gate in benchmark_success_gates() {
        output.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            gate.id, gate.title, gate.rule, gate.evidence
        ));
    }
    output.push('\n');

    fs::write(&path, output)
        .with_context(|| format!("failed to write benchmark plan file {}", path.display()))?;

    Ok(path)
}

pub(crate) fn write_benchmark_report(output_dir: &Path, rows: &[SummaryRow]) -> Result<PathBuf> {
    fs::create_dir_all(output_dir).with_context(|| {
        format!(
            "failed to create benchmark report directory {}",
            output_dir.display()
        )
    })?;

    let path = benchmark_report_path(output_dir);
    let mut output = String::new();
    output.push_str("# fast-dump Benchmark Report\n\n");
    output.push_str(
        "This artifact is generated from `results_summary.csv`. It highlights the best visible `v2` and `v3` rows per dataset/network pair and the fastest current `v3` tuning candidate.\n\n",
    );

    output.push_str("## Best Runtime Comparison\n\n");
    output.push_str("| dataset | network | v2 total ms | v3 total ms | v3 delta vs v2 % | v2 MiB/s | v3 MiB/s | v3 manifest chunk KiB | v3 ack records | v3 ack KiB |\n");
    output.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    let comparison_keys = rows
        .iter()
        .map(|row| {
            (
                row.scenario.dataset.clone(),
                row.scenario.network_profile.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    for (dataset, network_profile) in comparison_keys {
        let best_v2 = rows
            .iter()
            .filter(|row| {
                row.scenario.dataset == dataset
                    && row.scenario.network_profile == network_profile
                    && row.scenario.protocol_runtime == "v2"
            })
            .min_by(|left, right| left.total_duration_ms.total_cmp(&right.total_duration_ms));
        let best_v3 = rows
            .iter()
            .filter(|row| {
                row.scenario.dataset == dataset
                    && row.scenario.network_profile == network_profile
                    && row.scenario.protocol_runtime == "v3"
            })
            .min_by(|left, right| left.total_duration_ms.total_cmp(&right.total_duration_ms));
        let delta_pct = match (best_v2, best_v3) {
            (Some(v2), Some(v3)) if v2.total_duration_ms > 0.0 => {
                (v3.total_duration_ms - v2.total_duration_ms) * 100.0 / v2.total_duration_ms
            }
            _ => 0.0,
        };
        output.push_str(&format!(
            "| {} | {} | {} | {} | {:.2} | {} | {} | {} | {} | {} |\n",
            dataset,
            network_profile,
            best_v2
                .map(|row| format!("{:.2}", row.total_duration_ms))
                .unwrap_or_else(|| "-".to_owned()),
            best_v3
                .map(|row| format!("{:.2}", row.total_duration_ms))
                .unwrap_or_else(|| "-".to_owned()),
            delta_pct,
            best_v2
                .map(|row| format!("{:.2}", row.total_mib_s))
                .unwrap_or_else(|| "-".to_owned()),
            best_v3
                .map(|row| format!("{:.2}", row.total_mib_s))
                .unwrap_or_else(|| "-".to_owned()),
            best_v3
                .map(|row| row.scenario.manifest_chunk_kb.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            best_v3
                .map(|row| row.scenario.manifest_ack_records.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            best_v3
                .map(|row| row.scenario.manifest_ack_kb.to_string())
                .unwrap_or_else(|| "-".to_owned()),
        ));
    }
    output.push('\n');

    output.push_str("## Fastest Current V3 Rows\n\n");
    output.push_str("| dataset | network | executor | threads | frame MiB | buffer KiB | manifest chunk KiB | ack records | ack KiB | total ms | total MiB/s | ack wait ms | max RSS MiB |\n");
    output.push_str("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    let v3_keys = rows
        .iter()
        .filter(|row| row.scenario.protocol_runtime == "v3")
        .map(|row| {
            (
                row.scenario.dataset.clone(),
                row.scenario.network_profile.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    for (dataset, network_profile) in v3_keys {
        let Some(best_v3) = rows
            .iter()
            .filter(|row| {
                row.scenario.dataset == dataset
                    && row.scenario.network_profile == network_profile
                    && row.scenario.protocol_runtime == "v3"
            })
            .min_by(|left, right| left.total_duration_ms.total_cmp(&right.total_duration_ms))
        else {
            continue;
        };
        output.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {:.2} | {:.2} | {:.2} |\n",
            best_v3.scenario.dataset,
            best_v3.scenario.network_profile,
            best_v3.scenario.executor,
            best_v3.scenario.threads,
            best_v3.scenario.frame_target_mb,
            best_v3.scenario.read_buffer_kb,
            best_v3.scenario.manifest_chunk_kb,
            best_v3.scenario.manifest_ack_records,
            best_v3.scenario.manifest_ack_kb,
            best_v3.total_duration_ms,
            best_v3.total_mib_s,
            best_v3.ack_wait_ms,
            best_v3.max_rss_mib
        ));
    }
    output.push('\n');

    fs::write(&path, output)
        .with_context(|| format!("failed to write benchmark report {}", path.display()))?;
    Ok(path)
}

pub(crate) fn load_raw_results(path: &Path) -> Result<Vec<RawResultRow>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read raw results file {}", path.display()))?;
    let mut rows = Vec::new();
    for (line_no, line) in content.lines().enumerate() {
        if line_no == 0 {
            if line.trim() != RAW_HEADER && line.trim() != LEGACY_RAW_HEADER {
                bail!("unexpected raw results header in {}", path.display());
            }
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        rows.push(
            RawResultRow::parse(line).with_context(|| {
                format!("failed to parse {} line {}", path.display(), line_no + 1)
            })?,
        );
    }

    Ok(rows)
}

pub(crate) fn append_raw_result(path: &Path, row: &RawResultRow) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create results directory {}", parent.display()))?;
    }

    let write_header = !path.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open raw results file {}", path.display()))?;

    if write_header {
        writeln!(file, "{RAW_HEADER}")
            .with_context(|| format!("failed to write raw results header to {}", path.display()))?;
    }
    writeln!(file, "{}", row.to_csv_line())
        .with_context(|| format!("failed to append raw results row to {}", path.display()))?;

    Ok(())
}

pub(crate) fn aggregate_summary_rows(rows: &[RawResultRow]) -> Vec<SummaryRow> {
    let mut deduped_runs = BTreeMap::new();
    for row in rows {
        deduped_runs.insert(row.run_key(), row.clone());
    }

    let mut grouped = BTreeMap::<ScenarioKey, Vec<RawResultRow>>::new();
    for ((scenario, _repeat_index), row) in deduped_runs {
        grouped.entry(scenario).or_default().push(row);
    }

    grouped
        .into_iter()
        .map(|(scenario, scenario_rows)| {
            let repeats = scenario_rows.len() as u32;
            let sum_u64 = |selector: fn(&RawResultRow) -> u64| -> u64 {
                scenario_rows.iter().map(selector).sum::<u64>() / repeats.max(1) as u64
            };
            let avg_f64 = |selector: fn(&RawResultRow) -> f64| -> f64 {
                scenario_rows.iter().map(selector).sum::<f64>() / repeats.max(1) as f64
            };

            SummaryRow {
                scenario,
                completed_repeats: repeats,
                files_count: sum_u64(|row| row.files_count),
                skipped_files: sum_u64(|row| row.skipped_files),
                sent_frames: sum_u64(|row| row.sent_frames),
                frame_aborts: sum_u64(|row| row.frame_aborts),
                frame_rebuilds: sum_u64(|row| row.frame_rebuilds),
                avg_files_per_frame: avg_f64(|row| row.avg_files_per_frame),
                raw_read_bytes: sum_u64(|row| row.raw_read_bytes),
                tar_input_bytes: sum_u64(|row| row.tar_input_bytes),
                compressed_payload_bytes: sum_u64(|row| row.compressed_payload_bytes),
                wire_bytes: sum_u64(|row| row.wire_bytes),
                wire_overhead_bytes: sum_u64(|row| row.wire_overhead_bytes),
                read_duration_ms: avg_f64(|row| row.read_duration_ms),
                compress_duration_ms: avg_f64(|row| row.compress_duration_ms),
                send_duration_ms: avg_f64(|row| row.send_duration_ms),
                total_duration_ms: avg_f64(|row| row.total_duration_ms),
                traversal_wait_ms: avg_f64(|row| row.traversal_wait_ms),
                ack_wait_ms: avg_f64(|row| row.ack_wait_ms),
                flush_count: sum_u64(|row| row.flush_count),
                max_inflight_dirs: sum_u64(|row| row.max_inflight_dirs),
                max_dirfd_depth: sum_u64(|row| row.max_dirfd_depth),
                read_mib_s: avg_f64(|row| row.read_mib_s),
                compress_mib_s: avg_f64(|row| row.compress_mib_s),
                send_mib_s: avg_f64(|row| row.send_mib_s),
                total_mib_s: avg_f64(|row| row.total_mib_s),
                avg_cpu_pct: avg_f64(|row| row.avg_cpu_pct),
                max_cpu_pct: avg_f64(|row| row.max_cpu_pct),
                avg_rss_mib: avg_f64(|row| row.avg_rss_mib),
                max_rss_mib: avg_f64(|row| row.max_rss_mib),
                avg_private_mib: avg_f64(|row| row.avg_private_mib),
                max_private_mib: avg_f64(|row| row.max_private_mib),
                compression_ratio_pct: avg_f64(|row| row.compression_ratio_pct),
                wire_pct_of_read: avg_f64(|row| row.wire_pct_of_read),
                wire_overhead_pct: avg_f64(|row| row.wire_overhead_pct),
                manifest_build_bytes: sum_u64(|row| row.manifest_build_bytes),
                manifest_build_duration_ms: avg_f64(|row| row.manifest_build_duration_ms),
                manifest_transfer_bytes: sum_u64(|row| row.manifest_transfer_bytes),
                manifest_transfer_duration_ms: avg_f64(|row| row.manifest_transfer_duration_ms),
                manifest_spool_read_bytes: sum_u64(|row| row.manifest_spool_read_bytes),
                manifest_spool_read_duration_ms: avg_f64(|row| row.manifest_spool_read_duration_ms),
                manifest_replay_bytes: sum_u64(|row| row.manifest_replay_bytes),
                manifest_replay_duration_ms: avg_f64(|row| row.manifest_replay_duration_ms),
                manifest_replay_records: sum_u64(|row| row.manifest_replay_records),
                receiver_manifest_bytes: sum_u64(|row| row.receiver_manifest_bytes),
                receiver_issue_tail_bytes: sum_u64(|row| row.receiver_issue_tail_bytes),
                disk_budget_wait_ms: avg_f64(|row| row.disk_budget_wait_ms),
                time_to_first_frame_ms: avg_f64(|row| row.time_to_first_frame_ms),
                overlap_efficiency_pct: avg_f64(|row| row.overlap_efficiency_pct),
            }
        })
        .collect()
}

pub(crate) fn write_summary_csv(path: &Path, rows: &[SummaryRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create results directory {}", parent.display()))?;
    }

    let mut sorted_rows = rows.to_vec();
    sorted_rows.sort_by(|left, right| {
        left.scenario
            .dataset
            .cmp(&right.scenario.dataset)
            .then_with(|| left.total_duration_ms.total_cmp(&right.total_duration_ms))
            .then_with(|| right.total_mib_s.total_cmp(&left.total_mib_s))
            .then_with(|| left.scenario.threads.cmp(&right.scenario.threads))
            .then_with(|| {
                left.scenario
                    .frame_target_mb
                    .cmp(&right.scenario.frame_target_mb)
            })
            .then_with(|| {
                left.scenario
                    .read_buffer_kb
                    .cmp(&right.scenario.read_buffer_kb)
            })
            .then_with(|| {
                left.scenario
                    .large_file_single_frame_threshold_mib
                    .cmp(&right.scenario.large_file_single_frame_threshold_mib)
            })
            .then_with(|| {
                left.scenario
                    .protocol_runtime
                    .cmp(&right.scenario.protocol_runtime)
            })
            .then_with(|| {
                left.scenario
                    .network_profile
                    .cmp(&right.scenario.network_profile)
            })
            .then_with(|| {
                left.scenario
                    .manifest_chunk_kb
                    .cmp(&right.scenario.manifest_chunk_kb)
            })
            .then_with(|| {
                left.scenario
                    .manifest_ack_records
                    .cmp(&right.scenario.manifest_ack_records)
            })
            .then_with(|| {
                left.scenario
                    .manifest_ack_kb
                    .cmp(&right.scenario.manifest_ack_kb)
            })
            .then_with(|| left.scenario.executor.cmp(&right.scenario.executor))
    });

    let mut output = String::new();
    output.push_str(SUMMARY_HEADER);
    output.push('\n');
    for row in &sorted_rows {
        output.push_str(&row.to_csv_line());
        output.push('\n');
    }

    fs::write(path, output)
        .with_context(|| format!("failed to write summary file {}", path.display()))?;
    Ok(())
}

pub(crate) fn completed_runs(rows: &[RawResultRow]) -> BTreeMap<(ScenarioKey, u32), RawResultRow> {
    let mut deduped = BTreeMap::new();
    for row in rows {
        deduped.insert(row.run_key(), row.clone());
    }
    deduped
}

pub(crate) fn dataset_summary_rows<'a>(
    rows: &'a [SummaryRow],
    dataset: &str,
) -> Vec<&'a SummaryRow> {
    rows.iter()
        .filter(|row| row.scenario.dataset == dataset)
        .collect::<Vec<_>>()
}

pub(crate) fn print_dataset_markdown_tables(
    dataset: &str,
    rows: &[&SummaryRow],
    memory_budget_mib: u64,
) {
    println!("## Dataset `{dataset}`");
    println!();

    if rows.is_empty() {
        println!("_No completed scenarios yet._");
        println!();
        return;
    }

    let mut by_total = rows.to_vec();
    by_total.sort_by(|left, right| {
        left.total_duration_ms
            .total_cmp(&right.total_duration_ms)
            .then_with(|| right.total_mib_s.total_cmp(&left.total_mib_s))
    });
    print_markdown_table("Fastest total time", &by_total);

    let mut by_ratio = rows.to_vec();
    by_ratio.sort_by(|left, right| {
        right
            .compression_ratio_pct
            .total_cmp(&left.compression_ratio_pct)
            .then_with(|| left.total_duration_ms.total_cmp(&right.total_duration_ms))
    });
    print_markdown_table("Best compression", &by_ratio);

    let mut under_budget = rows
        .iter()
        .copied()
        .filter(|row| row.max_rss_mib <= memory_budget_mib as f64)
        .collect::<Vec<_>>();
    under_budget.sort_by(|left, right| {
        left.total_duration_ms
            .total_cmp(&right.total_duration_ms)
            .then_with(|| left.max_rss_mib.total_cmp(&right.max_rss_mib))
    });
    print_markdown_table(
        &format!("Fastest total time under {} MiB RSS", memory_budget_mib),
        &under_budget,
    );
}

fn print_markdown_table(title: &str, rows: &[&SummaryRow]) {
    println!("### {title}");
    println!();

    if rows.is_empty() {
        println!("_No rows match this view._");
        println!();
        return;
    }

    println!(
        "| runtime | network | executor | threads | frame MiB | buffer KiB | manifest chunk KiB | ack records | ack KiB | isolate MiB | repeats | total ms | total MiB/s | frames | files/frame | aborts | walk wait ms | ack wait ms | dirfd depth | max RSS MiB |"
    );
    println!(
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );

    for row in rows.iter().take(MARKDOWN_TOP_ROWS) {
        println!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {:.2} | {} | {:.2} | {} | {:.2} | {:.2} | {} | {:.2} |",
            row.scenario.protocol_runtime,
            row.scenario.network_profile,
            row.scenario.executor,
            row.scenario.threads,
            row.scenario.frame_target_mb,
            row.scenario.read_buffer_kb,
            row.scenario.manifest_chunk_kb,
            row.scenario.manifest_ack_records,
            row.scenario.manifest_ack_kb,
            row.scenario.large_file_single_frame_threshold_mib,
            row.completed_repeats,
            row.total_duration_ms,
            row.total_mib_s,
            row.sent_frames,
            row.avg_files_per_frame,
            row.frame_aborts,
            row.traversal_wait_ms,
            row.ack_wait_ms,
            row.max_dirfd_depth,
            row.max_rss_mib
        );
    }
    println!();
}

fn parse_u32(raw: &str, field: &str) -> Result<u32> {
    raw.parse::<u32>()
        .with_context(|| format!("failed to parse {field} as u32"))
}

fn parse_u64(raw: &str, field: &str) -> Result<u64> {
    raw.parse::<u64>()
        .with_context(|| format!("failed to parse {field} as u64"))
}

fn parse_usize(raw: &str, field: &str) -> Result<usize> {
    raw.parse::<usize>()
        .with_context(|| format!("failed to parse {field} as usize"))
}

fn parse_f64(raw: &str, field: &str) -> Result<f64> {
    raw.parse::<f64>()
        .with_context(|| format!("failed to parse {field} as f64"))
}
