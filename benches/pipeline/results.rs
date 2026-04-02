use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

const RAW_HEADER: &str = "dataset,threads,frame_target_mb,read_buffer_kb,large_file_single_frame_threshold_mib,repeat_index,files_count,skipped_files,sent_frames,frame_aborts,frame_rebuilds,avg_files_per_frame,raw_read_bytes,tar_input_bytes,compressed_payload_bytes,wire_bytes,wire_overhead_bytes,read_duration_ms,compress_duration_ms,send_duration_ms,total_duration_ms,traversal_wait_ms,ack_wait_ms,flush_count,max_inflight_dirs,read_mib_s,compress_mib_s,send_mib_s,total_mib_s,avg_cpu_pct,max_cpu_pct,avg_rss_mib,max_rss_mib,avg_private_mib,max_private_mib,compression_ratio_pct,wire_pct_of_read,wire_overhead_pct";
const SUMMARY_HEADER: &str = "dataset,threads,frame_target_mb,read_buffer_kb,large_file_single_frame_threshold_mib,completed_repeats,files_count,skipped_files,sent_frames,frame_aborts,frame_rebuilds,avg_files_per_frame,raw_read_bytes,tar_input_bytes,compressed_payload_bytes,wire_bytes,wire_overhead_bytes,read_duration_ms,compress_duration_ms,send_duration_ms,total_duration_ms,traversal_wait_ms,ack_wait_ms,flush_count,max_inflight_dirs,read_mib_s,compress_mib_s,send_mib_s,total_mib_s,avg_cpu_pct,max_cpu_pct,avg_rss_mib,max_rss_mib,avg_private_mib,max_private_mib,compression_ratio_pct,wire_pct_of_read,wire_overhead_pct";
const MARKDOWN_TOP_ROWS: usize = 10;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ScenarioKey {
    pub(crate) dataset: String,
    pub(crate) threads: u32,
    pub(crate) frame_target_mb: u64,
    pub(crate) read_buffer_kb: u64,
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

impl RawResultRow {
    fn run_key(&self) -> (ScenarioKey, u32) {
        (self.scenario.clone(), self.repeat_index)
    }

    fn to_csv_line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{:.6},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}",
            self.scenario.dataset,
            self.scenario.threads,
            self.scenario.frame_target_mb,
            self.scenario.read_buffer_kb,
            self.scenario.large_file_single_frame_threshold_mib,
            self.repeat_index,
            self.files_count,
            self.skipped_files,
            self.sent_frames,
            self.frame_aborts,
            self.frame_rebuilds,
            self.avg_files_per_frame,
            self.raw_read_bytes,
            self.tar_input_bytes,
            self.compressed_payload_bytes,
            self.wire_bytes,
            self.wire_overhead_bytes,
            self.read_duration_ms,
            self.compress_duration_ms,
            self.send_duration_ms,
            self.total_duration_ms,
            self.traversal_wait_ms,
            self.ack_wait_ms,
            self.flush_count,
            self.max_inflight_dirs,
            self.read_mib_s,
            self.compress_mib_s,
            self.send_mib_s,
            self.total_mib_s,
            self.avg_cpu_pct,
            self.max_cpu_pct,
            self.avg_rss_mib,
            self.max_rss_mib,
            self.avg_private_mib,
            self.max_private_mib,
            self.compression_ratio_pct,
            self.wire_pct_of_read,
            self.wire_overhead_pct
        )
    }

    fn parse(line: &str) -> Result<Self> {
        let columns = line.split(',').collect::<Vec<_>>();
        if columns.len() != 38 {
            bail!(
                "unexpected raw results column count: expected 38, got {}",
                columns.len()
            );
        }

        Ok(Self {
            scenario: ScenarioKey {
                dataset: columns[0].to_owned(),
                threads: parse_u32(columns[1], "threads")?,
                frame_target_mb: parse_u64(columns[2], "frame_target_mb")?,
                read_buffer_kb: parse_u64(columns[3], "read_buffer_kb")?,
                large_file_single_frame_threshold_mib: parse_u64(
                    columns[4],
                    "large_file_single_frame_threshold_mib",
                )?,
            },
            repeat_index: parse_u32(columns[5], "repeat_index")?,
            files_count: parse_u64(columns[6], "files_count")?,
            skipped_files: parse_u64(columns[7], "skipped_files")?,
            sent_frames: parse_u64(columns[8], "sent_frames")?,
            frame_aborts: parse_u64(columns[9], "frame_aborts")?,
            frame_rebuilds: parse_u64(columns[10], "frame_rebuilds")?,
            avg_files_per_frame: parse_f64(columns[11], "avg_files_per_frame")?,
            raw_read_bytes: parse_u64(columns[12], "raw_read_bytes")?,
            tar_input_bytes: parse_u64(columns[13], "tar_input_bytes")?,
            compressed_payload_bytes: parse_u64(columns[14], "compressed_payload_bytes")?,
            wire_bytes: parse_u64(columns[15], "wire_bytes")?,
            wire_overhead_bytes: parse_u64(columns[16], "wire_overhead_bytes")?,
            read_duration_ms: parse_f64(columns[17], "read_duration_ms")?,
            compress_duration_ms: parse_f64(columns[18], "compress_duration_ms")?,
            send_duration_ms: parse_f64(columns[19], "send_duration_ms")?,
            total_duration_ms: parse_f64(columns[20], "total_duration_ms")?,
            traversal_wait_ms: parse_f64(columns[21], "traversal_wait_ms")?,
            ack_wait_ms: parse_f64(columns[22], "ack_wait_ms")?,
            flush_count: parse_u64(columns[23], "flush_count")?,
            max_inflight_dirs: parse_u64(columns[24], "max_inflight_dirs")?,
            read_mib_s: parse_f64(columns[25], "read_mib_s")?,
            compress_mib_s: parse_f64(columns[26], "compress_mib_s")?,
            send_mib_s: parse_f64(columns[27], "send_mib_s")?,
            total_mib_s: parse_f64(columns[28], "total_mib_s")?,
            avg_cpu_pct: parse_f64(columns[29], "avg_cpu_pct")?,
            max_cpu_pct: parse_f64(columns[30], "max_cpu_pct")?,
            avg_rss_mib: parse_f64(columns[31], "avg_rss_mib")?,
            max_rss_mib: parse_f64(columns[32], "max_rss_mib")?,
            avg_private_mib: parse_f64(columns[33], "avg_private_mib")?,
            max_private_mib: parse_f64(columns[34], "max_private_mib")?,
            compression_ratio_pct: parse_f64(columns[35], "compression_ratio_pct")?,
            wire_pct_of_read: parse_f64(columns[36], "wire_pct_of_read")?,
            wire_overhead_pct: parse_f64(columns[37], "wire_overhead_pct")?,
        })
    }
}

impl SummaryRow {
    fn to_csv_line(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{},{:.6},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6}",
            self.scenario.dataset,
            self.scenario.threads,
            self.scenario.frame_target_mb,
            self.scenario.read_buffer_kb,
            self.scenario.large_file_single_frame_threshold_mib,
            self.completed_repeats,
            self.files_count,
            self.skipped_files,
            self.sent_frames,
            self.frame_aborts,
            self.frame_rebuilds,
            self.avg_files_per_frame,
            self.raw_read_bytes,
            self.tar_input_bytes,
            self.compressed_payload_bytes,
            self.wire_bytes,
            self.wire_overhead_bytes,
            self.read_duration_ms,
            self.compress_duration_ms,
            self.send_duration_ms,
            self.total_duration_ms,
            self.traversal_wait_ms,
            self.ack_wait_ms,
            self.flush_count,
            self.max_inflight_dirs,
            self.read_mib_s,
            self.compress_mib_s,
            self.send_mib_s,
            self.total_mib_s,
            self.avg_cpu_pct,
            self.max_cpu_pct,
            self.avg_rss_mib,
            self.max_rss_mib,
            self.avg_private_mib,
            self.max_private_mib,
            self.compression_ratio_pct,
            self.wire_pct_of_read,
            self.wire_overhead_pct
        )
    }
}

pub(crate) fn results_raw_path(output_dir: &Path) -> PathBuf {
    output_dir.join("results_raw.csv")
}

pub(crate) fn results_summary_path(output_dir: &Path) -> PathBuf {
    output_dir.join("results_summary.csv")
}

pub(crate) fn reset_results(output_dir: &Path) -> Result<()> {
    let raw_path = results_raw_path(output_dir);
    let summary_path = results_summary_path(output_dir);

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

    Ok(())
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
            if line.trim() != RAW_HEADER {
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
        "| threads | frame MiB | buffer KiB | isolate MiB | repeats | total ms | total MiB/s | frames | files/frame | aborts | walk wait ms | ack wait ms | max RSS MiB |"
    );
    println!(
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"
    );

    for row in rows.iter().take(MARKDOWN_TOP_ROWS) {
        println!(
            "| {} | {} | {} | {} | {} | {:.2} | {:.2} | {} | {:.2} | {} | {:.2} | {:.2} | {:.2} |",
            row.scenario.threads,
            row.scenario.frame_target_mb,
            row.scenario.read_buffer_kb,
            row.scenario.large_file_single_frame_threshold_mib,
            row.completed_repeats,
            row.total_duration_ms,
            row.total_mib_s,
            row.sent_frames,
            row.avg_files_per_frame,
            row.frame_aborts,
            row.traversal_wait_ms,
            row.ack_wait_ms,
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

fn parse_f64(raw: &str, field: &str) -> Result<f64> {
    raw.parse::<f64>()
        .with_context(|| format!("failed to parse {field} as f64"))
}
