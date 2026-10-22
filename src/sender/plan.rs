use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use xxhash_rust::xxh3::xxh3_64;

use crate::network::protocol::wire::MAX_CONTROL_BODY_BYTES;
use crate::network::protocol::{
    BundleDescriptor, BundleEntryDescriptor, BundleEntryKind, PreparedBundle, ResumeState,
    SkippedEntry,
};
use crate::profile::StageProfile;

use super::compress::BundleCompressor;
use super::read::{MaterializedEntry, ReadDecision, StreamedRegularFile};
use super::read_ahead::OrderedReader;
use super::scan::scan_source_tree;
use super::tar::build_compressed_bundle_with;

#[cfg(test)]
use super::tar::{
    StreamedBundleBuild, StreamedBundleOutcome, build_streamed_regular_bundle_payload,
};

#[derive(Debug, Clone, Copy)]
pub struct BundlePlanOptions {
    pub bundle_target_bytes: usize,
    pub compression_level: i32,
    pub compression_threads: u32,
    pub readers: usize,
}

impl Default for BundlePlanOptions {
    fn default() -> Self {
        Self {
            bundle_target_bytes: 8 * 1024 * 1024,
            compression_level: 3,
            compression_threads: 1,
            readers: std::thread::available_parallelism().map_or(1, |n| n.get().min(32)),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct PreparedDump {
    pub(crate) bundles: Vec<PreparedBundle>,
    pub(crate) new_terminal_skips: Vec<SkippedEntry>,
}

#[derive(Debug)]
pub(crate) struct StreamedBundleTask {
    pub(crate) bundle_no: u64,
    pub(crate) file: StreamedRegularFile,
}

#[derive(Debug)]
pub(crate) enum SendBundle {
    Prepared(PreparedBundle),
    Streamed(StreamedBundleTask),
}

#[cfg(test)]
pub(crate) fn prepare_dump(
    source_dir: &Path,
    resume_state: &ResumeState,
    options: BundlePlanOptions,
) -> Result<PreparedDump> {
    let mut planner = DumpPlanner::new(source_dir, resume_state, options)?;
    let mut bundles = Vec::new();
    let mut new_terminal_skips = planner.take_new_terminal_skips();
    while let Some(bundle) = planner.next_bundle()? {
        new_terminal_skips.extend(planner.take_new_terminal_skips());
        bundles.push(bundle);
    }
    new_terminal_skips.extend(planner.take_new_terminal_skips());
    sort_skipped_entries(&mut new_terminal_skips);
    Ok(PreparedDump {
        bundles,
        new_terminal_skips,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct PrepareProfile {
    pub(crate) scan: StageProfile,
    pub(crate) read: StageProfile,
    pub(crate) tar: StageProfile,
    pub(crate) compress: StageProfile,
    pub(crate) discovered_entries: u64,
    pub(crate) bundled_entries: u64,
    pub(crate) terminal_skips: u64,
}

impl PrepareProfile {
    fn record_built_bundle(
        &mut self,
        bundle: &PreparedBundle,
        tar_duration: std::time::Duration,
        compress_duration: std::time::Duration,
    ) {
        self.tar.bytes = self
            .tar
            .bytes
            .saturating_add(bundle.descriptor.tar_payload_bytes);
        self.tar.duration += tar_duration;
        self.compress.bytes = self
            .compress
            .bytes
            .saturating_add(bundle.descriptor.tar_payload_bytes);
        self.compress.duration += compress_duration;
        self.bundled_entries = self
            .bundled_entries
            .saturating_add(bundle.descriptor.entries.len() as u64);
    }
}

pub(crate) struct DumpPlanner {
    entries: OrderedReader,
    compressor: BundleCompressor,
    cancellation: Arc<AtomicBool>,
    pending_entry: Option<PendingEntry>,
    next_bundle_no: u64,
    options: BundlePlanOptions,
    known_terminal_paths: BTreeSet<String>,
    new_terminal_skips: Vec<SkippedEntry>,
    terminal_skips: Vec<SkippedEntry>,
    final_work_ordinal: u64,
    profile: PrepareProfile,
    #[cfg(test)]
    cancel_after_skips: Option<usize>,
}

enum PendingEntry {
    Materialized(MaterializedEntry),
    Streamed(StreamedRegularFile),
}

const FIRST_BUNDLE_TARGET_BYTES: usize = 4 * 1024 * 1024;
const BUNDLE_PLAN_FIXED_BODY_BYTES: usize = 44;

#[derive(Default)]
struct BundleBudget {
    payload_weight: usize,
    entry_descriptor_bytes: usize,
}

impl BundleBudget {
    fn try_add(&mut self, entry: &MaterializedEntry, target_bytes: usize) -> Result<bool> {
        validate_single_entry_control_size(&entry.descriptor)?;
        let payload_weight = self.payload_weight.saturating_add(bundle_weight(entry));
        let descriptor_bytes = self
            .entry_descriptor_bytes
            .saturating_add(bundle_entry_wire_bytes(&entry.descriptor));
        let body_bytes =
            bundle_plan_body_bytes(descriptor_bytes, entry.descriptor.relative_path.len());
        if self.payload_weight != 0
            && (payload_weight > target_bytes || body_bytes > MAX_CONTROL_BODY_BYTES)
        {
            return Ok(false);
        }
        self.payload_weight = payload_weight;
        self.entry_descriptor_bytes = descriptor_bytes;
        Ok(true)
    }
}

fn bundle_entry_wire_bytes(entry: &BundleEntryDescriptor) -> usize {
    // ordinal + длина пути + kind + size_at_open + best_effort; строки считаем в байтах UTF-8.
    let bytes = 22_usize.saturating_add(entry.relative_path.len());
    match &entry.kind {
        BundleEntryKind::Regular => bytes,
        BundleEntryKind::Symlink { target } => bytes.saturating_add(4).saturating_add(target.len()),
    }
}

fn bundle_plan_body_bytes(entry_descriptor_bytes: usize, last_path_bytes: usize) -> usize {
    // В body входят attempt_no, поля bundle и повтор последнего пути; packet header сюда не входит.
    BUNDLE_PLAN_FIXED_BODY_BYTES
        .saturating_add(last_path_bytes)
        .saturating_add(entry_descriptor_bytes)
}

fn validate_single_entry_control_size(entry: &BundleEntryDescriptor) -> Result<()> {
    let bytes = bundle_plan_body_bytes(bundle_entry_wire_bytes(entry), entry.relative_path.len());
    anyhow::ensure!(
        bytes <= MAX_CONTROL_BODY_BYTES,
        "v4 bundle entry cannot fit in one control message: {bytes} > {MAX_CONTROL_BODY_BYTES} bytes"
    );
    Ok(())
}

fn bundle_aggregation_target(bundle_no: u64, configured_bytes: usize) -> usize {
    if bundle_no == 1 {
        // Первый небольшой bundle раньше запускает передачу; порог streaming остаётся прежним.
        configured_bytes.min(FIRST_BUNDLE_TARGET_BYTES)
    } else {
        configured_bytes
    }
}

impl DumpPlanner {
    pub(crate) fn new(
        source_dir: &Path,
        resume_state: &ResumeState,
        options: BundlePlanOptions,
    ) -> Result<Self> {
        if options.bundle_target_bytes == 0 {
            bail!("v4 bundle_target_bytes must be greater than zero");
        }

        anyhow::ensure!(
            options.compression_threads > 0,
            "compression threads must be greater than zero"
        );
        let scan_started = Instant::now();
        let scan = scan_source_tree(source_dir)?;
        let mut profile = PrepareProfile::default();
        profile.scan.duration = scan_started.elapsed();
        profile.discovered_entries = scan.entries.len() as u64;
        validate_resume_boundary(resume_state, &scan.entries)?;

        let mut known_terminal_paths = resume_state
            .terminal_skips
            .iter()
            .map(|entry| entry.relative_path.clone())
            .collect::<BTreeSet<_>>();
        let mut new_terminal_skips = Vec::new();
        let mut terminal_skips = resume_state.terminal_skips.clone();

        for skipped in scan.skipped {
            if known_terminal_paths.insert(skipped.relative_path.clone()) {
                new_terminal_skips.push(skipped.clone());
                terminal_skips.push(skipped);
            }
        }

        let final_work_ordinal = scan
            .entries
            .last()
            .map(|entry| entry.work_ordinal)
            .unwrap_or(0);
        let entries = scan
            .entries
            .into_iter()
            .filter(|entry| {
                entry.work_ordinal > resume_state.last_committed_work_ordinal
                    && !known_terminal_paths.contains(&entry.relative_path)
            })
            .collect::<Vec<_>>();
        let compressor =
            BundleCompressor::new(options.compression_level, options.compression_threads)?;
        let entries =
            OrderedReader::new(entries, options.readers, options.bundle_target_bytes as u64)?;

        Ok(Self {
            entries,
            compressor,
            cancellation: Arc::new(AtomicBool::new(false)),
            pending_entry: None,
            next_bundle_no: resume_state.last_committed_bundle_no.saturating_add(1),
            options,
            known_terminal_paths,
            new_terminal_skips,
            terminal_skips,
            final_work_ordinal,
            profile,
            #[cfg(test)]
            cancel_after_skips: None,
        })
    }

    pub(crate) fn cancellation_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancellation)
    }

    pub(crate) fn check_cancelled(&self) -> Result<()> {
        anyhow::ensure!(
            !self.cancellation.load(Ordering::Relaxed),
            "v4 dump planning cancelled"
        );
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn next_bundle(&mut self) -> Result<Option<PreparedBundle>> {
        self.check_cancelled()?;
        let mut current_entries = Vec::<MaterializedEntry>::new();
        let mut current_budget = BundleBudget::default();
        let target_bytes =
            bundle_aggregation_target(self.next_bundle_no, self.options.bundle_target_bytes);

        if let Some(pending_entry) = self.pending_entry.take() {
            match pending_entry {
                PendingEntry::Materialized(pending_entry) => {
                    current_budget.try_add(&pending_entry, target_bytes)?;
                    current_entries.push(pending_entry);
                }
                PendingEntry::Streamed(mut streamed) => {
                    if let Some(bundle) = self.build_streamed_bundle_or_continue(&mut streamed)? {
                        return Ok(Some(bundle));
                    }
                }
            }
        }

        loop {
            self.check_cancelled()?;
            let read_started = Instant::now();
            let Some(decision) = self.entries.next()? else {
                break;
            };
            self.check_cancelled()?;
            match decision {
                ReadDecision::Accepted(materialized) => {
                    self.profile.read.duration += read_started.elapsed();
                    self.profile.read.bytes = self
                        .profile
                        .read
                        .bytes
                        .saturating_add(materialized.descriptor.size_at_open);
                    if !current_budget.try_add(&materialized, target_bytes)? {
                        // Переносим целый файл в следующий bundle: metadata cap не является причиной skip.
                        self.pending_entry = Some(PendingEntry::Materialized(materialized));
                        break;
                    }
                    current_entries.push(materialized);
                }
                ReadDecision::Streamed(mut streamed) => {
                    validate_single_entry_control_size(&streamed.descriptor)?;
                    self.profile.read.duration += read_started.elapsed();
                    if !current_entries.is_empty() {
                        self.pending_entry = Some(PendingEntry::Streamed(streamed));
                        break;
                    }
                    if let Some(bundle) = self.build_streamed_bundle_or_continue(&mut streamed)? {
                        return Ok(Some(bundle));
                    }
                }
                ReadDecision::Skipped(skipped) => {
                    self.profile.read.duration += read_started.elapsed();
                    self.record_terminal_skip(skipped);
                }
            }
        }

        self.check_cancelled()?;
        if current_entries.is_empty() {
            return Ok(None);
        }

        let (bundle, tar_duration, compress_duration) =
            build_bundle(self.next_bundle_no, current_entries, &mut self.compressor)?;
        self.next_bundle_no = self.next_bundle_no.saturating_add(1);
        self.profile
            .record_built_bundle(&bundle, tar_duration, compress_duration);
        Ok(Some(bundle))
    }

    pub(crate) fn next_send_bundle(&mut self) -> Result<Option<SendBundle>> {
        self.check_cancelled()?;
        let mut current_entries = Vec::<MaterializedEntry>::new();
        let mut current_budget = BundleBudget::default();
        let target_bytes =
            bundle_aggregation_target(self.next_bundle_no, self.options.bundle_target_bytes);

        if let Some(pending_entry) = self.pending_entry.take() {
            match pending_entry {
                PendingEntry::Materialized(pending_entry) => {
                    current_budget.try_add(&pending_entry, target_bytes)?;
                    current_entries.push(pending_entry);
                }
                PendingEntry::Streamed(streamed) => {
                    return Ok(Some(SendBundle::Streamed(StreamedBundleTask {
                        bundle_no: self.reserve_bundle_no(),
                        file: streamed,
                    })));
                }
            }
        }

        loop {
            // Даже длинная серия skips должна прерываться при обрыве сети.
            self.check_cancelled()?;
            let read_started = Instant::now();
            let Some(decision) = self.entries.next()? else {
                break;
            };
            self.check_cancelled()?;
            match decision {
                ReadDecision::Accepted(materialized) => {
                    self.profile.read.duration += read_started.elapsed();
                    self.profile.read.bytes = self
                        .profile
                        .read
                        .bytes
                        .saturating_add(materialized.descriptor.size_at_open);
                    if !current_budget.try_add(&materialized, target_bytes)? {
                        // Переносим целый файл в следующий bundle: metadata cap не является причиной skip.
                        self.pending_entry = Some(PendingEntry::Materialized(materialized));
                        break;
                    }
                    current_entries.push(materialized);
                }
                ReadDecision::Streamed(streamed) => {
                    validate_single_entry_control_size(&streamed.descriptor)?;
                    self.profile.read.duration += read_started.elapsed();
                    if !current_entries.is_empty() {
                        self.pending_entry = Some(PendingEntry::Streamed(streamed));
                        break;
                    }
                    return Ok(Some(SendBundle::Streamed(StreamedBundleTask {
                        bundle_no: self.reserve_bundle_no(),
                        file: streamed,
                    })));
                }
                ReadDecision::Skipped(skipped) => {
                    self.profile.read.duration += read_started.elapsed();
                    self.record_terminal_skip(skipped);
                }
            }
        }

        self.check_cancelled()?;
        if current_entries.is_empty() {
            return Ok(None);
        }

        let bundle_no = self.reserve_bundle_no();
        let (bundle, tar_duration, compress_duration) =
            build_bundle(bundle_no, current_entries, &mut self.compressor)?;
        self.profile
            .record_built_bundle(&bundle, tar_duration, compress_duration);
        Ok(Some(SendBundle::Prepared(bundle)))
    }

    pub(crate) fn take_new_terminal_skips(&mut self) -> Vec<SkippedEntry> {
        std::mem::take(&mut self.new_terminal_skips)
    }

    pub(crate) const fn final_work_ordinal(&self) -> u64 {
        self.final_work_ordinal
    }

    pub(crate) fn finish_profile(mut self) -> PrepareProfile {
        self.profile.terminal_skips = self.terminal_skips.len() as u64;
        self.profile
    }

    pub(crate) fn profile(&self) -> &PrepareProfile {
        &self.profile
    }

    fn record_terminal_skip(&mut self, skipped: SkippedEntry) {
        if self
            .known_terminal_paths
            .insert(skipped.relative_path.clone())
        {
            self.new_terminal_skips.push(skipped.clone());
            self.terminal_skips.push(skipped);
            #[cfg(test)]
            if self
                .cancel_after_skips
                .is_some_and(|limit| self.new_terminal_skips.len() >= limit)
            {
                self.cancellation.store(true, Ordering::Relaxed);
            }
        }
    }

    fn reserve_bundle_no(&mut self) -> u64 {
        let bundle_no = self.next_bundle_no;
        self.next_bundle_no = self.next_bundle_no.saturating_add(1);
        bundle_no
    }

    #[cfg(test)]
    fn build_streamed_bundle_or_continue(
        &mut self,
        streamed: &mut StreamedRegularFile,
    ) -> Result<Option<PreparedBundle>> {
        match build_streamed_bundle(
            self.next_bundle_no,
            streamed,
            self.options.compression_level,
            self.options.compression_threads,
        )? {
            StreamedPlannerOutcome::Built {
                bundle,
                read_bytes,
                read_duration,
                tar_duration,
                compress_duration,
            } => {
                self.profile.read.bytes = self.profile.read.bytes.saturating_add(read_bytes);
                self.profile.read.duration += read_duration;
                self.profile
                    .record_built_bundle(&bundle, tar_duration, compress_duration);
                self.next_bundle_no = self.next_bundle_no.saturating_add(1);
                Ok(Some(bundle))
            }
            StreamedPlannerOutcome::Skipped {
                skipped,
                read_bytes,
                read_duration,
            } => {
                self.profile.read.bytes = self.profile.read.bytes.saturating_add(read_bytes);
                self.profile.read.duration += read_duration;
                self.record_terminal_skip(skipped);
                Ok(None)
            }
        }
    }
}

pub(crate) fn plan_dump(
    source_dir: &Path,
    resume_state: &ResumeState,
    options: BundlePlanOptions,
) -> Result<DumpPlanner> {
    DumpPlanner::new(source_dir, resume_state, options)
}

#[cfg(test)]
fn sort_skipped_entries(entries: &mut [SkippedEntry]) {
    entries.sort_by(|left, right| {
        left.work_ordinal
            .cmp(&right.work_ordinal)
            .then_with(|| left.relative_path.cmp(&right.relative_path))
    });
}

fn validate_resume_boundary(
    resume_state: &ResumeState,
    entries: &[super::scan::ScannedEntry],
) -> Result<()> {
    if resume_state.last_committed_work_ordinal == 0 {
        return Ok(());
    }

    let Some(entry) = entries
        .iter()
        .find(|entry| entry.work_ordinal == resume_state.last_committed_work_ordinal)
    else {
        bail!(
            "v4 resume mismatch: committed work ordinal {} is missing from current scan",
            resume_state.last_committed_work_ordinal
        );
    };

    if entry.relative_path != resume_state.last_committed_relative_path {
        bail!(
            "v4 resume mismatch: committed path {} no longer matches current path {}",
            resume_state.last_committed_relative_path,
            entry.relative_path
        );
    }

    Ok(())
}

fn bundle_weight(entry: &MaterializedEntry) -> usize {
    entry.contents.len().max(1)
}

#[cfg(test)]
enum StreamedPlannerOutcome {
    Built {
        bundle: PreparedBundle,
        read_bytes: u64,
        read_duration: std::time::Duration,
        tar_duration: std::time::Duration,
        compress_duration: std::time::Duration,
    },
    Skipped {
        skipped: SkippedEntry,
        read_bytes: u64,
        read_duration: std::time::Duration,
    },
}

#[cfg(test)]
fn build_streamed_bundle(
    bundle_no: u64,
    streamed: &mut StreamedRegularFile,
    compression_level: i32,
    compression_threads: u32,
) -> Result<StreamedPlannerOutcome> {
    match build_streamed_regular_bundle_payload(streamed, compression_level, compression_threads)? {
        StreamedBundleOutcome::Built(StreamedBundleBuild {
            compressed_payload,
            tar_payload_bytes,
            raw_read_bytes,
            read_duration,
            compress_duration,
        }) => Ok(StreamedPlannerOutcome::Built {
            bundle: PreparedBundle {
                descriptor: BundleDescriptor {
                    bundle_no,
                    first_work_ordinal: streamed.descriptor.work_ordinal,
                    last_work_ordinal: streamed.descriptor.work_ordinal,
                    last_relative_path: streamed.descriptor.relative_path.clone(),
                    entries: vec![streamed.descriptor.clone()],
                    raw_payload_bytes: streamed.descriptor.size_at_open,
                    tar_payload_bytes,
                    compressed_payload_bytes: compressed_payload.len() as u64,
                    compressed_xxh3_64: xxh3_64(&compressed_payload),
                },
                compressed_payload,
            },
            read_bytes: raw_read_bytes,
            read_duration,
            tar_duration: std::time::Duration::ZERO,
            compress_duration,
        }),
        StreamedBundleOutcome::Skipped {
            skipped,
            raw_read_bytes,
            read_duration,
        } => Ok(StreamedPlannerOutcome::Skipped {
            skipped,
            read_bytes: raw_read_bytes,
            read_duration,
        }),
    }
}

fn build_bundle(
    bundle_no: u64,
    entries: Vec<MaterializedEntry>,
    compressor: &mut BundleCompressor,
) -> Result<(PreparedBundle, std::time::Duration, std::time::Duration)> {
    let first_work_ordinal = entries
        .first()
        .map(|entry| entry.descriptor.work_ordinal)
        .context("v4 bundle cannot be empty")?;
    let last_entry = entries.last().context("v4 bundle cannot be empty")?;
    let last_work_ordinal = last_entry.descriptor.work_ordinal;
    let last_relative_path = last_entry.descriptor.relative_path.clone();
    let build_started = Instant::now();
    let (compressed_payload, tar_payload_bytes) =
        build_compressed_bundle_with(&entries, compressor)?;
    let build_duration = build_started.elapsed();
    let mut raw_payload_bytes = 0_u64;
    // После compression переносим descriptors и сразу освобождаем payload каждого файла.
    let entries_descriptor = entries
        .into_iter()
        .map(|entry| {
            raw_payload_bytes += entry.descriptor.size_at_open;
            entry.descriptor
        })
        .collect::<Vec<_>>();

    Ok((
        PreparedBundle {
            descriptor: BundleDescriptor {
                bundle_no,
                first_work_ordinal,
                last_work_ordinal,
                last_relative_path,
                entries: entries_descriptor,
                raw_payload_bytes,
                tar_payload_bytes,
                compressed_payload_bytes: compressed_payload.len() as u64,
                compressed_xxh3_64: xxh3_64(&compressed_payload),
            },
            compressed_payload,
        },
        std::time::Duration::ZERO,
        build_duration,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::protocol::BundlePlanDescriptor;
    use crate::network::protocol::wire::{self, BundlePlan, Message};
    use crate::tests::TestDir;
    use std::fs;
    use std::io::Cursor;

    #[test]
    fn cancellation_stops_a_skip_run_between_entries() {
        let dir = TestDir::new();
        for ordinal in 1..=16 {
            fs::write(dir.path().join(format!("{ordinal:02}")), b"data").unwrap();
        }
        let mut planner = DumpPlanner::new(
            dir.path(),
            &ResumeState::default(),
            BundlePlanOptions {
                readers: 1,
                ..BundlePlanOptions::default()
            },
        )
        .unwrap();
        for ordinal in 1..=16 {
            fs::remove_file(dir.path().join(format!("{ordinal:02}"))).unwrap();
        }
        // Отмена после нескольких реальных open failures проверяет сам цикл,
        // а не только проверку флага перед входом в next_send_bundle.
        planner.cancel_after_skips = Some(3);
        let error = planner.next_send_bundle().unwrap_err();
        assert!(error.to_string().contains("planning cancelled"));
        let skipped = planner.take_new_terminal_skips();
        assert_eq!(skipped.len(), 3);
        assert_eq!(skipped.last().unwrap().work_ordinal, 3);
        let ReadDecision::Skipped(next) = planner.entries.next().unwrap().unwrap() else {
            panic!("deleted input must produce a skip");
        };
        assert_eq!(next.work_ordinal, 4);
    }

    #[test]
    fn cancellation_keeps_pending_entry_unconsumed() {
        let dir = TestDir::new();
        fs::write(dir.path().join("a"), b"aa").unwrap();
        fs::write(dir.path().join("b"), b"bb").unwrap();
        let mut planner = DumpPlanner::new(
            dir.path(),
            &ResumeState::default(),
            BundlePlanOptions {
                bundle_target_bytes: 2,
                readers: 1,
                ..BundlePlanOptions::default()
            },
        )
        .unwrap();
        assert!(matches!(
            planner.next_send_bundle().unwrap(),
            Some(SendBundle::Prepared(_))
        ));
        let cancellation = planner.cancellation_flag();
        cancellation.store(true, Ordering::Relaxed);
        assert!(planner.next_send_bundle().is_err());
        let Some(PendingEntry::Materialized(pending)) = planner.pending_entry.as_ref() else {
            panic!("cancelled planner consumed its pending entry");
        };
        assert_eq!(pending.descriptor.relative_path, "b");
        assert_eq!(pending.contents, b"bb");
    }

    fn empty_entry(ordinal: u64, path: String) -> MaterializedEntry {
        MaterializedEntry {
            descriptor: BundleEntryDescriptor {
                work_ordinal: ordinal,
                relative_path: path,
                kind: BundleEntryKind::Regular,
                size_at_open: 0,
                best_effort: false,
            },
            mode: 0o644,
            contents: Vec::new().into(),
        }
    }

    fn encode_and_decode_plan(
        bundle_no: u64,
        entries: Vec<BundleEntryDescriptor>,
    ) -> (Vec<BundleEntryDescriptor>, usize) {
        let descriptor = BundlePlanDescriptor {
            bundle_no,
            first_work_ordinal: entries.first().unwrap().work_ordinal,
            last_work_ordinal: entries.last().unwrap().work_ordinal,
            last_relative_path: entries.last().unwrap().relative_path.clone(),
            raw_payload_bytes: entries.iter().map(|entry| entry.size_at_open).sum(),
            entries,
        };
        let mut wire_bytes = Vec::new();
        wire::write_message(
            &mut wire_bytes,
            &Message::BundlePlan(BundlePlan {
                attempt_no: 1,
                descriptor,
            }),
        )
        .unwrap();
        let body_bytes = u32::from_le_bytes(wire_bytes[8..12].try_into().unwrap()) as usize;
        assert!(body_bytes <= MAX_CONTROL_BODY_BYTES);
        assert_eq!(wire_bytes.len(), 12 + body_bytes);
        let Message::BundlePlan(plan) = wire::read_message(&mut Cursor::new(wire_bytes)).unwrap()
        else {
            panic!("encoded bundle plan decoded as another message");
        };
        (plan.descriptor.entries, body_bytes)
    }

    #[test]
    fn metadata_budget_matches_wire_for_utf8_paths_and_symlink_targets() {
        let regular = empty_entry(1, "directory/ёж.txt".into());
        let mut symlink = empty_entry(2, "directory/ссылка".into());
        symlink.descriptor.kind = BundleEntryKind::Symlink {
            target: "../файл.bin".into(),
        };
        let mut budget = BundleBudget::default();
        assert!(budget.try_add(&regular, usize::MAX).unwrap());
        assert!(budget.try_add(&symlink, usize::MAX).unwrap());
        let expected_body = bundle_plan_body_bytes(
            budget.entry_descriptor_bytes,
            symlink.descriptor.relative_path.len(),
        );
        let entries = vec![regular.descriptor, symlink.descriptor];
        let (decoded, actual_body) = encode_and_decode_plan(1, entries.clone());
        assert_eq!(actual_body, expected_body);
        assert_eq!(decoded, entries);
    }

    #[test]
    fn metadata_limit_splits_empty_files_without_losing_entries() {
        let mut expected = Vec::new();
        let mut decoded = Vec::new();
        let mut current = Vec::new();
        let mut budget = BundleBudget::default();
        let mut bundle_no = 1;
        for ordinal in 1..=2048 {
            let entry = empty_entry(
                ordinal,
                format!("dir-{ordinal:04}/{}/empty", "component/".repeat(250)),
            );
            expected.push(entry.descriptor.clone());
            if !budget.try_add(&entry, 64 * 1024 * 1024).unwrap() {
                decoded.extend(encode_and_decode_plan(bundle_no, std::mem::take(&mut current)).0);
                bundle_no += 1;
                budget = BundleBudget::default();
                assert!(budget.try_add(&entry, 64 * 1024 * 1024).unwrap());
            }
            current.push(entry.descriptor);
        }
        decoded.extend(encode_and_decode_plan(bundle_no, current).0);
        assert!(bundle_no >= 2);
        assert_eq!(decoded, expected);
        assert_eq!(decoded.len(), 2048);
    }

    #[test]
    fn single_entry_accepts_exact_wire_limit_and_rejects_larger_descriptor() {
        // Для regular entry body = 44 + 22 + 2 * path.len().
        let path_bytes = (MAX_CONTROL_BODY_BYTES - BUNDLE_PLAN_FIXED_BODY_BYTES - 22) / 2;
        let mut entry = empty_entry(1, "p".repeat(path_bytes));
        let mut budget = BundleBudget::default();
        assert!(budget.try_add(&entry, usize::MAX).unwrap());
        let (_, body_bytes) = encode_and_decode_plan(1, vec![entry.descriptor.clone()]);
        assert_eq!(body_bytes, MAX_CONTROL_BODY_BYTES);

        entry.descriptor.relative_path.push('p');
        let error = BundleBudget::default()
            .try_add(&entry, usize::MAX)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot fit in one control message")
        );
    }

    #[test]
    fn first_bundle_target_does_not_change_configured_or_resume_target() {
        assert_eq!(
            bundle_aggregation_target(1, 64 * 1024 * 1024),
            4 * 1024 * 1024
        );
        assert_eq!(
            bundle_aggregation_target(2, 64 * 1024 * 1024),
            64 * 1024 * 1024
        );
        assert_eq!(bundle_aggregation_target(1, 1024), 1024);
    }
}
