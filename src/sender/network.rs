use std::collections::VecDeque;
use std::io::{self, ErrorKind, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::network::protocol::wire::{
    self, BundleAbort, BundleAbortAck, BundleAck, BundleCommit, BundlePlan, Finish, Message,
    SkipAck, SkipEntries,
};
use crate::network::protocol::{BundleCommitDescriptor, BundlePlanDescriptor, SkippedEntry};
use crate::profile::SenderProfile;

use super::compress::BundleCompressor;
use super::plan::{
    DumpPlanner, MaterializedBundleTask, PrepareProfile, SendBundle, StreamedBundleTask,
};
use super::stream::{
    StreamWorkspace, send_materialized_attempt_with_workspace, send_streamed_attempt_with_workspace,
};
use super::{BundlePlanOptions, plan_dump};

const INFLIGHT_BUNDLE_WINDOW: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendReport {
    pub dump_id: String,
    pub final_bundle_no: u64,
    pub final_work_ordinal: u64,
    pub bundles_sent: usize,
    pub terminal_skip_count: usize,
}

enum PlannedItem {
    Skips(Vec<SkippedEntry>),
    Bundle(SendBundle),
    Done {
        final_work_ordinal: u64,
        prepare_profile: PrepareProfile,
    },
}

pub fn send_dump<T>(
    control_stream: &mut T,
    data_stream: &mut impl Write,
    source_dir: PathBuf,
    dump_id: &str,
    options: BundlePlanOptions,
) -> Result<SendReport>
where
    T: Read + Write,
{
    Ok(send_dump_profile(control_stream, data_stream, source_dir, dump_id, options)?.0)
}

pub fn send_dump_profile<TControl, TData>(
    control_stream: &mut TControl,
    data_stream: &mut TData,
    source_dir: PathBuf,
    dump_id: &str,
    options: BundlePlanOptions,
) -> Result<(SendReport, SenderProfile)>
where
    TControl: Read + Write,
    TData: Write,
{
    let started = Instant::now();
    let resume_state = wire::request_resume_state(control_stream, dump_id)?;
    let planner = plan_dump(&source_dir, &resume_state, options)?;
    let mut profile = SenderProfile {
        scan: planner.profile().scan.clone(),
        discovered_entries: planner.profile().discovered_entries,
        ..SenderProfile::default()
    };
    let planned = spawn_planner_thread(planner);
    let mut compressor =
        BundleCompressor::new(options.compression_level, options.compression_threads)?;
    let mut workspace = StreamWorkspace::new();
    let mut bundles_sent = 0_usize;
    let mut final_bundle_no = resume_state.last_committed_bundle_no;
    let mut inflight_bundles = VecDeque::<u64>::new();
    let (final_work_ordinal, logical_bytes) = loop {
        match receive_planned_item(planned.receiver.as_ref().unwrap())? {
            PlannedItem::Skips(entries) => {
                drain_inflight_bundles(control_stream, &mut profile, &mut inflight_bundles, 0)?;
                send_skip_entries_profiled(control_stream, &entries, &mut profile)?;
            }
            PlannedItem::Bundle(bundle) => {
                if profile.time_to_first_bundle.is_zero() {
                    profile.record_time_to_first_bundle(started.elapsed());
                }
                match bundle {
                    SendBundle::Materialized(bundle) => {
                        drain_inflight_bundles(
                            control_stream,
                            &mut profile,
                            &mut inflight_bundles,
                            INFLIGHT_BUNDLE_WINDOW.saturating_sub(1),
                        )?;
                        let bundle_no = bundle.descriptor.bundle_no;
                        profile.total.bytes = profile
                            .total
                            .bytes
                            .saturating_add(bundle.descriptor.raw_payload_bytes);
                        final_bundle_no = bundle_no;
                        write_materialized_bundle_profiled(
                            control_stream,
                            data_stream,
                            bundle,
                            &mut compressor,
                            &mut workspace,
                            &mut profile,
                        )?;
                        inflight_bundles.push_back(bundle_no);
                        bundles_sent += 1;
                    }
                    SendBundle::Streamed(task) => {
                        drain_inflight_bundles(
                            control_stream,
                            &mut profile,
                            &mut inflight_bundles,
                            INFLIGHT_BUNDLE_WINDOW.saturating_sub(1),
                        )?;
                        match send_streamed_bundle_profiled(
                            control_stream,
                            data_stream,
                            task,
                            &mut compressor,
                            &mut workspace,
                            &mut profile,
                            &mut inflight_bundles,
                        )? {
                            StreamedBundleSendOutcome::Committed { bundle_no } => {
                                final_bundle_no = bundle_no;
                                inflight_bundles.push_back(bundle_no);
                                bundles_sent += 1;
                            }
                            StreamedBundleSendOutcome::Skipped(skipped) => {
                                drain_inflight_bundles(
                                    control_stream,
                                    &mut profile,
                                    &mut inflight_bundles,
                                    0,
                                )?;
                                send_skip_entries_profiled(
                                    control_stream,
                                    &[skipped],
                                    &mut profile,
                                )?;
                            }
                        }
                    }
                }
            }
            PlannedItem::Done {
                final_work_ordinal,
                prepare_profile,
            } => {
                drain_inflight_bundles(control_stream, &mut profile, &mut inflight_bundles, 0)?;
                merge_prepare_profile(&mut profile, &prepare_profile);
                break (final_work_ordinal, profile.total.bytes);
            }
        }
    };
    planned.join()?;

    let finish_send_started = Instant::now();
    wire::write_message(
        control_stream,
        &Message::Finish(Finish {
            final_bundle_no,
            final_work_ordinal: if final_work_ordinal == 0 {
                None
            } else {
                Some(final_work_ordinal)
            },
        }),
    )?;
    control_stream
        .flush()
        .context("failed to flush v4 finish message")?;
    profile.record_send(0, finish_send_started.elapsed());
    profile.record_flush();

    let finish_ack_started = Instant::now();
    match wire::read_message(control_stream)? {
        Message::FinishAck => {
            profile.record_ack_wait(finish_ack_started.elapsed());
            profile.record_total(logical_bytes, started.elapsed());
            profile.bundles_sent = bundles_sent as u64;
            Ok((
                SendReport {
                    dump_id: dump_id.to_owned(),
                    final_bundle_no,
                    final_work_ordinal,
                    bundles_sent,
                    terminal_skip_count: profile.terminal_skips as usize,
                },
                profile,
            ))
        }
        Message::Error(error) => bail!("peer rejected v4 finish: {}", error.message),
        unexpected => bail!(
            "unexpected v4 message while waiting for finish ack: {:?}",
            unexpected.kind()
        ),
    }
}

fn merge_prepare_profile(profile: &mut SenderProfile, prepare_profile: &PrepareProfile) {
    profile.scan = prepare_profile.scan.clone();
    profile.discovered_entries = prepare_profile.discovered_entries;
    profile.read.bytes = profile
        .read
        .bytes
        .saturating_add(prepare_profile.read.bytes);
    profile.read.duration += prepare_profile.read.duration;
    profile.tar.bytes = profile.tar.bytes.saturating_add(prepare_profile.tar.bytes);
    profile.tar.duration += prepare_profile.tar.duration;
    profile.compress.bytes = profile
        .compress
        .bytes
        .saturating_add(prepare_profile.compress.bytes);
    profile.compress.duration += prepare_profile.compress.duration;
    profile.bundled_entries = profile
        .bundled_entries
        .saturating_add(prepare_profile.bundled_entries);
    profile.terminal_skips = profile
        .terminal_skips
        .saturating_add(prepare_profile.terminal_skips);
}

fn send_skip_entries_profiled<T>(
    stream: &mut T,
    entries: &[SkippedEntry],
    profile: &mut SenderProfile,
) -> Result<()>
where
    T: Read + Write,
{
    if entries.is_empty() {
        return Ok(());
    }

    let mut start = 0_usize;
    while start < entries.len() {
        let mut end = start;
        let mut estimated_body_bytes = std::mem::size_of::<u32>();
        while end < entries.len() {
            let next = estimate_skipped_entry_wire_bytes(&entries[end]);
            if end > start
                && estimated_body_bytes.saturating_add(next) > wire::MAX_CONTROL_BODY_BYTES
            {
                break;
            }
            estimated_body_bytes = estimated_body_bytes.saturating_add(next);
            end += 1;
        }

        let chunk = entries[start..end].to_vec();
        let send_started = Instant::now();
        wire::write_message(
            stream,
            &Message::SkipEntries(SkipEntries { entries: chunk }),
        )?;
        stream
            .flush()
            .context("failed to flush v4 skip entries message")?;
        profile.record_send(0, send_started.elapsed());
        profile.record_flush();

        let ack_started = Instant::now();
        match wire::read_message(stream)? {
            Message::SkipAck(SkipAck {
                total_terminal_skips: _,
            }) => profile.record_ack_wait(ack_started.elapsed()),
            Message::Error(error) => bail!("peer rejected v4 skip entries: {}", error.message),
            unexpected => bail!(
                "unexpected v4 message while waiting for skip ack: {:?}",
                unexpected.kind()
            ),
        }

        start = end;
    }

    Ok(())
}

fn write_materialized_bundle_profiled<TControl, TData>(
    control_stream: &mut TControl,
    data_stream: &mut TData,
    bundle: MaterializedBundleTask,
    compressor: &mut BundleCompressor,
    workspace: &mut StreamWorkspace,
    profile: &mut SenderProfile,
) -> Result<()>
where
    TControl: Read + Write,
    TData: Write,
{
    // План владеет исходными descriptors; tar заимствует их после отправки,
    // поэтому строки путей не копируются для двух стадий pipeline.
    let plan_message = Message::BundlePlan(BundlePlan {
        attempt_no: 1,
        descriptor: bundle.descriptor,
    });
    let plan_started = Instant::now();
    wire::write_message(control_stream, &plan_message)?;
    control_stream
        .flush()
        .context("failed to flush v4 bundle plan on control channel")?;
    let mut control_send_duration = plan_started.elapsed();
    profile.record_flush();
    let Message::BundlePlan(plan) = plan_message else {
        unreachable!();
    };
    let sent = send_materialized_attempt_with_workspace(
        data_stream,
        &plan.descriptor,
        bundle.payloads,
        compressor,
        workspace,
    )?;
    let payload = sent.result?;
    profile.record_flush();

    let commit_started = Instant::now();
    wire::write_message(
        control_stream,
        &Message::BundleCommit(BundleCommit {
            attempt_no: 1,
            descriptor: BundleCommitDescriptor {
                bundle_no: plan.descriptor.bundle_no,
                tar_payload_bytes: payload.tar_bytes,
                compressed_payload_bytes: payload.compressed_bytes,
                compressed_xxh3_64: payload.checksum,
            },
        }),
    )?;
    control_stream
        .flush()
        .context("failed to flush v4 bundle commit on control channel")?;
    control_send_duration += commit_started.elapsed();
    profile.record_send(
        payload.compressed_bytes,
        control_send_duration + sent.send_duration,
    );
    profile.record_flush();
    profile.tar.bytes = profile.tar.bytes.saturating_add(payload.tar_bytes);
    profile.compress.bytes = profile.compress.bytes.saturating_add(payload.tar_bytes);
    profile.compress.duration += sent.compress_duration;
    profile.bundled_entries = profile
        .bundled_entries
        .saturating_add(plan.descriptor.entries.len() as u64);
    Ok(())
}

enum StreamedBundleSendOutcome {
    Committed { bundle_no: u64 },
    Skipped(SkippedEntry),
}

fn send_streamed_bundle_profiled<TControl, TData>(
    control_stream: &mut TControl,
    data_stream: &mut TData,
    mut task: StreamedBundleTask,
    compressor: &mut BundleCompressor,
    workspace: &mut StreamWorkspace,
    profile: &mut SenderProfile,
    inflight_bundles: &mut VecDeque<u64>,
) -> Result<StreamedBundleSendOutcome>
where
    TControl: Read + Write,
    TData: Write,
{
    let mut total_read_bytes = 0_u64;
    let mut total_read_duration = Duration::ZERO;

    let attempt_limit = task.file.remaining_attempts();
    for attempt in 1..=attempt_limit {
        if attempt > 1 {
            match task.file.reopen_for_retry() {
                Ok(()) => {}
                Err(_) if attempt == attempt_limit => {
                    profile.read.bytes = profile.read.bytes.saturating_add(total_read_bytes);
                    profile.read.duration += total_read_duration;
                    profile.terminal_skips = profile.terminal_skips.saturating_add(1);
                    return Ok(StreamedBundleSendOutcome::Skipped(SkippedEntry {
                        work_ordinal: task.file.descriptor.work_ordinal,
                        relative_path: task.file.descriptor.relative_path.clone(),
                        reason: crate::network::protocol::SkipReason::OpenFailed,
                    }));
                }
                Err(_) => continue,
            }
        }

        let mut control_send_duration = Duration::ZERO;
        let plan_descriptor = streamed_bundle_plan_descriptor(&task);
        let attempt_no = attempt;
        if attempt > 1 {
            profile.bundle_rebuilds += 1;
        }

        let plan_started = Instant::now();
        wire::write_message(
            control_stream,
            &Message::BundlePlan(BundlePlan {
                attempt_no,
                descriptor: plan_descriptor,
            }),
        )?;
        control_stream
            .flush()
            .context("failed to flush v4 streamed bundle plan on control channel")?;
        control_send_duration += plan_started.elapsed();
        profile.record_flush();

        let sent = send_streamed_attempt_with_workspace(
            data_stream,
            &mut task.file,
            task.bundle_no,
            attempt_no,
            compressor,
            workspace,
        )?;
        total_read_bytes = total_read_bytes.saturating_add(sent.read_bytes);
        total_read_duration += sent.read_duration;
        profile.compress.duration += sent.compress_duration;

        match sent.result {
            Ok(payload) => {
                profile.record_flush();

                let commit_started = Instant::now();
                wire::write_message(
                    control_stream,
                    &Message::BundleCommit(BundleCommit {
                        attempt_no,
                        descriptor: BundleCommitDescriptor {
                            bundle_no: task.bundle_no,
                            tar_payload_bytes: payload.tar_bytes,
                            compressed_payload_bytes: payload.compressed_bytes,
                            compressed_xxh3_64: payload.checksum,
                        },
                    }),
                )?;
                control_send_duration += commit_started.elapsed();

                let flush_started = Instant::now();
                control_stream
                    .flush()
                    .context("failed to flush v4 streamed bundle commit on control channel")?;
                control_send_duration += flush_started.elapsed();

                profile.record_send(
                    payload.compressed_bytes,
                    control_send_duration + sent.send_duration,
                );
                profile.record_flush();
                profile.read.bytes = profile.read.bytes.saturating_add(total_read_bytes);
                profile.read.duration += total_read_duration;
                profile.tar.bytes = profile.tar.bytes.saturating_add(payload.tar_bytes);
                profile.compress.bytes = profile.compress.bytes.saturating_add(payload.tar_bytes);
                profile.bundled_entries = profile.bundled_entries.saturating_add(1);
                // Throughput считает только итоговые файлы; повторно прочитанные bytes остаются в read.
                profile.total.bytes = profile
                    .total
                    .bytes
                    .saturating_add(task.file.descriptor.size_at_open);
                return Ok(StreamedBundleSendOutcome::Committed {
                    bundle_no: task.bundle_no,
                });
            }
            Err(error) if is_retryable_stream_read_error(&error) => {
                send_bundle_abort_profiled(
                    control_stream,
                    task.bundle_no,
                    attempt_no,
                    profile,
                    inflight_bundles,
                )?;
                profile.bundle_aborts += 1;
                if attempt == attempt_limit {
                    profile.read.bytes = profile.read.bytes.saturating_add(total_read_bytes);
                    profile.read.duration += total_read_duration;
                    profile.terminal_skips = profile.terminal_skips.saturating_add(1);
                    return Ok(StreamedBundleSendOutcome::Skipped(SkippedEntry {
                        work_ordinal: task.file.descriptor.work_ordinal,
                        relative_path: task.file.descriptor.relative_path.clone(),
                        reason: crate::network::protocol::SkipReason::UnstableAfterRetries,
                    }));
                }
                task.file.descriptor.best_effort = true;
            }
            Err(error) => {
                return Err(error);
            }
        }
    }

    Ok(StreamedBundleSendOutcome::Skipped(SkippedEntry {
        work_ordinal: task.file.descriptor.work_ordinal,
        relative_path: task.file.descriptor.relative_path.clone(),
        reason: crate::network::protocol::SkipReason::UnstableAfterRetries,
    }))
}

fn streamed_bundle_plan_descriptor(task: &StreamedBundleTask) -> BundlePlanDescriptor {
    BundlePlanDescriptor {
        bundle_no: task.bundle_no,
        first_work_ordinal: task.file.descriptor.work_ordinal,
        last_work_ordinal: task.file.descriptor.work_ordinal,
        last_relative_path: task.file.descriptor.relative_path.clone(),
        entries: vec![task.file.descriptor.clone()],
        raw_payload_bytes: task.file.descriptor.size_at_open,
    }
}

fn send_bundle_abort_profiled<T>(
    stream: &mut T,
    bundle_no: u64,
    attempt_no: u32,
    profile: &mut SenderProfile,
    inflight_bundles: &mut VecDeque<u64>,
) -> Result<BundleAbortAck>
where
    T: Read + Write,
{
    let send_started = Instant::now();
    wire::write_message(
        stream,
        &Message::BundleAbort(BundleAbort {
            bundle_no,
            attempt_no,
        }),
    )?;
    stream.flush().context("failed to flush v4 bundle abort")?;
    profile.record_send(0, send_started.elapsed());
    profile.record_flush();

    loop {
        let ack_started = Instant::now();
        match wire::read_message(stream)? {
            Message::BundleAck(ack) => {
                profile.record_ack_wait(ack_started.elapsed());
                apply_cumulative_bundle_ack(inflight_bundles, &ack)?;
                continue;
            }
            Message::BundleAbortAck(ack) => {
                if ack.bundle_no != bundle_no || ack.attempt_no != attempt_no {
                    bail!(
                        "v4 bundle abort ack mismatch: expected bundle {} attempt {} got bundle {} attempt {}",
                        bundle_no,
                        attempt_no,
                        ack.bundle_no,
                        ack.attempt_no
                    );
                }
                profile.record_ack_wait(ack_started.elapsed());
                return Ok(ack);
            }
            Message::Error(error) => bail!("peer rejected v4 bundle abort: {}", error.message),
            unexpected => bail!(
                "unexpected v4 message while waiting for bundle abort ack: {:?}",
                unexpected.kind()
            ),
        }
    }
}

fn drain_inflight_bundles<T>(
    stream: &mut T,
    profile: &mut SenderProfile,
    inflight_bundles: &mut VecDeque<u64>,
    max_inflight: usize,
) -> Result<()>
where
    T: Read + Write,
{
    while inflight_bundles.len() > max_inflight {
        read_next_bundle_ack(stream, profile, inflight_bundles)?;
    }
    Ok(())
}

fn read_next_bundle_ack<T>(
    stream: &mut T,
    profile: &mut SenderProfile,
    inflight_bundles: &mut VecDeque<u64>,
) -> Result<BundleAck>
where
    T: Read + Write,
{
    let ack_started = Instant::now();
    match wire::read_message(stream)? {
        Message::BundleAck(ack) => {
            profile.record_ack_wait(ack_started.elapsed());
            apply_cumulative_bundle_ack(inflight_bundles, &ack)?;
            Ok(ack)
        }
        Message::Error(error) => bail!("peer rejected v4 bundle: {}", error.message),
        unexpected => bail!(
            "unexpected v4 message while waiting for bundle ack: {:?}",
            unexpected.kind()
        ),
    }
}

fn apply_cumulative_bundle_ack(
    inflight_bundles: &mut VecDeque<u64>,
    ack: &BundleAck,
) -> Result<()> {
    let Some(first_inflight) = inflight_bundles.front().copied() else {
        bail!(
            "unexpected v4 bundle ack {} without in-flight bundles",
            ack.bundle_no
        );
    };
    if ack.bundle_no < first_inflight {
        bail!(
            "v4 bundle ack moved backwards: first in-flight {} got ack {}",
            first_inflight,
            ack.bundle_no
        );
    }

    let mut last_popped = None;
    while let Some(bundle_no) = inflight_bundles.front().copied() {
        if bundle_no > ack.bundle_no {
            break;
        }
        last_popped = inflight_bundles.pop_front();
    }

    match last_popped {
        Some(last_popped) if last_popped == ack.bundle_no => Ok(()),
        Some(last_popped) => bail!(
            "v4 bundle ack jumped beyond in-flight window: ack {} last_popped {}",
            ack.bundle_no,
            last_popped
        ),
        None => bail!(
            "v4 bundle ack {} did not match current in-flight boundary",
            ack.bundle_no
        ),
    }
}

fn is_retryable_stream_read_error(error: &anyhow::Error) -> bool {
    error
        .chain()
        .filter_map(|source| source.downcast_ref::<io::Error>())
        .any(|io_error| io_error.kind() == ErrorKind::UnexpectedEof)
}

fn estimate_skipped_entry_wire_bytes(entry: &SkippedEntry) -> usize {
    std::mem::size_of::<u64>()
        + std::mem::size_of::<u8>()
        + std::mem::size_of::<u32>()
        + entry.relative_path.len()
}

struct PlannerThread {
    receiver: Option<Receiver<Result<PlannedItem>>>,
    handle: Option<thread::JoinHandle<()>>,
    cancellation: Arc<AtomicBool>,
}

impl PlannerThread {
    fn join(mut self) -> Result<()> {
        self.receiver.take();
        self.handle
            .take()
            .unwrap()
            .join()
            .map_err(|_| anyhow::anyhow!("v4 planner thread panicked"))?;
        Ok(())
    }
}

impl Drop for PlannerThread {
    fn drop(&mut self) {
        // При ошибке сети останавливаем новые чтения и снимаем backpressure до join.
        // Его Drop также завершает readers и закрывает открытые исходные файлы.
        self.cancellation.store(true, Ordering::Relaxed);
        self.receiver.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn spawn_planner_thread(planner: DumpPlanner) -> PlannerThread {
    let cancellation = planner.cancellation_flag();
    // Rendezvous не накапливает очередь сырых bundle: параллельно с текущим
    // planner готовит только следующий и может держать один перенесённый файл.
    // Окно ACK остаётся независимым и не требует хранить отправленные payload.
    let (sender, receiver) = mpsc::sync_channel(0);
    let handle = thread::spawn(move || {
        let result = run_planner_thread(planner, &sender);
        if let Err(error) = result {
            let _ = sender.send(Err(error));
        }
    });
    PlannerThread {
        receiver: Some(receiver),
        handle: Some(handle),
        cancellation,
    }
}

fn run_planner_thread(
    mut planner: DumpPlanner,
    sender: &SyncSender<Result<PlannedItem>>,
) -> Result<()> {
    planner.check_cancelled()?;
    send_planned_skips(sender, planner.take_new_terminal_skips())?;

    while let Some(bundle) = planner.next_send_bundle()? {
        planner.check_cancelled()?;
        send_planned_skips(sender, planner.take_new_terminal_skips())?;
        if sender.send(Ok(PlannedItem::Bundle(bundle))).is_err() {
            return Ok(());
        }
    }

    planner.check_cancelled()?;
    send_planned_skips(sender, planner.take_new_terminal_skips())?;
    if sender
        .send(Ok(PlannedItem::Done {
            final_work_ordinal: planner.final_work_ordinal(),
            prepare_profile: planner.finish_profile(),
        }))
        .is_err()
    {
        return Ok(());
    }

    Ok(())
}

fn send_planned_skips(
    sender: &SyncSender<Result<PlannedItem>>,
    entries: Vec<SkippedEntry>,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    sender
        .send(Ok(PlannedItem::Skips(entries)))
        .map_err(|_| anyhow::anyhow!("v4 planner receiver disconnected"))
}

fn receive_planned_item(receiver: &Receiver<Result<PlannedItem>>) -> Result<PlannedItem> {
    receiver
        .recv()
        .context("v4 planner thread exited before completing dump")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::protocol::SkipReason;

    fn skipped_entry() -> SkippedEntry {
        SkippedEntry {
            work_ordinal: 1,
            relative_path: "unreadable".into(),
            reason: SkipReason::OpenFailed,
        }
    }

    #[test]
    fn skip_send_propagates_disconnected_receiver() {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(receiver);
        let error = send_planned_skips(&sender, vec![skipped_entry()]).unwrap_err();
        assert!(error.to_string().contains("receiver disconnected"));
    }

    #[test]
    fn planner_drop_cancels_before_unblocking_and_joins_worker() {
        let cancellation = Arc::new(AtomicBool::new(false));
        let worker_cancellation = Arc::clone(&cancellation);
        let (sender, receiver) = mpsc::sync_channel(0);
        let (finished, completion) = mpsc::channel();
        let handle = thread::spawn(move || {
            assert!(
                sender
                    .send(Ok(PlannedItem::Skips(vec![skipped_entry()])))
                    .is_err()
            );
            finished
                .send(worker_cancellation.load(Ordering::Relaxed))
                .unwrap();
        });
        let planned = PlannerThread {
            receiver: Some(receiver),
            handle: Some(handle),
            cancellation,
        };
        drop(planned);
        assert!(completion.try_recv().unwrap());
    }
}
