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
use crate::progress::{self, Phase, PipelineStage, PipelineState};
use crate::runtime::SessionControl;

use super::compress::BundleCompressor;
use super::plan::{
    DumpPlanner, MaterializedBundleTask, PrepareProfile, SendBundle, StreamedBundleTask,
};
use super::read::ReopenDecision;
use super::stream::{
    StreamWorkspace, send_materialized_attempt_controlled, send_streamed_attempt_controlled,
};
use super::{BundlePlanOptions, plan_dump};

const INFLIGHT_BUNDLE_WINDOW: usize = 4;
const SKIP_LOG_SAMPLES: usize = 8;

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
    send_dump_inner(
        control_stream,
        data_stream,
        source_dir,
        dump_id,
        options,
        None,
    )
}

pub(crate) fn send_dump_controlled<TControl: Read + Write, TData: Write>(
    control_stream: &mut TControl,
    data_stream: &mut TData,
    source_dir: PathBuf,
    dump_id: &str,
    options: BundlePlanOptions,
    control: &SessionControl,
) -> Result<(SendReport, SenderProfile)> {
    let result = send_dump_inner(
        control_stream,
        data_stream,
        source_dir,
        dump_id,
        options,
        Some(control),
    );
    if result.is_err() {
        control.check()?;
    }
    result
}

fn send_dump_inner<TControl: Read + Write, TData: Write>(
    control_stream: &mut TControl,
    data_stream: &mut TData,
    source_dir: PathBuf,
    dump_id: &str,
    options: BundlePlanOptions,
    control: Option<&SessionControl>,
) -> Result<(SendReport, SenderProfile)> {
    super::check_control(control)?;
    if let Some(progress) = control.and_then(SessionControl::progress) {
        progress.set_phase(Phase::Preparing);
    }
    let started = Instant::now();
    let resume_state = {
        let _waiting = progress::stage(
            control,
            PipelineStage::Control,
            PipelineState::WaitingControl,
        );
        wire::request_resume_state(control_stream, dump_id)
            .map_err(|error| control_io_error(control, error))?
    };
    let planner = match control {
        Some(control) => {
            DumpPlanner::controlled(&source_dir, &resume_state, options, Some(control.clone()))?
        }
        None => plan_dump(&source_dir, &resume_state, options)?,
    };
    if let Some(progress) = control.and_then(SessionControl::progress) {
        progress
            .resumed_bytes
            .store(resume_state.archive_len, Ordering::Relaxed);
        progress
            .durable_bytes
            .store(resume_state.archive_len, Ordering::Relaxed);
        progress
            .skips
            .store(resume_state.terminal_skips.len() as u64, Ordering::Relaxed);
    }
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
    let mut inflight_bundles = InflightBundles {
        pending: VecDeque::with_capacity(INFLIGHT_BUNDLE_WINDOW),
        archive_len: resume_state.archive_len,
        control: control.cloned(),
    };
    let (final_work_ordinal, logical_bytes) = loop {
        super::check_control(control)?;
        match receive_planned_item(planned.receiver.as_ref().unwrap(), control)? {
            PlannedItem::Skips(entries) => {
                drain_inflight_bundles(control_stream, &mut profile, &mut inflight_bundles, 0)?;
                send_skip_entries_profiled(control_stream, &entries, &mut profile, control)?;
            }
            PlannedItem::Bundle(bundle) => {
                if let Some(progress) = control.and_then(SessionControl::progress) {
                    progress.set_phase(Phase::Sending);
                }
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
                        let sent = write_materialized_bundle_profiled(
                            control_stream,
                            data_stream,
                            bundle,
                            &mut compressor,
                            &mut workspace,
                            &mut profile,
                            control,
                        )?;
                        inflight_bundles.sent(sent);
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
                            StreamedBundleSendOutcome::Committed(sent) => {
                                final_bundle_no = sent.bundle_no;
                                inflight_bundles.sent(sent);
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
                                    control,
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

    super::check_control(control)?;
    if let Some(progress) = control.and_then(SessionControl::progress) {
        progress.set_phase(Phase::Finalizing);
    }
    let finish_send_started = Instant::now();
    let sending = progress::stage(control, PipelineStage::Control, PipelineState::Sending);
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
    )
    .map_err(|error| control_io_error(control, error))?;
    control_stream
        .flush()
        .map_err(crate::network::network_io)
        .context("failed to flush v4 finish message")
        .map_err(|error| control_io_error(control, error))?;
    profile.record_send(0, finish_send_started.elapsed());
    profile.record_flush();

    drop(sending);

    let finish_ack_started = Instant::now();
    match receive_control_message(control_stream, control)? {
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
    control: Option<&SessionControl>,
) -> Result<()>
where
    T: Read + Write,
{
    if entries.is_empty() {
        return Ok(());
    }

    let mut start = 0_usize;
    while start < entries.len() {
        super::check_control(control)?;
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
        let sending = progress::stage(control, PipelineStage::Control, PipelineState::Sending);
        wire::write_message(
            stream,
            &Message::SkipEntries(SkipEntries { entries: chunk }),
        )
        .map_err(|error| control_io_error(control, error))?;
        stream
            .flush()
            .map_err(crate::network::network_io)
            .context("failed to flush v4 skip entries message")
            .map_err(|error| control_io_error(control, error))?;
        profile.record_send(0, send_started.elapsed());
        profile.record_flush();

        drop(sending);

        let ack_started = Instant::now();
        match receive_control_message(stream, control)? {
            Message::SkipAck(SkipAck {
                total_terminal_skips: _,
            }) => {
                profile.record_ack_wait(ack_started.elapsed());
                if let Some(control) = control
                    && let Some(progress) = control.progress()
                {
                    progress
                        .skips
                        .fetch_add((end - start) as u64, Ordering::Relaxed);
                    // Журнал получает только durable skips; длинная серия не
                    // создаёт неограниченный поток строк и копий исходных путей.
                    for entry in entries[start..end].iter().take(SKIP_LOG_SAMPLES) {
                        control.log(
                            log::Level::Warn,
                            &format!(
                                "Skipped {}: {} (durable)",
                                crate::progress::display_text(&entry.relative_path, 512),
                                entry.reason.as_str(),
                            ),
                        );
                    }
                    if end - start > SKIP_LOG_SAMPLES {
                        control.log(
                            log::Level::Warn,
                            &format!(
                                "{} additional skips durably recorded in this batch",
                                end - start - SKIP_LOG_SAMPLES,
                            ),
                        );
                    }
                }
            }
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
    control: Option<&SessionControl>,
) -> Result<InflightBundle>
where
    TControl: Read + Write,
    TData: Write,
{
    super::check_control(control)?;
    // План владеет исходными descriptors; tar заимствует их после отправки,
    // поэтому строки путей не копируются для двух стадий pipeline.
    let plan_message = Message::BundlePlan(BundlePlan {
        attempt_no: 1,
        descriptor: bundle.descriptor,
    });
    let plan_started = Instant::now();
    let sending = progress::stage(control, PipelineStage::Control, PipelineState::Sending);
    wire::write_message(control_stream, &plan_message)
        .map_err(|error| control_io_error(control, error))?;
    control_stream
        .flush()
        .map_err(crate::network::network_io)
        .context("failed to flush v4 bundle plan on control channel")
        .map_err(|error| control_io_error(control, error))?;
    let mut control_send_duration = plan_started.elapsed();
    profile.record_flush();
    drop(sending);
    let Message::BundlePlan(plan) = plan_message else {
        unreachable!();
    };
    let sent = send_materialized_attempt_controlled(
        data_stream,
        &plan.descriptor,
        bundle.payloads,
        compressor,
        workspace,
        control,
    )?;
    let payload = sent.result?;
    profile.record_flush();

    super::check_control(control)?;
    let commit_started = Instant::now();
    let _sending = progress::stage(control, PipelineStage::Control, PipelineState::Sending);
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
    )
    .map_err(|error| control_io_error(control, error))?;
    control_stream
        .flush()
        .map_err(crate::network::network_io)
        .context("failed to flush v4 bundle commit on control channel")
        .map_err(|error| control_io_error(control, error))?;
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
    Ok(InflightBundle {
        bundle_no: plan.descriptor.bundle_no,
        entries: plan.descriptor.entries.len() as u64,
        last_work_ordinal: plan.descriptor.last_work_ordinal,
        compressed_bytes: payload.compressed_bytes,
    })
}

enum StreamedBundleSendOutcome {
    Committed(InflightBundle),
    Skipped(SkippedEntry),
}

fn send_streamed_bundle_profiled<TControl, TData>(
    control_stream: &mut TControl,
    data_stream: &mut TData,
    mut task: StreamedBundleTask,
    compressor: &mut BundleCompressor,
    workspace: &mut StreamWorkspace,
    profile: &mut SenderProfile,
    inflight_bundles: &mut InflightBundles,
) -> Result<StreamedBundleSendOutcome>
where
    TControl: Read + Write,
    TData: Write,
{
    let session = inflight_bundles.control.clone();
    let control = session.as_ref();
    let mut total_read_bytes = 0_u64;
    let mut total_read_duration = Duration::ZERO;

    let attempt_limit = task.file.remaining_attempts();
    for attempt in 1..=attempt_limit {
        super::read::before_read_attempt(control, attempt > 1)?;
        if attempt > 1 {
            let _reading =
                progress::stage(control, PipelineStage::Compress, PipelineState::Reading);
            let skip_reason = match task.file.reopen_for_retry() {
                Ok(ReopenDecision::Opened) => None,
                Ok(ReopenDecision::Unsupported(filesystem)) => {
                    super::check_control(control)?;
                    super::source_policy::log_exclusion(
                        control,
                        &task.file.descriptor.relative_path,
                        filesystem,
                    );
                    Some(crate::network::protocol::SkipReason::UnsupportedEntry)
                }
                Err(_) if attempt == attempt_limit => {
                    super::check_control(control)?;
                    Some(crate::network::protocol::SkipReason::OpenFailed)
                }
                Err(_) => continue,
            };
            if let Some(reason) = skip_reason {
                // Предыдущая попытка уже получила AbortAck. Запрещённый новый
                // handle не создаёт BundlePlan и сразу переходит в terminal skip.
                profile.read.bytes = profile.read.bytes.saturating_add(total_read_bytes);
                profile.read.duration += total_read_duration;
                profile.terminal_skips = profile.terminal_skips.saturating_add(1);
                return Ok(StreamedBundleSendOutcome::Skipped(SkippedEntry {
                    work_ordinal: task.file.descriptor.work_ordinal,
                    relative_path: task.file.descriptor.relative_path.clone(),
                    reason,
                }));
            }
        }

        let mut control_send_duration = Duration::ZERO;
        let plan_descriptor = streamed_bundle_plan_descriptor(&task);
        let attempt_no = attempt;
        if attempt > 1 {
            profile.bundle_rebuilds += 1;
        }

        let plan_started = Instant::now();
        let sending = progress::stage(control, PipelineStage::Control, PipelineState::Sending);
        wire::write_message(
            control_stream,
            &Message::BundlePlan(BundlePlan {
                attempt_no,
                descriptor: plan_descriptor,
            }),
        )
        .map_err(|error| control_io_error(control, error))?;
        control_stream
            .flush()
            .map_err(crate::network::network_io)
            .context("failed to flush v4 streamed bundle plan on control channel")
            .map_err(|error| control_io_error(control, error))?;
        control_send_duration += plan_started.elapsed();
        profile.record_flush();
        drop(sending);

        let sent = send_streamed_attempt_controlled(
            data_stream,
            &mut task.file,
            task.bundle_no,
            attempt_no,
            compressor,
            workspace,
            control,
        )?;
        super::check_control(control)?;
        total_read_bytes = total_read_bytes.saturating_add(sent.read_bytes);
        total_read_duration += sent.read_duration;
        profile.compress.duration += sent.compress_duration;

        match sent.result {
            Ok(payload) => {
                profile.record_flush();

                let commit_started = Instant::now();
                let _sending =
                    progress::stage(control, PipelineStage::Control, PipelineState::Sending);
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
                )
                .map_err(|error| control_io_error(control, error))?;
                control_send_duration += commit_started.elapsed();

                let flush_started = Instant::now();
                control_stream
                    .flush()
                    .map_err(crate::network::network_io)
                    .context("failed to flush v4 streamed bundle commit on control channel")
                    .map_err(|error| control_io_error(control, error))?;
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
                return Ok(StreamedBundleSendOutcome::Committed(InflightBundle {
                    bundle_no: task.bundle_no,
                    entries: 1,
                    last_work_ordinal: task.file.descriptor.work_ordinal,
                    compressed_bytes: payload.compressed_bytes,
                }));
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
    inflight_bundles: &mut InflightBundles,
) -> Result<BundleAbortAck>
where
    T: Read + Write,
{
    super::check_control(inflight_bundles.control.as_ref())?;
    let send_started = Instant::now();
    let sending = progress::stage(
        inflight_bundles.control.as_ref(),
        PipelineStage::Control,
        PipelineState::Sending,
    );
    wire::write_message(
        stream,
        &Message::BundleAbort(BundleAbort {
            bundle_no,
            attempt_no,
        }),
    )
    .map_err(|error| control_io_error(inflight_bundles.control.as_ref(), error))?;
    stream
        .flush()
        .map_err(crate::network::network_io)
        .context("failed to flush v4 bundle abort")
        .map_err(|error| control_io_error(inflight_bundles.control.as_ref(), error))?;
    profile.record_send(0, send_started.elapsed());
    profile.record_flush();
    drop(sending);

    let _waiting = progress::stage(
        inflight_bundles.control.as_ref(),
        PipelineStage::Control,
        PipelineState::WaitingAck,
    );
    loop {
        super::check_control(inflight_bundles.control.as_ref())?;
        let ack_started = Instant::now();
        match receive_control_message(stream, inflight_bundles.control.as_ref())? {
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
    inflight_bundles: &mut InflightBundles,
    max_inflight: usize,
) -> Result<()>
where
    T: Read + Write,
{
    let _waiting = if inflight_bundles.pending.len() > max_inflight {
        Some(progress::stage(
            inflight_bundles.control.as_ref(),
            PipelineStage::Data,
            PipelineState::WaitingAck,
        ))
    } else {
        None
    };
    while inflight_bundles.pending.len() > max_inflight {
        read_next_bundle_ack(stream, profile, inflight_bundles)?;
    }
    Ok(())
}

fn read_next_bundle_ack<T>(
    stream: &mut T,
    profile: &mut SenderProfile,
    inflight_bundles: &mut InflightBundles,
) -> Result<BundleAck>
where
    T: Read + Write,
{
    super::check_control(inflight_bundles.control.as_ref())?;
    let ack_started = Instant::now();
    match receive_control_message(stream, inflight_bundles.control.as_ref())? {
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

fn receive_control_message(
    stream: &mut impl Read,
    control: Option<&SessionControl>,
) -> Result<Message> {
    let _waiting = progress::stage(control, PipelineStage::Control, PipelineState::WaitingAck);
    wire::read_message(stream).map_err(|error| control_io_error(control, error))
}

fn control_io_error(control: Option<&SessionControl>, error: anyhow::Error) -> anyhow::Error {
    if !control.is_some_and(SessionControl::is_cancelled) {
        progress::set_stage(control, PipelineStage::Control, PipelineState::Failed);
    }
    error
}

struct InflightBundle {
    bundle_no: u64,
    entries: u64,
    last_work_ordinal: u64,
    compressed_bytes: u64,
}

struct InflightBundles {
    // Храним только счётчики четырёх bundles, без отправленного payload.
    pending: VecDeque<InflightBundle>,
    archive_len: u64,
    control: Option<SessionControl>,
}

impl InflightBundles {
    fn sent(&mut self, bundle: InflightBundle) {
        if let Some(progress) = self.control.as_ref().and_then(SessionControl::progress) {
            progress
                .sent_entries
                .fetch_add(bundle.entries, Ordering::Relaxed);
            progress.bundles.fetch_add(1, Ordering::Relaxed);
        }
        self.pending.push_back(bundle);
    }
}

fn apply_cumulative_bundle_ack(inflight: &mut InflightBundles, ack: &BundleAck) -> Result<()> {
    let boundary = inflight
        .pending
        .iter()
        .position(|bundle| bundle.bundle_no == ack.bundle_no)
        .context("v4 bundle ack did not match current in-flight boundary")?;
    let last = &inflight.pending[boundary];
    let committed = inflight.pending.iter().take(boundary + 1);
    let archive_len = committed
        .clone()
        .try_fold(inflight.archive_len, |bytes, bundle| {
            bytes
                .checked_add(bundle.compressed_bytes)
                .context("v4 archive length overflow")
        })?;
    anyhow::ensure!(
        ack.last_work_ordinal == last.last_work_ordinal,
        "v4 bundle ack work ordinal mismatch"
    );
    anyhow::ensure!(
        ack.archive_len == archive_len,
        "v4 bundle ack archive length mismatch"
    );
    let entries: u64 = committed.map(|bundle| bundle.entries).sum();
    // Сначала проверяем весь ACK: ошибочный peer не меняет durable-счётчики.
    inflight.pending.drain(..=boundary);
    inflight.archive_len = archive_len;
    if let Some(progress) = inflight.control.as_ref().and_then(SessionControl::progress) {
        progress
            .committed_entries
            .fetch_add(entries, Ordering::Relaxed);
        progress.durable_bytes.store(archive_len, Ordering::Relaxed);
    }
    Ok(())
}

fn is_retryable_stream_read_error(error: &anyhow::Error) -> bool {
    // Сетевой EOF требует новой сессии, а не повторного чтения или пропуска файла.
    if crate::network::is_retryable_error(error) {
        return false;
    }
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
    let session = planner.control();
    let control = session.as_ref();
    let prepared = (|| -> Result<bool> {
        planner.check_cancelled()?;
        send_planned_skips(sender, planner.take_new_terminal_skips(), control)?;

        while let Some(bundle) = planner.next_send_bundle()? {
            planner.check_cancelled()?;
            send_planned_skips(sender, planner.take_new_terminal_skips(), control)?;
            let _waiting =
                progress::stage(control, PipelineStage::Plan, PipelineState::WaitingConsumer);
            if sender.send(Ok(PlannedItem::Bundle(bundle))).is_err() {
                return Ok(false);
            }
        }
        planner.check_cancelled()?;
        send_planned_skips(sender, planner.take_new_terminal_skips(), control)?;
        Ok(true)
    })();
    match prepared {
        Ok(true) => {}
        Ok(false) => {
            progress::set_stage(control, PipelineStage::Plan, PipelineState::Stopped);
            return Ok(());
        }
        Err(error) => {
            // Проверяем local cancellation до Drop planner: освобождение scanner
            // само выставляет этот флаг даже после успешного завершения.
            let state = if planner.check_cancelled().is_err() {
                PipelineState::Stopped
            } else {
                PipelineState::Failed
            };
            progress::set_stage(control, PipelineStage::Plan, state);
            return Err(error);
        }
    }

    let _waiting = progress::stage(control, PipelineStage::Plan, PipelineState::WaitingConsumer);
    if sender
        .send(Ok(PlannedItem::Done {
            final_work_ordinal: planner.final_work_ordinal(),
            prepare_profile: planner.finish_profile(),
        }))
        .is_err()
    {
        progress::set_stage(control, PipelineStage::Plan, PipelineState::Stopped);
        return Ok(());
    }

    progress::set_stage(control, PipelineStage::Plan, PipelineState::Done);
    Ok(())
}

fn send_planned_skips(
    sender: &SyncSender<Result<PlannedItem>>,
    entries: Vec<SkippedEntry>,
    control: Option<&SessionControl>,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let _waiting = progress::stage(control, PipelineStage::Plan, PipelineState::WaitingConsumer);
    sender
        .send(Ok(PlannedItem::Skips(entries)))
        .map_err(|_| anyhow::anyhow!("v4 planner receiver disconnected"))
}

fn receive_planned_item(
    receiver: &Receiver<Result<PlannedItem>>,
    control: Option<&SessionControl>,
) -> Result<PlannedItem> {
    let _waiting = progress::stage(control, PipelineStage::Data, PipelineState::WaitingPlan);
    if control.is_none() {
        return receiver
            .recv()
            .context("v4 planner thread exited before completing dump")?;
    }
    loop {
        super::check_control(control)?;
        match receiver.recv_timeout(Duration::from_millis(50)) {
            Ok(result) => return result,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(error) => {
                return Err(error).context("v4 planner thread exited before completing dump");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::protocol::SkipReason;
    use crate::progress::ProgressHub;
    use crate::role::Role;
    use crate::runtime::{Cancelled, RunControl};

    struct Duplex {
        input: io::Cursor<Vec<u8>>,
        output: Vec<u8>,
    }

    impl Read for Duplex {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.input.read(bytes)
        }
    }

    impl Write for Duplex {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.output.write(bytes)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn observed_session() -> SessionControl {
        RunControl::new(Some(Arc::new(ProgressHub::new()))).session(Role::Sender, "dump", "peer")
    }

    #[test]
    fn streamed_retry_exclusion_aborts_before_skip_and_never_sends_a_new_plan() {
        use crate::sender::read::{ReadDecision, materialize_owned_entry, test_hooks};

        let _guard = test_hooks::lock();
        test_hooks::clear();
        let source = crate::tests::TestDir::new();
        let path = source.path().join("file");
        std::fs::write(&path, b"original").unwrap();
        test_hooks::classify_next_open(&path, test_hooks::FilesystemResult::Allowed);
        test_hooks::classify_next_open(
            &path,
            test_hooks::FilesystemResult::Unsupported(
                super::super::source_policy::ExcludedFilesystem::procfs_for_test(),
            ),
        );
        test_hooks::classify_next_open(&path, test_hooks::FilesystemResult::Allowed);
        test_hooks::force_short_read_after_next_open(&path, 2);
        let entry = super::super::scan::scan_source_tree(source.path())
            .unwrap()
            .entries
            .remove(0);
        let ReadDecision::Streamed(file) = materialize_owned_entry(entry, 1).unwrap() else {
            panic!("file must reach streaming");
        };
        let mut replies = Vec::new();
        wire::write_message(
            &mut replies,
            &Message::BundleAbortAck(BundleAbortAck {
                bundle_no: 1,
                attempt_no: 1,
                archive_len: 0,
            }),
        )
        .unwrap();
        wire::write_message(
            &mut replies,
            &Message::SkipAck(SkipAck {
                total_terminal_skips: 1,
            }),
        )
        .unwrap();
        let mut stream = Duplex {
            input: io::Cursor::new(replies),
            output: Vec::new(),
        };
        let control = observed_session();
        let mut inflight = InflightBundles {
            pending: VecDeque::new(),
            archive_len: 0,
            control: Some(control.clone()),
        };
        let mut profile = SenderProfile::default();
        let result = send_streamed_bundle_profiled(
            &mut stream,
            &mut Vec::new(),
            StreamedBundleTask { bundle_no: 1, file },
            &mut BundleCompressor::new(1, 1).unwrap(),
            &mut StreamWorkspace::new(),
            &mut profile,
            &mut inflight,
        )
        .unwrap();
        let StreamedBundleSendOutcome::Skipped(skipped) = result else {
            panic!("excluded replacement must produce a terminal skip");
        };
        assert_eq!(skipped.reason, SkipReason::UnsupportedEntry);
        assert_eq!(test_hooks::remaining_filesystem_results(&path), 1);
        assert_eq!(profile.bundle_aborts, 1);
        assert_eq!(profile.bundle_rebuilds, 0);
        assert_eq!(profile.terminal_skips, 1);
        assert_eq!(profile.read.bytes, 2);
        let progress = control.progress().unwrap();
        assert_eq!(progress.retries.load(Ordering::Relaxed), 1);
        assert_eq!(progress.skips.load(Ordering::Relaxed), 0);
        send_skip_entries_profiled(&mut stream, &[skipped], &mut profile, Some(&control)).unwrap();
        assert_eq!(progress.skips.load(Ordering::Relaxed), 1);

        let mut messages = io::Cursor::new(stream.output);
        assert!(matches!(
            wire::read_message(&mut messages).unwrap(),
            Message::BundlePlan(BundlePlan { attempt_no: 1, .. })
        ));
        assert!(matches!(
            wire::read_message(&mut messages).unwrap(),
            Message::BundleAbort(BundleAbort { attempt_no: 1, .. })
        ));
        assert!(matches!(
            wire::read_message(&mut messages).unwrap(),
            Message::SkipEntries(_)
        ));
        assert_eq!(messages.position(), messages.get_ref().len() as u64);
        test_hooks::clear();
    }

    #[test]
    fn planner_distinguishes_local_cancellation_from_completed_cleanup() {
        for cancelled in [false, true] {
            let source = crate::tests::TestDir::new();
            let control = observed_session();
            let planner = DumpPlanner::controlled(
                source.path(),
                &crate::network::protocol::ResumeState::default(),
                BundlePlanOptions::default(),
                Some(control.clone()),
            )
            .unwrap();
            let local_cancellation = planner.cancellation_flag();
            if cancelled {
                local_cancellation.store(true, Ordering::Relaxed);
                progress::set_stage(Some(&control), PipelineStage::Data, PipelineState::Failed);
            }
            let (sender, _receiver) = mpsc::sync_channel(1);
            let result = run_planner_thread(planner, &sender);
            assert_eq!(result.is_err(), cancelled);
            assert!(!control.is_cancelled());
            assert!(local_cancellation.load(Ordering::Relaxed));
            let snapshot = control.progress().unwrap().snapshot();
            assert_eq!(
                snapshot
                    .pipelines
                    .iter()
                    .find(|stage| stage.stage == PipelineStage::Plan)
                    .unwrap()
                    .state,
                if cancelled {
                    PipelineState::Stopped
                } else {
                    PipelineState::Done
                }
            );
            if cancelled {
                assert_eq!(
                    snapshot
                        .pipelines
                        .iter()
                        .find(|stage| stage.stage == PipelineStage::Data)
                        .unwrap()
                        .state,
                    PipelineState::Failed
                );
            }
        }
    }

    #[test]
    fn network_eof_is_not_a_local_file_retry() {
        let local = anyhow::Error::new(io::Error::from(ErrorKind::UnexpectedEof))
            .context("source file became shorter");
        assert!(is_retryable_stream_read_error(&local));
        assert!(!crate::network::is_retryable_error(&local));

        let network = crate::network::network_io(io::Error::from(ErrorKind::UnexpectedEof))
            .context("peer disconnected while acknowledging the bundle");
        assert!(!is_retryable_stream_read_error(&network));
        assert!(crate::network::is_retryable_error(&network));
    }

    #[test]
    fn control_flush_failure_preserves_network_error_without_acknowledging_skips() {
        struct FailingFlush;

        impl Read for FailingFlush {
            fn read(&mut self, _bytes: &mut [u8]) -> io::Result<usize> {
                panic!("must not wait for an ACK after the control flush failed");
            }
        }

        impl Write for FailingFlush {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::from(ErrorKind::BrokenPipe))
            }
        }

        let control = observed_session();
        let error = send_skip_entries_profiled(
            &mut FailingFlush,
            &[skipped_entry()],
            &mut SenderProfile::default(),
            Some(&control),
        )
        .unwrap_err();
        assert!(crate::network::is_retryable_error(&error));
        assert_eq!(
            error.downcast_ref::<io::Error>().unwrap().kind(),
            ErrorKind::BrokenPipe
        );
        assert_eq!(control.progress().unwrap().skips.load(Ordering::Relaxed), 0);
    }

    fn pending_bundles(control: &SessionControl) -> InflightBundles {
        InflightBundles {
            pending: VecDeque::from([
                InflightBundle {
                    bundle_no: 6,
                    entries: 2,
                    last_work_ordinal: 103,
                    compressed_bytes: 20,
                },
                InflightBundle {
                    bundle_no: 7,
                    entries: 3,
                    last_work_ordinal: 108,
                    compressed_bytes: 30,
                },
            ]),
            archive_len: 100,
            control: Some(control.clone()),
        }
    }

    #[test]
    fn cumulative_ack_counts_entries_without_counting_resumed_or_skipped_ordinals() {
        let control = observed_session();
        let mut inflight = pending_bundles(&control);
        apply_cumulative_bundle_ack(
            &mut inflight,
            &BundleAck {
                bundle_no: 7,
                last_work_ordinal: 108,
                archive_len: 150,
            },
        )
        .unwrap();
        let progress = control.progress().unwrap();
        assert_eq!(progress.committed_entries.load(Ordering::Relaxed), 5);
        assert_eq!(progress.durable_bytes.load(Ordering::Relaxed), 150);
        assert!(inflight.pending.is_empty());
    }

    #[test]
    fn invalid_ack_preserves_pending_bundles_and_durable_progress() {
        let control = observed_session();
        let mut inflight = pending_bundles(&control);
        for ack in [
            BundleAck {
                bundle_no: 8,
                last_work_ordinal: 108,
                archive_len: 150,
            },
            BundleAck {
                bundle_no: 7,
                last_work_ordinal: 109,
                archive_len: 150,
            },
            BundleAck {
                bundle_no: 7,
                last_work_ordinal: 108,
                archive_len: 151,
            },
        ] {
            assert!(apply_cumulative_bundle_ack(&mut inflight, &ack).is_err());
            assert_eq!(inflight.pending.len(), 2);
            assert_eq!(inflight.archive_len, 100);
        }
        let progress = control.progress().unwrap();
        assert_eq!(progress.committed_entries.load(Ordering::Relaxed), 0);
        assert_eq!(progress.durable_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn abort_wait_applies_interleaved_durable_ack() {
        let control = observed_session();
        let mut inflight = pending_bundles(&control);
        let mut replies = Vec::new();
        wire::write_message(
            &mut replies,
            &Message::BundleAck(BundleAck {
                bundle_no: 7,
                last_work_ordinal: 108,
                archive_len: 150,
            }),
        )
        .unwrap();
        wire::write_message(
            &mut replies,
            &Message::BundleAbortAck(BundleAbortAck {
                bundle_no: 8,
                attempt_no: 1,
                archive_len: 150,
            }),
        )
        .unwrap();
        let mut stream = Duplex {
            input: io::Cursor::new(replies),
            output: Vec::new(),
        };
        send_bundle_abort_profiled(
            &mut stream,
            8,
            1,
            &mut SenderProfile::default(),
            &mut inflight,
        )
        .unwrap();
        assert_eq!(
            control
                .progress()
                .unwrap()
                .committed_entries
                .load(Ordering::Relaxed),
            5
        );
        assert_eq!(
            control
                .progress()
                .unwrap()
                .durable_bytes
                .load(Ordering::Relaxed),
            150
        );
    }

    #[test]
    fn cancellation_wakes_planner_wait_with_sender_still_alive() {
        let control = observed_session();
        let worker_control = control.clone();
        let (sender, receiver) = mpsc::sync_channel(0);
        let (done, completed) = mpsc::channel();
        let worker = thread::spawn(move || {
            let error = receive_planned_item(&receiver, Some(&worker_control))
                .err()
                .unwrap();
            done.send(error.downcast_ref::<Cancelled>().is_some())
                .unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !control
            .progress()
            .unwrap()
            .snapshot()
            .pipelines
            .iter()
            .any(|stage| {
                stage.stage == PipelineStage::Data && stage.state == PipelineState::WaitingPlan
            })
        {
            assert!(
                Instant::now() < deadline,
                "sender did not begin waiting for its plan"
            );
            thread::yield_now();
        }
        assert!(matches!(
            completed.recv_timeout(Duration::from_millis(75)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        let snapshot = control.progress().unwrap().snapshot();
        let stage = snapshot
            .pipelines
            .iter()
            .find(|stage| stage.stage == PipelineStage::Data)
            .unwrap();
        assert_eq!(stage.state, PipelineState::WaitingPlan);
        assert!(stage.elapsed >= Duration::from_millis(50));
        control.cancel();
        assert!(completed.recv_timeout(Duration::from_secs(2)).unwrap());
        worker.join().unwrap();
        assert_eq!(
            control
                .progress()
                .unwrap()
                .snapshot()
                .pipelines
                .iter()
                .find(|stage| stage.stage == PipelineStage::Data)
                .unwrap()
                .state,
            PipelineState::Idle
        );
        drop(sender);
    }

    #[test]
    fn control_read_reports_ack_wait_and_retains_network_failure() {
        struct BlockedRead {
            entered: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
        }
        impl Read for BlockedRead {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                self.entered.send(()).unwrap();
                self.release.recv().unwrap();
                Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "test peer reset",
                ))
            }
        }
        let control = observed_session();
        let (entered, entry) = mpsc::channel();
        let (release, released) = mpsc::channel();
        thread::scope(|scope| {
            let worker = scope.spawn(|| {
                receive_control_message(
                    &mut BlockedRead {
                        entered,
                        release: released,
                    },
                    Some(&control),
                )
            });
            entry.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(
                control
                    .progress()
                    .unwrap()
                    .snapshot()
                    .pipelines
                    .iter()
                    .find(|stage| stage.stage == PipelineStage::Control)
                    .unwrap()
                    .state,
                PipelineState::WaitingAck
            );
            release.send(()).unwrap();
            assert!(worker.join().unwrap().is_err());
        });
        assert_eq!(
            control
                .progress()
                .unwrap()
                .snapshot()
                .pipelines
                .iter()
                .find(|stage| stage.stage == PipelineStage::Control)
                .unwrap()
                .state,
            PipelineState::Failed
        );
    }

    fn skipped_entry() -> SkippedEntry {
        SkippedEntry {
            work_ordinal: 1,
            relative_path: "unreadable".into(),
            reason: SkipReason::OpenFailed,
        }
    }

    #[test]
    fn skip_diagnostics_are_bounded_and_require_a_successful_ack() {
        let entries = (0..12)
            .map(|index| SkippedEntry {
                work_ordinal: index + 1,
                relative_path: format!("{index}-{}", "long-path".repeat(100)),
                reason: SkipReason::OpenFailed,
            })
            .collect::<Vec<_>>();
        for accepted in [false, true] {
            let hub = Arc::new(ProgressHub::new());
            let run = RunControl::new(Some(hub.clone()));
            let control = run.session(Role::Sender, "dump", "peer");
            let progress = control.progress().unwrap();
            progress.skips.store(3, Ordering::Relaxed);
            let mut replies = Vec::new();
            if accepted {
                wire::write_message(
                    &mut replies,
                    &Message::SkipAck(SkipAck {
                        total_terminal_skips: 15,
                    }),
                )
                .unwrap();
            } else {
                wire::send_error(&mut replies, "skip catalog write failed").unwrap();
            }
            let mut stream = Duplex {
                input: io::Cursor::new(replies),
                output: Vec::new(),
            };
            let result = send_skip_entries_profiled(
                &mut stream,
                &entries,
                &mut SenderProfile::default(),
                Some(&control),
            );
            assert_eq!(result.is_ok(), accepted);
            let logs = hub.logs();
            if accepted {
                assert_eq!(progress.skips.load(Ordering::Relaxed), 15);
                assert_eq!(logs.len(), SKIP_LOG_SAMPLES + 1);
                assert!(
                    logs.iter()
                        .all(|entry| entry.level == log::Level::Warn && entry.message.len() < 600)
                );
                assert!(logs[0].message.contains("open_failed"));
                assert!(logs.last().unwrap().message.contains("4 additional skips"));
            } else {
                assert_eq!(progress.skips.load(Ordering::Relaxed), 3);
                assert!(logs.is_empty());
            }
        }
    }

    #[test]
    fn skip_send_propagates_disconnected_receiver() {
        let (sender, receiver) = mpsc::sync_channel(1);
        drop(receiver);
        let error = send_planned_skips(&sender, vec![skipped_entry()], None).unwrap_err();
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
