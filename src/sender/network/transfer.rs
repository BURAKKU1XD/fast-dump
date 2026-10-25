use std::io::{self, ErrorKind, Read, Write};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use super::ack::{InflightBundle, InflightBundles, send_bundle_abort_profiled};
use super::{SKIP_LOG_SAMPLES, control_io_error, receive_control_message};
use crate::network::protocol::wire::{
    self, BundleCommit, BundlePlan, Message, SkipAck, SkipEntries,
};
use crate::network::protocol::{BundleCommitDescriptor, BundlePlanDescriptor, SkippedEntry};
use crate::profile::SenderProfile;
use crate::progress::{self, PipelineStage, PipelineState};
use crate::runtime::SessionControl;
use crate::sender::compress::BundleCompressor;
use crate::sender::plan::{MaterializedBundleTask, StreamedBundleTask};
use crate::sender::read::ReopenDecision;
use crate::sender::stream::{
    StreamWorkspace, send_materialized_attempt_controlled, send_streamed_attempt_controlled,
};

pub(super) fn send_skip_entries_profiled<T>(
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
        crate::sender::check_control(control)?;
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

pub(super) fn write_materialized_bundle_profiled<TControl, TData>(
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
    crate::sender::check_control(control)?;
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

    crate::sender::check_control(control)?;
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

pub(super) enum StreamedBundleSendOutcome {
    Committed(InflightBundle),
    Skipped(SkippedEntry),
}

pub(super) fn send_streamed_bundle_profiled<TControl, TData>(
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
        crate::sender::read::before_read_attempt(control, attempt > 1)?;
        if attempt > 1 {
            let _reading =
                progress::stage(control, PipelineStage::Compress, PipelineState::Reading);
            let skip_reason = match task.file.reopen_for_retry() {
                Ok(ReopenDecision::Opened) => None,
                Ok(ReopenDecision::Unsupported(filesystem)) => {
                    crate::sender::check_control(control)?;
                    crate::sender::source_policy::log_exclusion(
                        control,
                        &task.file.descriptor.relative_path,
                        filesystem,
                    );
                    Some(crate::network::protocol::SkipReason::UnsupportedEntry)
                }
                Err(_) if attempt == attempt_limit => {
                    crate::sender::check_control(control)?;
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
        crate::sender::check_control(control)?;
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

pub(super) fn streamed_bundle_plan_descriptor(task: &StreamedBundleTask) -> BundlePlanDescriptor {
    BundlePlanDescriptor {
        bundle_no: task.bundle_no,
        first_work_ordinal: task.file.descriptor.work_ordinal,
        last_work_ordinal: task.file.descriptor.work_ordinal,
        last_relative_path: task.file.descriptor.relative_path.clone(),
        entries: vec![task.file.descriptor.clone()],
        raw_payload_bytes: task.file.descriptor.size_at_open,
    }
}

pub(super) fn is_retryable_stream_read_error(error: &anyhow::Error) -> bool {
    // Сетевой EOF требует новой сессии, а не повторного чтения или пропуска файла.
    if crate::network::is_retryable_error(error) {
        return false;
    }
    error
        .chain()
        .filter_map(|source| source.downcast_ref::<io::Error>())
        .any(|io_error| io_error.kind() == ErrorKind::UnexpectedEof)
}

pub(super) fn estimate_skipped_entry_wire_bytes(entry: &SkippedEntry) -> usize {
    std::mem::size_of::<u64>()
        + std::mem::size_of::<u8>()
        + std::mem::size_of::<u32>()
        + entry.relative_path.len()
}
