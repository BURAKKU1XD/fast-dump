mod events;
mod pending;
mod readers;

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use super::session::ReceiverSession;
use crate::checksum::Xxh3;
use crate::network::network_io;
use crate::network::protocol::ResumeState;
use crate::network::protocol::wire::{
    self, BundleAbort, BundleAbortAck, BundleCommit, Finish, Message, ResumeQuery, SkipAck,
};
use crate::network::transport::CloneableStream;
use crate::profile::ReceiverProfile;
use crate::progress::{self, Phase, PipelineStage, PipelineState};
use crate::runtime::SessionControl;
use events::{ReceiveEvent, receive_event_before_commit, wait_for_pending_bundle_data};
use pending::{
    PendingBundle, abort_bundle, apply_data_chunk, apply_orphan_data, drain_ready_commits,
};
use readers::{PendingDataChunk, spawn_control_reader_observed, spawn_data_reader_observed};

#[derive(Debug, Clone)]
pub struct ReceiveReport {
    pub final_archive_path: PathBuf,
    pub issue_report_path: Option<PathBuf>,
    pub resume_state: ResumeState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AttemptKey {
    bundle_no: u64,
    attempt_no: u32,
}

impl AttemptKey {
    const fn new(bundle_no: u64, attempt_no: u32) -> Self {
        Self {
            bundle_no,
            attempt_no,
        }
    }
}

pub fn receive_dump<TControl, TData>(
    control_stream: TControl,
    data_stream: TData,
    data_dir: PathBuf,
    dump_id: &str,
) -> Result<ReceiveReport>
where
    TControl: CloneableStream,
    TData: CloneableStream,
{
    Ok(receive_dump_profile(control_stream, data_stream, data_dir, dump_id)?.0)
}

pub fn receive_dump_profile<TControl, TData>(
    control_stream: TControl,
    data_stream: TData,
    data_dir: PathBuf,
    dump_id: &str,
) -> Result<(ReceiveReport, ReceiverProfile)>
where
    TControl: CloneableStream,
    TData: CloneableStream,
{
    receive_dump_profile_inner(control_stream, data_stream, data_dir, dump_id, None)
}

pub(crate) fn receive_dump_profile_controlled<TControl, TData>(
    control_stream: TControl,
    data_stream: TData,
    data_dir: PathBuf,
    dump_id: &str,
    control: &SessionControl,
) -> Result<(ReceiveReport, ReceiverProfile)>
where
    TControl: CloneableStream,
    TData: CloneableStream,
{
    let result = receive_dump_profile_inner(
        control_stream,
        data_stream,
        data_dir,
        dump_id,
        Some(control),
    );
    if result.is_err() {
        control.check()?;
    } else {
        progress::set_stage(Some(control), PipelineStage::Storage, PipelineState::Done);
    }
    result
}

fn receive_dump_profile_inner<TControl, TData>(
    mut control_stream: TControl,
    data_stream: TData,
    data_dir: PathBuf,
    dump_id: &str,
    control: Option<&SessionControl>,
) -> Result<(ReceiveReport, ReceiverProfile)>
where
    TControl: CloneableStream,
    TData: CloneableStream,
{
    let total_started = Instant::now();
    let recovery_started = Instant::now();
    if let Some(progress) = control.and_then(SessionControl::progress) {
        progress.set_phase(Phase::Preparing);
    }
    let mut receiver = match ReceiverSession::open_controlled(data_dir, dump_id, 3, control) {
        Ok(receiver) => receiver,
        Err(error) => {
            progress::set_stage(
                control,
                PipelineStage::Storage,
                if control.is_some_and(SessionControl::is_cancelled) {
                    PipelineState::Stopped
                } else {
                    PipelineState::Failed
                },
            );
            if !control.is_some_and(SessionControl::is_cancelled) {
                // Ошибка storage должна дойти до peer как Error, а не выглядеть
                // как временный EOF. Не заменяем её неудачной отправкой ответа.
                let _ = wire::send_error(&mut control_stream, &format!("{error:#}"));
            }
            return Err(error);
        }
    };
    let mut profile = ReceiverProfile::default();
    profile.record_recovery(
        receiver.resume_state().archive_len,
        recovery_started.elapsed(),
    );
    respond_resume_query(
        &mut control_stream,
        dump_id,
        receiver.resume_state().clone(),
        control,
    )?;
    if let Some(progress) = &receiver.progress {
        progress.set_phase(Phase::Receiving);
    }

    let control_rx = spawn_control_reader_observed(
        control_stream.try_clone_stream()?,
        receiver.progress.clone(),
        control.cloned(),
    )?;
    let data_rx =
        spawn_data_reader_observed(data_stream, receiver.progress.clone(), control.cloned())?;

    let mut pending_bundles = VecDeque::<PendingBundle>::new();
    let mut orphan_data = HashMap::<AttemptKey, Vec<PendingDataChunk>>::new();
    let mut aborted_attempts = HashSet::<AttemptKey>::new();
    let mut consecutive_controls = 0;

    loop {
        if let Some(control) = control {
            control.check()?;
        }
        let wait = if !orphan_data.is_empty() {
            PipelineState::WaitingPlan
        } else if pending_bundles.iter().any(|pending| !pending.data_finished) {
            PipelineState::WaitingData
        } else if !pending_bundles.is_empty() {
            PipelineState::WaitingControl
        } else {
            PipelineState::WaitingEvent
        };
        let waiting = progress::stage(control, PipelineStage::Storage, wait);
        let event = receive_event_before_commit(
            &control_rx,
            &data_rx,
            &data_rx.failed,
            !orphan_data.is_empty(),
            &mut consecutive_controls,
            || {
                drain_ready_commits(
                    &mut control_stream,
                    &mut receiver,
                    &mut pending_bundles,
                    &mut profile,
                    false,
                )
            },
        )?;
        drop(waiting);
        if let Some(control) = control {
            control.check()?;
        }
        match event {
            ReceiveEvent::Control(event) => match event {
                Message::SkipEntries(chunk) => {
                    if !pending_bundles.is_empty() {
                        protocol_error(
                            &mut control_stream,
                            "unexpected v4 skip entries while bundle payload is in progress",
                        );
                        bail!("unexpected v4 skip entries while bundle payload is in progress");
                    }
                    let skip_duration = receiver.record_terminal_skips_profiled(&chunk.entries)?;
                    profile.record_skip_catalog(chunk.entries.len() as u64, skip_duration);
                    let ack_flush_started = Instant::now();
                    let _sending =
                        progress::stage(control, PipelineStage::Storage, PipelineState::Sending);
                    wire::write_message(
                        &mut control_stream,
                        &Message::SkipAck(SkipAck {
                            total_terminal_skips: receiver.resume_state().terminal_skips.len()
                                as u64,
                        }),
                    )?;
                    control_stream
                        .flush()
                        .map_err(network_io)
                        .context("failed to flush v4 skip ack")?;
                    profile.record_bundle_ack_flush(ack_flush_started.elapsed());
                }
                Message::BundlePlan(plan) => {
                    let key = AttemptKey::new(plan.descriptor.bundle_no, plan.attempt_no);
                    if pending_bundles.iter().any(|pending| pending.key == key) {
                        protocol_error(
                            &mut control_stream,
                            "duplicate v4 bundle attempt in control stream",
                        );
                        bail!("duplicate v4 bundle attempt in control stream");
                    }
                    pending_bundles.push_back(PendingBundle {
                        key,
                        descriptor: plan.descriptor,
                        bytes_received: 0,
                        archive_len_before_append: None,
                        archive_len_after_append: None,
                        checksum: Xxh3::new(),
                        write_duration: Duration::ZERO,
                        periodic_sync_duration: Duration::ZERO,
                        periodic_sync_count: 0,
                        commit: None,
                        data_finished: false,
                    });
                    apply_orphan_data(
                        &mut receiver,
                        &mut pending_bundles,
                        &mut orphan_data,
                        &aborted_attempts,
                        key,
                    )?;
                }
                Message::BundleCommit(BundleCommit {
                    attempt_no,
                    descriptor,
                }) => {
                    let key = AttemptKey::new(descriptor.bundle_no, attempt_no);
                    let Some(pending) = pending_bundles
                        .iter_mut()
                        .find(|pending| pending.key == key)
                    else {
                        protocol_error(
                            &mut control_stream,
                            "unexpected v4 bundle commit without bundle plan",
                        );
                        bail!("unexpected v4 bundle commit without bundle plan");
                    };
                    pending.commit = Some(descriptor);
                }
                Message::BundleAbort(BundleAbort {
                    bundle_no,
                    attempt_no,
                }) => {
                    if let Some(progress) = &receiver.progress {
                        progress.retries.fetch_add(1, Ordering::Relaxed);
                    }
                    let key = AttemptKey::new(bundle_no, attempt_no);
                    abort_bundle(
                        &mut receiver,
                        &mut pending_bundles,
                        &mut orphan_data,
                        &mut aborted_attempts,
                        key,
                    )?;

                    let ack_flush_started = Instant::now();
                    let _sending =
                        progress::stage(control, PipelineStage::Storage, PipelineState::Sending);
                    wire::write_message(
                        &mut control_stream,
                        &Message::BundleAbortAck(BundleAbortAck {
                            bundle_no,
                            attempt_no,
                            archive_len: receiver.resume_state().archive_len,
                        }),
                    )?;
                    control_stream
                        .flush()
                        .map_err(network_io)
                        .context("failed to flush v4 bundle abort ack")?;
                    profile.record_bundle_ack_flush(ack_flush_started.elapsed());
                }
                Message::Finish(Finish { .. }) => {
                    if !orphan_data.is_empty() {
                        protocol_error(
                            &mut control_stream,
                            "unexpected orphaned v4 data before finish",
                        );
                        bail!("unexpected orphaned v4 data before finish");
                    }
                    if pending_bundles
                        .iter()
                        .any(|pending| pending.commit.is_none())
                    {
                        protocol_error(
                            &mut control_stream,
                            "unexpected v4 finish before bundle commit",
                        );
                        bail!("unexpected v4 finish before bundle commit");
                    }
                    wait_for_pending_bundle_data(
                        &control_rx,
                        &data_rx,
                        &mut receiver,
                        &mut pending_bundles,
                        &mut orphan_data,
                        &aborted_attempts,
                    )?;
                    if !orphan_data.is_empty() {
                        protocol_error(
                            &mut control_stream,
                            "unexpected orphaned v4 data after finish",
                        );
                        bail!("unexpected orphaned v4 data after finish");
                    }
                    drain_ready_commits(
                        &mut control_stream,
                        &mut receiver,
                        &mut pending_bundles,
                        &mut profile,
                        true,
                    )?;
                    if !pending_bundles.is_empty() {
                        protocol_error(
                            &mut control_stream,
                            "unexpected incomplete v4 bundle state at finish",
                        );
                        bail!("unexpected incomplete v4 bundle state at finish");
                    }

                    if let Some(control) = control {
                        control.check()?;
                    }
                    if let Some(progress) = &receiver.progress {
                        progress.set_phase(Phase::Finalizing);
                    }
                    let storage_progress = receiver.progress.clone();
                    let (finalize_report, finalize_profile) =
                        receiver.finalize_profiled().inspect_err(|_| {
                            if let Some(progress) = &storage_progress {
                                progress.set_stage(PipelineStage::Storage, PipelineState::Failed);
                            }
                        })?;
                    profile.record_finalize(
                        finalize_profile.total_duration,
                        finalize_profile.archive_finalize_duration,
                        finalize_profile.issue_finalize_duration,
                    );
                    let finish_ack_started = Instant::now();
                    let _sending =
                        progress::stage(control, PipelineStage::Storage, PipelineState::Sending);
                    wire::write_message(&mut control_stream, &Message::FinishAck)?;
                    control_stream
                        .flush()
                        .map_err(network_io)
                        .context("failed to flush v4 finish ack")?;
                    profile.record_finish_ack_flush(finish_ack_started.elapsed());
                    profile.record_total(total_started.elapsed());
                    return Ok((
                        ReceiveReport {
                            final_archive_path: finalize_report.final_archive_path,
                            resume_state: finalize_report.resume_state,
                            issue_report_path: finalize_report.issue_report_path,
                        },
                        profile,
                    ));
                }
                Message::Error(error) => bail!("peer reported v4 error: {}", error.message),
                unexpected => {
                    protocol_error(
                        &mut control_stream,
                        &format!(
                            "unexpected v4 message in receiver loop: {:?}",
                            unexpected.kind()
                        ),
                    );
                    bail!(
                        "unexpected v4 message in receiver loop: {:?}",
                        unexpected.kind()
                    );
                }
            },
            ReceiveEvent::Data(data) => {
                apply_data_chunk(
                    &mut receiver,
                    &mut pending_bundles,
                    &mut orphan_data,
                    &aborted_attempts,
                    data,
                )?;
            }
        }
    }
}

fn respond_resume_query<T>(
    stream: &mut T,
    dump_id: &str,
    state: ResumeState,
    control: Option<&SessionControl>,
) -> Result<()>
where
    T: Read + Write,
{
    let _receiving = progress::stage(control, PipelineStage::Control, PipelineState::Receiving);
    match wire::read_message(stream)? {
        Message::ResumeQuery(ResumeQuery {
            dump_id: requested_dump_id,
        }) => {
            if requested_dump_id != dump_id {
                protocol_error(
                    stream,
                    &format!(
                        "v4 resume query dump id mismatch: expected {} got {}",
                        dump_id, requested_dump_id
                    ),
                );
                bail!(
                    "v4 resume query dump id mismatch: expected {} got {}",
                    dump_id,
                    requested_dump_id
                );
            }

            let _sending = progress::stage(control, PipelineStage::Control, PipelineState::Sending);
            wire::write_message(stream, &Message::ResumeState(state))?;
            stream
                .flush()
                .map_err(network_io)
                .context("failed to flush v4 resume state")?;
            Ok(())
        }
        Message::Error(error) => bail!(
            "peer reported v4 error before resume query: {}",
            error.message
        ),
        unexpected => {
            protocol_error(stream, "expected v4 resume query before session flow");
            bail!(
                "unexpected v4 message before resume query: {:?}",
                unexpected.kind()
            );
        }
    }
}

fn protocol_error<T>(stream: &mut T, message: &str)
where
    T: Write,
{
    // Причина отказа уже известна; ошибка ответа не должна разрешать reconnect.
    let _ = wire::send_error(stream, message);
}

#[cfg(test)]
#[path = "network_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod lifecycle_tests;
