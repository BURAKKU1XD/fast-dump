use std::collections::VecDeque;
use std::io::{Read, Write};
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::{Context, Result, bail};

use super::{control_io_error, receive_control_message};
use crate::network::protocol::wire::{self, BundleAbort, BundleAbortAck, BundleAck, Message};
use crate::profile::SenderProfile;
use crate::progress::{self, PipelineStage, PipelineState};
use crate::runtime::SessionControl;

pub(super) fn send_bundle_abort_profiled<T>(
    stream: &mut T,
    bundle_no: u64,
    attempt_no: u32,
    profile: &mut SenderProfile,
    inflight_bundles: &mut InflightBundles,
) -> Result<BundleAbortAck>
where
    T: Read + Write,
{
    crate::sender::check_control(inflight_bundles.control.as_ref())?;
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
        crate::sender::check_control(inflight_bundles.control.as_ref())?;
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

pub(super) fn drain_inflight_bundles<T>(
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

pub(super) fn read_next_bundle_ack<T>(
    stream: &mut T,
    profile: &mut SenderProfile,
    inflight_bundles: &mut InflightBundles,
) -> Result<BundleAck>
where
    T: Read + Write,
{
    crate::sender::check_control(inflight_bundles.control.as_ref())?;
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

pub(super) struct InflightBundle {
    pub(super) bundle_no: u64,
    pub(super) entries: u64,
    pub(super) last_work_ordinal: u64,
    pub(super) compressed_bytes: u64,
}

pub(super) struct InflightBundles {
    // Храним только счётчики четырёх bundles, без отправленного payload.
    pub(super) pending: VecDeque<InflightBundle>,
    pub(super) archive_len: u64,
    pub(super) control: Option<SessionControl>,
}

impl InflightBundles {
    pub(super) fn sent(&mut self, bundle: InflightBundle) {
        if let Some(progress) = self.control.as_ref().and_then(SessionControl::progress) {
            progress
                .sent_entries
                .fetch_add(bundle.entries, Ordering::Relaxed);
            progress.bundles.fetch_add(1, Ordering::Relaxed);
        }
        self.pending.push_back(bundle);
    }
}

pub(super) fn apply_cumulative_bundle_ack(
    inflight: &mut InflightBundles,
    ack: &BundleAck,
) -> Result<()> {
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
