use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::checksum::Xxh3;

use super::readers::PendingDataChunk;
use super::{AttemptKey, protocol_error};
use crate::network::network_io;
use crate::network::protocol::wire::{self, BundleAck, Message};
use crate::network::protocol::{BundleCommitDescriptor, BundleDescriptor, BundlePlanDescriptor};
use crate::profile::ReceiverProfile;
use crate::progress::{PipelineStage, PipelineState};
use crate::receiver::session::ReceiverSession;

const COMMIT_GROUP_MAX_BUNDLES: usize = 4;
const COMMIT_GROUP_MAX_BYTES: u64 = 1024 * 1024 * 1024;

pub(super) struct PendingBundle {
    pub(super) key: AttemptKey,
    pub(super) descriptor: BundlePlanDescriptor,
    pub(super) bytes_received: u64,
    pub(super) archive_len_before_append: Option<u64>,
    pub(super) archive_len_after_append: Option<u64>,
    pub(super) checksum: Xxh3,
    pub(super) write_duration: Duration,
    pub(super) periodic_sync_duration: Duration,
    pub(super) periodic_sync_count: u64,
    pub(super) commit: Option<BundleCommitDescriptor>,
    pub(super) data_finished: bool,
}

pub(super) struct ReadyBundle {
    pub(super) descriptor: BundleDescriptor,
    pub(super) archive_len: u64,
    pub(super) write_duration: Duration,
}

pub(super) fn apply_orphan_data(
    receiver: &mut ReceiverSession,
    pending_bundles: &mut VecDeque<PendingBundle>,
    orphan_data: &mut HashMap<AttemptKey, Vec<PendingDataChunk>>,
    aborted_attempts: &HashSet<AttemptKey>,
    key: AttemptKey,
) -> Result<()> {
    let Some(chunks) = orphan_data.remove(&key) else {
        return Ok(());
    };
    for chunk in chunks {
        apply_data_chunk(
            receiver,
            pending_bundles,
            orphan_data,
            aborted_attempts,
            chunk,
        )?;
    }
    Ok(())
}

pub(super) fn apply_data_chunk(
    receiver: &mut ReceiverSession,
    pending_bundles: &mut VecDeque<PendingBundle>,
    orphan_data: &mut HashMap<AttemptKey, Vec<PendingDataChunk>>,
    aborted_attempts: &HashSet<AttemptKey>,
    chunk: PendingDataChunk,
) -> Result<()> {
    if aborted_attempts.contains(&chunk.key) {
        return Ok(());
    }
    let Some(index) = pending_bundles
        .iter()
        .position(|pending| pending.key == chunk.key)
    else {
        orphan_data.entry(chunk.key).or_default().push(chunk);
        return Ok(());
    };
    if pending_bundles
        .iter()
        .take(index)
        .any(|pending| !pending.data_finished)
    {
        bail!("v4 bundle data arrived before preceding bundle completed");
    }
    let pending = &mut pending_bundles[index];
    if pending.data_finished {
        bail!("unexpected v4 bundle data after final chunk");
    }
    let _writing = receiver
        .progress
        .as_ref()
        .map(|progress| progress.stage(PipelineStage::Storage, PipelineState::Writing));
    let next_len = pending
        .bytes_received
        .checked_add(chunk.bytes.len() as u64)
        .context("v4 pending bundle payload is too large")?;
    let write_started = Instant::now();
    // Control может обогнать data: граница возникает только при фактической записи.
    pending
        .archive_len_before_append
        .get_or_insert(receiver.current_archive_len());
    let periodic_sync_duration = receiver.append_bundle_chunk(&chunk.bytes)?;
    if let Some(progress) = &receiver.progress {
        progress
            .transferred_bytes
            .fetch_add(chunk.bytes.len() as u64, Ordering::Relaxed);
    }
    pending.write_duration += write_started.elapsed();
    if let Some(duration) = periodic_sync_duration {
        pending.periodic_sync_duration += duration;
        pending.periodic_sync_count += 1;
    }
    pending.checksum.update(&chunk.bytes);
    pending.bytes_received = next_len;
    if chunk.is_last {
        pending.data_finished = true;
        pending.archive_len_after_append = Some(receiver.current_archive_len());
        if let Some(progress) = &receiver.progress {
            progress
                .sent_entries
                .fetch_add(pending.descriptor.entries.len() as u64, Ordering::Relaxed);
        }
    }
    Ok(())
}

pub(super) fn abort_bundle(
    receiver: &mut ReceiverSession,
    pending_bundles: &mut VecDeque<PendingBundle>,
    orphan_data: &mut HashMap<AttemptKey, Vec<PendingDataChunk>>,
    aborted_attempts: &mut HashSet<AttemptKey>,
    key: AttemptKey,
) -> Result<()> {
    let index = pending_bundles
        .iter()
        .position(|pending| pending.key == key)
        .context("unexpected v4 bundle abort without bundle plan")?;
    if index + 1 != pending_bundles.len() {
        bail!("cannot abort v4 bundle with later attempts in progress");
    }
    let pending = pending_bundles
        .remove(index)
        .context("pending v4 bundle disappeared during abort handling")?;
    // Если data ещё не пришли, предыдущие бандлы должны остаться нетронутыми.
    if let Some(start) = pending.archive_len_before_append {
        receiver.rollback_staged_bundle(start)?;
    }
    if pending.data_finished
        && let Some(progress) = &receiver.progress
    {
        // Завершённый payload ещё не означает commit: abort снимает его записи,
        // а объём записанных байтов остаётся монотонным при повторной попытке.
        progress
            .sent_entries
            .fetch_sub(pending.descriptor.entries.len() as u64, Ordering::Relaxed);
    }
    aborted_attempts.insert(key);
    orphan_data.remove(&key);
    Ok(())
}

pub(super) fn drain_ready_commits<T>(
    control_stream: &mut T,
    receiver: &mut ReceiverSession,
    pending_bundles: &mut VecDeque<PendingBundle>,
    profile: &mut ReceiverProfile,
    force: bool,
) -> Result<()>
where
    T: Write,
{
    let mut ready_prefix = 0_usize;
    let mut ready_prefix_bytes = 0_u64;
    for pending in pending_bundles.iter() {
        let Some(commit) = pending.commit else {
            break;
        };
        if !pending.data_finished {
            break;
        }
        ready_prefix += 1;
        ready_prefix_bytes = ready_prefix_bytes.saturating_add(commit.compressed_payload_bytes);
        if !force
            && (ready_prefix >= COMMIT_GROUP_MAX_BUNDLES
                || ready_prefix_bytes >= COMMIT_GROUP_MAX_BYTES)
        {
            break;
        }
    }
    let has_blocked_suffix = ready_prefix < pending_bundles.len();

    if ready_prefix == 0 {
        return Ok(());
    }
    if !force
        && ready_prefix < COMMIT_GROUP_MAX_BUNDLES
        && ready_prefix_bytes < COMMIT_GROUP_MAX_BYTES
        && has_blocked_suffix
    {
        return Ok(());
    }

    let mut ready = Vec::<ReadyBundle>::new();
    let mut group_bytes = 0_u64;
    let mut write_duration = Duration::ZERO;
    let mut periodic_sync_duration = Duration::ZERO;
    let mut periodic_sync_count = 0_u64;

    for _ in 0..ready_prefix {
        let commit = pending_bundles
            .front()
            .and_then(|pending| pending.commit)
            .context("ready v4 pending bundle lost its commit descriptor")?;
        let pending = pending_bundles
            .pop_front()
            .context("ready v4 pending bundle disappeared from queue")?;
        let bundle_write_duration = pending.write_duration;
        periodic_sync_duration += pending.periodic_sync_duration;
        periodic_sync_count += pending.periodic_sync_count;
        let archive_len = pending
            .archive_len_after_append
            .context("ready v4 bundle is missing its archive boundary")?;
        let descriptor = validate_ready_bundle(control_stream, receiver, pending, commit)?;
        group_bytes = group_bytes.saturating_add(descriptor.compressed_payload_bytes);
        write_duration += bundle_write_duration;
        ready.push(ReadyBundle {
            descriptor,
            archive_len,
            write_duration: bundle_write_duration,
        });
    }

    let sync_started = Instant::now();
    let archive_synced = receiver.sync_archive()?;
    let archive_sync_duration = sync_started.elapsed();

    let wal_started = Instant::now();
    let mut records = Vec::with_capacity(ready.len());
    for bundle in &ready {
        records.push(receiver.buffered_commit_record(&bundle.descriptor, bundle.archive_len)?);
    }
    receiver.sync_wal()?;
    let wal_duration = wal_started.elapsed();

    for record in records {
        receiver.apply_commit_record(record);
    }
    if let Some(progress) = &receiver.progress {
        progress
            .durable_bytes
            .store(receiver.resume_state().archive_len, Ordering::Relaxed);
        progress
            .bundles
            .fetch_add(ready.len() as u64, Ordering::Relaxed);
        let entries = ready
            .iter()
            .map(|bundle| bundle.descriptor.entries.len() as u64)
            .sum::<u64>();
        progress
            .committed_entries
            .fetch_add(entries, Ordering::Relaxed);
    }

    let commit_duration = ready
        .iter()
        .fold(Duration::ZERO, |acc, bundle| acc + bundle.write_duration)
        + archive_sync_duration
        + wal_duration;
    profile.record_bundle_write(group_bytes, write_duration);
    profile.record_bundle_write_details(
        group_bytes,
        write_duration.saturating_sub(periodic_sync_duration),
        periodic_sync_duration,
        periodic_sync_count,
    );
    profile.record_bundle_sync(archive_sync_duration);
    if !archive_synced {
        profile.bundle_sync_skipped = profile.bundle_sync_skipped.saturating_add(1);
    }
    profile.record_wal_append(group_bytes, wal_duration);
    profile.record_bundle_commit_batch(group_bytes, commit_duration, ready.len() as u64);

    let ack_flush_started = Instant::now();
    let _sending = receiver
        .progress
        .as_ref()
        .map(|progress| progress.stage(PipelineStage::Storage, PipelineState::Sending));
    wire::write_message(
        control_stream,
        &Message::BundleAck(BundleAck {
            bundle_no: receiver.resume_state().last_committed_bundle_no,
            last_work_ordinal: receiver.resume_state().last_committed_work_ordinal,
            archive_len: receiver.resume_state().archive_len,
        }),
    )?;
    control_stream
        .flush()
        .map_err(network_io)
        .context("failed to flush v4 bundle ack")?;
    profile.record_bundle_ack_flush(ack_flush_started.elapsed());
    Ok(())
}

pub(super) fn validate_ready_bundle<T>(
    control_stream: &mut T,
    receiver: &mut ReceiverSession,
    pending: PendingBundle,
    commit: BundleCommitDescriptor,
) -> Result<BundleDescriptor>
where
    T: Write,
{
    if pending.descriptor.bundle_no != commit.bundle_no {
        receiver.rollback_staged_bundle(
            pending
                .archive_len_before_append
                .context("ready v4 bundle is missing its archive boundary")?,
        )?;
        protocol_error(
            control_stream,
            "v4 bundle commit number does not match active plan",
        );
        bail!("v4 bundle commit number does not match active plan");
    }
    if pending.bytes_received != commit.compressed_payload_bytes {
        receiver.rollback_staged_bundle(
            pending
                .archive_len_before_append
                .context("ready v4 bundle is missing its archive boundary")?,
        )?;
        protocol_error(
            control_stream,
            "v4 bundle payload length does not match descriptor",
        );
        bail!("v4 bundle payload length does not match descriptor");
    }
    let checksum = pending.checksum.digest();
    if checksum != commit.compressed_xxh3_64 {
        receiver.rollback_staged_bundle(
            pending
                .archive_len_before_append
                .context("ready v4 bundle is missing its archive boundary")?,
        )?;
        protocol_error(control_stream, "v4 bundle payload checksum mismatch");
        bail!("v4 bundle payload checksum mismatch");
    }
    Ok(pending.descriptor.into_bundle_descriptor(commit))
}
