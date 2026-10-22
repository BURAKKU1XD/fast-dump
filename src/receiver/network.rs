use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use crossbeam_channel::{Receiver, bounded, select};
use xxhash_rust::xxh3::Xxh3Default;

use crate::network::protocol::wire::{
    self, BundleAbort, BundleAbortAck, BundleAck, BundleCommit, BundleData, Finish, Message,
    ResumeQuery, SkipAck,
};
use crate::network::protocol::{
    BundleCommitDescriptor, BundleDescriptor, BundlePlanDescriptor, ResumeState,
};
use crate::network::transport::CloneableStream;
use crate::profile::ReceiverProfile;

use super::session::ReceiverSession;

const COMMIT_GROUP_MAX_BUNDLES: usize = 4;
const COMMIT_GROUP_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const READER_QUEUE_CAPACITY: usize = 8;

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

struct PendingBundle {
    key: AttemptKey,
    descriptor: BundlePlanDescriptor,
    bytes_received: u64,
    archive_len_before_append: Option<u64>,
    archive_len_after_append: Option<u64>,
    checksum: Xxh3Default,
    write_duration: Duration,
    commit: Option<BundleCommitDescriptor>,
    data_finished: bool,
}

#[derive(Debug, Clone)]
struct PendingDataChunk {
    key: AttemptKey,
    is_last: bool,
    bytes: Vec<u8>,
}

struct ReadyBundle {
    descriptor: BundleDescriptor,
    archive_len: u64,
    write_duration: Duration,
}

enum ReceiveEvent {
    Control(Message),
    Data(BundleData),
}

fn receive_next_event(
    control_rx: &Receiver<Result<Message>>,
    data_rx: &Receiver<Result<BundleData>>,
    wait_for_plan: bool,
) -> Result<ReceiveEvent> {
    // Один data-пакет может обогнать план. Дальше читаем только control, чтобы
    // ограниченная очередь и TCP удерживали оставшийся payload вне orphan_data.
    if wait_for_plan {
        return control_rx
            .recv()
            .context("v4 control reader thread terminated unexpectedly")?
            .map(ReceiveEvent::Control);
    }
    select! {
        recv(control_rx) -> event => {
            event.context("v4 control reader thread terminated unexpectedly")?
                .map(ReceiveEvent::Control)
        }
        recv(data_rx) -> event => {
            event.context("v4 data reader thread terminated unexpectedly")?
                .map(ReceiveEvent::Data)
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
    TData: Read + Send + 'static,
{
    Ok(receive_dump_profile(control_stream, data_stream, data_dir, dump_id)?.0)
}

pub fn receive_dump_profile<TControl, TData>(
    mut control_stream: TControl,
    data_stream: TData,
    data_dir: PathBuf,
    dump_id: &str,
) -> Result<(ReceiveReport, ReceiverProfile)>
where
    TControl: CloneableStream,
    TData: Read + Send + 'static,
{
    let total_started = Instant::now();
    let recovery_started = Instant::now();
    let mut receiver = ReceiverSession::open(data_dir, dump_id, 3)?;
    let mut profile = ReceiverProfile::default();
    profile.record_recovery(
        receiver.resume_state().archive_len,
        recovery_started.elapsed(),
    );
    respond_resume_query(
        &mut control_stream,
        dump_id,
        receiver.resume_state().clone(),
    )?;

    let control_rx = spawn_control_reader(control_stream.try_clone_stream()?);
    let data_rx = spawn_data_reader(data_stream);

    let mut pending_bundles = VecDeque::<PendingBundle>::new();
    let mut orphan_data = HashMap::<AttemptKey, Vec<PendingDataChunk>>::new();
    let mut aborted_attempts = HashSet::<AttemptKey>::new();

    loop {
        drain_ready_commits(
            &mut control_stream,
            &mut receiver,
            &mut pending_bundles,
            &mut profile,
            false,
        )?;

        match receive_next_event(&control_rx, &data_rx, !orphan_data.is_empty())? {
            ReceiveEvent::Control(event) => match event {
                Message::SkipEntries(chunk) => {
                    if !pending_bundles.is_empty() {
                        protocol_error(
                            &mut control_stream,
                            "unexpected v4 skip entries while bundle payload is in progress",
                        )?;
                        bail!("unexpected v4 skip entries while bundle payload is in progress");
                    }
                    let skip_duration = receiver.record_terminal_skips_profiled(&chunk.entries)?;
                    profile.record_skip_catalog(chunk.entries.len() as u64, skip_duration);
                    let ack_flush_started = Instant::now();
                    wire::write_message(
                        &mut control_stream,
                        &Message::SkipAck(SkipAck {
                            total_terminal_skips: receiver.resume_state().terminal_skips.len()
                                as u64,
                        }),
                    )?;
                    control_stream
                        .flush()
                        .context("failed to flush v4 skip ack")?;
                    profile.record_bundle_ack_flush(ack_flush_started.elapsed());
                }
                Message::BundlePlan(plan) => {
                    let key = AttemptKey::new(plan.descriptor.bundle_no, plan.attempt_no);
                    if pending_bundles.iter().any(|pending| pending.key == key) {
                        protocol_error(
                            &mut control_stream,
                            "duplicate v4 bundle attempt in control stream",
                        )?;
                        bail!("duplicate v4 bundle attempt in control stream");
                    }
                    pending_bundles.push_back(PendingBundle {
                        key,
                        descriptor: plan.descriptor,
                        bytes_received: 0,
                        archive_len_before_append: None,
                        archive_len_after_append: None,
                        checksum: Xxh3Default::new(),
                        write_duration: Duration::ZERO,
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
                        )?;
                        bail!("unexpected v4 bundle commit without bundle plan");
                    };
                    pending.commit = Some(descriptor);
                }
                Message::BundleAbort(BundleAbort {
                    bundle_no,
                    attempt_no,
                }) => {
                    let key = AttemptKey::new(bundle_no, attempt_no);
                    abort_bundle(
                        &mut receiver,
                        &mut pending_bundles,
                        &mut orphan_data,
                        &mut aborted_attempts,
                        key,
                    )?;

                    let ack_flush_started = Instant::now();
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
                        .context("failed to flush v4 bundle abort ack")?;
                    profile.record_bundle_ack_flush(ack_flush_started.elapsed());
                }
                Message::Finish(Finish { .. }) => {
                    if !orphan_data.is_empty() {
                        protocol_error(
                            &mut control_stream,
                            "unexpected orphaned v4 data before finish",
                        )?;
                        bail!("unexpected orphaned v4 data before finish");
                    }
                    if pending_bundles
                        .iter()
                        .any(|pending| pending.commit.is_none())
                    {
                        protocol_error(
                            &mut control_stream,
                            "unexpected v4 finish before bundle commit",
                        )?;
                        bail!("unexpected v4 finish before bundle commit");
                    }
                    wait_for_pending_bundle_data(
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
                        )?;
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
                        )?;
                        bail!("unexpected incomplete v4 bundle state at finish");
                    }

                    let (finalize_report, finalize_profile) = receiver.finalize_profiled()?;
                    profile.record_finalize(
                        finalize_profile.total_duration,
                        finalize_profile.archive_finalize_duration,
                        finalize_profile.issue_finalize_duration,
                    );
                    let finish_ack_started = Instant::now();
                    wire::write_message(&mut control_stream, &Message::FinishAck)?;
                    control_stream
                        .flush()
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
                    )?;
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
                    PendingDataChunk {
                        key: AttemptKey::new(data.bundle_no, data.attempt_no),
                        is_last: data.is_last,
                        bytes: data.bytes,
                    },
                )?;
            }
        }
    }
}

fn spawn_control_reader<T>(stream: T) -> Receiver<Result<Message>>
where
    T: Read + Send + 'static,
{
    let (tx, rx) = bounded::<Result<Message>>(READER_QUEUE_CAPACITY);
    thread::spawn(move || {
        let mut stream = stream;
        loop {
            let item = wire::read_message(&mut stream);
            if tx.send(item).is_err() {
                return;
            }
        }
    });
    rx
}

fn spawn_data_reader<T>(stream: T) -> Receiver<Result<BundleData>>
where
    T: Read + Send + 'static,
{
    let (tx, rx) = bounded::<Result<BundleData>>(READER_QUEUE_CAPACITY);
    thread::spawn(move || {
        let mut stream = stream;
        loop {
            let item = match wire::read_message(&mut stream) {
                Ok(Message::BundleData(data)) => Ok(data),
                Ok(Message::Error(error)) => Err(anyhow::anyhow!(
                    "peer reported v4 data-channel error: {}",
                    error.message
                )),
                Ok(other) => Err(anyhow::anyhow!(
                    "unexpected v4 message on data channel: {:?}",
                    other.kind()
                )),
                Err(error) => Err(error),
            };
            if tx.send(item).is_err() {
                break;
            }
        }
    });
    rx
}

fn apply_orphan_data(
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

fn apply_data_chunk(
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
    let next_len = pending
        .bytes_received
        .checked_add(chunk.bytes.len() as u64)
        .context("v4 pending bundle payload is too large")?;
    let write_started = Instant::now();
    // Control может обогнать data: граница возникает только при фактической записи.
    pending
        .archive_len_before_append
        .get_or_insert(receiver.current_archive_len());
    receiver.append_bundle_chunk(&chunk.bytes)?;
    pending.write_duration += write_started.elapsed();
    pending.checksum.update(&chunk.bytes);
    pending.bytes_received = next_len;
    if chunk.is_last {
        pending.data_finished = true;
        pending.archive_len_after_append = Some(receiver.current_archive_len());
    }
    Ok(())
}

fn abort_bundle(
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
    aborted_attempts.insert(key);
    orphan_data.remove(&key);
    Ok(())
}

fn wait_for_pending_bundle_data(
    data_rx: &Receiver<Result<BundleData>>,
    receiver: &mut ReceiverSession,
    pending_bundles: &mut VecDeque<PendingBundle>,
    orphan_data: &mut HashMap<AttemptKey, Vec<PendingDataChunk>>,
    aborted_attempts: &HashSet<AttemptKey>,
) -> Result<()> {
    while pending_bundles.iter().any(|pending| !pending.data_finished) {
        let data = data_rx
            .recv()
            .context("v4 data reader channel closed while waiting for pending bundle data")??;
        apply_data_chunk(
            receiver,
            pending_bundles,
            orphan_data,
            aborted_attempts,
            PendingDataChunk {
                key: AttemptKey::new(data.bundle_no, data.attempt_no),
                is_last: data.is_last,
                bytes: data.bytes,
            },
        )?;
    }
    Ok(())
}

fn drain_ready_commits<T>(
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

    for _ in 0..ready_prefix {
        let commit = pending_bundles
            .front()
            .and_then(|pending| pending.commit)
            .context("ready v4 pending bundle lost its commit descriptor")?;
        let pending = pending_bundles
            .pop_front()
            .context("ready v4 pending bundle disappeared from queue")?;
        let bundle_write_duration = pending.write_duration;
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
    receiver.sync_archive()?;
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

    let commit_duration = ready
        .iter()
        .fold(Duration::ZERO, |acc, bundle| acc + bundle.write_duration)
        + archive_sync_duration
        + wal_duration;
    profile.record_bundle_write(group_bytes, write_duration);
    profile.record_bundle_sync(archive_sync_duration);
    profile.record_wal_append(group_bytes, wal_duration);
    profile.record_bundle_commit_batch(group_bytes, commit_duration, ready.len() as u64);

    let ack_flush_started = Instant::now();
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
        .context("failed to flush v4 bundle ack")?;
    profile.record_bundle_ack_flush(ack_flush_started.elapsed());
    Ok(())
}

fn validate_ready_bundle<T>(
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
        )?;
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
        )?;
        bail!("v4 bundle payload length does not match descriptor");
    }
    let checksum = pending.checksum.digest();
    if checksum != commit.compressed_xxh3_64 {
        receiver.rollback_staged_bundle(
            pending
                .archive_len_before_append
                .context("ready v4 bundle is missing its archive boundary")?,
        )?;
        protocol_error(control_stream, "v4 bundle payload checksum mismatch")?;
        bail!("v4 bundle payload checksum mismatch");
    }
    Ok(pending.descriptor.into_bundle_descriptor(commit))
}

fn respond_resume_query<T>(stream: &mut T, dump_id: &str, state: ResumeState) -> Result<()>
where
    T: Read + Write,
{
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
                )?;
                bail!(
                    "v4 resume query dump id mismatch: expected {} got {}",
                    dump_id,
                    requested_dump_id
                );
            }

            wire::write_message(stream, &Message::ResumeState(state))?;
            stream.flush().context("failed to flush v4 resume state")?;
            Ok(())
        }
        Message::Error(error) => bail!(
            "peer reported v4 error before resume query: {}",
            error.message
        ),
        unexpected => {
            protocol_error(stream, "expected v4 resume query before session flow")?;
            bail!(
                "unexpected v4 message before resume query: {:?}",
                unexpected.kind()
            );
        }
    }
}

fn protocol_error<T>(stream: &mut T, message: &str) -> Result<()>
where
    T: Write,
{
    wire::send_error(stream, message)
}

#[cfg(test)]
#[path = "network_tests.rs"]
mod tests;
