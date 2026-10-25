use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{Read, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded, select};
use xxhash_rust::xxh3::Xxh3Default;

use crate::network::network_io;
use crate::network::protocol::wire::{
    self, BundleAbort, BundleAbortAck, BundleAck, BundleCommit, Finish, Message, ResumeQuery,
    SkipAck,
};
use crate::network::protocol::{
    BundleCommitDescriptor, BundleDescriptor, BundlePlanDescriptor, ResumeState,
};
use crate::network::transport::CloneableStream;
use crate::profile::ReceiverProfile;
use crate::progress::{self, Phase, PipelineStage, PipelineState, SessionProgress};
use crate::runtime::SessionControl;

use super::session::ReceiverSession;

const COMMIT_GROUP_MAX_BUNDLES: usize = 4;
const COMMIT_GROUP_MAX_BYTES: u64 = 1024 * 1024 * 1024;
const READER_QUEUE_CAPACITY: usize = 8;
const DATA_BUFFER_COUNT: usize = READER_QUEUE_CAPACITY + 2;

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
    periodic_sync_duration: Duration,
    periodic_sync_count: u64,
    commit: Option<BundleCommitDescriptor>,
    data_finished: bool,
}

#[derive(Debug)]
struct PendingDataChunk {
    key: AttemptKey,
    is_last: bool,
    bytes: DataBuffer,
}

#[derive(Debug)]
struct DataBuffer {
    storage: Vec<u8>,
    payload_len: usize,
    recycle: Option<Sender<Vec<u8>>>,
}

impl Deref for DataBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.storage[..self.payload_len]
    }
}

impl Drop for DataBuffer {
    fn drop(&mut self) {
        if let Some(recycle) = &self.recycle {
            // Возврат не блокирует отмену; тот же путь обслуживает abort и orphan.
            let _ = recycle.try_send(std::mem::take(&mut self.storage));
        }
    }
}

#[cfg(test)]
impl From<Vec<u8>> for DataBuffer {
    fn from(storage: Vec<u8>) -> Self {
        Self {
            payload_len: storage.len(),
            storage,
            recycle: None,
        }
    }
}

struct ReadyBundle {
    descriptor: BundleDescriptor,
    archive_len: u64,
    write_duration: Duration,
}

enum ReceiveEvent {
    Control(Message),
    Data(PendingDataChunk),
}

fn receive_next_event(
    control_rx: &Receiver<Result<Message>>,
    data_rx: &Receiver<Result<PendingDataChunk>>,
    data_failed: &Receiver<()>,
    wait_for_plan: bool,
) -> Result<ReceiveEvent> {
    // Один data-пакет может обогнать план. Дальше читаем только control, чтобы
    // ограниченная очередь и TCP удерживали оставшийся payload вне orphan_data.
    if wait_for_plan {
        return select! {
            recv(control_rx) -> event => event
                .context("v4 control reader thread terminated unexpectedly")?
                .map(ReceiveEvent::Control),
            recv(data_failed) -> _ => {
                // Reader публикует сигнал перед ошибкой в ограниченной очереди.
                // Сессия уже завершается: освобождаем payload до исходной ошибки,
                // не теряя её тип и не оставляя producer на заполненном канале.
                loop {
                    match data_rx.recv() {
                        Ok(Ok(_)) => {},
                        Ok(Err(error)) => return Err(error)
                            .context("v4 data reader failed while waiting for bundle plan"),
                        Err(error) => return Err(error)
                            .context("v4 data reader failed while waiting for bundle plan"),
                    }
                }
            },
        };
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

fn receive_event_before_commit<F>(
    control_rx: &Receiver<Result<Message>>,
    data_rx: &Receiver<Result<PendingDataChunk>>,
    data_failed: &Receiver<()>,
    wait_for_plan: bool,
    consecutive_controls: &mut usize,
    commit_ready: F,
) -> Result<ReceiveEvent>
where
    F: FnOnce() -> Result<()>,
{
    // Уже пришедший следующий план может объединить готовый prefix в одну
    // commit-группу. Не ждём новых control и ограничиваем приоритет очередью.
    if *consecutive_controls < READER_QUEUE_CAPACITY {
        match control_rx.try_recv() {
            Ok(event) => {
                *consecutive_controls += 1;
                return event.map(ReceiveEvent::Control);
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }
    }

    commit_ready()?;
    *consecutive_controls = 0;
    // После control-burst готовый payload обязательно получает ход. При orphan
    // по-прежнему нужен его план: второй data-пакет нельзя брать из очереди.
    if !wait_for_plan {
        match data_rx.try_recv() {
            Ok(event) => return event.map(ReceiveEvent::Data),
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => {}
        }
    }
    let event = receive_next_event(control_rx, data_rx, data_failed, wait_for_plan)?;
    if matches!(&event, ReceiveEvent::Control(_)) {
        *consecutive_controls = 1;
    }
    Ok(event)
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
                        checksum: Xxh3Default::new(),
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

struct ReaderThread<T: CloneableStream, Item> {
    stream: T,
    receiver: Option<Receiver<Result<Item>>>,
    cancellation: Option<Sender<()>>,
    failed: Receiver<()>,
    handle: Option<thread::JoinHandle<()>>,
}

impl<T: CloneableStream, Item: Send + 'static> ReaderThread<T, Item> {
    fn spawn(
        name: &str,
        stream: T,
        read: impl FnOnce(T, Sender<Result<Item>>, Receiver<()>, Sender<()>) + Send + 'static,
    ) -> Result<Self> {
        let interrupt = stream.try_clone_stream()?;
        let (tx, rx) = bounded(READER_QUEUE_CAPACITY);
        let (cancel_tx, cancel_rx) = bounded(0);
        let (failure_tx, failure_rx) = bounded(1);
        let handle = thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || read(stream, tx, cancel_rx, failure_tx))
            .context("failed to spawn v4 receiver reader")?;
        Ok(Self {
            stream: interrupt,
            receiver: Some(rx),
            cancellation: Some(cancel_tx),
            failed: failure_rx,
            handle: Some(handle),
        })
    }
}

impl<T: CloneableStream, Item> Deref for ReaderThread<T, Item> {
    type Target = Receiver<Result<Item>>;

    fn deref(&self) -> &Self::Target {
        self.receiver.as_ref().unwrap()
    }
}

impl<T: CloneableStream, Item> Drop for ReaderThread<T, Item> {
    fn drop(&mut self) {
        // Снимаем все три вида ожидания: socket read, send в заполненную очередь
        // и получение буфера из пула. Join завершается до освобождения слота сессии.
        self.cancellation.take();
        let _ = self.stream.shutdown_stream();
        self.receiver.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn spawn_control_reader_observed<T>(
    stream: T,
    progress: Option<Arc<SessionProgress>>,
    control: Option<SessionControl>,
) -> Result<ReaderThread<T, Message>>
where
    T: CloneableStream,
{
    ReaderThread::spawn(
        "dump-control-read",
        stream,
        move |mut stream, tx, cancel, failed_tx| {
            loop {
                if let Some(progress) = &progress {
                    progress.set_stage(PipelineStage::Control, PipelineState::Receiving);
                }
                let item = wire::read_message(&mut stream);
                let failed = item.is_err();
                if let Some(progress) = &progress {
                    progress.set_stage(
                        PipelineStage::Control,
                        reader_delivery_state(failed, &cancel, control.as_ref()),
                    );
                }
                if failed {
                    let _ = failed_tx.try_send(());
                }
                if tx.send(item).is_err() {
                    if !failed && let Some(progress) = &progress {
                        progress.set_stage(PipelineStage::Control, PipelineState::Stopped);
                    }
                    return;
                }
                if failed {
                    return;
                }
            }
        },
    )
}

#[cfg(test)]
fn spawn_data_reader<T>(stream: T) -> Result<ReaderThread<T, PendingDataChunk>>
where
    T: CloneableStream,
{
    spawn_data_reader_observed(stream, None, None)
}

fn spawn_data_reader_observed<T>(
    stream: T,
    progress: Option<Arc<SessionProgress>>,
    control: Option<SessionControl>,
) -> Result<ReaderThread<T, PendingDataChunk>>
where
    T: CloneableStream,
{
    let (recycle_tx, recycle_rx) = bounded(DATA_BUFFER_COUNT);
    ReaderThread::spawn(
        "dump-data-read",
        stream,
        move |mut stream, tx, cancel, failed_tx| {
            let mut allocated_buffers = 0;
            loop {
                // Сначала используем вернувшийся буфер; новые нужны только при
                // отставании writer. Общий предел: очередь + текущий/orphan + reader.
                let mut storage = match recycle_rx.try_recv() {
                    Ok(storage) => storage,
                    Err(TryRecvError::Empty) if allocated_buffers < DATA_BUFFER_COUNT => {
                        allocated_buffers += 1;
                        Vec::new()
                    }
                    Err(_) => {
                        if let Some(progress) = &progress {
                            progress.set_stage(PipelineStage::Data, PipelineState::WaitingBuffer);
                        }
                        select! {
                            recv(recycle_rx) -> storage => match storage {
                                Ok(storage) => storage,
                                Err(_) => break,
                            },
                            recv(cancel) -> _ => break,
                        }
                    }
                };
                if let Some(progress) = &progress {
                    progress.set_stage(PipelineStage::Data, PipelineState::Receiving);
                }
                let item =
                    wire::read_bundle_data_message_into(&mut stream, &mut storage).map(|header| {
                        PendingDataChunk {
                            key: AttemptKey::new(header.bundle_no, header.attempt_no),
                            is_last: header.is_last,
                            bytes: DataBuffer {
                                storage,
                                payload_len: header.payload_len,
                                recycle: Some(recycle_tx.clone()),
                            },
                        }
                    });
                let failed = item.is_err();
                if let Some(progress) = &progress {
                    progress.set_stage(
                        PipelineStage::Data,
                        reader_delivery_state(failed, &cancel, control.as_ref()),
                    );
                }
                if failed {
                    // Основная очередь может быть заполнена: orphan-ожидание должно
                    // увидеть ошибку без чтения дополнительных payload-пакетов.
                    let _ = failed_tx.try_send(());
                }
                let disconnected = tx.send(item).is_err();
                if failed {
                    return;
                }
                if disconnected {
                    break;
                }
            }
            if let Some(progress) = &progress {
                progress.set_stage(PipelineStage::Data, PipelineState::Stopped);
            }
        },
    )
}

fn reader_delivery_state(
    failed: bool,
    cancellation: &Receiver<()>,
    control: Option<&SessionControl>,
) -> PipelineState {
    if !failed {
        PipelineState::WaitingConsumer
    } else if control.is_some_and(SessionControl::is_cancelled)
        || matches!(cancellation.try_recv(), Err(TryRecvError::Disconnected))
    {
        // Ctrl+C закрывает сокеты до Drop reader-а: его локальный канал отмены
        // ещё жив, пока storage заканчивает текущий sync или filesystem I/O.
        PipelineState::Stopped
    } else {
        PipelineState::Failed
    }
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

fn wait_for_pending_bundle_data(
    control_rx: &Receiver<Result<Message>>,
    data_rx: &Receiver<Result<PendingDataChunk>>,
    receiver: &mut ReceiverSession,
    pending_bundles: &mut VecDeque<PendingBundle>,
    orphan_data: &mut HashMap<AttemptKey, Vec<PendingDataChunk>>,
    aborted_attempts: &HashSet<AttemptKey>,
) -> Result<()> {
    while pending_bundles.iter().any(|pending| !pending.data_finished) {
        let waiting = receiver
            .progress
            .as_ref()
            .map(|progress| progress.stage(PipelineStage::Storage, PipelineState::WaitingData));
        let data = select! {
            recv(data_rx) -> data => data
                .context("v4 data reader channel closed while waiting for pending bundle data")??,
            recv(control_rx) -> control => {
                let message = control.context("v4 control reader closed while finishing dump")??;
                if let Message::Error(error) = message {
                    bail!("peer reported v4 error while finishing dump: {}", error.message);
                }
                bail!("unexpected v4 control message after Finish: {:?}", message.kind());
            },
        };
        drop(waiting);
        apply_data_chunk(
            receiver,
            pending_bundles,
            orphan_data,
            aborted_attempts,
            data,
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
