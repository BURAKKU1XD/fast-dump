use std::ops::Deref;
use std::sync::Arc;
use std::thread;

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, TryRecvError, bounded, select};

use super::AttemptKey;
use crate::network::protocol::wire::{self, Message};
use crate::network::transport::CloneableStream;
use crate::progress::{PipelineStage, PipelineState, SessionProgress};
use crate::runtime::SessionControl;

pub(super) const READER_QUEUE_CAPACITY: usize = 8;
pub(super) const DATA_BUFFER_COUNT: usize = READER_QUEUE_CAPACITY + 2;

#[derive(Debug)]
pub(super) struct PendingDataChunk {
    pub(super) key: AttemptKey,
    pub(super) is_last: bool,
    pub(super) bytes: DataBuffer,
}

#[derive(Debug)]
pub(super) struct DataBuffer {
    pub(super) storage: Vec<u8>,
    pub(super) payload_len: usize,
    pub(super) recycle: Option<Sender<Vec<u8>>>,
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

pub(super) struct ReaderThread<T: CloneableStream, Item> {
    pub(super) stream: T,
    pub(super) receiver: Option<Receiver<Result<Item>>>,
    pub(super) cancellation: Option<Sender<()>>,
    pub(super) failed: Receiver<()>,
    pub(super) handle: Option<thread::JoinHandle<()>>,
}

impl<T: CloneableStream, Item: Send + 'static> ReaderThread<T, Item> {
    pub(super) fn spawn(
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

pub(super) fn spawn_control_reader_observed<T>(
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
pub(super) fn spawn_data_reader<T>(stream: T) -> Result<ReaderThread<T, PendingDataChunk>>
where
    T: CloneableStream,
{
    spawn_data_reader_observed(stream, None, None)
}

pub(super) fn spawn_data_reader_observed<T>(
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

pub(super) fn reader_delivery_state(
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
