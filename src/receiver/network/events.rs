use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::{Context, Result, bail};
use crossbeam_channel::{Receiver, TryRecvError, select};

use super::AttemptKey;
use super::pending::{PendingBundle, apply_data_chunk};
use super::readers::{PendingDataChunk, READER_QUEUE_CAPACITY};
use crate::network::protocol::wire::Message;
use crate::progress::{PipelineStage, PipelineState};
use crate::receiver::session::ReceiverSession;

pub(super) enum ReceiveEvent {
    Control(Message),
    Data(PendingDataChunk),
}

pub(super) fn receive_next_event(
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

pub(super) fn receive_event_before_commit<F>(
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

pub(super) fn wait_for_pending_bundle_data(
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
