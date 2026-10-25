use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::network::protocol::SkippedEntry;
use crate::progress::{self, PipelineStage, PipelineState};
use crate::runtime::SessionControl;
use crate::sender::plan::{DumpPlanner, PrepareProfile, SendBundle};

pub(super) enum PlannedItem {
    Skips(Vec<SkippedEntry>),
    Bundle(SendBundle),
    Done {
        final_work_ordinal: u64,
        prepare_profile: PrepareProfile,
    },
}

pub(super) struct PlannerThread {
    pub(super) receiver: Option<Receiver<Result<PlannedItem>>>,
    pub(super) handle: Option<thread::JoinHandle<()>>,
    pub(super) cancellation: Arc<AtomicBool>,
}

impl PlannerThread {
    pub(super) fn join(mut self) -> Result<()> {
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

pub(super) fn spawn_planner_thread(planner: DumpPlanner) -> PlannerThread {
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

pub(super) fn run_planner_thread(
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

pub(super) fn send_planned_skips(
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

pub(super) fn receive_planned_item(
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
        crate::sender::check_control(control)?;
        match receiver.recv_timeout(Duration::from_millis(50)) {
            Ok(result) => return result,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(error) => {
                return Err(error).context("v4 planner thread exited before completing dump");
            }
        }
    }
}
