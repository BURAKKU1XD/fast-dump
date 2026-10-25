mod ack;
mod planner;
mod transfer;

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Instant;

use anyhow::{Context, Result, bail};

use super::compress::BundleCompressor;
use super::plan::{DumpPlanner, PrepareProfile, SendBundle};
use super::stream::StreamWorkspace;
use super::{BundlePlanOptions, plan_dump};
use crate::network::protocol::wire::{self, Finish, Message};
use crate::profile::SenderProfile;
use crate::progress::{self, Phase, PipelineStage, PipelineState};
use crate::runtime::SessionControl;
use ack::{InflightBundles, drain_inflight_bundles};
use planner::{PlannedItem, receive_planned_item, spawn_planner_thread};
use transfer::{
    StreamedBundleSendOutcome, send_skip_entries_profiled, send_streamed_bundle_profiled,
    write_materialized_bundle_profiled,
};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::protocol::SkipReason;
    use crate::network::protocol::SkippedEntry;
    use crate::network::protocol::wire::{
        BundleAbort, BundleAbortAck, BundleAck, BundlePlan, SkipAck,
    };
    use crate::progress::ProgressHub;
    use crate::role::Role;
    use crate::runtime::{Cancelled, RunControl};
    use crate::sender::plan::StreamedBundleTask;
    use ack::{InflightBundle, apply_cumulative_bundle_ack, send_bundle_abort_profiled};
    use planner::{PlannerThread, run_planner_thread, send_planned_skips};
    use std::io::{self, ErrorKind};
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;
    use transfer::is_retryable_stream_read_error;

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
