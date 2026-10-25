use super::readers::{DATA_BUFFER_COUNT, READER_QUEUE_CAPACITY};
use crate::network::protocol::{BundleCommitDescriptor, BundlePlanDescriptor};
use crossbeam_channel::bounded;
use std::io;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use super::*;
use crate::progress::{ProgressHub, SessionProgress};
use crate::tests::TestDir;

#[test]
fn cancelled_receiver_joins_readers_and_preserves_acknowledged_progress() {
    use crate::role::Role;
    use crate::runtime::{Cancelled, RunControl};
    let directory = TestDir::new();
    let source = TestDir::new();
    std::fs::write(source.path().join("durable.txt"), b"durable payload").unwrap();
    let prepared = crate::sender::prepare_dump(
        source.path(),
        &ResumeState::default(),
        crate::sender::BundlePlanOptions::default(),
    )
    .unwrap();
    let bundle = &prepared.bundles[0];
    let live_handles = Arc::new(AtomicUsize::new(0));
    let (mut control, server_control) = pair(&live_handles);
    let (mut data, server_data) = pair(&live_handles);
    let hub = Arc::new(ProgressHub::new());
    let run = RunControl::new(Some(hub));
    let session = run.session(Role::Receiver, "cancelled", "peer");
    let progress = session.progress().unwrap().clone();
    let _control_guard = session.register(&server_control.stream).unwrap();
    let _data_guard = session.register(&server_data.stream).unwrap();
    let path = directory.path().to_owned();
    let (done_tx, done_rx) = bounded(1);
    let worker = thread::spawn(move || {
        let result = receive_dump_profile_controlled(
            server_control,
            server_data,
            path,
            "cancelled",
            &session,
        );
        done_tx.send(result).unwrap();
    });
    wire::request_resume_state(&mut control, "cancelled").unwrap();
    assert_pipeline_state(
        &progress,
        PipelineStage::Storage,
        PipelineState::WaitingEvent,
    );
    assert_pipeline_state(&progress, PipelineStage::Control, PipelineState::Receiving);
    assert_pipeline_state(&progress, PipelineStage::Data, PipelineState::Receiving);
    wire::write_message(
        &mut control,
        &Message::BundlePlan(wire::BundlePlan {
            attempt_no: 1,
            descriptor: BundlePlanDescriptor::from_bundle_descriptor(&bundle.descriptor),
        }),
    )
    .unwrap();
    wire::write_bundle_data_message(&mut data, 1, 1, true, &bundle.compressed_payload).unwrap();
    wire::write_message(
        &mut control,
        &Message::BundleCommit(BundleCommit {
            attempt_no: 1,
            descriptor: BundleCommitDescriptor::from_bundle_descriptor(&bundle.descriptor),
        }),
    )
    .unwrap();
    let Message::BundleAck(ack) = wire::read_message(&mut control).unwrap() else {
        panic!("receiver did not durably acknowledge the bundle");
    };
    run.cancel();
    let completed = done_rx.recv_timeout(Duration::from_secs(2));
    let live_at_return = live_handles.load(Ordering::SeqCst);
    let _ = control.shutdown_stream();
    let _ = data.shutdown_stream();
    worker.join().unwrap();
    assert!(
        completed
            .expect("receiver ignored cancellation")
            .unwrap_err()
            .is::<Cancelled>()
    );
    assert_eq!(
        live_at_return, 0,
        "reader handles survived cancelled session"
    );
    let reopened = ReceiverSession::open(directory.path().to_owned(), "cancelled", 1).unwrap();
    assert!(reopened.resume_state().terminal_skips.is_empty());
    assert_eq!(reopened.resume_state().archive_len, ack.archive_len);
    assert_eq!(reopened.resume_state().last_committed_bundle_no, 1);
    assert_eq!(
        std::fs::read(directory.path().join("cancelled.tar.zst.part")).unwrap(),
        bundle.compressed_payload
    );
}

struct TrackedStream {
    stream: TcpStream,
    live_handles: Arc<AtomicUsize>,
}

impl Read for TrackedStream {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.stream.read(bytes)
    }
}

impl Write for TrackedStream {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.stream.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl CloneableStream for TrackedStream {
    fn try_clone_stream(&self) -> Result<Self> {
        let stream = self.stream.try_clone()?;
        self.live_handles.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            stream,
            live_handles: Arc::clone(&self.live_handles),
        })
    }

    fn shutdown_stream(&self) -> Result<()> {
        self.stream.shutdown_stream()
    }
}

impl Drop for TrackedStream {
    fn drop(&mut self) {
        self.live_handles.fetch_sub(1, Ordering::SeqCst);
    }
}

fn pair(live_handles: &Arc<AtomicUsize>) -> (TcpStream, TrackedStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let peer = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    peer.set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let (stream, _) = listener.accept().unwrap();
    live_handles.fetch_add(1, Ordering::SeqCst);
    (
        peer,
        TrackedStream {
            stream,
            live_handles: Arc::clone(live_handles),
        },
    )
}

#[test]
fn initialization_failure_reports_a_permanent_error_and_preserves_completed_archive() {
    let directory = TestDir::new();
    let unavailable_dir = directory.path().join("storage-file");
    std::fs::write(&unavailable_dir, b"not a directory").unwrap();
    let completed_dir = directory.path().join("completed");
    std::fs::create_dir(&completed_dir).unwrap();
    let archive = completed_dir.join("failed-init.tar.zst");
    std::fs::write(&archive, b"completed archive").unwrap();

    for (path, expected) in [
        (unavailable_dir, "failed to create v4 receiver directory"),
        (completed_dir, "completed v4 dump already exists"),
    ] {
        for request_resume in [false, true] {
            let live_handles = Arc::new(AtomicUsize::new(0));
            let (mut control, server_control) = pair(&live_handles);
            let (data, server_data) = pair(&live_handles);
            let (done_tx, done_rx) = bounded(1);
            let path = path.clone();
            let worker = thread::spawn(move || {
                let result = receive_dump(server_control, server_data, path, "failed-init");
                done_tx.send(result).unwrap();
            });
            let response = if request_resume {
                wire::request_resume_state(&mut control, "failed-init").map(Message::ResumeState)
            } else {
                wire::read_message(&mut control)
            };
            let completed = done_rx.recv_timeout(Duration::from_secs(5));
            let live_at_return = live_handles.load(Ordering::SeqCst);
            // Проверяем завершение до закрытия второго peer-канала.
            let _ = control.shutdown(Shutdown::Both);
            let _ = data.shutdown(Shutdown::Both);
            worker.join().unwrap();

            let error = completed
                .expect("initialization failure did not stop session")
                .unwrap_err();
            assert!(error.to_string().contains(expected), "{error:#}");
            assert!(!crate::network::is_retryable_error(&error));
            match (request_resume, response) {
                (false, Ok(Message::Error(response))) => {
                    assert!(response.message.contains(expected), "{}", response.message);
                }
                (true, Err(error)) => {
                    // ResumeQuery может пересечься с закрытием сокета: peer видит
                    // исходный Error или сетевой reset, но никогда успешный resume.
                    assert!(
                        format!("{error:#}").contains(expected)
                            || crate::network::is_retryable_error(&error),
                        "unexpected sender resume error: {error:#}"
                    );
                }
                (_, response) => panic!("unexpected initialization failure response: {response:?}"),
            }
            assert_eq!(live_at_return, 0);
            assert_eq!(std::fs::read(&archive).unwrap(), b"completed archive");
        }
    }
    assert_eq!(std::fs::read(archive).unwrap(), b"completed archive");
}

fn fail_pending_session(
    arrange: impl FnOnce(&mut TcpStream, &mut TcpStream) -> Result<()>,
) -> anyhow::Error {
    let directory = TestDir::new();
    let live_handles = Arc::new(AtomicUsize::new(0));
    let (mut control, server_control) = pair(&live_handles);
    let (mut data, server_data) = pair(&live_handles);
    let path = directory.path().to_owned();
    let dump_id = "pending-lifecycle";
    let (done_tx, done_rx) = bounded(1);
    let receiver = thread::spawn(move || {
        let result = receive_dump(server_control, server_data, path, dump_id);
        let _ = done_tx.send(result);
    });
    let arranged = wire::request_resume_state(&mut control, dump_id)
        .and_then(|_| arrange(&mut control, &mut data));
    let completed = done_rx.recv_timeout(Duration::from_secs(5));
    let live_at_return = live_handles.load(Ordering::SeqCst);
    let reply = completed
        .as_ref()
        .ok()
        .map(|_| wire::read_message(&mut control));

    // Даже при регрессии закрываем оба peer-сокета, затем ограниченно ждём cleanup.
    // Само проверяемое завершение должно произойти до этого принудительного закрытия.
    let _ = control.shutdown(Shutdown::Both);
    let _ = data.shutdown(Shutdown::Both);
    drop(control);
    drop(data);
    if completed.is_ok()
        || done_rx.recv_timeout(Duration::from_secs(5)).is_ok()
        || receiver.is_finished()
    {
        receiver.join().unwrap();
    } else {
        panic!("receiver did not stop even after both peer sockets were closed");
    }
    arranged.expect("failed to arrange pending session");
    let error = completed
        .expect("receiver remained blocked with one channel open")
        .expect_err("incomplete dump must fail");
    assert_eq!(live_at_return, 0, "reader handles survived session return");
    assert!(
        reply.unwrap().is_err(),
        "incomplete dump produced an acknowledgement"
    );
    assert!(!directory.path().join(format!("{dump_id}.tar.zst")).exists());
    let reopened = ReceiverSession::open(directory.path().to_owned(), dump_id, 3).unwrap();
    assert_eq!(reopened.resume_state().last_committed_bundle_no, 0);
    assert_eq!(reopened.resume_state().archive_len, 0);
    error
}

fn send_finish_with_pending_payload(control: &mut TcpStream) -> Result<()> {
    use crate::network::protocol::{BundleEntryDescriptor, BundleEntryKind};

    wire::write_message(
        control,
        &Message::BundlePlan(wire::BundlePlan {
            attempt_no: 1,
            descriptor: BundlePlanDescriptor {
                bundle_no: 1,
                first_work_ordinal: 1,
                last_work_ordinal: 1,
                last_relative_path: "pending.bin".into(),
                entries: vec![BundleEntryDescriptor {
                    work_ordinal: 1,
                    relative_path: "pending.bin".into(),
                    kind: BundleEntryKind::Regular,
                    size_at_open: 1,
                    best_effort: false,
                }],
                raw_payload_bytes: 1,
            },
        }),
    )?;
    wire::write_message(
        control,
        &Message::BundleCommit(BundleCommit {
            attempt_no: 1,
            descriptor: BundleCommitDescriptor {
                bundle_no: 1,
                tar_payload_bytes: 1024,
                compressed_payload_bytes: 1,
                compressed_xxh3_64: 0,
            },
        }),
    )?;
    wire::write_message(
        control,
        &Message::Finish(Finish {
            final_bundle_no: 1,
            final_work_ordinal: Some(1),
        }),
    )
}

#[test]
fn orphan_data_eof_stops_session_while_control_remains_open() {
    let error = fail_pending_session(|_control, data| {
        // FIFO помещает payload в orphan раньше EOF: сессия ждёт отсутствующий plan.
        wire::write_bundle_data_message(data, 1, 1, false, b"orphan")?;
        data.shutdown(Shutdown::Write)?;
        Ok(())
    });
    assert!(
        error.to_string().contains("while waiting for bundle plan"),
        "{error:#}"
    );
    assert!(crate::network::is_retryable_error(&error), "{error:#}");
    assert_eq!(
        error.downcast_ref::<io::Error>().map(io::Error::kind),
        Some(io::ErrorKind::UnexpectedEof),
    );
}

#[test]
fn finish_control_eof_stops_session_while_payload_channel_remains_open() {
    let error = fail_pending_session(|control, _data| {
        // Control FIFO гарантирует обработку Finish до EOF, а data не присылает payload.
        send_finish_with_pending_payload(control)?;
        control.shutdown(Shutdown::Write)?;
        Ok(())
    });
    assert_eq!(
        error.downcast_ref::<io::Error>().map(io::Error::kind),
        Some(io::ErrorKind::UnexpectedEof),
        "{error:#}",
    );
}

#[test]
fn finish_peer_error_stops_session_while_both_channels_remain_open() {
    let error = fail_pending_session(|control, _data| {
        send_finish_with_pending_payload(control)?;
        wire::send_error(control, "injected failure after Finish")
    });
    assert!(
        error
            .to_string()
            .contains("peer reported v4 error while finishing dump"),
        "{error:#}",
    );
    assert!(error.to_string().contains("injected failure after Finish"));
}

#[test]
fn session_exit_joins_readers_while_peer_keeps_other_channel_open() {
    let directory = TestDir::new();
    let live_handles = Arc::new(AtomicUsize::new(0));
    // Повторяем нормальное завершение, EOF одного канала и ошибку протокола:
    // оставшийся peer сокет намеренно живёт дольше receive_dump.
    for index in 0..32 {
        let (mut control, server_control) = pair(&live_handles);
        let (data, server_data) = pair(&live_handles);
        let path = directory.path().to_owned();
        let dump_id = format!("lifecycle-{index}");
        let server_id = dump_id.clone();
        let (done_tx, done_rx) = bounded(1);
        let receiver = thread::spawn(move || {
            let result = receive_dump(server_control, server_data, path, &server_id);
            done_tx.send(result).unwrap();
        });
        wire::request_resume_state(&mut control, &dump_id).unwrap();
        match index % 4 {
            0 => control.shutdown(Shutdown::Write).unwrap(),
            1 => data.shutdown(Shutdown::Write).unwrap(),
            2 => wire::write_message(&mut control, &Message::FinishAck).unwrap(),
            _ => {
                wire::write_message(
                    &mut control,
                    &Message::Finish(Finish {
                        final_bundle_no: 0,
                        final_work_ordinal: None,
                    }),
                )
                .unwrap();
                assert!(matches!(
                    wire::read_message(&mut control).unwrap(),
                    Message::FinishAck
                ));
            }
        }
        let completed = done_rx.recv_timeout(Duration::from_secs(5));
        // Даже при регрессии тест освобождает сокеты, прежде чем сообщить об ошибке.
        let live_at_return = live_handles.load(Ordering::SeqCst);
        let _ = control.shutdown(Shutdown::Both);
        let _ = data.shutdown(Shutdown::Both);
        receiver.join().unwrap();
        let result = completed.expect("receiver did not stop its readers");
        assert_eq!(result.is_ok(), index % 4 == 3, "{result:?}");
        assert_eq!(live_at_return, 0, "session {index} leaked reader handles");
    }
}

#[test]
fn session_cancellation_stops_readers_before_their_cleanup() {
    use crate::role::Role;
    use crate::runtime::RunControl;

    let live_handles = Arc::new(AtomicUsize::new(0));
    let (control_peer, control_stream) = pair(&live_handles);
    let (data_peer, data_stream) = pair(&live_handles);
    let run = RunControl::new(Some(Arc::new(ProgressHub::new())));
    let session = run.session(Role::Receiver, "cancel-readers", "peer");
    let progress = session.progress().unwrap().clone();
    let _control_registration = session.register(&control_stream.stream).unwrap();
    let _data_registration = session.register(&data_stream.stream).unwrap();
    let control_reader = spawn_control_reader_observed(
        control_stream,
        Some(progress.clone()),
        Some(session.clone()),
    )
    .unwrap();
    let data_reader =
        spawn_data_reader_observed(data_stream, Some(progress.clone()), Some(session)).unwrap();
    assert_pipeline_state(&progress, PipelineStage::Control, PipelineState::Receiving);
    assert_pipeline_state(&progress, PipelineStage::Data, PipelineState::Receiving);

    run.cancel();
    // Владельцы reader-ов ещё живы, как при долгом sync storage после Ctrl+C.
    assert!(
        control_reader
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert!(
        data_reader
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    assert_pipeline_state(&progress, PipelineStage::Control, PipelineState::Stopped);
    assert_pipeline_state(&progress, PipelineStage::Data, PipelineState::Stopped);
    drop(control_reader);
    drop(data_reader);
    assert_eq!(live_handles.load(Ordering::SeqCst), 0);
    drop(control_peer);
    drop(data_peer);
}

#[test]
fn cancellation_unblocks_full_control_queue() {
    let live_handles = Arc::new(AtomicUsize::new(0));
    let (mut peer, stream) = pair(&live_handles);
    let progress = ProgressHub::new().session("receiver".into(), "queue", "peer");
    let reader = spawn_control_reader_observed(stream, Some(progress.clone()), None).unwrap();
    assert_pipeline_state(&progress, PipelineStage::Control, PipelineState::Receiving);
    for _ in 0..READER_QUEUE_CAPACITY + 2 {
        wire::write_message(&mut peer, &Message::FinishAck).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while reader.len() < READER_QUEUE_CAPACITY && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(reader.len(), READER_QUEUE_CAPACITY);
    assert_pipeline_state(
        &progress,
        PipelineStage::Control,
        PipelineState::WaitingConsumer,
    );
    let (done_tx, done_rx) = bounded(1);
    let cleanup = thread::spawn(move || {
        drop(reader);
        done_tx.send(()).unwrap();
    });
    let result = done_rx.recv_timeout(Duration::from_secs(5));
    let _ = peer.shutdown(Shutdown::Both);
    result.expect("full reader queue blocked cancellation");
    cleanup.join().unwrap();
    assert_eq!(live_handles.load(Ordering::SeqCst), 0);
    assert_pipeline_state(&progress, PipelineStage::Control, PipelineState::Stopped);
}

#[test]
fn data_failure_notification_bypasses_full_payload_queue() {
    let live_handles = Arc::new(AtomicUsize::new(0));
    let (mut peer, stream) = pair(&live_handles);
    let progress = ProgressHub::new().session("receiver".into(), "failure", "peer");
    let reader = spawn_data_reader_observed(stream, Some(progress.clone()), None).unwrap();
    let arranged: Result<()> = (|| {
        for _ in 0..READER_QUEUE_CAPACITY {
            wire::write_bundle_data_message(&mut peer, 1, 1, false, b"queued")?;
        }
        peer.shutdown(Shutdown::Write)?;
        Ok(())
    })();
    // Основная очередь заполнена payload: EOF должен сообщаться отдельно, без её чтения.
    let failure = reader.failed.recv_timeout(Duration::from_secs(5));
    assert_pipeline_state(&progress, PipelineStage::Data, PipelineState::Failed);
    let queued_payloads = reader.len();
    let (done_tx, done_rx) = bounded(1);
    let cleanup = thread::spawn(move || {
        drop(reader);
        let _ = done_tx.send(());
    });
    let completed = done_rx.recv_timeout(Duration::from_secs(5));
    let _ = peer.shutdown(Shutdown::Both);
    drop(peer);
    if completed.is_ok()
        || done_rx.recv_timeout(Duration::from_secs(5)).is_ok()
        || cleanup.is_finished()
    {
        cleanup.join().unwrap();
    } else {
        panic!("data reader cleanup remained blocked after peer close");
    }
    arranged.unwrap();
    failure.expect("full payload queue hid data reader failure");
    completed.expect("full data queue blocked reader cancellation");
    assert_eq!(queued_payloads, READER_QUEUE_CAPACITY);
    assert_eq!(live_handles.load(Ordering::SeqCst), 0);
    assert_pipeline_state(&progress, PipelineStage::Data, PipelineState::Failed);
}

#[test]
fn cancellation_unblocks_pool_with_all_payload_buffers_retained() {
    let live_handles = Arc::new(AtomicUsize::new(0));
    let (mut peer, stream) = pair(&live_handles);
    let progress = ProgressHub::new().session("receiver".into(), "pool", "peer");
    let reader = spawn_data_reader_observed(stream, Some(progress.clone()), None).unwrap();
    let mut held = Vec::new();
    for index in 0..DATA_BUFFER_COUNT {
        wire::write_bundle_data_message(&mut peer, 1, 1, false, &[index as u8]).unwrap();
        held.push(
            reader
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap(),
        );
    }
    assert_pipeline_state(&progress, PipelineStage::Data, PipelineState::WaitingBuffer);
    let (done_tx, done_rx) = bounded(1);
    let cleanup = thread::spawn(move || {
        drop(reader);
        done_tx.send(()).unwrap();
    });
    // Отмена обязана завершить reader, даже если consumer ещё держит весь пул.
    let result = done_rx.recv_timeout(Duration::from_secs(5));
    drop(held);
    let _ = peer.shutdown(Shutdown::Both);
    cleanup.join().unwrap();
    result.expect("retained buffers blocked reader cancellation");
    assert_eq!(live_handles.load(Ordering::SeqCst), 0);
    assert_pipeline_state(&progress, PipelineStage::Data, PipelineState::Stopped);
}

fn assert_pipeline_state(progress: &SessionProgress, stage: PipelineStage, state: PipelineState) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = progress.snapshot();
        let pipeline = snapshot
            .pipelines
            .iter()
            .find(|pipeline| pipeline.stage == stage)
            .unwrap();
        if pipeline.state == state {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {stage:?} {state:?}, got {pipeline:?}"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
#[ignore = "release correctness endurance: 128 complete transfers; run explicitly"]
fn repeated_full_transfers_release_all_reader_handles() {
    use crate::sender::{BundlePlanOptions, send_dump};
    use crate::tests::read_archive_entries;
    use std::fs;

    const SESSIONS: usize = 128;
    let source_dir = TestDir::new();
    let data_dir = TestDir::new();
    let mut expected = Vec::<(String, Vec<u8>)>::new();
    let mut random_state = 0xa173_d902_65bf_40e1_u64;
    let mut deterministic_bytes = |length: usize| {
        let mut bytes = vec![0; length];
        for chunk in bytes.chunks_mut(8) {
            random_state ^= random_state << 13;
            random_state ^= random_state >> 7;
            random_state ^= random_state << 17;
            chunk.copy_from_slice(&random_state.to_le_bytes()[..chunk.len()]);
        }
        bytes
    };
    // Порядок задан независимо от scanner: materialized, streamed, materialized.
    // 65 entries включают parallel read-ahead, а большой файл превышает target в 8 раз.
    for group in ["a-small", "z-small"] {
        fs::create_dir(source_dir.path().join(group)).unwrap();
        for index in 0..32 {
            let path = format!("{group}/file-{index:02}.bin");
            let bytes = deterministic_bytes(2048 + index * 37);
            fs::write(source_dir.path().join(&path), &bytes).unwrap();
            expected.push((path, bytes));
        }
        if group == "a-small" {
            let bytes = deterministic_bytes(2 * 1024 * 1024 + 37);
            fs::write(source_dir.path().join("middle-streamed.bin"), &bytes).unwrap();
            expected.push(("middle-streamed.bin".into(), bytes));
        }
    }
    assert_eq!(expected.len(), 65);
    let bytes_per_session: u64 = expected.iter().map(|(_, bytes)| bytes.len() as u64).sum();
    let options = BundlePlanOptions {
        bundle_target_bytes: 256 * 1024,
        compression_level: 1,
        compression_threads: 2,
        readers: 2,
    };
    let live_handles = Arc::new(AtomicUsize::new(0));
    for index in 0..SESSIONS {
        let (mut control, server_control) = pair(&live_handles);
        let (mut data, server_data) = pair(&live_handles);
        for stream in [&control, &data] {
            stream
                .set_read_timeout(Some(Duration::from_secs(30)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(30)))
                .unwrap();
        }
        let dump_id = format!("endurance-{index:03}");
        let server_id = dump_id.clone();
        let output = data_dir.path().to_owned();
        let (done_tx, done_rx) = bounded(1);
        let receiver = thread::spawn(move || {
            let result = receive_dump(server_control, server_data, output, &server_id);
            let _ = done_tx.send(result);
        });
        let sent = send_dump(
            &mut control,
            &mut data,
            source_dir.path().to_owned(),
            &dump_id,
            options,
        );
        let completed = done_rx.recv_timeout(Duration::from_secs(30));
        let live_at_return = live_handles.load(Ordering::SeqCst);
        // Сначала проверяем возврат receive_dump при живом peer; закрытие ниже — cleanup.
        let _ = control.shutdown(Shutdown::Both);
        let _ = data.shutdown(Shutdown::Both);
        drop(control);
        drop(data);
        if completed.is_ok()
            || done_rx.recv_timeout(Duration::from_secs(5)).is_ok()
            || receiver.is_finished()
        {
            receiver.join().unwrap();
        } else {
            panic!("session {index} remained blocked after both peer sockets were closed");
        }
        let sent = sent.unwrap_or_else(|error| panic!("session {index} sender: {error:#}"));
        let received = completed
            .unwrap_or_else(|error| panic!("session {index} receiver completion: {error}"))
            .unwrap_or_else(|error| panic!("session {index} receiver: {error:#}"));
        assert_eq!(live_at_return, 0, "session {index} leaked reader handles");
        assert_eq!(sent.terminal_skip_count, 0);
        assert_eq!(sent.final_work_ordinal, expected.len() as u64);
        assert!(sent.bundles_sent >= 3);
        assert_eq!(
            received.resume_state.last_committed_bundle_no,
            sent.final_bundle_no
        );
        assert_eq!(
            received.resume_state.last_committed_work_ordinal,
            expected.len() as u64
        );
        assert!(received.issue_report_path.is_none());

        let entries = read_archive_entries(&received.final_archive_path);
        assert_eq!(entries.len(), expected.len(), "session {index}");
        for (entry, (path, bytes)) in entries.iter().zip(&expected) {
            assert_eq!(&entry.path, path, "session {index}: archive order");
            assert!(entry.entry_type.is_file(), "session {index}: {path}");
            assert_eq!(entry.bytes.len(), bytes.len(), "session {index}: {path}");
            assert!(
                entry.bytes == *bytes,
                "session {index}: wrong content in {path}"
            );
            assert!(entry.link_name.is_none());
        }
        drop(entries);
        if matches!(index + 1, 1 | 64 | SESSIONS) {
            println!(
                "[endurance-progress] sessions={} live_reader_handles={}",
                index + 1,
                live_handles.load(Ordering::SeqCst),
            );
        }
    }
    println!(
        "[endurance-complete] sessions={SESSIONS} files_per_session={} raw_bytes_per_session={bytes_per_session} verified_files={} verified_bytes={} live_reader_handles={}",
        expected.len(),
        expected.len() * SESSIONS,
        bytes_per_session * SESSIONS as u64,
        live_handles.load(Ordering::SeqCst),
    );
}
