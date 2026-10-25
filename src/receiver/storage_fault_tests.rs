use std::io::Read;
use std::net::{TcpListener, TcpStream};

use super::*;
use crate::network::protocol::SkipReason;
use crate::receiver::storage_faults::{self, Point};
use crate::receiver::{ReceiverPaths, wal};

fn paths(directory: &TestDirectory) -> ReceiverPaths {
    ReceiverPaths::new(directory.0.clone(), "boundaries")
}

fn prepare_group(receiver: &mut ReceiverSession, first: u64, last: u64) -> PendingState {
    let mut state = PendingState::default();
    for bundle_no in first..=last {
        let bundle = bundle(bundle_no);
        state.plan(&bundle, 1);
        state
            .data(receiver, bundle_no, 1, &bundle.compressed_payload, true)
            .unwrap();
    }
    state
}

fn assert_commit_failure_without_ack(receiver: &mut ReceiverSession, state: &mut PendingState) {
    let before = receiver.resume_state().clone();
    let mut output = Vec::new();
    let mut profile = ReceiverProfile::default();
    assert!(
        drain_ready_commits(
            &mut output,
            receiver,
            &mut state.bundles,
            &mut profile,
            true,
        )
        .is_err()
    );
    assert!(output.is_empty(), "failed storage barrier emitted an ACK");
    assert_eq!(profile.committed_bundles, 0);
    assert_eq!(
        receiver.resume_state().last_committed_bundle_no,
        before.last_committed_bundle_no
    );
    assert_eq!(receiver.resume_state().archive_len, before.archive_len);
}

fn expected_payload(last_bundle: u64) -> Vec<u8> {
    (1..=last_bundle)
        .flat_map(|number| bundle(number).compressed_payload)
        .collect()
}

#[test]
fn failed_wal_sync_does_not_publish_durable_progress() {
    use crate::progress::ProgressHub;
    use crate::role::Role;
    use crate::runtime::RunControl;
    let directory = TestDirectory::new();
    let run = RunControl::new(Some(std::sync::Arc::new(ProgressHub::new())));
    let control = run.session(Role::Receiver, "boundaries", "peer");
    let mut receiver =
        ReceiverSession::open_controlled(directory.0.clone(), "boundaries", 1, Some(&control))
            .unwrap();
    let mut state = prepare_group(&mut receiver, 1, 2);
    let fault = storage_faults::arm(Point::WalSync, 0, 0);
    assert_commit_failure_without_ack(&mut receiver, &mut state);
    fault.assert_consumed();
    let progress = control.progress().unwrap();
    assert!(progress.transferred_bytes.load(Ordering::Relaxed) > 0);
    assert_eq!(progress.durable_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(progress.committed_entries.load(Ordering::Relaxed), 0);
    assert_eq!(progress.bundles.load(Ordering::Relaxed), 0);
}

#[test]
fn disk_full_after_partial_archive_write_preserves_acknowledged_prefix() {
    for partial_bytes in [0, 5] {
        let directory = TestDirectory::new();
        let paths = paths(&directory);
        let mut receiver = directory.open();
        receiver.commit_bundle(&bundle(1)).unwrap();
        let wal_before = fs::read(&paths.wal_path).unwrap();
        let mut state = PendingState::default();
        let next = bundle(2);
        state.plan(&next, 1);
        let fault = storage_faults::arm(Point::ArchiveWrite, 0, partial_bytes);
        let error = state
            .data(&mut receiver, 2, 1, &next.compressed_payload, true)
            .unwrap_err();
        fault.assert_consumed();
        let io_error = error.downcast_ref::<std::io::Error>().unwrap();
        #[cfg(windows)]
        assert_eq!(io_error.raw_os_error(), Some(112));
        #[cfg(unix)]
        assert_eq!(io_error.raw_os_error(), Some(28));
        #[cfg(not(any(windows, unix)))]
        assert_eq!(io_error.kind(), std::io::ErrorKind::Other);
        assert_eq!(
            directory.archive_bytes(),
            [
                expected_payload(1),
                next.compressed_payload[..partial_bytes].to_vec()
            ]
            .concat()
        );
        let mut output = Vec::new();
        drain_ready_commits(
            &mut output,
            &mut receiver,
            &mut state.bundles,
            &mut ReceiverProfile::default(),
            true,
        )
        .unwrap();
        assert!(output.is_empty(), "an incomplete write must not be ACKed");
        assert_eq!(fs::read(&paths.wal_path).unwrap(), wal_before);
        drop(receiver);

        let mut recovered = directory.open();
        assert_eq!(recovered.resume_state().last_committed_bundle_no, 1);
        assert_eq!(directory.archive_bytes(), expected_payload(1));
        recovered.commit_bundle(&next).unwrap();
        assert_eq!(directory.archive_bytes(), expected_payload(2));
    }
}

#[test]
fn archive_sync_failure_leaves_wal_and_acknowledged_prefix_unchanged() {
    let directory = TestDirectory::new();
    let paths = paths(&directory);
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    let wal_before = fs::read(&paths.wal_path).unwrap();
    let mut state = prepare_group(&mut receiver, 2, 3);
    let fault = storage_faults::arm(Point::ArchiveSync, 0, 0);
    assert_commit_failure_without_ack(&mut receiver, &mut state);
    fault.assert_consumed();
    assert_eq!(fs::read(&paths.wal_path).unwrap(), wal_before);
    drop(receiver);

    let recovered = directory.open();
    assert_eq!(recovered.resume_state().last_committed_bundle_no, 1);
    assert_eq!(directory.archive_bytes(), expected_payload(1));
}

#[test]
fn periodic_archive_sync_failure_never_advances_wal() {
    let directory = TestDirectory::new();
    let paths = paths(&directory);
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    let wal_before = fs::read(&paths.wal_path).unwrap();
    let chunk = vec![0xa5; 4 * 1024 * 1024];
    let fault = storage_faults::arm(Point::ArchiveSync, 0, 0);
    for _ in 0..15 {
        assert!(receiver.append_bundle_chunk(&chunk).unwrap().is_none());
    }
    assert!(receiver.append_bundle_chunk(&chunk).is_err());
    fault.assert_consumed();
    assert_eq!(receiver.resume_state().last_committed_bundle_no, 1);
    assert_eq!(fs::read(&paths.wal_path).unwrap(), wal_before);
    drop(receiver);

    let recovered = directory.open();
    assert_eq!(recovered.resume_state().last_committed_bundle_no, 1);
    assert_eq!(directory.archive_bytes(), expected_payload(1));
}

#[test]
fn torn_wal_append_recovers_complete_records_and_accepts_subsequent_commits() {
    let directory = TestDirectory::new();
    let paths = paths(&directory);
    let mut receiver = directory.open();
    receiver.commit_bundle(&bundle(1)).unwrap();
    let mut state = prepare_group(&mut receiver, 2, 4);
    // Первая запись группы целая; вторая оборвана посреди поля bundle_no.
    let fault = storage_faults::arm(Point::WalAppend, 1, 8);
    assert_commit_failure_without_ack(&mut receiver, &mut state);
    fault.assert_consumed();
    assert!(!fs::read(&paths.wal_path).unwrap().ends_with(b"\n"));
    drop(receiver);

    let mut recovered = directory.open();
    assert_eq!(recovered.resume_state().last_committed_bundle_no, 2);
    assert_eq!(directory.archive_bytes(), expected_payload(2));
    assert!(fs::read(&paths.wal_path).unwrap().ends_with(b"\n"));
    recovered.commit_bundle(&bundle(3)).unwrap();
    drop(recovered);
    let recovered = directory.open();
    assert_eq!(recovered.resume_state().last_committed_bundle_no, 3);
    assert_eq!(directory.archive_bytes(), expected_payload(3));
}

#[test]
fn wal_sync_failure_sends_no_ack_and_recovers_either_persisted_or_lost_group() {
    for retain_unacknowledged_group in [false, true] {
        let directory = TestDirectory::new();
        let paths = paths(&directory);
        let mut receiver = directory.open();
        receiver.commit_bundle(&bundle(1)).unwrap();
        let durable_wal_len = fs::metadata(&paths.wal_path).unwrap().len();
        let mut state = prepare_group(&mut receiver, 2, 3);
        let fault = storage_faults::arm(Point::WalSync, 0, 0);
        assert_commit_failure_without_ack(&mut receiver, &mut state);
        fault.assert_consumed();
        drop(receiver);

        // Ошибка sync не доказывает потерю уже записанных байт. Проверяем оба
        // допустимых исхода: полный хвост сохранился либо потеряна вся новая группа.
        // Удаление хвоста моделирует потерю записи, а не реальное отключение питания.
        if !retain_unacknowledged_group {
            fs::OpenOptions::new()
                .write(true)
                .open(&paths.wal_path)
                .unwrap()
                .set_len(durable_wal_len)
                .unwrap();
        }
        let mut recovered = directory.open();
        let last = if retain_unacknowledged_group { 3 } else { 1 };
        assert_eq!(recovered.resume_state().last_committed_bundle_no, last);
        assert_eq!(directory.archive_bytes(), expected_payload(last));
        assert_eq!(
            wal::load_state(&paths.wal_path).unwrap().archive_len,
            recovered.resume_state().archive_len
        );
        recovered.commit_bundle(&bundle(last + 1)).unwrap();
        assert_eq!(directory.archive_bytes(), expected_payload(last + 1));
    }
}

fn tar_bundle() -> PreparedBundle {
    let content = b"durable file before finalization failure";
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_mode(0o644);
    header.set_size(content.len() as u64);
    header.set_cksum();
    builder
        .append_data(&mut header, "1.bin", &content[..])
        .unwrap();
    let tar = builder.into_inner().unwrap();
    let payload = zstd::stream::encode_all(tar.as_slice(), 1).unwrap();
    let mut bundle = bundle(1);
    bundle.descriptor.raw_payload_bytes = content.len() as u64;
    bundle.descriptor.tar_payload_bytes = tar.len() as u64;
    bundle.descriptor.compressed_payload_bytes = payload.len() as u64;
    bundle.descriptor.compressed_xxh3_64 = xxhash_rust::xxh3::xxh3_64(&payload);
    bundle.compressed_payload = payload;
    bundle
}

fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    (client, server)
}

fn finish_with_storage_failure(directory: &TestDirectory, point: Point, successful_calls: usize) {
    let (mut control, server_control) = tcp_pair();
    let (_data, server_data) = tcp_pair();
    let data_dir = directory.0.clone();
    let (finished_tx, finished_rx) = bounded(1);
    let worker = thread::spawn(move || {
        let partial_bytes = usize::from(point == Point::ArchiveWrite) * 5;
        let fault = storage_faults::arm(point, successful_calls, partial_bytes);
        let result = receive_dump(server_control, server_data, data_dir, "boundaries");
        fault.assert_consumed();
        finished_tx.send(result).unwrap();
    });
    wire::write_message(
        &mut control,
        &Message::ResumeQuery(ResumeQuery {
            dump_id: "boundaries".into(),
        }),
    )
    .unwrap();
    let Message::ResumeState(resume) = wire::read_message(&mut control).unwrap() else {
        panic!("receiver did not return resume state");
    };
    assert_eq!(resume.last_committed_bundle_no, 1);
    wire::write_message(
        &mut control,
        &Message::Finish(Finish {
            final_bundle_no: 1,
            final_work_ordinal: Some(1),
        }),
    )
    .unwrap();
    assert!(
        finished_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_err()
    );
    worker.join().unwrap();
    let mut response = Vec::new();
    control.read_to_end(&mut response).unwrap();
    assert!(response.is_empty(), "failed finalization emitted FinishAck");
}

fn assert_complete_archive(path: &std::path::Path, prefix: &[u8]) {
    let archive_bytes = fs::read(path).unwrap();
    assert!(archive_bytes.starts_with(prefix));
    assert!(archive_bytes.len() > prefix.len());
    let decoded = zstd::stream::decode_all(archive_bytes.as_slice()).unwrap();
    let mut archive = tar::Archive::new(decoded.as_slice());
    let mut entries = archive.entries().unwrap();
    let mut entry = entries.next().unwrap().unwrap();
    assert_eq!(entry.path().unwrap(), std::path::Path::new("1.bin"));
    let mut content = Vec::new();
    entry.read_to_end(&mut content).unwrap();
    assert_eq!(content, b"durable file before finalization failure");
    assert!(entries.next().is_none());
}

#[test]
fn finalization_failures_before_rename_keep_resume_state_and_send_no_finish_ack() {
    for point in [
        Point::ArchiveWrite,
        Point::ArchiveSync,
        Point::ArchiveRename,
    ] {
        let directory = TestDirectory::new();
        let paths = paths(&directory);
        let bundle = tar_bundle();
        let mut receiver = directory.open();
        receiver.commit_bundle(&bundle).unwrap();
        drop(receiver);
        let wal_before = fs::read(&paths.wal_path).unwrap();

        finish_with_storage_failure(&directory, point, 0);
        assert!(!paths.final_archive_path.exists());
        assert_eq!(fs::read(&paths.wal_path).unwrap(), wal_before);
        let recovered = directory.open();
        assert_eq!(recovered.resume_state().last_committed_bundle_no, 1);
        assert_eq!(directory.archive_bytes(), bundle.compressed_payload);
        recovered.finalize().unwrap();
        assert_complete_archive(&paths.final_archive_path, &bundle.compressed_payload);
    }
}

#[test]
fn failures_after_rename_preserve_completed_archive_and_send_no_finish_ack() {
    for (point, successful_calls) in [
        (Point::ArchiveParentSync, 0),
        (Point::CleanupRemove, 0),
        (Point::CleanupRemove, 1),
        (Point::CleanupParentSync, 0),
    ] {
        let directory = TestDirectory::new();
        let paths = paths(&directory);
        let bundle = tar_bundle();
        let mut receiver = directory.open();
        receiver.commit_bundle(&bundle).unwrap();
        receiver
            .record_terminal_skips(&[crate::network::protocol::SkippedEntry {
                work_ordinal: 2,
                relative_path: "missing.bin".into(),
                reason: SkipReason::OpenFailed,
            }])
            .unwrap();
        drop(receiver);

        finish_with_storage_failure(&directory, point, successful_calls);
        assert_complete_archive(&paths.final_archive_path, &bundle.compressed_payload);
        assert!(!paths.archive_part_path.exists());
        assert!(
            fs::read_to_string(&paths.issues_final_path)
                .unwrap()
                .contains("missing.bin")
        );
        let final_bytes = fs::read(&paths.final_archive_path).unwrap();
        let error = ReceiverSession::open(directory.0.clone(), "boundaries", 1)
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("completed v4 dump already exists")
        );
        assert_eq!(fs::read(&paths.final_archive_path).unwrap(), final_bytes);
        if point == Point::ArchiveParentSync
            || (point == Point::CleanupRemove && successful_calls == 0)
        {
            assert!(
                paths.wal_path.exists(),
                "resume state disappeared before final archive barrier"
            );
        }
        if point == Point::CleanupRemove && successful_calls == 1 {
            assert!(!paths.wal_path.exists());
            assert!(paths.catalog_path.exists());
        }
    }
}
